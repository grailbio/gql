package gql

import (
	"context"
	"fmt"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/gql/hash"
	"github.com/grailbio/gql/marshal"
	"github.com/grailbio/gql/symbol"
)

// JoinMaxTables is the max # of tables that can appear in a single join
// expression.
const joinMaxTables = 4

// joinSubTable is an internal representation of a leaf table during join.  When
// a table is self-joined, e.g., join({t0:xxx, t1:xxx}, ...), "xxx" will appear
// in two joinSubTables, {0, 2, symbol.Intern("t0"), xxx}, and {1, 2,
// symbol.Intern("t1"), xxx}.
type joinSubTable struct {
	index int       // order of appearance (0,1,2..) in the 1st arg of join() expression.
	total int       // total # of tables being joined.
	name  symbol.ID // the name specified in the 1st arg of the join expression
	table Table     // the leaf table.
}

// JoinSubTableList is an ordered set of leaf tables.  Tables are stored in the
// order listed in the 1st arg of join().
type joinSubTableList struct {
	tables [joinMaxTables]*joinSubTable // Some entries may be nil.
	n      int                          // # of non-nil entries in tables[].
}

func (tl *joinSubTableList) list() []*joinSubTable {
	l := make([]*joinSubTable, 0, tl.n)
	for _, t := range tl.tables {
		if t != nil {
			l = append(l, t)
		}
	}
	return l
}

// Len returns the number of tables stored in the list.
func (tl *joinSubTableList) len() int { return tl.n }

// Add adds a table to the list. If the table already exists, it panics.
func (tl *joinSubTableList) add(t *joinSubTable) {
	if tl.tables[t.index] != nil {
		panic(*t)
	}
	tl.tables[t.index] = t
}

// Merge adds every table in the other list. If the table already exists, it
// panics.
func (tl *joinSubTableList) merge(other *joinSubTableList) {
	for _, t := range other.tables {
		if t != nil {
			tl.add(t)
		}
	}
}

// getByIndex returns the i'th table in the list.
//
// REQUIRES: 0 < = i < # of tables listed in the join expression.
func (tl *joinSubTableList) getByIndex(i int) *joinSubTable {
	return tl.tables[i]
}

// getByIndex returns the table with the given name. Name is the struct field
// tag attached to the table in the 1st arg of the join expression. For example,
// For "join({t0: table0, t1: table1}, ...)", getByName(symbol.Intern("t0"))
// will return table0.  It panics if no table with the given name is found.
func (tl *joinSubTableList) getByName(tableName symbol.ID) *joinSubTable {
	for i, t := range tl.tables {
		if t.name == tableName {
			if t.index != i {
				panic(t)
			}
			return t
		}
	}
	log.Panicf("Table %v not found", tableName.Str())
	return nil
}

// JoinColumn represents a column named in a join "where" expression.
type joinColumn struct {
	table   *joinSubTable // leaf table.
	col     symbol.ID     // column in table.
	keyExpr *Func         // closure to extract the key from a row.
}

func (jc joinColumn) equals(other joinColumn) bool {
	return jc.table == other.table && jc.col == other.col
}

// String implements the stringer interface.
func (jc joinColumn) String() string {
	return fmt.Sprintf("%s#%d.%s", jc.table.name.Str(), jc.table.index, jc.col.Str())
}

// joinConstraint represents one "where" clause.
type joinConstraint struct {
	// Op is the constraint between the two tables. Currently it is always one of
	// the '==' variants; eqeqSymbolID for '==', eqeqqSymbolID for '?==', etc.
	op symbol.ID
	// left & right hand sides of the constraint.
	tables [2]joinColumn
	// boolean expression that tells whether the joined row satisifies the constraint.
	filterExpr *Func
}

// String implements the stringer interface
func (jc joinConstraint) String() string {
	return fmt.Sprintf("{L:%v, R:%v, filter: %v}", jc.tables[0], jc.tables[1], jc.filterExpr)
}

// JoinNode is a node in the dataflow graph created by join.  It corresponds to
// a leaf table, or result of joining two or more leaf nodes. A row produced by
// a joinNode is always a struct of form {table1: row1, table2: row2,
// ...}.  For example, join({t0: X, t1: Y, t2: Z}, ...)  creates a two-level
// join tree:
//
//					   joinnodeB
//          /    	 			 \
//				 /     	 				\
//			 	joinnodeA				t2
//			 /    \
//			t0    t1
//
// joinNoneA emits rows of form {t0: row in X, t1: row in Y}.  joinnodeB emits
// rows of form {t0: row in X, t1: row in Y, t2: row in Z}.  Each row emit by
// joinNode is a nested struct. To read column C in table X, you must refer to
// it as t0.C.
type joinNode interface {
	Table

	// IsSorted checks if this node yields rows sorted by column c.sortCol of
	// c.table.
	isSorted(c joinColumn) bool

	// SubTables returns the list of tables (in the original join expression) that
	// contribute to rows yielded by this node.
	subTables() *joinSubTableList
}

// JoinLeafNode is a simple node that reads rows from a Table and wraps then
// into a struct of form {tableName: row}.
type joinLeafNode struct {
	table *joinSubTable
	attrs TableAttrs
}

func newJoinLeafNode(table *joinSubTable) *joinLeafNode {
	return &joinLeafNode{table: table, attrs: TableAttrs{Name: "join:" + table.name.Str()}}
}

// Attrs implements Table.
func (t *joinLeafNode) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Hash implements Table.
func (t *joinLeafNode) Hash() hash.Hash {
	h := hash.Hash{
		0x84, 0x3b, 0xf6, 0x5d, 0xba, 0x56, 0x82, 0x7e,
		0x8c, 0x6a, 0x02, 0x10, 0xac, 0xa9, 0xaa, 0xcd,
		0x46, 0xbe, 0xc0, 0x34, 0x55, 0x08, 0x61, 0xea,
		0x65, 0xe0, 0x64, 0xbd, 0x97, 0x0c, 0xed, 0xec}
	return h.Merge(t.table.table.Hash())
}

// Len implements Table.
func (t *joinLeafNode) Len(ctx context.Context, mode CountMode) int {
	return t.table.table.Len(ctx, mode)
}

// Marshal implements Table, but it shall never be called.
// Marshaling of a join table is done always at the root level.
func (t *joinLeafNode) Marshal(ctx MarshalContext, enc *marshal.Encoder) { panic("Not implemented") }

// Prefetch implements Table.
func (t *joinLeafNode) Prefetch(ctx context.Context) {}

// isSorted implements joinNode.
func (t *joinLeafNode) isSorted(c joinColumn) bool { return false }

// subTables implements joinNode.
func (t *joinLeafNode) subTables() *joinSubTableList {
	s := &joinSubTableList{}
	s.add(t.table)
	return s
}

// Scanner implements Table.
func (t *joinLeafNode) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	return &joinLeafScanner{
		sc:        t.table.table.Scanner(ctx, 0, 1, 1),
		tableName: t.table.name,
	}
}

// JoinLeafScanner implements the TableScanner for joinLeafNode.
type joinLeafScanner struct {
	sc        TableScanner
	tableName symbol.ID // for debugging only.
	curValue  Value     // current row. a struct of form {tableName: row}.
}

// Scan implements TableScanner.
func (t *joinLeafScanner) Scan() bool {
	if !t.sc.Scan() {
		return false
	}
	s := &simpleStruct2Impl{
		nFields: 1,
	}
	InitStruct(s)
	s.names[0] = t.tableName
	s.values[0] = t.sc.Value()
	t.curValue = NewStruct(s)
	return true
}

// Value implements TableScanner.
func (t *joinLeafScanner) Value() Value { return t.curValue }

// JoinSortingNode is a joinNode that sorts rows of another joinNode.
type joinSortingNode struct {
	table   joinNode   // Source table
	sortCol joinColumn // The column to sort
	sorted  Table
	attrs   TableAttrs
}

func newJoinSortingNode(ctx context.Context, table joinNode, sortCol joinColumn) joinNode {
	log.Printf("joinsort: table %+v, col %s", table.Attrs(ctx), sortCol.col.Str())
	if t := table.subTables().getByIndex(sortCol.table.index); t == nil {
		log.Panicf("join: subtable %+v not found", sortCol)
	}
	if table.isSorted(sortCol) {
		return table
	}
	n := &joinSortingNode{
		table:   table,
		sortCol: sortCol,
		attrs:   TableAttrs{Name: fmt.Sprintf("join:sort(%s/%s)", table.Attrs(ctx).Name, sortCol.col.Str())},
	}
	n.sorted = NewMinNTable(ctx, astUnknown /*TODO:fix*/, TableAttrs{Name: "join"}, n.table, sortCol.keyExpr, -1, 0)
	return n
}

// Attrs implements Table.
func (t *joinSortingNode) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Hash implements Table.
func (t *joinSortingNode) Hash() hash.Hash {
	h := hash.Hash{
		0xaa, 0xe4, 0xb1, 0xa1, 0xfe, 0x70, 0x9f, 0x32,
		0xef, 0x9e, 0x59, 0xea, 0x15, 0x04, 0xde, 0x04,
		0xfc, 0x98, 0xd1, 0x31, 0x57, 0xda, 0x29, 0x0c,
		0x1a, 0xd1, 0xee, 0xab, 0xec, 0x8b, 0xd8, 0x3b}
	h = h.Merge(t.table.Hash())
	h.Merge(t.sortCol.keyExpr.Hash())
	return h
}

// Len implements Table.
func (t *joinSortingNode) Len(ctx context.Context, mode CountMode) int {
	return t.table.Len(ctx, mode)
}

// Marshal implements Table, but it shall never be called.
// Marshaling of a join table is done always at the root level.
func (t *joinSortingNode) Marshal(ctx MarshalContext, enc *marshal.Encoder) { panic("Not implemented") }

// Prefetch implements Table.
func (t *joinSortingNode) Prefetch(ctx context.Context) {}

// isSorted implements joinNode.
func (t *joinSortingNode) isSorted(c joinColumn) bool { return t.sortCol.equals(c) }

// subTables implements joinNode.
func (t *joinSortingNode) subTables() *joinSubTableList { return t.table.subTables() }

// Scanner implements Table.
func (t *joinSortingNode) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	return t.sorted.Scanner(ctx, 0, 1, 1)
}

func newJoinKeyAST(tableName symbol.ID, sortCol symbol.ID) *ASTStructFieldRef {
	return &ASTStructFieldRef{
		Parent: &ASTColumnRef{Col: tableName},
		Field:  sortCol}
}

func newJoinKeyClosure(ast *ASTStructFieldRef) *Func {
	// Finalize sortKeyAST. Otherwise, FreeVars and other methods are unmappy.
	//
	// TODO(saito) Don't fake bindings. The below code doesn't allow accessing
	// global variables.
	frame := aiBindings{Frames: []aiFrame{
		aiGlobalConsts,
		aiFrame{symbol.AnonRow: AIAnyType}},
	}
	types := newASTTypes() // TODO(saito) this is screwy. Why are we throwing away the typeinfo?
	types.add(ast, &frame)

	return NewUserDefinedFunc(ast, &bindings{},
		[]FormalArg{{Name: symbol.AnonRow, Positional: true, Required: true}}, ast)
}

// JoinSortingMergeNode joins two tables via mergesort
type joinSortingMergeNode struct {
	parent *joinTable
	attrs  TableAttrs
	// Child are two tables to join.
	child [2]joinNode
	// Constraint defines the natural-join condition. constraint.table[x] defines
	// the value to extract from child[x] (x={0,1}). The two values extracted from
	// child[*] are compared using constraint.filterExpr.
	constraint joinConstraint
}

func newJoinSortingMergeNode(ctx context.Context, parent *joinTable, child0, child1 joinNode, constraint joinConstraint) joinNode {
	child := [2]joinNode{
		newJoinSortingNode(ctx, child0, constraint.tables[0]),
		newJoinSortingNode(ctx, child1, constraint.tables[1]),
	}
	return &joinSortingMergeNode{
		parent:     parent,
		attrs:      TableAttrs{Name: fmt.Sprintf("join:sortmerge(lhs:=%s,rhs:=%s,cond=%+v)", child[0].Attrs(ctx).Name, child[1].Attrs(ctx).Name, constraint)},
		child:      child,
		constraint: constraint,
	}
}

// Attrs implements Table.
func (t *joinSortingMergeNode) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Hash implements Table.
func (t *joinSortingMergeNode) Hash() hash.Hash {
	h := hash.Hash{
		0x02, 0xc8, 0x1b, 0xf9, 0x22, 0x43, 0xfd, 0x8a,
		0xc3, 0x58, 0xac, 0xf8, 0x70, 0xd1, 0xc5, 0xaf,
		0x50, 0xee, 0x2a, 0x68, 0x9f, 0xc7, 0x3e, 0x33,
		0xe4, 0x55, 0x78, 0x61, 0x8f, 0x73, 0xa4, 0xb7}
	h.Merge(t.parent.hash)
	h = h.Merge(t.child[0].Hash())
	h = h.Merge(t.child[1].Hash())
	h = h.Merge(t.constraint.filterExpr.Hash())
	return h
}

// Len implements Table.
func (t *joinSortingMergeNode) Len(ctx context.Context, mode CountMode) int {
	if mode == Exact {
		panic("not implemented")
	}
	l0 := t.child[0].Len(ctx, mode)
	if l1 := t.child[0].Len(ctx, mode); l1 < l0 {
		return l1
	}
	return l0
}

// Marshal implements Table.
func (t *joinSortingMergeNode) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	panic("Not implemented")
}

// Prefetch implements Table.
func (t *joinSortingMergeNode) Prefetch(ctx context.Context) {}

// isSorted implements joinNode.
func (t *joinSortingMergeNode) isSorted(c joinColumn) bool {
	if t.constraint.op != eqeqSymbolID {
		// for non-inner joins, the some of the keys may be NA, and they screw the
		// sort order.
		return false
	}
	for _, table := range t.constraint.tables {
		if table.equals(c) {
			return true
		}
	}
	// TODO(saito): For "A.a = B.a && B.a == C.b && A.a = D.a", it creates a tree
	//
	// Merge3(Merge2(Merge1(A,B, {A.a=B.a}) ,C,{B.a=C.b}),{A.a=D.a})
	//
	// Now, when joinSortingNode asks Merge2 if it is sorted by A.a, It will check
	// if A.a is equal to B.a or C.b and return false.
	return false
}

// subTables implements joinNode.
func (t *joinSortingMergeNode) subTables() *joinSubTableList {
	subTables := &joinSubTableList{}
	for _, child := range t.child {
		subTables.merge(child.subTables())
	}
	return subTables
}

// Scanner implements Table.
func (t *joinSortingMergeNode) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	sc := &joinSortingMergeScanner{
		ctx:        ctx,
		parent:     t.parent,
		subTables:  t.subTables(),
		filterExpr: t.constraint.filterExpr,
		label:      t.Attrs(ctx).Name,
	}
	for i := 0; i < 2; i++ {
		c := &sc.child[i]
		c.sc = t.child[i].Scanner(ctx, 0, 1, 1)
		c.keyExpr = t.constraint.tables[i].keyExpr
		if !c.sc.Scan() {
			c.eof = true
			continue
		}
		c.nextRow = c.sc.Value()
		sc.readNextRows(c)
	}
	return sc
}

// JoinSortingMergeChild scans one table while doing merge-join.
type joinSortingMergeChild struct {
	sc      TableScanner // reader of the source table.
	keyExpr *Func     // for extracting the join key from a row in "sc".

	eof     bool
	key     Value   // current join key.
	values  []Value // set of rows with the same "key"
	nextRow Value   // stores one row read ahead.
}

// JoinSortingMergeScanner implements TableScanner for joinSortingMergeNode.
type joinSortingMergeScanner struct {
	ctx    context.Context
	parent *joinTable
	label  string // For debugging only
	// tables contributing the values to the rows produced by this scanner.
	// subTables.len() may be > 2, since it counts the number of leaf tables.
	subTables *joinSubTableList
	// Join condition. E.g., "==" for inner join.
	filterExpr *Func
	// Child tables.
	child [2]joinSortingMergeChild
	// For enumerating a cartesian product when there are multiple rows with the
	// same joinkey.
	rowCP *joinCartesianProduct
}

// ReadNextRows reads the set of rows that the same join key for the given
// child. It updates c.key, c.values, c.eof. It assumes that the underlying
// child scanner yields sorts rows by the join key.
func (t *joinSortingMergeScanner) readNextRows(c *joinSortingMergeChild) {
	if c.eof {
		panic(c)
	}
	if !c.nextRow.Valid() {
		c.eof = true
		return
	}
	c.values = nil
	curRow := c.nextRow
	c.values = append(c.values, curRow)
	c.key = c.keyExpr.Eval(t.ctx, curRow)
	for c.sc.Scan() {
		// Find all values with same key as c.key and combine them into c.values.
		c.nextRow = c.sc.Value()
		curKey := c.keyExpr.Eval(t.ctx, c.nextRow)
		cmp := Compare(t.parent.ast, curKey, c.key)
		if cmp < 0 {
			log.Panicf("%s: Unsorted keys: %v < %v", t.label, curKey, c.key)
		}
		if cmp != 0 {
			return
		}
		c.values = append(c.values, c.nextRow)
	}
	c.nextRow = Value{} // Mark EOF
}

// ReadNext reads the next record from the underlying table.  Returns the
// (value, true) on success, returns false on EOF or an error.
func (t *joinSortingMergeScanner) readNext() *joinCartesianProduct {
	for {
		// Find the smallest key.
		minIdx := -1
		var minKey Value
		for i := 0; i < 2; i++ {
			c := &t.child[i]
			if c.eof {
				continue
			}
			if c.key.Type() == InvalidType {
				log.Panicf("expr %v, val %v", c.keyExpr, PrintValueList(c.values))
			}
			if minIdx < 0 || Compare(t.parent.ast, c.key, minKey) < 0 {
				minIdx = i
				minKey = c.key
			}
		}
		if minIdx == -1 { // exhausted all subtables.
			return nil
		}
		// Merge the child rows @ minKey.
		valsPerSubTable := [2][]Value{}
		for i := 0; i < 2; i++ {
			c := &t.child[i]
			if c.eof {
				continue
			}
			if c := Compare(t.parent.ast, c.key, minKey); c != 0 {
				if c <= 0 {
					log.Panicf("CompareValues: %v %v", t, minKey)
				}
				continue
			}
			valsPerSubTable[i] = c.values
			if len(c.values) <= 0 {
				log.Panic(c)
			}
			t.readNextRows(c)
		}
		return newJoinCartesianProduct(t.ctx, t.parent, t.subTables, t.filterExpr, valsPerSubTable, t.label)
	}
}

// Scan implements TableScanner.
func (t *joinSortingMergeScanner) Scan() bool {
	for {
		if t.rowCP != nil && t.rowCP.scan() {
			return true
		}
		t.rowCP = t.readNext()
		if t.rowCP == nil {
			return false
		}
		if t.rowCP.scan() {
			return true
		}
	}
}

// Value implements TableScanner.
func (t *joinSortingMergeScanner) Value() Value {
	return t.rowCP.value()
}

// JoinCrossMergeNode merges tables using brute-force cartesian join.  It's used
// only when the two tables have no usable natural-join condition.
type joinCrossMergeNode struct {
	parent *joinTable
	attrs  TableAttrs
	child  [2]joinNode

	// TODO(saito) Don't read all the rows in memory if child1 is very large.
	once       sync.Once // Are child1Rows filled?
	child1Rows []Value   // In-memory copy of child[1] contents.
}

func newJoinCrossMergeNode(ctx context.Context, parent *joinTable, child0, child1 joinNode) *joinCrossMergeNode {
	n := &joinCrossMergeNode{
		parent: parent,
		attrs:  TableAttrs{Name: fmt.Sprintf("join:cross(%s,%s)", child0.Attrs(ctx).Name, child1.Attrs(ctx).Name)},
		child:  [2]joinNode{child0, child1},
	}
	return n
}

func (t *joinCrossMergeNode) init(ctx context.Context) {
	t.once.Do(func() {
		sc := t.child[1].Scanner(ctx, 0, 1, 1)
		for sc.Scan() {
			t.child1Rows = append(t.child1Rows, sc.Value())
		}
	})
}

// Attrs implements Table.
func (t *joinCrossMergeNode) Attrs(ctx context.Context) TableAttrs { return t.attrs }

// Hash implements Table.
func (t *joinCrossMergeNode) Hash() hash.Hash {
	h := hash.Hash{
		0xd9, 0xd3, 0xe0, 0x1f, 0x56, 0x68, 0x8e, 0xd8,
		0xa4, 0xbe, 0x75, 0x44, 0x8a, 0x34, 0xde, 0xa9,
		0xec, 0xc6, 0xcf, 0x7b, 0x5b, 0xcc, 0x85, 0x79,
		0x44, 0x65, 0x95, 0xe9, 0xa1, 0xa8, 0xa7, 0xc8}
	h = h.Merge(t.parent.hash)
	h = h.Merge(t.child[0].Hash())
	h = h.Merge(t.child[1].Hash())
	return h
}

// Len implements Table.
func (t *joinCrossMergeNode) Len(ctx context.Context, mode CountMode) int {
	if mode == Exact {
		panic("not implemented")
	}
	t.init(ctx)
	return t.child[0].Len(ctx, mode) * len(t.child1Rows) // TODO(saito) fix
}

// Marshal implements Table.
func (t *joinCrossMergeNode) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	panic("Not implemented")
}

// Prefetch implements Table.
func (t *joinCrossMergeNode) Prefetch(ctx context.Context) {}

// isSorted implements joinNode.
func (t *joinCrossMergeNode) isSorted(c joinColumn) bool {
	// Cross-merge iterates the child[0] in order.
	return t.child[0].isSorted(c)
}

// subTables implements joinNode.
func (t *joinCrossMergeNode) subTables() *joinSubTableList {
	subTables := &joinSubTableList{}
	for _, child := range t.child {
		subTables.merge(child.subTables())
	}
	return subTables
}

// Scanner implements TableScanner.
func (t *joinCrossMergeNode) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	if start > 0 {
		return &NullTableScanner{}
	}
	t.init(ctx)
	return &joinCrossMergeScanner{
		ctx:        ctx,
		parent:     t.parent,
		subTables:  t.subTables(),
		sc0:        t.child[0].Scanner(ctx, 0, 1, 1),
		child1Rows: t.child1Rows,
		label:      t.Attrs(ctx).Name,
	}
}

// JoinCrossMergeScanner implements TableScanner for joinCrossMergeNode.
type joinCrossMergeScanner struct {
	ctx    context.Context
	parent *joinTable
	label  string // For debugging only
	// tables contributing the values to the rows produced by this scanner.
	// subTables.len() may be > 2, since it counts the number of leaf tables.
	subTables *joinSubTableList
	// Scanner for child[0].
	sc0 TableScanner
	// Contents of child[1].
	child1Rows []Value
	// For enumerating a cartesian product when there are multiple rows with the
	// same joinkey.
	rowCP *joinCartesianProduct
}

// Scanner implements Table.
func (t *joinCrossMergeScanner) Scan() bool {
	for {
		if t.rowCP != nil && t.rowCP.scan() {
			return true
		}
		if !t.sc0.Scan() {
			return false
		}
		t.rowCP = newJoinCartesianProduct(t.ctx, t.parent, t.subTables, nil /*todo*/, [2][]Value{[]Value{t.sc0.Value()}, t.child1Rows}, t.label)
		if t.rowCP.scan() {
			return true
		}
	}
}

// Value implements Table.
func (t *joinCrossMergeScanner) Value() Value {
	return t.rowCP.value()
}

// joinTable is a Table implementation for join().
type joinTable struct {
	hash      hash.Hash
	ast       ASTNode // location in the source code. Only for error reporting.
	subTables *joinSubTableList
	joinExpr  *Func
	mapExpr   *Func // maybe null.
	root      joinNode // tree of joinNodes.
	approxLen int

	once              sync.Once
	materializedTable Table // fully materialized btsv table.
	exactLenOnce      sync.Once
	exactLen          int
}

// Len implements Table.
func (t *joinTable) Len(ctx context.Context, mode CountMode) int {
	if mode == Approx {
		return t.approxLen
	}
	t.exactLenOnce.Do(func() {
		t.exactLen = DefaultTableLen(ctx, t)
	})
	return t.exactLen
}

// Marshal implements Table.
func (t *joinTable) Marshal(ctx MarshalContext, enc *marshal.Encoder) {
	t.init(ctx.ctx)
	t.materializedTable.Marshal(ctx, enc)
}

// init creates a materialized btsv table.
func (t *joinTable) init(ctx context.Context) {
	t.once.Do(func() {
		t.materializedTable = materializeTable(ctx, t,
			func(w *BTSVShardWriter) {
				sc := t.scanner(ctx)
				for sc.Scan() {
					w.Append(sc.Value())
				}
			})
	})
}

// joinTableScanner implements TableScanner for joinTable.
type joinTableScanner struct {
	ctx    context.Context
	parent *joinTable
	sc     TableScanner

	joinExpr *Func
	mapExpr  *Func
	value    Value
}

func (t *joinTable) scanner(ctx context.Context) TableScanner {
	return &joinTableScanner{
		ctx:      ctx,
		parent:   t,
		sc:       t.root.Scanner(ctx, 0, 1, 1),
		joinExpr: t.joinExpr,
		mapExpr:  t.mapExpr,
	}
}

// explodeRow creates a N-element array of values (N = # of tables joined) from
// a struct with N fields, yielded by a joinNode.
//
// TODO(saito) This is hacky. There should be a way to evaluate the value
// without exploding.
func (t *joinTableScanner) explodeRow(v Value) []Value {
	s := v.Struct(t.parent.ast)
	values := make([]Value, t.parent.subTables.len())
	for i := 0; i < s.Len(); i++ {
		f := s.Field(i)
		fi := t.parent.subTables.getByName(f.Name).index
		values[fi] = f.Value
	}
	for i := range values {
		if !values[i].Valid() {
			values[i] = Null
		}
	}
	return values
}

// Value implements TableScanner.
func (t *joinTableScanner) Value() Value {
	return t.value
}

// Scan implements TableScanner.
func (t *joinTableScanner) Scan() bool {
	for {
		if !t.sc.Scan() {
			return false
		}
		exploded := t.explodeRow(t.sc.Value())
		if t.joinExpr != nil {
			if !t.joinExpr.Eval(t.ctx, exploded...).Bool(t.parent.ast) {
				continue
			}
			if t.mapExpr != nil {
				t.value = t.mapExpr.Eval(t.ctx, exploded...)
			} else {
				rowVals := []StructField{}
				for ti, val := range exploded {
					switch val.Type() {
					case NullType:
					case StructType:
						sv := val.Struct(t.parent.ast)
						nFields := sv.Len()
						for i := 0; i < nFields; i++ {
							v := sv.Field(i)
							colName := symbol.Intern(t.parent.subTables.getByIndex(ti).name.Str() + "_" + v.Name.Str())
							rowVals = append(rowVals, StructField{Name: colName, Value: v.Value})
						}
					default:
						rowVals = append(rowVals, StructField{Name: t.parent.subTables.getByIndex(ti).name, Value: val})
					}
				}
				t.value = NewStruct(NewSimpleStruct(rowVals...))
			}
		}
		return true
	}
}

// Scanner implements Table.
func (t *joinTable) Scanner(ctx context.Context, start, limit, total int) TableScanner {
	t.init(ctx)
	return t.materializedTable.Scanner(ctx, start, limit, total)
}

// Prefetch implements Table.
func (t *joinTable) Prefetch(ctx context.Context) {}

// Hash implements Table.
func (t *joinTable) Hash() hash.Hash { return t.hash }

// Attrs implements Table.
func (t *joinTable) Attrs(ctx context.Context) TableAttrs {
	return TableAttrs{Name: "join"}
}

type joinCartesianProduct struct {
	ctx        context.Context
	parent     *joinTable
	subTables  *joinSubTableList
	filterExpr *Func

	// Set of values from each subtable.
	values [2][]Value

	row Value // Current row

	totalRows int
	index     int
	label     string
}

func newJoinCartesianProduct(ctx context.Context, parent *joinTable, subTables *joinSubTableList, filterExpr *Func, values [2][]Value, label string) *joinCartesianProduct {
	if parent == nil {
		panic("nil parent")
	}
	totalRows := 1
	for _, v := range values {
		if len(v) > 0 {
			totalRows *= len(v)
		}
	}
	return &joinCartesianProduct{
		ctx:        ctx,
		parent:     parent,
		subTables:  subTables,
		filterExpr: filterExpr,
		values:     values,
		totalRows:  totalRows,
		index:      -1,
		label:      label,
	}
}

func (cp *joinCartesianProduct) scan() bool {
	for {
		cp.index++
		if cp.index >= cp.totalRows {
			return false
		}
		v := cp.index

		rowComponents := [2]Value{}
		nNulls := 0
		for ti := 0; ti < 2; ti++ {
			nValsInSubTable := len(cp.values[ti])
			if nValsInSubTable == 0 {
				nNulls++
				rowComponents[ti] = Null
				continue
			}
			val := cp.values[ti][v%nValsInSubTable]
			rowComponents[ti] = val
			v /= nValsInSubTable
			if val.Null() != NotNull {
				nNulls++
			}
		}
		if nNulls == len(cp.values) {
			// This can happen when no equality constraints exist and all the
			// subtables are doing batch scans.
			continue
		}
		cp.row = cp.mergeValues(rowComponents[:])
		if cp.filterExpr != nil {
			cond := cp.filterExpr.Eval(cp.ctx, cp.row).Bool(cp.parent.ast)
			if !cond {
				continue
			}
		}
		cp.row = cp.mergeValues(rowComponents[:])
		return true
	}
}

func (cp *joinCartesianProduct) mergeValues(values []Value) Value {
	fields := [joinMaxTables]StructField{}
	for _, v := range values {
		s := v.Struct(cp.parent.ast)
		for fi := 0; fi < s.Len(); fi++ {
			field := s.Field(fi)
			d := &fields[cp.parent.subTables.getByName(field.Name).index]
			if d.Name != symbol.Invalid {
				log.Panicf("joincp: Duplicate subtable found in %s", PrintValueList(values))
			}
			*d = field
		}
	}
	j := 0
	for i := range fields {
		if fields[i].Name == symbol.Invalid {
			if t := cp.subTables.getByIndex(i); t != nil {
				fields[i].Name = t.name
				fields[i].Value = Null
			}
		}
		if fields[i].Name != symbol.Invalid {
			fields[j] = fields[i]
			j++
		}
	}
	return NewStruct(NewSimpleStruct(fields[:j]...))
}

func (cp *joinCartesianProduct) value() Value {
	return cp.row
}

// findEqJoinConstraints constructs joinConstraints given the join "where"
// condition.  Note that the extracted constraints may be a subset of what
// "expr" specifies. So the toplevel scanner must post-filter the yielded rows
// using the expr.
func findEqJoinConstraints(expr ASTNode, tables *joinSubTableList) (constraints []joinConstraint) {
	findTable := func(expr ASTNode) *joinSubTable {
		varRefExpr, ok := expr.(*ASTVarRef)
		if !ok {
			return nil
		}
		tableName := varRefExpr.Var
		return tables.getByName(tableName)
	}
	if andand, ok := expr.(*ASTLogicalOp); ok && andand.AndAnd {
		constraints = append(constraints, findEqJoinConstraints(andand.LHS, tables)...)
		constraints = append(constraints, findEqJoinConstraints(andand.RHS, tables)...)
		return
	}
	funcallExpr, ok := expr.(*ASTFuncall)
	if !ok {
		return
	}
	if op := isEqualEqual(funcallExpr.Function); op != symbol.Invalid {
		c := joinConstraint{op: op}
		if len(funcallExpr.Raw) != 2 { // ==, ==? etc are always binary.
			log.Panic(funcallExpr)
		}
		var keyAST [2]*ASTStructFieldRef
		for i := range funcallExpr.Raw {
			v, ok := funcallExpr.Raw[i].Expr.(*ASTStructFieldRef)
			if !ok {
				return
			}
			subTable := findTable(v.Parent)
			if subTable == nil {
				return
			}
			c.tables[i].table = subTable
			c.tables[i].col = v.Field
			keyAST[i] = newJoinKeyAST(subTable.name, v.Field)
			c.tables[i].keyExpr = newJoinKeyClosure(keyAST[i])
		}
		filterAST := NewASTFuncall(
			funcallExpr.Function,
			[]ASTParamVal{
				ASTParamVal{Name: funcallExpr.Raw[0].Name, Expr: keyAST[0]},
				ASTParamVal{Name: funcallExpr.Raw[1].Name, Expr: keyAST[1]}})
		// TODO(saito) Don't fake bindings. The below code doesn't allow accessing
		// global variables.
		frame := aiBindings{Frames: []aiFrame{
			aiGlobalConsts,
			aiFrame{symbol.AnonRow: AIAnyType}}}
		types := newASTTypes() // TODO(saito) this is screwy. Why are we throwing away the typeinfo?
		types.add(filterAST, &frame)

		c.filterExpr = NewUserDefinedFunc(filterAST, &bindings{},
			[]FormalArg{{Name: symbol.AnonRow, Positional: true, Required: true}},
			filterAST)
		constraints = append(constraints, c)
	}
	return
}

func (t *joinTable) parseJoinExpr(ctx context.Context, ast ASTNode, tableList Struct, joinExpr ASTNode) (*joinSubTableList, joinNode) {
	nTable := tableList.Len()
	tables := &joinSubTableList{n: nTable}
	nodes := make([]joinNode, nTable)

	// Create a joinLeafNode for each leaf table.
	for ti := 0; ti < nTable; ti++ {
		f := tableList.Field(ti)
		st := &joinSubTable{
			index: ti,
			total: nTable,
			name:  f.Name,
			table: f.Value.Table(t.ast),
		}
		tables.add(st)
		nodes[ti] = newJoinLeafNode(st)
	}

	constraints := findEqJoinConstraints(joinExpr, tables)
	removeConstraint := func(i int) {
		copy(constraints[i:], constraints[i+1:])
		constraints = constraints[:len(constraints)-1]
	}

	// Create a join tree from the eqjoin constraints.
	var node joinNode // the current root node.
DoneConstraint:
	for len(constraints) > 0 {
		if node != nil {
			nodeSubtables := node.subTables()
			for ci, c := range constraints {
				if nodeSubtables.getByIndex(c.tables[0].table.index) != nil &&
					nodes[c.tables[1].table.index] != nil {
					// c.tables[0] appears in the node, and c.tables[1] is a new table.
					node = newJoinSortingMergeNode(ctx, t, node, nodes[c.tables[1].table.index], c)
					nodes[c.tables[1].table.index] = nil
					removeConstraint(ci)
					continue DoneConstraint
				}
				if nodeSubtables.getByIndex(c.tables[1].table.index) != nil &&
					nodes[c.tables[0].table.index] != nil {
					// c.tables[1] appears in the node, and c.tables[0] is a new table.
					node = newJoinSortingMergeNode(ctx, t, nodes[c.tables[0].table.index], node, c)
					nodes[c.tables[0].table.index] = nil
					removeConstraint(ci)
					continue DoneConstraint
				}
				if nodeSubtables.getByIndex(c.tables[0].table.index) != nil &&
					nodeSubtables.getByIndex(c.tables[1].table.index) != nil {
					// This constraint can be just added to the existing tree. But
					// currently, we post-filter rows using this constraint at the very
					// root of the tree. So here, it's ok to remove it.
					removeConstraint(ci)
					continue DoneConstraint
				}
			}
		}
		// Failed to attach any constraints to the existing tree.
		//
		// Create a node that merges two tables listed in constraints[0].
		c := constraints[0]
		removeConstraint(0)
		var child [2]joinNode
		for i := 0; i < 2; i++ {
			st := c.tables[i].table
			if nodes[st.index] == nil {
				// The "if node!=nil" branch above should have handled this case.
				Panicf(ast, "expr %v, constraint %+v, index %+v", joinExpr, c, st)
			}
			child[i], nodes[st.index] = nodes[st.index], nil
		}
		if node == nil { // First constraint
			node = newJoinSortingMergeNode(ctx, t, child[0], child[1], c)
			continue DoneConstraint
		}
		// Unusual case: a join expression looks like A.x==B.y && C.z==D.w We just
		// do bruteforce merging.
		node = newJoinCrossMergeNode(ctx, t, node, newJoinSortingMergeNode(ctx, t, child[0], child[1], c))
	}

	// Add the remaining tables and do a brute-force crossjoin.
	for _, child := range nodes {
		if child != nil {
			if node == nil {
				node = child
			} else {
				node = newJoinCrossMergeNode(ctx, t, node, child)
			}
		}
	}
	return tables, node
}

func builtinJoin(ctx context.Context, ast ASTNode, args []ActualArg) Value {
	t := &joinTable{ast: ast}
	tables, node := t.parseJoinExpr(ctx, ast, args[0].Struct(), args[1].Expr.(*ASTLambda).Body)
	log.Debug.Printf("join: parse %v -> %v", args[1].Expr, node.Attrs(ctx).Name)
	tableNames := make([]symbol.ID, tables.len())
	for i := range tableNames {
		tableNames[i] = tables.getByIndex(i).name
	}
	joinExpr := args[1].Func()
	mapExpr := args[2].Func()
	approxLen := 1
	for _, t := range tables.list() {
		if len := t.table.Len(ctx, Approx); len > approxLen {
			approxLen = len
		}
	}

	// TODO(saito) Enable caching
	t.hash = hashJoinCall(tables, joinExpr, mapExpr)
	t.subTables = tables
	t.joinExpr = joinExpr
	t.mapExpr = mapExpr
	t.root = node
	t.approxLen = approxLen
	return NewTable(t)
}

func hashJoinCall(tables *joinSubTableList, joinExpr, mapExpr *Func) hash.Hash {
	h := hash.Hash{
		0x7c, 0x35, 0xa9, 0xab, 0x32, 0xa2, 0xa6, 0x4a,
		0x49, 0x4b, 0x16, 0x34, 0xb4, 0xbc, 0x3a, 0x99,
		0xfd, 0x5a, 0xb9, 0x8f, 0x31, 0x53, 0xb1, 0xd8,
		0x00, 0x10, 0x5d, 0x6f, 0xce, 0x4b, 0xf4, 0xc9}
	for _, table := range tables.list() {
		h = h.Merge(table.table.Hash())
	}
	h = h.Merge(joinExpr.Hash())
	if mapExpr != nil {
		h = h.Merge(mapExpr.Hash())
	}
	return h
}

func init() {
	RegisterBuiltinFunc("join",
		`
    join({t0:tbl0,t1:tbl1,t2:tbl2}, t0.colA==t1.colB && t1.colB == t2.colC [, map:={colx:t0.colA, coly:t2.colC}])

Arg types:

- _tbl0_, _tbl1_, ..: table

Join function joins multiple tables into one. The first argument lists the table
name and its mnemonic in a struct form. The 2nd arg is the join condition.
The ::map:: arg specifies the format of the output rows.

Imagine the following tables:

table0:

        ║colA ║ colB║
        ├─────┼─────┤
        │Cat  │ 3   │
        │Dog  │ 8   │

table1:

        ║colA ║ colC║
        ├─────┼─────┤
        │Cat  │ red │
        │Bat  │ blue│


Example:

1. ::join({t0:table0, t1:table1}, t0.colA==t1.colA, map:={colA:t0.colA, colB: t0.colB, colC: t1.colC})::

This expression performs an inner join of t0 and t1.

        ║colA ║ colB║ colC║
        ├─────┼─────┼─────┤
        │Cat  │ 3   │ red │


2. ::join({t0:table0, t1:table1}, t0.A?==?t1.A,map:={A:t0.A, A2:t1.A,B:t0.B, c:t1.C})::

This expression performs an outer join of t0 and t1.

	      ║   A║  A2║  B║    c║
        ├────┼────┼───┼─────┤
        │  NA│ bat│ NA│ blue│
        │ cat│ cat│  3│  red│
        │ dog│  NA│  8│   NA│


The join condition doesn't need to be just "=="s connected by "&&"s. It can be
any expression, although join provides a special fast-execution path for flat,
conjunctive "=="s, so use them as much as possible.

Caution: join currently is very slow on large tables. Talk to ysaito if you see
any problem.


TODO: describe left/right joins (use ==?, ?==)
TODO: describe cross joins (set non-equality join conditions, such as t0.colA >= t1.colB)`,
		builtinJoin,
		func(ast ASTNode, args []AIArg) AIType { return AITableType },
		FormalArg{Positional: true, Required: true},                                // tables
		FormalArg{Positional: true, Required: true, JoinClosure: true},             // join expr
		FormalArg{Name: symbol.Map, JoinClosure: true, DefaultValue: NewFunc(nil)}) // map:=expr
}
