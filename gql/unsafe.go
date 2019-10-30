package gql

import (
	"reflect"
	"unsafe"
)

func Unsafeuint8sToBytes(recs []uint8) []uint8 { return recs }

func Unsafeint32sToBytes(recs []int32) (b []uint8) {
	// This function is used only to compute hashes in int32_table. The hash will
	// change if ever use a non-little-endian CPU, but that will only cause
	// spurious cache misses (not hits), so not a huge problem.
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&recs))
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	*dh = *sh
	dh.Len *= 4
	dh.Cap *= 4
	return
}
