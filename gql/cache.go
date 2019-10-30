package gql

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
)

// LookupCache looks up "name" in the persistent cache. If found, it returns the
// abspathname of the data and true.
//
// If the cache entry is not found, it generates a new unique file path. The
// caller should produce cache contents in the given file, then call activateCache
// once done to activate the cache entry.
//
// Example:
//   path, found := lookupCache("foo.btsv")
//   if !found {
//     w := NewBTSVShardWriter(path, 0, 1, TableAttrs{})
//     .. fill w ...
//     w.Close()
//     activateCache("foo.btsv", path)
//   }
//   r := NewBTSVTable(path, ...)
//   ... use r ...
func LookupCache(ctx context.Context, name string) (string, bool) {
	absPath := fmt.Sprintf("%s/%s.link", cacheRoot, name)
	backoff := retry.Backoff(500*time.Millisecond, time.Minute, 1.2)
	var (
		data []byte
		err  error
	)
	for retries := 0; ; retries++ {
		data, err = file.ReadFile(ctx, absPath)
		if !errors.Is(errors.Precondition, err) {
			break
		}
		// Precondition error typically happens when the s3 object's etag has
		// changed in the background. Cf. s3file.go.
		log.Printf("lookupCache %s: %v, retries=%d", absPath, err, retries)
		if err = retry.Wait(ctx, backoff, retries); err != nil {
			break
		}
	}
	if err == nil {
		return string(data), true
	}
	return generateUniqueCachePath(name), false
}

// GenerateUniqueCachePath generates a unique path using "name" as a template.
// name should be of form "prefix.extension". The generated pathname is currently
// of the form cacheRoot/prefix.randomuid.extension but this may change in
// future implementations.
func generateUniqueCachePath(name string) string {
	ext := filepath.Ext(name)
	prefix := name[:len(name)-len(ext)]
	return fmt.Sprintf("%s/%s-%016x-%x-%x%s",
		cacheRoot, prefix, time.Now().UnixNano(), rand.Uint64(), rand.Uint64(), ext)
}

// GenerateStableCachePath generates a stable path using "name" as a template.
// name should be of form "prefix.extension". The generated pathname is currently
// of the form cacheRoot/name but this may change in
// future implementations.
func GenerateStableCachePath(name string) string {
	return file.Join(cacheRoot, name)
}

// activateCache arranges so that future calls to lookupCache(name) will return
// uniquePath.  This function is implemented by creating a symlink-like file
// that stores the uniquePath as the contents.
func ActivateCache(ctx context.Context, name, uniquePath string) {
	absPath := fmt.Sprintf("%s/%s.link", cacheRoot, name)
	err := file.WriteFile(ctx, absPath, []byte(uniquePath))
	if err != nil {
		log.Panicf("activateCache %s <- %s: %v", absPath, uniquePath, err)
	}
}

// TestClearCache deletes all the files in cacheRoot. For unittests only.
func TestClearCache() {
	os.RemoveAll(cacheRoot) // nolint: errcheck
}
