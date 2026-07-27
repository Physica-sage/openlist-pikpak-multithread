package op

import (
	"context"
	"fmt"
	"os"
	stdpath "path"
	"strconv"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

const (
	objsUpdateHookBatchConcurrencyEnv = "OPENLIST_BATCH_HOOK_CONCURRENCY"
	defaultObjsUpdateHookConcurrency  = 2
	maxObjsUpdateHookConcurrency      = 4
)

type objsUpdateHookBatchContextKey struct{}

type objsUpdateHookTarget struct {
	storage   driver.Driver
	dirPath   string
	recursive bool
}

// ObjsUpdateHookBatch collects successful direct-operation hook targets and
// dispatches non-overlapping targets with bounded concurrency after a batch
// request has finished.
type ObjsUpdateHookBatch struct {
	mu         sync.Mutex
	targets    []objsUpdateHookTarget
	seen       map[string]struct{}
	dispatched bool
}

// WithObjsUpdateHookBatch returns a context that makes Move and Copy collect
// their exact hook targets instead of launching one hook goroutine per item.
func WithObjsUpdateHookBatch(ctx context.Context) (context.Context, *ObjsUpdateHookBatch) {
	batch := &ObjsUpdateHookBatch{seen: make(map[string]struct{})}
	return context.WithValue(ctx, objsUpdateHookBatchContextKey{}, batch), batch
}

func enqueueObjsUpdateHook(ctx context.Context, storage driver.Driver, dirPath string, recursive bool) bool {
	batch, ok := ctx.Value(objsUpdateHookBatchContextKey{}).(*ObjsUpdateHookBatch)
	if !ok || batch == nil {
		return false
	}
	return batch.add(storage, dirPath, recursive)
}

func (b *ObjsUpdateHookBatch) add(storage driver.Driver, dirPath string, recursive bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dispatched {
		return false
	}
	cleanedPath := stdpath.Clean(dirPath)
	key := fmt.Sprintf("%p\x00%s\x00%t", storage.GetStorage(), cleanedPath, recursive)
	if _, ok := b.seen[key]; ok {
		return true
	}
	b.seen[key] = struct{}{}
	b.targets = append(b.targets, objsUpdateHookTarget{
		storage:   storage,
		dirPath:   cleanedPath,
		recursive: recursive,
	})
	return true
}

func objsUpdateHookBatchConcurrency() int {
	value := strings.TrimSpace(os.Getenv(objsUpdateHookBatchConcurrencyEnv))
	concurrency, err := strconv.Atoi(value)
	if err != nil || concurrency < 1 || concurrency > maxObjsUpdateHookConcurrency {
		return defaultObjsUpdateHookConcurrency
	}
	return concurrency
}

func objsUpdateHookRateLimitEnabled() bool {
	item, err := GetSettingItemByKey(conf.HandleHookRateLimit)
	if err != nil {
		return true
	}
	if item == nil {
		return false
	}
	limit, err := strconv.ParseFloat(strings.TrimSpace(item.Value), 64)
	return err == nil && limit > 0
}

func objsUpdateHookPathContains(parent string, child string) bool {
	parent = stdpath.Clean(parent)
	child = stdpath.Clean(child)
	if parent == child {
		return true
	}
	if parent == "/" {
		return strings.HasPrefix(child, "/")
	}
	return strings.HasPrefix(child, parent+"/")
}

func objsUpdateHookTargetsOverlap(targets []objsUpdateHookTarget) bool {
	for i := range targets {
		for j := i + 1; j < len(targets); j++ {
			if targets[i].storage.GetStorage() != targets[j].storage.GetStorage() {
				continue
			}
			if objsUpdateHookPathContains(targets[i].dirPath, targets[j].dirPath) ||
				objsUpdateHookPathContains(targets[j].dirPath, targets[i].dirPath) {
				return true
			}
		}
	}
	return false
}

// Dispatch launches a bounded set of hook workers. Calling Dispatch more than
// once is safe; only the first call consumes the collected targets.
func (b *ObjsUpdateHookBatch) Dispatch(ctx context.Context) {
	if b == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	b.mu.Lock()
	if b.dispatched {
		b.mu.Unlock()
		return
	}
	b.dispatched = true
	targets := append([]objsUpdateHookTarget(nil), b.targets...)
	b.mu.Unlock()
	if len(targets) == 0 || !needHandleObjsUpdateHook() {
		return
	}
	go func() {
		concurrency := min(objsUpdateHookBatchConcurrency(), len(targets))
		if objsUpdateHookTargetsOverlap(targets) || objsUpdateHookRateLimitEnabled() {
			concurrency = 1
		}
		jobs := make(chan objsUpdateHookTarget)
		var workers sync.WaitGroup
		workers.Add(concurrency)
		for range concurrency {
			go func() {
				defer workers.Done()
				for target := range jobs {
					objsUpdateHook(ctx, target.storage, target.dirPath, target.recursive)
				}
			}()
		}
		for _, target := range targets {
			jobs <- target
		}
		close(jobs)
		workers.Wait()
	}()
}
