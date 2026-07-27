package op

import (
	"context"
	"errors"
	"fmt"
	stdpath "path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type batchHookTestDriver struct {
	model.Storage
	mu           sync.Mutex
	objects      map[string]*model.Object
	listFailures map[string]int
	listAttempts map[string]int
}

func newBatchHookTestDriver(paths map[string]bool) *batchHookTestDriver {
	d := &batchHookTestDriver{
		Storage: model.Storage{
			MountPath:       "/test",
			Status:          WORK,
			CacheExpiration: 1,
			Modified:        time.Now(),
		},
		objects:      make(map[string]*model.Object, len(paths)),
		listFailures: make(map[string]int),
		listAttempts: make(map[string]int),
	}
	for p, isDir := range paths {
		cleaned := stdpath.Clean(p)
		d.objects[cleaned] = &model.Object{
			ID:       cleaned,
			Path:     cleaned,
			Name:     stdpath.Base(cleaned),
			IsFolder: isDir,
			Modified: time.Now(),
		}
	}
	return d
}

func (d *batchHookTestDriver) Config() driver.Config {
	return driver.Config{Name: "BatchHookTest", NoCache: true}
}

func (d *batchHookTestDriver) GetAddition() driver.Additional { return nil }
func (d *batchHookTestDriver) Init(context.Context) error     { return nil }
func (d *batchHookTestDriver) Drop(context.Context) error     { return nil }
func (d *batchHookTestDriver) GetRootPath() string            { return "/" }

func (d *batchHookTestDriver) Get(_ context.Context, p string) (model.Obj, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	obj, ok := d.objects[stdpath.Clean(p)]
	if !ok {
		return nil, errs.ObjectNotFound
	}
	clone := *obj
	return &clone, nil
}

func (d *batchHookTestDriver) List(_ context.Context, dir model.Obj, _ model.ListArgs) ([]model.Obj, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	parent := stdpath.Clean(dir.GetPath())
	d.listAttempts[parent]++
	if d.listFailures[parent] > 0 {
		d.listFailures[parent]--
		return nil, errors.New("transient list failure")
	}
	objs := make([]model.Obj, 0)
	for p, obj := range d.objects {
		if p == parent || stdpath.Dir(p) != parent {
			continue
		}
		clone := *obj
		objs = append(objs, &clone)
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].GetName() < objs[j].GetName() })
	return objs, nil
}

func (d *batchHookTestDriver) Link(context.Context, model.Obj, model.LinkArgs) (*model.Link, error) {
	return nil, errs.NotImplement
}

func (d *batchHookTestDriver) Move(_ context.Context, srcObj, dstDir model.Obj) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	src := stdpath.Clean(srcObj.GetPath())
	dst := stdpath.Join(dstDir.GetPath(), srcObj.GetName())
	moved := make(map[string]*model.Object)
	for p, obj := range d.objects {
		if p != src && !strings.HasPrefix(p, src+"/") {
			continue
		}
		newPath := stdpath.Clean(dst + strings.TrimPrefix(p, src))
		clone := *obj
		clone.ID = newPath
		clone.Path = newPath
		clone.Name = stdpath.Base(newPath)
		moved[newPath] = &clone
		delete(d.objects, p)
	}
	for p, obj := range moved {
		d.objects[p] = obj
	}
	return nil
}

func TestObjsUpdateHookTargetsOverlap(t *testing.T) {
	storageA := newBatchHookTestDriver(map[string]bool{"/": true})
	storageB := newBatchHookTestDriver(map[string]bool{"/": true})
	tests := []struct {
		name    string
		targets []objsUpdateHookTarget
		want    bool
	}{
		{
			name: "root is ancestor",
			targets: []objsUpdateHookTarget{
				{storage: storageA, dirPath: "/", recursive: true},
				{storage: storageA, dirPath: "/A", recursive: true},
			},
			want: true,
		},
		{
			name: "parent and child overlap",
			targets: []objsUpdateHookTarget{
				{storage: storageA, dirPath: "/A", recursive: true},
				{storage: storageA, dirPath: "/A/B", recursive: true},
			},
			want: true,
		},
		{
			name: "sibling prefixes do not overlap",
			targets: []objsUpdateHookTarget{
				{storage: storageA, dirPath: "/A", recursive: true},
				{storage: storageA, dirPath: "/AB", recursive: true},
			},
		},
		{
			name: "same path with different recursion overlaps",
			targets: []objsUpdateHookTarget{
				{storage: storageA, dirPath: "/A", recursive: false},
				{storage: storageA, dirPath: "/A", recursive: true},
			},
			want: true,
		},
		{
			name: "same path on distinct storages does not overlap",
			targets: []objsUpdateHookTarget{
				{storage: storageA, dirPath: "/A", recursive: true},
				{storage: storageB, dirPath: "/A", recursive: true},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := objsUpdateHookTargetsOverlap(test.targets); got != test.want {
				t.Fatalf("overlap = %t, want %t", got, test.want)
			}
		})
	}
}

func TestObjsUpdateHookBatchNormalizesPathsBeforeDeduplication(t *testing.T) {
	storage := newBatchHookTestDriver(map[string]bool{"/": true, "/A": true})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/A", true)
	batch.add(storage, "/A/./", true)
	if got := len(batch.targets); got != 1 {
		t.Fatalf("target count = %d, want 1", got)
	}
	if got := batch.targets[0].dirPath; got != "/A" {
		t.Fatalf("normalized path = %q, want /A", got)
	}
}

func TestObjsUpdateHookBatchDoesNotCaptureAfterDispatch(t *testing.T) {
	storage := newBatchHookTestDriver(map[string]bool{"/": true})
	ctx, batch := WithObjsUpdateHookBatch(context.Background())
	batch.Dispatch(context.Background())
	if enqueueObjsUpdateHook(ctx, storage, "/late", false) {
		t.Fatal("dispatched batch captured a late hook target")
	}
}

func TestObjsUpdateHookBatchDispatchesEveryMovedDirectory(t *testing.T) {
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{
		Key:   conf.HandleHookAfterWriting,
		Value: "true",
	})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{
		Key:   conf.HandleHookRateLimit,
		Value: "0",
	})

	hooked := make(chan string, 4)
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			hooked <- parent
		},
	}

	storage := newBatchHookTestDriver(map[string]bool{
		"/":            true,
		"/src":         true,
		"/src/A":       true,
		"/src/A/a.mkv": false,
		"/src/B":       true,
		"/src/B/b.mkv": false,
		"/destination": true,
	})

	ctx, batch := WithObjsUpdateHookBatch(context.Background())
	if err := Move(ctx, storage, "/src/A", "/destination"); err != nil {
		t.Fatalf("move A: %v", err)
	}
	if err := Move(ctx, storage, "/src/B", "/destination"); err != nil {
		t.Fatalf("move B: %v", err)
	}
	batch.Dispatch(context.Background())

	got := make([]string, 0, 2)
	for len(got) < 2 {
		select {
		case parent := <-hooked:
			got = append(got, parent)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for hooks; got %v", got)
		}
	}
	sort.Strings(got)
	want := []string{"/test/destination/A", "/test/destination/B"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hooked paths = %v, want %v", got, want)
		}
	}
}

func TestObjsUpdateHookBatchConcurrencyConfig(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int
	}{
		{name: "default", want: 2},
		{name: "one disables parallelism", value: "1", want: 1},
		{name: "maximum", value: "4", want: 4},
		{name: "trims whitespace", value: " 3 ", want: 3},
		{name: "zero falls back", value: "0", want: 2},
		{name: "above maximum falls back", value: "5", want: 2},
		{name: "invalid falls back", value: "invalid", want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(objsUpdateHookBatchConcurrencyEnv, test.value)
			if got := objsUpdateHookBatchConcurrency(); got != test.want {
				t.Fatalf("concurrency = %d, want %d", got, test.want)
			}
		})
	}
}

func TestObjsUpdateHookBatchRunsIndependentTargetsConcurrently(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{
		Key:   conf.HandleHookAfterWriting,
		Value: "true",
	})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{
		Key:   conf.HandleHookRateLimit,
		Value: "0",
	})

	started := make(chan string, 2)
	completed := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	var active atomic.Int32
	var maximum atomic.Int32
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- parent
			<-release
			active.Add(-1)
			completed <- struct{}{}
		},
	}

	storage := newBatchHookTestDriver(map[string]bool{
		"/":  true,
		"/A": true,
		"/B": true,
	})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	if !batch.add(storage, "/A", false) || !batch.add(storage, "/B", false) {
		t.Fatal("failed to enqueue independent targets")
	}
	batch.Dispatch(context.Background())

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("target %d did not start while the first target was blocked; max concurrency=%d", i+1, maximum.Load())
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("max concurrency = %d, want 2", got)
	}

	releaseAll()
	for i := 0; i < 2; i++ {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent hooks to finish")
		}
	}
}

func TestObjsUpdateHookBatchKeepsConcurrencyWhenHookRateLimitIsEnabled(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "1"})

	started := make(chan string, 2)
	completed := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			started <- parent
			<-release
			completed <- struct{}{}
		},
	}

	storage := newBatchHookTestDriver(map[string]bool{
		"/":  true,
		"/A": true,
		"/B": true,
	})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/A", false)
	batch.add(storage, "/B", false)
	batch.Dispatch(context.Background())

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("rate-limited target %d did not start with configured concurrency", i+1)
		}
	}
	releaseAll()
	for i := 0; i < 2; i++ {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for rate-limited hooks to finish")
		}
	}
}

func TestObjsUpdateHookBatchStartsEveryTopLevelTargetBeforeBlockedDescendants(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "0"})

	topStarted := make(chan string, 3)
	allCalls := make(chan string, 5)
	releaseDescendants := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(releaseDescendants) }) }
	t.Cleanup(releaseAll)
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			allCalls <- parent
			switch parent {
			case "/test/A", "/test/B", "/test/C":
				topStarted <- parent
			case "/test/A/child", "/test/B/child":
				<-releaseDescendants
			}
		},
	}

	storage := newBatchHookTestDriver(map[string]bool{
		"/":        true,
		"/A":       true,
		"/A/child": true,
		"/B":       true,
		"/B/child": true,
		"/C":       true,
	})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/A", true)
	batch.add(storage, "/B", true)
	batch.add(storage, "/C", true)
	batch.Dispatch(context.Background())

	gotTop := make(map[string]struct{}, 3)
	for len(gotTop) < 3 {
		select {
		case parent := <-topStarted:
			gotTop[parent] = struct{}{}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("later top-level target starved behind blocked descendants; started=%v", gotTop)
		}
	}
	releaseAll()
	gotCalls := make(map[string]struct{}, 5)
	for len(gotCalls) < 5 {
		select {
		case parent := <-allCalls:
			gotCalls[parent] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatalf("not every directory hook completed; calls=%v", gotCalls)
		}
	}
}

func TestObjsUpdateHookBatchRetriesTransientListFailures(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "0"})

	hooked := make(chan string, 2)
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			hooked <- parent
		},
	}
	storage := newBatchHookTestDriver(map[string]bool{
		"/":         true,
		"/healthy":  true,
		"/eventual": true,
	})
	storage.listFailures["/eventual"] = 2
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/eventual", false)
	batch.add(storage, "/healthy", false)
	batch.Dispatch(context.Background())

	got := make(map[string]struct{}, 2)
	for len(got) < 2 {
		select {
		case parent := <-hooked:
			got[parent] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatalf("transiently unavailable directory was dropped; hooks=%v attempts=%v", got, storage.listAttempts)
		}
	}
	if attempts := storage.listAttempts["/eventual"]; attempts != 3 {
		t.Fatalf("eventual list attempts = %d, want 3", attempts)
	}
}

func TestObjsUpdateHookBatchRefillsWorkerWithoutBatchBarrier(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "0"})

	started := make(chan string, 3)
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }
	t.Cleanup(release)
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			started <- parent
			if parent == "/test/A" {
				<-releaseA
			}
		},
	}

	storage := newBatchHookTestDriver(map[string]bool{
		"/":  true,
		"/A": true,
		"/B": true,
		"/C": true,
	})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/A", false)
	batch.add(storage, "/B", false)
	batch.add(storage, "/C", false)
	batch.Dispatch(context.Background())

	seen := make(map[string]struct{}, 3)
	for len(seen) < 3 {
		select {
		case parent := <-started:
			seen[parent] = struct{}{}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("worker was not refilled while A remained blocked; started=%v", seen)
		}
	}
	if _, ok := seen["/test/C"]; !ok {
		t.Fatalf("C did not start before blocked A completed: %v", seen)
	}
	release()
}

func TestObjsUpdateHookBatchSlowHookDoesNotBlockOtherHooks(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "4")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "0"})

	fastHookCalls := make(chan string, 9)
	slowHookCalls := make(chan string, 9)
	releaseSlowHook := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(releaseSlowHook) }) }
	t.Cleanup(releaseAll)
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			fastHookCalls <- parent
		},
		func(_ context.Context, parent string, _ []model.Obj) {
			slowHookCalls <- parent
			<-releaseSlowHook
		},
	}

	paths := map[string]bool{"/": true}
	_, batch := WithObjsUpdateHookBatch(context.Background())
	storage := newBatchHookTestDriver(paths)
	for i := 1; i <= 9; i++ {
		path := fmt.Sprintf("/dir-%d", i)
		storage.objects[path] = &model.Object{ID: path, Path: path, Name: stdpath.Base(path), IsFolder: true, Modified: time.Now()}
		batch.add(storage, path, false)
	}
	batch.Dispatch(context.Background())

	for i := 0; i < 9; i++ {
		select {
		case <-fastHookCalls:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("fast hook stalled after %d of 9 directories because another hook occupied the worker pool", i)
		}
	}
	releaseAll()
	for i := 0; i < 9; i++ {
		select {
		case <-slowHookCalls:
		case <-time.After(2 * time.Second):
			t.Fatalf("slow hook did not eventually receive all directories; got %d of 9", i)
		}
	}
}

func TestObjsUpdateHookBatchSerializesOverlappingTargets(t *testing.T) {
	t.Setenv("OPENLIST_BATCH_HOOK_CONCURRENCY", "2")
	oldHooks := objsUpdateHooks
	Cache.ClearAll()
	t.Cleanup(func() {
		objsUpdateHooks = oldHooks
		Cache.ClearAll()
	})
	Cache.SetSetting(conf.HandleHookAfterWriting, &model.SettingItem{Key: conf.HandleHookAfterWriting, Value: "true"})
	Cache.SetSetting(conf.HandleHookRateLimit, &model.SettingItem{Key: conf.HandleHookRateLimit, Value: "0"})

	started := make(chan string, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var calls atomic.Int32
	objsUpdateHooks = []ObjsUpdateHook{
		func(_ context.Context, parent string, _ []model.Obj) {
			call := calls.Add(1)
			started <- parent
			if call == 1 {
				<-firstRelease
				return
			}
			<-secondRelease
		},
	}
	t.Cleanup(func() {
		select {
		case <-firstRelease:
		default:
			close(firstRelease)
		}
		select {
		case <-secondRelease:
		default:
			close(secondRelease)
		}
	})

	storage := newBatchHookTestDriver(map[string]bool{
		"/":    true,
		"/A":   true,
		"/A/B": true,
	})
	_, batch := WithObjsUpdateHookBatch(context.Background())
	batch.add(storage, "/A", false)
	batch.add(storage, "/A/B", false)
	batch.Dispatch(context.Background())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first overlapping target did not start")
	}
	select {
	case parent := <-started:
		t.Fatalf("overlapping target %q started before the first finished", parent)
	case <-time.After(200 * time.Millisecond):
	}
	close(firstRelease)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("second overlapping target did not start after the first finished")
	}
	close(secondRelease)
}
