package op

import (
	"context"
	stdpath "path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type batchHookTestDriver struct {
	model.Storage
	mu      sync.Mutex
	objects map[string]*model.Object
}

func newBatchHookTestDriver(paths map[string]bool) *batchHookTestDriver {
	d := &batchHookTestDriver{
		Storage: model.Storage{
			MountPath:       "/test",
			Status:          WORK,
			CacheExpiration: 1,
			Modified:        time.Now(),
		},
		objects: make(map[string]*model.Object, len(paths)),
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
