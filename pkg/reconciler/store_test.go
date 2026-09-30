package reconciler

import (
	"path/filepath"
	"testing"
)

func TestFileStore_RoundTripAndAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "state.json")
	store := NewFileStore(path)

	// 不存在时返回空快照而非错误
	snap, err := store.Load()
	if err != nil {
		t.Fatalf("空存储 Load 不应报错: %v", err)
	}
	if len(snap.Resources) != 0 {
		t.Fatal("空存储应返回空快照")
	}
	t.Log("关键判定: 快照文件不存在时返回空快照（首次启动可正常引导）")

	r := res("K/a", "1.0.0")
	snap.Resources["K/a"] = &ResourceState{
		Target: r, ObservedRev: 1, Status: StatusApplied,
		LiveVersion: "1.0.0", Exist: true, CreatedAt: nowTS(), UpdatedAt: nowTS(),
	}
	snap.Audit = append(snap.Audit, AuditRecord{ResourceKey: "K/a", Action: "create", Result: "ok", Timestamp: nowTS()})
	snap.Revision = 1
	if err := store.Save(snap); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Resources) != 1 || loaded.Resources["K/a"].LiveVersion != "1.0.0" {
		t.Fatalf("快照往返错误: %+v", loaded.Resources)
	}
	if len(loaded.Audit) != 1 || loaded.Revision != 1 {
		t.Fatal("审计链/版本号往返错误")
	}
	t.Log("关键判定: JSON 快照完整往返（资源状态+审计+revision）")

	// 原子写：目录下不应残留临时文件
	entries, _ := readDirNames(filepath.Dir(path))
	for _, e := range entries {
		if matchTmp(e) {
			t.Fatalf("发现残留临时文件 %s（原子写应 rename 清理）", e)
		}
	}
	t.Log("关键判定: 临时文件经 rename 落盘，无残留")
}

func TestMemoryStore_DeepCopyIsolation(t *testing.T) {
	store := NewMemoryStore()
	snap := NewSnapshot()
	snap.Resources["K/a"] = &ResourceState{Target: res("K/a", "1.0.0"), Status: StatusPending}
	if err := store.Save(snap); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Load()
	got.Resources["K/a"].Status = StatusApplied // 修改读出的副本
	got2, _ := store.Load()
	if got2.Resources["K/a"].Status != StatusPending {
		t.Fatal("Load 必须返回深拷贝，调用方修改不得污染存储")
	}
	t.Log("关键判定: MemoryStore 读写均深拷贝，内外状态隔离")
}
