package reconciler

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 是引擎状态的持久化抽象。
type Store interface {
	// Load 读取最近一次保存的快照；不存在时返回空快照。
	Load() (*Snapshot, error)
	// Save 原子地持久化整个快照。
	Save(snap *Snapshot) error
}

// NewSnapshot 返回一个初始化好的空快照。
func NewSnapshot() *Snapshot {
	return &Snapshot{Resources: map[string]*ResourceState{}, Audit: []AuditRecord{}}
}

// MemoryStore 是进程内存储（用于测试）。
type MemoryStore struct {
	mu   sync.Mutex
	snap *Snapshot
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{snap: NewSnapshot()}
}

func (s *MemoryStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSnapshot(s.snap), nil
}

func (s *MemoryStore) Save(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = cloneSnapshot(snap)
	return nil
}

// FileStore 将快照以 JSON 原子写入单个文件（写临时文件后 rename）。
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore 创建基于文件路径的存储。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return NewSnapshot(), nil
		}
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	if snap.Resources == nil {
		snap.Resources = map[string]*ResourceState{}
	}
	if snap.Audit == nil {
		snap.Audit = []AuditRecord{}
	}
	return &snap, nil
}

func (s *FileStore) Save(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".snap-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// rename 在同一文件系统上是原子的，保证快照完整可恢复。
	return os.Rename(tmpName, s.path)
}

// cloneSnapshot 深拷贝快照，避免调用方修改内部状态。
func cloneSnapshot(in *Snapshot) *Snapshot {
	if in == nil {
		return NewSnapshot()
	}
	out := &Snapshot{Revision: in.Revision, Resources: map[string]*ResourceState{}, Audit: make([]AuditRecord, len(in.Audit))}
	copy(out.Audit, in.Audit)
	for k, v := range in.Resources {
		out.Resources[k] = cloneState(v)
	}
	return out
}

func cloneState(in *ResourceState) *ResourceState {
	if in == nil {
		return nil
	}
	out := *in
	if in.Target != nil {
		t := *in.Target
		if in.Target.DependsOn != nil {
			t.DependsOn = append([]string(nil), in.Target.DependsOn...)
		}
		t.Spec = cloneSpec(in.Target.Spec)
		out.Target = &t
	}
	out.LiveSpec = cloneSpec(in.LiveSpec)
	if in.Failure != nil {
		f := *in.Failure
		out.Failure = &f
	}
	return &out
}

func cloneSpec(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
