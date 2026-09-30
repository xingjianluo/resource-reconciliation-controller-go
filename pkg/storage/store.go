// Package storage provides durable, file-backed persistence for resources,
// the latest desired generation and the append-only audit trail. Writes are
// atomic (temp file + rename) so a process crash never leaves a torn file;
// on restart the controller reloads the last consistent snapshot and resumes
// reconciliation from it.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

// Store persists controller state. An in-memory mirror is the source of
// truth while running; every mutation is flushed atomically to disk so the
// process can resume after a restart.
type Store struct {
	mu   sync.Mutex
	path string

	generation int64
	resources  map[string]*v1.Resource
	auditSeq   int64
	audit      []v1.AuditEvent
}

// Snapshot is the durable root object written atomically to disk.
type Snapshot struct {
	Generation int64                   `json:"generation"`
	Resources  map[string]*v1.Resource `json:"resources"`
	AuditSeq   int64                   `json:"auditSeq"`
	Audit      []v1.AuditEvent         `json:"audit"`
}

// New opens (or creates) the store at path and loads the last snapshot.
// A missing file means a fresh, empty state.
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("storage: empty path")
	}
	s := &Store{
		path:      path,
		resources: map[string]*v1.Resource{},
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: create dir: %w", err)
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("storage: read: %w", err)
	}
	if len(b) == 0 {
		return nil
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("storage: corrupt snapshot: %w", err)
	}
	s.generation = snap.Generation
	s.resources = snap.Resources
	if s.resources == nil {
		s.resources = map[string]*v1.Resource{}
	}
	s.auditSeq = snap.AuditSeq
	s.audit = snap.Audit
	return nil
}

// flush writes the whole snapshot atomically: write a temp file in the same
// directory, fsync it, then rename it over the target. Crash at any point
// leaves either the old file or the new one, never a torn file.
// Caller must hold s.mu.
func (s *Store) flushLocked() error {
	snap := Snapshot{
		Generation: s.generation,
		Resources:  s.resources,
		AuditSeq:   s.auditSeq,
		Audit:      s.audit,
	}
	b, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return fmt.Errorf("storage: marshal: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".snap-*")
	if err != nil {
		return fmt.Errorf("storage: temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: close: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("storage: rename: %w", err)
	}
	return nil
}

// Generation returns the latest accepted desired generation.
func (s *Store) Generation() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

// SetGeneration records the newest accepted desired generation.
func (s *Store) SetGeneration(gen int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen < s.generation {
		return fmt.Errorf("storage: cannot move generation backwards (%d < %d)", gen, s.generation)
	}
	s.generation = gen
	return s.persistLocked("set generation")
}

// Get returns a deep copy of a resource and whether it exists.
func (s *Store) Get(key string) (*v1.Resource, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.resources[key]
	if !ok {
		return nil, false
	}
	return cloneResource(r), true
}

// List returns deep copies of all stored resources keyed by key.
func (s *Store) List() map[string]*v1.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*v1.Resource, len(s.resources))
	for k, r := range s.resources {
		out[k] = cloneResource(r)
	}
	return out
}

// Put inserts or replaces a resource and stamps UpdatedAt.
func (s *Store) Put(r *v1.Resource) error {
	if r == nil {
		return errors.New("storage: nil resource")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneResource(r)
	cp.UpdatedAt = time.Now().UTC()
	s.resources[cp.Key()] = cp
	return s.persistLocked("put " + cp.Key())
}

// Delete removes a resource. Deleting a missing resource is not an error.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.resources, key)
	return s.persistLocked("delete " + key)
}

// AppendAudit records an audit event, assigning its sequence number and
// timestamp, and persists it durably.
func (s *Store) AppendAudit(e v1.AuditEvent) (v1.AuditEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditSeq++
	e.Seq = s.auditSeq
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	s.audit = append(s.audit, e)
	if err := s.persistLocked("audit " + e.Action); err != nil {
		return e, err
	}
	return e, nil
}

// Audit returns a copy of the audit trail in sequence order.
func (s *Store) Audit() []v1.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]v1.AuditEvent, len(s.audit))
	copy(out, s.audit)
	return out
}

// Close releases the store. The in-memory state is already flushed on every
// mutation, so Close only needs to do a final durable flush.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

// persistLocked flushes and maps filesystem failures onto the stable
// StorageUnavailable reason. Caller must hold s.mu.
func (s *Store) persistLocked(_ string) error {
	if err := s.flushLocked(); err != nil {
		return &v1.ReconcileError{Reason: v1.ReasonStorageUnavailable, Message: err.Error()}
	}
	return nil
}

func cloneResource(r *v1.Resource) *v1.Resource {
	if r == nil {
		return nil
	}
	cp := *r
	if r.DependsOn != nil {
		cp.DependsOn = append([]string(nil), r.DependsOn...)
	}
	if r.Spec != nil {
		cp.Spec = map[string]string{}
		for k, v := range r.Spec {
			cp.Spec[k] = v
		}
	}
	if r.Annotations != nil {
		cp.Annotations = map[string]string{}
		for k, v := range r.Annotations {
			cp.Annotations[k] = v
		}
	}
	if r.Conditions != nil {
		cp.Conditions = append([]v1.Condition(nil), r.Conditions...)
	}
	return &cp
}
