package applier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

// FailRule conditionally injects a failure for a named resource/action.
type FailRule struct {
	Resource string // match by declaration/resource Key()
	Action   string // "create" | "update" | "delete" | "get"; empty matches all
	Times    int    // fail this many times, then succeed (-1 = always fail)
	Err      error  // returned failure; defaults to a generic backend error
}

// FakeApplier is an in-memory Applier with fault injection and side-effect
// counters. Its world map is the backend's own truth and is independent of
// the controller store, so duplicate side effects under concurrent or
// repeated reconciliation are directly observable.
type FakeApplier struct {
	mu sync.Mutex

	apiVersion string
	world      map[string]v1.Declaration

	// side-effect counters keyed by resource key
	Creates map[string]int
	Updates map[string]int
	Deletes map[string]int

	FailRules []FailRule
	failLeft  map[string]int
}

// NewFake builds a FakeApplier reporting the given backend API version.
func NewFake(apiVersion string) *FakeApplier {
	if apiVersion == "" {
		apiVersion = "1.0.0"
	}
	return &FakeApplier{
		apiVersion: apiVersion,
		world:      map[string]v1.Declaration{},
		Creates:    map[string]int{},
		Updates:    map[string]int{},
		Deletes:    map[string]int{},
		failLeft:   map[string]int{},
	}
}

// Exists reports whether the resource exists in the backend world. It does
// NOT model observation failures; tests that need a backend outage use Get.
func (f *FakeApplier) Exists(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.world[key]
	return ok
}

// Get returns the backend's live declaration for a resource. It honors
// injected "get" failures (simulating an unreadable backend), in which case
// it returns an error without revealing whether the object exists, so the
// engine is forced to keep the resource un-satisfied rather than trusting
// local state.
func (f *FakeApplier) Get(ctx context.Context, key string) (v1.Declaration, bool, error) {
	if err := ctx.Err(); err != nil {
		return v1.Declaration{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkFailureLocked(key, "get"); err != nil {
		return v1.Declaration{}, false, err
	}
	d, ok := f.world[key]
	if !ok {
		return v1.Declaration{}, false, nil
	}
	return cloneDecl(d), true, nil
}

// GetDeclaration returns the backend's current declaration for a resource.
func (f *FakeApplier) GetDeclaration(key string) (v1.Declaration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.world[key]
	return d, ok
}

// SeedWorld installs a resource directly into the backend, simulating a
// resource created outside (or by a previous incarnation of) the
// controller. Useful for ownership and process-restart tests.
func (f *FakeApplier) SeedWorld(d v1.Declaration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.world[d.Key()] = d
}

// RemoveWorld removes a resource directly from the backend without counting
// a side effect.
func (f *FakeApplier) RemoveWorld(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.world, key)
}

// Counts returns (creates, updates, deletes) observed for a resource.
func (f *FakeApplier) Counts(key string) (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Creates[key], f.Updates[key], f.Deletes[key]
}

func (f *FakeApplier) Create(ctx context.Context, d v1.Declaration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := d.Key()
	if err := f.checkFailureLocked(key, "create"); err != nil {
		return err
	}
	if _, ok := f.world[key]; ok {
		// Real backends reject duplicate creation; this is what makes a
		// double-create visible rather than silently idempotent.
		return fmt.Errorf("backend: %s already exists", key)
	}
	f.world[key] = cloneDecl(d)
	f.Creates[key]++
	return nil
}

func (f *FakeApplier) Update(ctx context.Context, existing *v1.Resource, d v1.Declaration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := d.Key()
	if err := f.checkFailureLocked(key, "update"); err != nil {
		return err
	}
	if _, ok := f.world[key]; !ok {
		// The object the controller believed existed is gone: report it so
		// the controller can recreate rather than blindly overwrite.
		return fmt.Errorf("backend: %s not found for update", key)
	}
	f.world[key] = cloneDecl(d)
	f.Updates[key]++
	return nil
}

func (f *FakeApplier) Delete(ctx context.Context, r *v1.Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.Key()
	if err := f.checkFailureLocked(key, "delete"); err != nil {
		return err
	}
	// Deleting an already-absent backend object is an idempotent no-op: it
	// still counts as no side effect.
	if _, ok := f.world[key]; !ok {
		return nil
	}
	delete(f.world, key)
	f.Deletes[key]++
	return nil
}

func (f *FakeApplier) APIVersion() string { return f.apiVersion }

// checkFailureLocked consults injected FailRules. A rule with Times > 0
// fails that many times and then heals (used for partial-failure-retry
// tests); Times == -1 fails forever. Caller holds f.mu.
func (f *FakeApplier) checkFailureLocked(key, action string) error {
	for _, rule := range f.FailRules {
		if rule.Resource != key {
			continue
		}
		if rule.Action != "" && rule.Action != action {
			continue
		}
		counterKey := ruleKey(rule.Resource, rule.Action)
		left, seen := f.failLeft[counterKey]
		if !seen {
			left = rule.Times
		}
		if left == 0 {
			continue // rule exhausted: healed
		}
		if left > 0 {
			f.failLeft[counterKey] = left - 1
		}
		// left == -1 stays -1 (always fail)
		if rule.Err != nil {
			return rule.Err
		}
		return errors.New("backend: injected " + action + " failure for " + key)
	}
	return nil
}

func ruleKey(resource, action string) string {
	return resource + "#" + action
}

func cloneDecl(d v1.Declaration) v1.Declaration {
	cp := d
	if d.DependsOn != nil {
		cp.DependsOn = append([]string(nil), d.DependsOn...)
	}
	if d.Spec != nil {
		cp.Spec = map[string]string{}
		for k, v := range d.Spec {
			cp.Spec[k] = v
		}
	}
	if d.Annotations != nil {
		cp.Annotations = map[string]string{}
		for k, v := range d.Annotations {
			cp.Annotations[k] = v
		}
	}
	return cp
}
