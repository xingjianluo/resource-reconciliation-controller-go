// Package applier models the external system that resources are reconciled
// against. The FakeApplier is an in-memory backend used for deterministic
// tests; it independently tracks the real world so duplicate side effects
// from concurrent/repeated reconciliation are observable.
package applier

import (
	"context"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

// Applier performs real side effects against the managed backend.
type Applier interface {
	// Create creates the resource described by d. It must fail if the
	// resource already exists.
	Create(ctx context.Context, d v1.Declaration) error
	// Update brings an existing resource to the desired declaration.
	Update(ctx context.Context, existing *v1.Resource, d v1.Declaration) error
	// Delete removes the resource described by r.
	Delete(ctx context.Context, r *v1.Resource) error
	// Exists reports whether the resource currently exists in the backend.
	Exists(key string) bool
	// Get returns the resource's current live state as observed in the
	// backend, exactly as the backend holds it. The second return value is
	// false when the object is absent (e.g. externally deleted); a non-nil
	// error means the live state could not be observed at all (backend
	// outage, timeout) and callers must NOT infer satisfaction from local
	// state in that case. The engine treats the backend observation, not
	// its durable store, as the source of truth.
	Get(ctx context.Context, key string) (v1.Declaration, bool, error)
	// APIVersion reports the backend API version, used for compatibility
	// checks against AnnotationMinApplierVersion.
	APIVersion() string
}
