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
	// The engine uses this to distinguish a real no-op from a resource
	// that must be (re)created after a failed attempt or a crash.
	Exists(key string) bool
	// APIVersion reports the backend API version, used for compatibility
	// checks against AnnotationMinApplierVersion.
	APIVersion() string
}
