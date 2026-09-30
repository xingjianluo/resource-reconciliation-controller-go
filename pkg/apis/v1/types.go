// Package v1 defines the resource declaration model, stored state, audit
// records and the stable, distinguishable error reasons used across the
// reconciliation controller.
package v1

import "time"

// Phase is the lifecycle phase of a stored resource.
type Phase string

const (
	PhasePending  Phase = "Pending"
	PhaseReady    Phase = "Ready"
	PhaseFailed   Phase = "Failed"
	PhaseDeleting Phase = "Deleting"
)

// Reason is a stable, machine-readable rejection/failure reason. Every
// reason is distinct so callers and tests can tell failure causes apart.
type Reason string

const (
	ReasonNone                 Reason = ""
	ReasonCycleDetected       Reason = "CycleDetected"
	ReasonMissingDependency   Reason = "MissingDependency"
	ReasonInvalidDeclaration  Reason = "InvalidDeclaration"
	ReasonStaleGeneration      Reason = "StaleGeneration"
	ReasonProtectedOwnership   Reason = "ProtectedOwnership"
	ReasonRuleConflict         Reason = "RuleConflict"
	ReasonIncompatibleVersion  Reason = "IncompatibleVersion"
	ReasonApplyFailed          Reason = "ApplyFailed"
	ReasonDeleteFailed         Reason = "DeleteFailed"
	ReasonStorageUnavailable   Reason = "StorageUnavailable"
)

// Well-known annotation keys understood by the controller.
const (
	// AnnotationOwner marks which controller/identity owns a resource.
	AnnotationOwner = "controller.xingjianluo.io/owner"
	// AnnotationProtected marks a resource as protected. Protected
	// resources can only be changed/deleted with the force flag.
	AnnotationProtected = "controller.xingjianluo.io/protected"
	// AnnotationMinApplierVersion is the minimum applier API version the
	// resource requires.
	AnnotationMinApplierVersion = "controller.xingjianluo.io/min-applier-version"

	// ControllerOwner is the identity this controller claims on resources
	// it creates. Resources owned by someone else are protected.
	ControllerOwner = "rrc-go"
)

// Condition types recorded on stored resources.
const (
	ConditionTypeReconciled = "Reconciled"
	ConditionTypeRejected   = "Rejected"
)

// Declaration is the user-supplied desired state for one resource.
type Declaration struct {
	// Kind is the resource kind (e.g. "Bucket", "Topic").
	Kind string `json:"kind"`
	// Name is the unique resource name within the controller's scope.
	Name string `json:"name"`
	// Version is the schema version of the declaration.
	Version string `json:"version"`
	// DependsOn lists names of resources that must be Ready before this
	// resource is created/updated, and that must outlive this resource
	// (this resource is deleted before its dependencies).
	DependsOn []string `json:"dependsOn,omitempty"`
	// Spec is the versioned, desired configuration payload.
	Spec map[string]string `json:"spec,omitempty"`
	// Annotations carry ownership, protection and compatibility metadata.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// Key returns the unique identifier of the declared resource.
func (d Declaration) Key() string { return d.Kind + "/" + d.Name }

// SpecHash returns a stable hash of the meaningful desired state so the
// controller can skip no-op updates (idempotent convergence).
func (d Declaration) SpecHash() string {
	return specHash(d)
}

// Condition is a point-in-time reconciliation observation.
type Condition struct {
	Type    string    `json:"type"`
	Status  string    `json:"status"` // "True" | "False"
	Reason  Reason    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Time    time.Time `json:"time"`
}

// Resource is the persisted observed state of one resource.
type Resource struct {
	Kind        string            `json:"kind"`
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	DependsOn   []string          `json:"dependsOn,omitempty"`
	Spec        map[string]string `json:"spec,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`

	Phase      Phase       `json:"phase"`
	Generation int64       `json:"generation"`
	SpecHash   string      `json:"specHash"`
	Owner      string      `json:"owner"`
	Conditions []Condition `json:"conditions,omitempty"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}

// Key returns the unique identifier of the resource.
func (r *Resource) Key() string { return r.Kind + "/" + r.Name }

// AuditEvent is an append-only, traceable record of a reconciliation
// decision or side effect.
type AuditEvent struct {
	Seq        int64     `json:"seq"`
	Time       time.Time `json:"time"`
	Generation int64     `json:"generation"`
	Resource   string    `json:"resource,omitempty"`
	Action     string    `json:"action"` // "validate" | "create" | "update" | "noop" | "delete" | "reject" | "commit"
	Reason     Reason    `json:"reason,omitempty"`
	Message    string    `json:"message,omitempty"`
}

// ReconcileError is a structured reconciliation failure with a stable,
// distinguishable Reason.
type ReconcileError struct {
	Resource string
	Reason   Reason
	Message  string
}

func (e *ReconcileError) Error() string { return reconcileErrorString(e) }

// ReconcileResult summarises one Reconcile invocation.
type ReconcileResult struct {
	Generation int64
	Applied    []string // resources created or updated (real side effects)
	Deleted    []string // resources deleted (real side effects)
	Noop       []string // resources already at desired state
	Failed     []*ReconcileError
}

// Succeeded reports whether the whole desired set converged.
func (r *ReconcileResult) Succeeded() bool { return len(r.Failed) == 0 }