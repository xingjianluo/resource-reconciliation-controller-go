// Package engine implements the reconciliation loop: it validates the
// desired set, orders create/update/delete work by dependency waves, and
// converges idempotently (no duplicate side effects, no no-op rewrites,
// stale generations rejected) with the BACKEND LIVE STATE as the sole
// source of truth — every pass re-observes each resource and therefore
// self-heals external mutation/deletion and retries writes that never
// actually landed, including across process restarts. It enforces
// protection/compatibility/ownership rules with an auditable trail and
// recovers after partial failure or restart from durable state.
package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

// Logger is the minimal logging surface used by the engine.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Options configures an Engine.
type Options struct {
	// Force allows mutating/deleting protected or externally owned
	// resources.
	Force bool
	// Log receives human-readable decision traces.
	Log Logger
}

// Engine runs reconciliation against a Store and an Applier.
type Engine struct {
	store   *storage.Store
	applier applier.Applier
	opts    Options
	log     Logger

	// commitMu serializes whole Reconcile commits. Combined with per-wave
	// barriers this means concurrent triggers can never interleave two
	// generations, while independent resources inside one commit still
	// apply in parallel.
	commitMu sync.Mutex
}

// New constructs an Engine over the given store and backend.
func New(st *storage.Store, ap applier.Applier, opts Options) *Engine {
	log := opts.Log
	if log == nil {
		log = nopLogger{}
	}
	return &Engine{store: st, applier: ap, opts: opts, log: log}
}

// Reconcile accepts a desired set (with a monotonically increasing
// generation) and converges the world toward it.
//
//   - A generation older than the last accepted one is rejected as
//     StaleGeneration before any side effect. Re-running the same
//     generation is allowed and is an idempotent resume/retry.
//   - Structural problems (invalid declaration, missing dependency, cycle)
//     reject the whole set before any side effect, so no half-finished
//     state can remain.
//   - Per-resource guard violations (ownership, protection, compatibility)
//     are recorded as distinguishable failures with an audit trail and
//     leave the existing object untouched.
func (e *Engine) Reconcile(ctx context.Context, generation int64, decls []v1.Declaration) (*v1.ReconcileResult, error) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()

	result := &v1.ReconcileResult{Generation: generation}
	input := make([]string, len(decls))
	for i, d := range decls {
		input[i] = d.Key()
	}
	e.log.Printf("[reconcile] START gen=%d force=%t desired=%v stored=%d backendAPI=%s",
		generation, e.opts.Force, input, len(e.store.List()), e.applier.APIVersion())

	// 1) Stale generation guard — a stale update must not overwrite newer
	//    effective declarations.
	if generation < e.store.Generation() {
		err := &v1.ReconcileError{
			Reason:  v1.ReasonStaleGeneration,
			Message: fmt.Sprintf("generation %d is older than current %d", generation, e.store.Generation()),
		}
		e.log.Printf("[reconcile] REJECT basis=stale-generation incoming=%d current=%d", generation, e.store.Generation())
		e.audit(generation, "", "reject", err.Reason, err.Message)
		return nil, err
	}

	// 2) Whole-set structural validation, before any side effect.
	if err := validateDeclarations(decls); err != nil {
		re := asReconcileError(err)
		e.log.Printf("[reconcile] REJECT basis=structural reason=%s resource=%s msg=%q", re.Reason, re.Resource, re.Message)
		e.audit(generation, re.Resource, "reject", re.Reason, re.Message)
		return nil, err
	}

	// 3) Durably record the accepted generation *before* applying, so a
	// crash mid-commit leaves the desired generation on record and the same
	// commit can be resumed idempotently on restart.
	if err := e.store.SetGeneration(generation); err != nil {
		return nil, err
	}
	e.audit(generation, "", "commit", v1.ReasonNone, fmt.Sprintf("accepted %d declarations", len(decls)))

	desired := make(map[string]v1.Declaration, len(decls))
	for _, d := range decls {
		desired[d.Key()] = d
	}
	existing := e.store.List()

	// 4) Build the combined graph (desired nodes + nodes to delete) and
	// dependency waves.
	deleteOnly := map[string]bool{}
	for key := range existing {
		if _, ok := desired[key]; !ok {
			deleteOnly[key] = true
		}
	}
	combined := make([]v1.Declaration, 0, len(desired)+len(deleteOnly))
	for _, d := range desired {
		combined = append(combined, d)
	}
	for key := range deleteOnly {
		r := existing[key]
		combined = append(combined, v1.Declaration{
			Kind: r.Kind, Name: r.Name, DependsOn: r.DependsOn,
		})
	}
	waves, err := topoWaves(combined)
	if err != nil {
		re := asReconcileError(err)
		e.audit(generation, "", "reject", re.Reason, re.Message)
		return nil, err
	}
	dependents := buildDependents(combined)

	// 5) Forward waves: create/update desired resources. notReady tracks
	// both directly failed nodes and nodes transitively blocked by them.
	notReady := map[string]bool{}

	for wi, wave := range waves {
		type waveOutcome struct {
			key    string
			action string
			err    *v1.ReconcileError
		}
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			outcomes []waveOutcome
		)
		for _, key := range wave {
			if deleteOnly[key] {
				continue // deletions happen in the reverse pass
			}
			d := desired[key]
			if blocker := dependencyBlocker(d.DependsOn, notReady); blocker != "" {
				err := &v1.ReconcileError{
					Resource: key,
					Reason:   v1.ReasonApplyFailed,
					Message:  fmt.Sprintf("skipped: dependency %q did not become Ready", blocker),
				}
				mu.Lock()
				outcomes = append(outcomes, waveOutcome{key: key, err: err})
				mu.Unlock()
				e.log.Printf("[reconcile] SKIP gen=%d resource=%s basis=dependency-blocked by=%s", generation, key, blocker)
				e.audit(generation, key, "reject", err.Reason, err.Message)
				continue
			}
			wg.Add(1)
			go func(key string, d v1.Declaration) {
				defer wg.Done()
				action, re := e.applyOne(ctx, generation, existing[key], d)
				mu.Lock()
				outcomes = append(outcomes, waveOutcome{key: key, action: action, err: re})
				mu.Unlock()
			}(key, d)
		}
		wg.Wait()
		for _, oc := range outcomes {
			if oc.err != nil {
				notReady[oc.key] = true
				result.Failed = append(result.Failed, oc.err)
				// Audit record is already written at the decision site
				// (guard / markFailed / skip) to avoid duplicates.
				continue
			}
			switch oc.action {
			case "noop":
				result.Noop = append(result.Noop, oc.key)
			default: // create | update
				result.Applied = append(result.Applied, oc.key)
			}
		}
		e.log.Printf("[reconcile] wave=%d/%d done appliedSoFar=%d failedSoFar=%d", wi+1, len(waves), len(result.Applied), len(result.Failed))
	}

	// 6) Reverse waves: delete undesired resources, dependents first. A
	// failed/kept deletion leaves the resource present, which keeps its own
	// dependencies protected from deletion.
	stillPresent := map[string]bool{}
	for key := range deleteOnly {
		stillPresent[key] = true
	}
	for wi := len(waves) - 1; wi >= 0; wi-- {
		for _, key := range waves[wi] {
			if !deleteOnly[key] {
				continue
			}
			r := existing[key]
			if users := dependentsInUse(key, dependents, stillPresent, desired); len(users) > 0 {
				err := &v1.ReconcileError{
					Resource: key,
					Reason:   v1.ReasonDeleteFailed,
					Message:  fmt.Sprintf("kept: still referenced by %v", users),
				}
				result.Failed = append(result.Failed, err)
				e.audit(generation, key, "reject", err.Reason, err.Message)
				e.log.Printf("[reconcile] KEEP gen=%d resource=%s basis=still-referenced by=%v", generation, key, users)
				continue // stays present, continues to block its deps
			}
			if re := e.deleteOne(ctx, generation, r); re != nil {
				result.Failed = append(result.Failed, re)
				continue // stays present
			}
			stillPresent[key] = false
			result.Deleted = append(result.Deleted, key)
		}
	}

	sort.Strings(result.Applied)
	// result.Deleted intentionally keeps execution order (dependents first)
	// as evidence of reverse-dependency deletion; do not sort it.
	sort.Strings(result.Noop)
	sort.SliceStable(result.Failed, func(i, j int) bool { return result.Failed[i].Resource < result.Failed[j].Resource })

	e.log.Printf("[reconcile] DONE gen=%d applied=%v noop=%v deleted=%v failed=%d",
		generation, result.Applied, result.Noop, result.Deleted, len(result.Failed))
	return result, nil
}

// applyOne reconciles one desired resource against the BACKEND's live
// state, which is the sole source of truth: the durable store is only
// bookkeeping. It returns the action performed ("create", "update" or
// "noop" — noop covers both an already-satisfied object and a side-effect
// free adoption of a live object) and a structured failure (nil on
// success).
//
// Because satisfaction is decided by observing the backend on every pass,
// this detects (a) external mutation (live content differs from the
// declaration -> one corrective update), (b) external deletion (live
// object absent -> recreate), and (c) a write that previously failed or
// never landed (live content still old -> retried). A previously recorded
// Ready/Failed phase can never cause a satisfied verdict by itself.
func (e *Engine) applyOne(ctx context.Context, generation int64, existing *v1.Resource, d v1.Declaration) (string, *v1.ReconcileError) {
	key := d.Key()
	hash := d.SpecHash()

	// Guard: minimum backend API version (applies to create and update).
	if min := d.Annotations[v1.AnnotationMinApplierVersion]; min != "" {
		if compareVersion(e.applier.APIVersion(), min) < 0 {
			return "", e.guardReject(generation, key, &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonIncompatibleVersion,
				Message:  fmt.Sprintf("requires applier >= %s, backend is %s", min, e.applier.APIVersion()),
			})
		}
	}

	// Stale declaration version cannot overwrite a newer effective one.
	if existing != nil && existing.Version != "" && compareVersion(d.Version, existing.Version) < 0 {
		return "", e.guardReject(generation, key, &v1.ReconcileError{
			Resource: key,
			Reason:   v1.ReasonIncompatibleVersion,
			Message:  fmt.Sprintf("declaration version %s older than stored %s", d.Version, existing.Version),
		})
	}

	// Observe the backend's live state. When the backend cannot be
	// observed the resource MUST stay un-satisfied: we cannot claim
	// success from local memory, and a later pass keeps retrying.
	live, liveExists, getErr := e.applier.Get(ctx, key)
	if getErr != nil {
		re := &v1.ReconcileError{
			Resource: key,
			Reason:   v1.ReasonBackendUnavailable,
			Message:  "cannot observe backend live state: " + getErr.Error(),
		}
		e.log.Printf("[observe] GET gen=%d resource=%s basis=backend-unavailable err=%q storedPhase=%s",
			generation, key, getErr.Error(), phaseOf(existing))
		e.markFailed(generation, d, existing, re)
		return "", re
	}
	liveHash := live.SpecHash()
	e.log.Printf("[observe] GET gen=%d resource=%s basis=live-truth liveExists=%t liveHash=%s desiredHash=%s storedPhase=%s storedHash=%s",
		generation, key, liveExists, shortHash(liveHash), shortHash(hash), phaseOf(existing), shortHash(hashOf(existing)))

	// Externally owned object: refuse an over-privileged modification.
	// Ownership is read from the live backend object; the stored owner is
	// only a fallback when the live annotations carry none. A stored
	// foreign owner whose object has been deleted does not block recreate.
	if liveExists && !e.opts.Force {
		owner := live.Annotations[v1.AnnotationOwner]
		if owner == "" && existing != nil {
			owner = existing.Owner
		}
		if owner != "" && owner != v1.ControllerOwner {
			return "", e.guardReject(generation, key, &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonProtectedOwnership,
				Message:  fmt.Sprintf("owned by %q, not %q", owner, v1.ControllerOwner),
			})
		}
	}

	switch {
	case !liveExists:
		// Missing in the backend: never created, lost after a failed
		// attempt/crash, or externally deleted. (Re)create regardless of
		// what the local store remembers (it may wrongly say Ready).
		e.log.Printf("[apply] CREATE gen=%d resource=%s basis=backend-missing storedExists=%t storedPhase=%s",
			generation, key, existing != nil, phaseOf(existing))
		if err := e.applier.Create(ctx, d); err != nil {
			re := &v1.ReconcileError{Resource: key, Reason: v1.ReasonApplyFailed, Message: err.Error()}
			e.markFailed(generation, d, existing, re)
			return "", re
		}
		e.recordReady(generation, d, hash)
		return "create", nil

	case liveHash == hash:
		// The live backend content already equals the declaration. This is
		// the ONLY satisfied verdict; it comes from observation, not from
		// the stored phase. If local bookkeeping disagrees (a prior Failed
		// mark, or a crash after the write landed), repair it WITHOUT any
		// backend side effect.
		if existing == nil || existing.Phase != v1.PhaseReady || existing.SpecHash != hash {
			e.log.Printf("[apply] ADOPT gen=%d resource=%s basis=live-hash-equal hash=%s storedPhase=%s (no backend write)",
				generation, key, shortHash(hash), phaseOf(existing))
			e.recordReadyAction(generation, d, hash, "adopt",
				"live backend state matches declaration; adopted without side effect")
		} else {
			e.log.Printf("[apply] NOOP gen=%d resource=%s basis=live-hash-equal hash=%s", generation, key, shortHash(hash))
		}
		return "noop", nil

	default:
		// Live content differs from the declaration: external drift, or a
		// previous create/update never actually landed. Exactly one
		// corrective update converges it.
		protected := (existing != nil && isProtected(existing)) ||
			live.Annotations[v1.AnnotationProtected] == "true"
		if protected && !e.opts.Force {
			return "", e.guardReject(generation, key, &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonRuleConflict,
				Message:  "resource is protected; refusing in-place modification without force",
			})
		}
		e.log.Printf("[apply] UPDATE gen=%d resource=%s basis=external-drift liveHash=%s desiredHash=%s storedHash=%s",
			generation, key, shortHash(liveHash), shortHash(hash), shortHash(hashOf(existing)))
		eff := existing
		if eff == nil {
			// Live object exists but the store lost it (crash/adoption):
			// give the applier the observed object as its base.
			eff = resourceFromDecl(live)
		}
		if err := e.applier.Update(ctx, eff, d); err != nil {
			re := &v1.ReconcileError{Resource: key, Reason: v1.ReasonApplyFailed, Message: err.Error()}
			e.markFailed(generation, d, existing, re)
			return "", re
		}
		e.recordReady(generation, d, hash)
		return "update", nil
	}
}

// deleteOne removes one undesired resource after protection/ownership
// checks. A rejected or failed delete leaves the resource in place.
func (e *Engine) deleteOne(ctx context.Context, generation int64, r *v1.Resource) *v1.ReconcileError {
	key := r.Key()
	if r.Owner != "" && r.Owner != v1.ControllerOwner && !e.opts.Force {
		return e.guardReject(generation, key, &v1.ReconcileError{
			Resource: key,
			Reason:   v1.ReasonProtectedOwnership,
			Message:  fmt.Sprintf("owned by %q; refusing delete without force", r.Owner),
		})
	}
	if isProtected(r) && !e.opts.Force {
		return e.guardReject(generation, key, &v1.ReconcileError{
			Resource: key,
			Reason:   v1.ReasonRuleConflict,
			Message:  "resource is protected; refusing deletion without force",
		})
	}
	e.log.Printf("[delete] DELETE gen=%d resource=%s basis=not-desired", generation, key)
	if err := e.applier.Delete(ctx, r); err != nil {
		re := &v1.ReconcileError{Resource: key, Reason: v1.ReasonDeleteFailed, Message: err.Error()}
		cp := *r
		cp.Phase = v1.PhaseFailed
		cp.Generation = generation
		cp.Conditions = []v1.Condition{rejectedCondition(re)}
		_ = e.store.Put(&cp)
		e.audit(generation, key, "reject", re.Reason, re.Message)
		return re
	}
	if err := e.store.Delete(key); err != nil {
		re := asReconcileError(err)
		re.Resource = key
		return re
	}
	e.audit(generation, key, "delete", v1.ReasonNone, "removed as no longer desired")
	return nil
}

// recordReady persists the converged state and audits the real side effect
// ("create"/"update") that produced it.
func (e *Engine) recordReady(generation int64, d v1.Declaration, hash string) {
	e.recordReadyAction(generation, d, hash, "reconciled", "converged to desired state")
}

// recordReadyAction persists Ready bookkeeping with an explicit audit
// action. "reconciled" follows a real backend write; "adopt" records a
// side-effect-free adoption of live backend state that already matched.
func (e *Engine) recordReadyAction(generation int64, d v1.Declaration, hash, action, msg string) {
	r := &v1.Resource{
		Kind:        d.Kind,
		Name:        d.Name,
		Version:     d.Version,
		DependsOn:   append([]string(nil), d.DependsOn...),
		Spec:        cloneStringMap(d.Spec),
		Annotations: cloneStringMap(d.Annotations),
		Phase:       v1.PhaseReady,
		Generation:  generation,
		SpecHash:    hash,
		Owner:       v1.ControllerOwner,
		Conditions: []v1.Condition{{
			Type: v1.ConditionTypeReconciled, Status: "True", Time: time.Now().UTC(),
		}},
	}
	_ = e.store.Put(r)
	e.audit(generation, d.Key(), action, v1.ReasonNone, msg)
}

// markFailed persists a Failed phase for an apply attempt so retry/resume
// (including after restart) can see and recover it. Crucially it never
// rewrites the stored SpecHash to the un-reached desired hash: a failed
// write must not be mistaken for satisfaction on the next pass. Existing
// ownership is preserved when known.
func (e *Engine) markFailed(generation int64, d v1.Declaration, existing *v1.Resource, re *v1.ReconcileError) {
	owner := v1.ControllerOwner
	if existing != nil && existing.Owner != "" {
		owner = existing.Owner
	}
	storedHash := ""
	if existing != nil {
		storedHash = existing.SpecHash
	}
	r := &v1.Resource{
		Kind:        d.Kind,
		Name:        d.Name,
		Version:     d.Version,
		DependsOn:   append([]string(nil), d.DependsOn...),
		Spec:        cloneStringMap(d.Spec),
		Annotations: cloneStringMap(d.Annotations),
		Phase:       v1.PhaseFailed,
		Generation:  generation,
		SpecHash:    storedHash,
		Owner:       owner,
		Conditions:  []v1.Condition{rejectedCondition(re)},
	}
	_ = e.store.Put(r)
	e.audit(generation, d.Key(), "reject", re.Reason, re.Message)
}

func (e *Engine) guardReject(generation int64, key string, re *v1.ReconcileError) *v1.ReconcileError {
	e.log.Printf("[guard] REJECT gen=%d resource=%s reason=%s basis=%q", generation, key, re.Reason, re.Message)
	e.audit(generation, key, "reject", re.Reason, re.Message)
	return re
}

func (e *Engine) audit(generation int64, resource, action string, reason v1.Reason, msg string) {
	_, _ = e.store.AppendAudit(v1.AuditEvent{
		Generation: generation,
		Resource:   resource,
		Action:     action,
		Reason:     reason,
		Message:    msg,
	})
}

// ---- small helpers ----

func asReconcileError(err error) *v1.ReconcileError {
	if re, ok := err.(*v1.ReconcileError); ok {
		return re
	}
	return &v1.ReconcileError{Reason: v1.ReasonApplyFailed, Message: err.Error()}
}

func isProtected(r *v1.Resource) bool {
	return r != nil && r.Annotations[v1.AnnotationProtected] == "true"
}

func rejectedCondition(re *v1.ReconcileError) v1.Condition {
	return v1.Condition{
		Type: v1.ConditionTypeRejected, Status: "True",
		Reason: re.Reason, Message: re.Message, Time: time.Now().UTC(),
	}
}

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func shortHash(h string) string {
	if len(h) >= 8 {
		return h[:8]
	}
	return h
}

// phaseOf renders a possibly-nil stored resource's phase for logs.
func phaseOf(r *v1.Resource) string {
	if r == nil {
		return "<absent>"
	}
	return string(r.Phase)
}

// hashOf renders a possibly-nil stored resource's hash for logs.
func hashOf(r *v1.Resource) string {
	if r == nil {
		return ""
	}
	return r.SpecHash
}

// resourceFromDecl converts a live backend observation into a minimal
// stored-style resource, used only as the base of an Update when local
// bookkeeping has no record of an existing object.
func resourceFromDecl(d v1.Declaration) *v1.Resource {
	return &v1.Resource{
		Kind:        d.Kind,
		Name:        d.Name,
		Version:     d.Version,
		DependsOn:   append([]string(nil), d.DependsOn...),
		Spec:        cloneStringMap(d.Spec),
		Annotations: cloneStringMap(d.Annotations),
		SpecHash:    d.SpecHash(),
		Owner:       d.Annotations[v1.AnnotationOwner],
	}
}

// dependencyBlocker returns a dependency that is not Ready (directly failed
// or transitively blocked), or "" when every dependency is usable.
func dependencyBlocker(deps []string, notReady map[string]bool) string {
	for _, dep := range deps {
		if notReady[dep] {
			return dep
		}
	}
	return ""
}

func buildDependents(decls []v1.Declaration) map[string][]string {
	out := map[string][]string{}
	for _, d := range decls {
		for _, dep := range d.DependsOn {
			out[dep] = append(out[dep], d.Key())
		}
	}
	return out
}

// dependentsInUse names resources that still require key to exist: they are
// either delete-only resources still present, or resources that remain in
// the desired set.
func dependentsInUse(key string, dependents map[string][]string, present map[string]bool, desired map[string]v1.Declaration) []string {
	var users []string
	for _, user := range dependents[key] {
		if _, keep := desired[user]; keep {
			users = append(users, user)
			continue
		}
		if present[user] {
			users = append(users, user)
		}
	}
	sort.Strings(users)
	return users
}

// compareVersion compares dotted numeric versions (e.g. "1.10.2" vs
// "1.9.0"); non-numeric chunks fall back to lexicographic comparison.
// Returns -1/0/1.
func compareVersion(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	n := len(pa)
	if len(pb) > n {
		n = len(pb)
	}
	for i := 0; i < n; i++ {
		var x, y string
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		if x == "" {
			x = "0" // a missing trailing segment equals ".0"
		}
		if y == "" {
			y = "0"
		}
		if x == y {
			continue
		}
		nx, errx := strconv.Atoi(x)
		ny, erry := strconv.Atoi(y)
		if errx == nil && erry == nil {
			if nx < ny {
				return -1
			}
			return 1
		}
		return strings.Compare(x, y)
	}
	return 0
}
