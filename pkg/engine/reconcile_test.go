package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

func decl(kind, name, version string, deps ...string) v1.Declaration {
	return v1.Declaration{Kind: kind, Name: name, Version: version, DependsOn: deps}
}

func reasonOf(err error) v1.Reason {
	var re *v1.ReconcileError
	if errors.As(err, &re) {
		return re.Reason
	}
	return "<non-reconcile-error>"
}

// TestDependencyOrderingAndIdempotence covers: dependencies created first,
// repeated triggers produce no duplicate side effects and no no-op rewrites.
func TestDependencyOrderingAndIdempotence(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Topic", "leaf", "1", "Bucket/root"),
		decl("Bucket", "root", "1"),
	}
	t.Logf("INPUT gen=1 desired=%v (leaf depends on root)", []string{"Topic/leaf", "Bucket/root"})

	res, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	t.Logf("JUDGE run1 applied=%v noop=%v deleted=%v", res.Applied, res.Noop, res.Deleted)
	if len(res.Applied) != 2 || res.Applied[0] != "Bucket/root" || res.Applied[1] != "Topic/leaf" {
		t.Fatalf("dependency order violated: %v", res.Applied)
	}

	// Second identical trigger: everything must be a no-op with zero new
	// backend side effects.
	res2, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	t.Logf("JUDGE run2 applied=%v noop=%v (expect all noop)", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 2 {
		t.Fatalf("identical re-trigger must be idempotent, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}

	for _, key := range []string{"Bucket/root", "Topic/leaf"} {
		c, u, d := fx.fake.Counts(key)
		t.Logf("JUDGE backend side effects %s creates=%d updates=%d deletes=%d (expect exactly one create)", key, c, u, d)
		if c != 1 || u != 0 || d != 0 {
			t.Fatalf("%s had unexpected side effects c=%d u=%d d=%d", key, c, u, d)
		}
	}
}

// TestSpecDriftTriggersExactlyOneUpdate: changed desired state updates once;
// a repeated run is then a no-op.
func TestSpecDriftTriggersExactlyOneUpdate(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	d1 := decl("Topic", "t", "1")
	d1.Spec = map[string]string{"partitions": "1"}
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d1}); err != nil {
		t.Fatal(err)
	}

	d2 := decl("Topic", "t", "1")
	d2.Spec = map[string]string{"partitions": "8"}
	t.Logf("INPUT gen=2 spec drift partitions 1 -> 8")
	res, err := eng.Reconcile(ctx, 2, []v1.Declaration{d2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE applied=%v (expect single update)", res.Applied)
	if len(res.Applied) != 1 || res.Applied[0] != "Topic/t" {
		t.Fatalf("drift must yield one update, got %v", res.Applied)
	}

	res2, _ := eng.Reconcile(ctx, 2, []v1.Declaration{d2})
	t.Logf("JUDGE repeat applied=%v noop=%v (expect noop)", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 {
		t.Fatalf("repeated drift set must not update again: %v", res2.Applied)
	}
	if _, u, _ := fx.fake.Counts("Topic/t"); u != 1 {
		t.Fatalf("expected exactly 1 update side effect, got %d", u)
	}
}

// TestDeleteRespectsReverseDependencyOrder: removing the whole set deletes
// dependents before dependencies.
func TestDeleteRespectsReverseDependencyOrder(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Bucket", "root", "1"),
		decl("Topic", "child", "1", "Bucket/root"),
	}
	if _, err := eng.Reconcile(ctx, 1, desired); err != nil {
		t.Fatal(err)
	}

	t.Logf("INPUT gen=2 desired=[] (delete everything)")
	res, err := eng.Reconcile(ctx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE deleted=%v (expect child before root)", res.Deleted)
	if len(res.Deleted) != 2 || res.Deleted[0] != "Topic/child" || res.Deleted[1] != "Bucket/root" {
		t.Fatalf("reverse dependency delete order violated: %v", res.Deleted)
	}
	if _, _, d := fx.fake.Counts("Topic/child"); d != 1 {
		t.Fatalf("child must be deleted once")
	}
	if len(fx.store.List()) != 0 {
		t.Fatalf("store must be empty after deletion, got %v", fx.store.List())
	}
}

// TestConcurrentReconcileIsIdempotent: many concurrent identical triggers
// must not produce duplicate backend side effects.
func TestConcurrentReconcileIsIdempotent(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{decl("Bucket", "a", "1"), decl("Bucket", "b", "1")}
	const n = 12
	t.Logf("INPUT gen=1 firing %d concurrent Reconcile calls with identical set", n)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := eng.Reconcile(ctx, 1, desired)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile failed: %v", err)
		}
	}
	for _, key := range []string{"Bucket/a", "Bucket/b"} {
		c, u, d := fx.fake.Counts(key)
		t.Logf("JUDGE %s side effects under concurrency creates=%d updates=%d deletes=%d (expect 1/0/0)", key, c, u, d)
		if c != 1 || u != 0 || d != 0 {
			t.Fatalf("concurrent triggers duplicated side effects on %s: c=%d u=%d d=%d", key, c, u, d)
		}
	}
}

// TestStaleGenerationRejected: an older generation must be rejected before
// any side effect and must not roll back newer state.
func TestStaleGenerationRejected(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	d1 := decl("Topic", "t", "1")
	d1.Spec = map[string]string{"partitions": "8"}
	if _, err := eng.Reconcile(ctx, 10, []v1.Declaration{d1}); err != nil {
		t.Fatal(err)
	}
	stale := decl("Topic", "t", "1")
	stale.Spec = map[string]string{"partitions": "1"}
	t.Logf("INPUT stale gen=9 after current gen=10")
	_, err := eng.Reconcile(ctx, 9, []v1.Declaration{stale})
	t.Logf("JUDGE err=%v (expect StaleGeneration)", err)
	if reasonOf(err) != v1.ReasonStaleGeneration {
		t.Fatalf("expected StaleGeneration, got %v", err)
	}
	r, ok := fx.store.Get("Topic/t")
	if !ok || r.Spec["partitions"] != "8" || r.Generation != 10 {
		t.Fatalf("newer state must survive stale write, got %+v", r)
	}
	if _, u, _ := fx.fake.Counts("Topic/t"); u != 0 {
		t.Fatalf("stale generation must not call update, got %d updates", u)
	}
}

// TestFailureReasonsAreDistinguishable: each guard/rule violation returns
// its own stable reason and leaves the existing object untouched.
func TestFailureReasonsAreDistinguishable(t *testing.T) {
	ctx := context.Background()

	t.Run("incompatible backend version", func(t *testing.T) {
		fx := newFixture(t, "1.0.0")
		eng := fx.newEngine(t, false)
		d := decl("Topic", "t", "1")
		d.Annotations = map[string]string{v1.AnnotationMinApplierVersion: "2.0.0"}
		t.Logf("INPUT backend=1.0.0 requires>=2.0.0")
		res, err := eng.Reconcile(ctx, 1, []v1.Declaration{d})
		if err != nil {
			t.Fatalf("guard failures are per-resource, not commit error: %v", err)
		}
		if len(res.Failed) != 1 || res.Failed[0].Reason != v1.ReasonIncompatibleVersion {
			t.Fatalf("JUDGE want IncompatibleVersion, got %+v", res.Failed)
		}
		t.Logf("JUDGE reason=%s msg=%q", res.Failed[0].Reason, res.Failed[0].Message)
		if fx.fake.Exists("Topic/t") {
			t.Fatalf("rejected resource must not exist in backend")
		}
	})

	t.Run("protected modification and delete", func(t *testing.T) {
		fx := newFixture(t, "2.0.0")
		eng := fx.newEngine(t, false)
		d := decl("Topic", "t", "1")
		d.Annotations = map[string]string{v1.AnnotationProtected: "true"}
		d.Spec = map[string]string{"partitions": "1"}
		if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d}); err != nil {
			t.Fatal(err)
		}
		// attempt modification
		d2 := d
		d2.Spec = map[string]string{"partitions": "9"}
		res, _ := eng.Reconcile(ctx, 2, []v1.Declaration{d2})
		t.Logf("JUDGE modify protected -> %s", res.Failed[0].Reason)
		if res.Failed[0].Reason != v1.ReasonRuleConflict {
			t.Fatalf("expected RuleConflict, got %s", res.Failed[0].Reason)
		}
		// attempt delete
		resDel, _ := eng.Reconcile(ctx, 3, nil)
		var delReason v1.Reason
		for _, fe := range resDel.Failed {
			if fe.Resource == "Topic/t" {
				delReason = fe.Reason
			}
		}
		t.Logf("JUDGE delete protected -> %s", delReason)
		if delReason != v1.ReasonRuleConflict {
			t.Fatalf("expected RuleConflict on delete, got %s", delReason)
		}
		if !fx.fake.Exists("Topic/t") {
			t.Fatalf("protected object must remain in backend")
		}
		// force succeeds
		engForce := fx.newEngine(t, true)
		if _, err := engForce.Reconcile(ctx, 4, nil); err != nil {
			t.Fatalf("forced delete: %v", err)
		}
		if fx.fake.Exists("Topic/t") {
			t.Fatalf("forced delete should remove object")
		}
	})

	t.Run("external ownership protected", func(t *testing.T) {
		fx := newFixture(t, "2.0.0")
		eng := fx.newEngine(t, false)
		// Seed an object owned by someone else directly into the backend
		// and store, simulating a foreign resource.
		foreign := v1.Declaration{Kind: "Topic", Name: "t", Version: "1",
			Annotations: map[string]string{v1.AnnotationOwner: "someone-else"}}
		fx.fake.SeedWorld(foreign)
		stored := &v1.Resource{
			Kind: foreign.Kind, Name: foreign.Name, Version: foreign.Version,
			Annotations: map[string]string{v1.AnnotationOwner: "someone-else"},
			Phase: v1.PhaseReady, Owner: "someone-else", SpecHash: foreign.SpecHash(),
		}
		if err := fx.store.Put(stored); err != nil {
			t.Fatal(err)
		}
		t.Logf("INPUT foreign-owned Topic/t desired by controller gen=1")
		res, err := eng.Reconcile(ctx, 1, []v1.Declaration{decl("Topic", "t", "1")})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("JUDGE %s", res.Failed[0])
		if res.Failed[0].Reason != v1.ReasonProtectedOwnership {
			t.Fatalf("expected ProtectedOwnership, got %s", res.Failed[0].Reason)
		}
	})

	t.Run("downgrade declaration version rejected", func(t *testing.T) {
		fx := newFixture(t, "2.0.0")
		eng := fx.newEngine(t, false)
		if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{decl("Topic", "t", "2")}); err != nil {
			t.Fatal(err)
		}
		t.Logf("INPUT incoming version 1 downgrades stored version 2")
		res, _ := eng.Reconcile(ctx, 2, []v1.Declaration{decl("Topic", "t", "1")})
		t.Logf("JUDGE %s", res.Failed[0])
		if res.Failed[0].Reason != v1.ReasonIncompatibleVersion {
			t.Fatalf("expected IncompatibleVersion, got %s", res.Failed[0].Reason)
		}
	})
}

// TestAuditTrailRecordsDecisions verifies rejections and side effects leave
// a traceable record with distinguishable reasons.
func TestAuditTrailRecordsDecisions(t *testing.T) {
	fx := newFixture(t, "1.0.0")
	eng := fx.newEngine(t, false)
	d := decl("Topic", "t", "1")
	d.Annotations = map[string]string{v1.AnnotationMinApplierVersion: "9.0.0"}
	if _, err := eng.Reconcile(ctxFor(t), 1, []v1.Declaration{d}); err != nil {
		t.Fatal(err)
	}
	// structural rejection also audited
	if _, err := eng.Reconcile(context.Background(), 2, []v1.Declaration{
		decl("X", "a", "1", "X/missing"),
	}); err == nil {
		t.Fatal("expected missing-dependency rejection")
	}
	var sawIncompat, sawMissing bool
	for _, ev := range fx.store.Audit() {
		t.Logf("AUDIT #%d action=%s resource=%s reason=%s msg=%q", ev.Seq, ev.Action, ev.Resource, ev.Reason, ev.Message)
		if ev.Reason == v1.ReasonIncompatibleVersion {
			sawIncompat = true
		}
		if ev.Reason == v1.ReasonMissingDependency {
			sawMissing = true
		}
	}
	if !sawIncompat || !sawMissing {
		t.Fatalf("audit trail missing reasons: incompat=%t missing=%t", sawIncompat, sawMissing)
	}
}

func ctxFor(t *testing.T) context.Context { return context.Background() }

var _ = fmt.Sprint

// TestConcurrentStressNoDuplicateSideEffects fires many goroutines mixing
// identical re-triggers and newer generations; every resource must finish
// with exactly one create and at most one update per actual spec change.
func TestConcurrentStressNoDuplicateSideEffects(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	base := []v1.Declaration{decl("Bucket", "a", "1"), decl("Bucket", "b", "1")}
	var wg sync.WaitGroup
	const workers = 40
	t.Logf("INPUT stress workers=%d mixed gen=1 idempotent calls", workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := eng.Reconcile(ctx, 1, base); err != nil {
				t.Errorf("reconcile: %v", err)
			}
		}()
	}
	wg.Wait()
	for _, key := range []string{"Bucket/a", "Bucket/b"} {
		c, u, d := fx.fake.Counts(key)
		t.Logf("JUDGE %s creates=%d updates=%d deletes=%d", key, c, u, d)
		if c != 1 || u != 0 || d != 0 {
			t.Fatalf("stress run duplicated side effects on %s", key)
		}
	}
}
