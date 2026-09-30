package engine

import (
	"context"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

// TestPartialFailureRetryConverges: a dependency fails N times then heals.
// Repeated reconciliation retries the failed resource, never re-applies
// healthy resources, and the final state converges exactly once.
func TestPartialFailureRetryConverges(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	// The dependent fails its first two create attempts, then succeeds.
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Topic/events", Action: "create", Times: 2},
	}
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Bucket", "archive", "1"),
		decl("Topic", "events", "1", "Bucket/archive"),
	}

	for attempt := 1; attempt <= 2; attempt++ {
		t.Logf("INPUT attempt=%d gen=1 (injected failure on Topic/events)", attempt)
		res, err := eng.Reconcile(ctx, 1, desired)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if res.Succeeded() {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt)
		}
		t.Logf("JUDGE attempt=%d applied=%v noop=%v failedReason=%s",
			attempt, res.Applied, res.Noop, res.Failed[0].Reason)
	}

	// Third attempt: backend failure rule is exhausted -> convergence.
	res, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT attempt=3 gen=1 after backend healed")
	t.Logf("JUDGE applied=%v noop=%v failed=%d succeeded=%t", res.Applied, res.Noop, len(res.Failed), res.Succeeded())
	if !res.Succeeded() {
		t.Fatalf("expected convergence after healing, failures=%+v", res.Failed)
	}
	if len(res.Applied) != 1 || res.Applied[0] != "Topic/events" {
		t.Fatalf("only the failed resource should converge late, got %v", res.Applied)
	}
	if len(res.Noop) != 1 || res.Noop[0] != "Bucket/archive" {
		t.Fatalf("healthy dependency must stay no-op across retries, got %v", res.Noop)
	}

	// Idempotence assertion on side effects.
	for key, want := range map[string][3]int{
		"Bucket/archive": {1, 0, 0},
		"Topic/events":   {1, 0, 0},
	} {
		c, u, d := fx.fake.Counts(key)
		t.Logf("JUDGE side effects %s c=%d u=%d d=%d want=%v", key, c, u, d, want)
		if [3]int{c, u, d} != want {
			t.Fatalf("%s duplicate side effects", key)
		}
	}

	// A fourth run must be a pure no-op even though one resource has a
	// prior Failed history.
	res4, _ := eng.Reconcile(ctx, 1, desired)
	if !res4.Succeeded() || len(res4.Applied) != 0 || len(res4.Noop) != 2 {
		t.Fatalf("post-recovery run must be all no-op, got applied=%v noop=%v failed=%+v",
			res4.Applied, res4.Noop, res4.Failed)
	}
}

// TestDependencyFailureBlocksDependents: if a dependency fails, its
// dependents must not be created (no orphan half-state) and must report a
// distinguishable skip; after healing, everything converges in order.
func TestDependencyFailureBlocksDependents(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Bucket/base", Action: "create", Times: 1},
	}
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Bucket", "base", "1"),
		decl("Topic", "above", "1", "Bucket/base"),
	}
	res, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE partial run applied=%v failed=%d", res.Applied, len(res.Failed))
	if len(res.Applied) != 0 || len(res.Failed) != 2 {
		t.Fatalf("expected dependency failure + dependent skip, got applied=%v failed=%+v", res.Applied, res.Failed)
	}
	for _, fe := range res.Failed {
		t.Logf("JUDGE failure resource=%s reason=%s msg=%q", fe.Resource, fe.Reason, fe.Message)
	}
	if fx.fake.Exists("Topic/above") {
		t.Fatalf("dependent must not exist when its dependency failed")
	}

	// Heal and re-run.
	res2, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Succeeded() {
		t.Fatalf("expected convergence after healing: %+v", res2.Failed)
	}
	t.Logf("JUDGE converged applied=%v", res2.Applied)
}

// TestRestartResumesFromDurableState simulates a process restart by opening
// a brand-new Engine/Store over the same file while the backend keeps its
// state. No duplicate side effects may occur and the desired generation is
// retained.
func TestRestartResumesFromDurableState(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Bucket", "a", "1"),
		decl("Topic", "b", "1", "Bucket/a"),
	}
	if _, err := eng.Reconcile(ctx, 1, desired); err != nil {
		t.Fatal(err)
	}
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("INPUT simulate process restart, reopening store file (backend untouched)")
	reloaded, err := storage.New(fx.dir + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reloaded.Close() })

	t.Logf("JUDGE reloaded generation=%d resources=%d", reloaded.Generation(), len(reloaded.List()))
	if reloaded.Generation() != 1 || len(reloaded.List()) != 2 {
		t.Fatalf("durable state not restored: gen=%d n=%d", reloaded.Generation(), len(reloaded.List()))
	}

	eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})
	res, err := eng2.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-restart applied=%v noop=%v (expect all noop)", res.Applied, res.Noop)
	if len(res.Applied) != 0 || len(res.Noop) != 2 {
		t.Fatalf("restarted controller must see satisfied state as no-op")
	}
	for _, key := range []string{"Bucket/a", "Topic/b"} {
		c, u, d := fx.fake.Counts(key)
		t.Logf("JUDGE side effects after restart %s c=%d u=%d d=%d", key, c, u, d)
		if c != 1 {
			t.Fatalf("%s created again after restart", key)
		}
	}
	// Stale generation must still be rejected after restart.
	if _, err := eng2.Reconcile(ctx, 0, desired); reasonOf(err) != v1.ReasonStaleGeneration {
		t.Fatalf("stale protection must survive restart, got %v", err)
	}
}

// TestRestartAfterPartialFailure: failure + restart + resume converges
// without recreating resources that already made it to the backend.
func TestRestartAfterPartialFailure(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Topic/b", Action: "create", Times: 1},
	}
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := []v1.Declaration{
		decl("Bucket", "a", "1"),
		decl("Topic", "b", "1", "Bucket/a"),
	}
	res, err := eng.Reconcile(ctx, 1, desired)
	if err != nil || res.Succeeded() {
		t.Fatalf("expected partial failure, err=%v res=%+v", err, res)
	}
	t.Logf("JUDGE before restart: Bucket/a exists=%t Topic/b exists=%t",
		fx.fake.Exists("Bucket/a"), fx.fake.Exists("Topic/b"))
	fx.store.Close()

	t.Logf("INPUT restart after partial failure; backend retains Bucket/a")
	reloaded, err := storage.New(fx.dir + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reloaded.Close() })
	eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})

	res2, err := eng2.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE resumed applied=%v noop=%v failed=%d", res2.Applied, res2.Noop, len(res2.Failed))
	if !res2.Succeeded() {
		t.Fatalf("expected convergence after restart: %+v", res2.Failed)
	}
	if len(res2.Applied) != 1 || res2.Applied[0] != "Topic/b" {
		t.Fatalf("resume must only create the missing resource, got %v", res2.Applied)
	}
	if c, _, _ := fx.fake.Counts("Bucket/a"); c != 1 {
		t.Fatalf("already-created dependency must not be recreated after restart, c=%d", c)
	}
}

// TestReconcileIdempotentAcrossDriftAndRestart is an end-to-end assertion
// that satisfied state is never rewritten, even interleaved with a restart.
func TestReconcileIdempotentAcrossDriftAndRestart(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()
	d := decl("Bucket", "a", "1")
	d.Spec = map[string]string{"region": "cn"}
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d}); err != nil {
		t.Fatal(err)
	}
	// restart
	fx.store.Close()
	reloaded, _ := storage.New(fx.dir + "/state.json")
	t.Cleanup(func() { reloaded.Close() })
	eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})
	res, _ := eng2.Reconcile(ctx, 1, []v1.Declaration{d})
	if len(res.Noop) != 1 {
		t.Fatalf("identical desired state across restart must noop: %+v", res)
	}
	if _, u, _ := fx.fake.Counts("Bucket/a"); u != 0 {
		t.Fatalf("no update side effect allowed when satisfied, got %d", u)
	}
}
