package engine

import (
	"context"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

// TestExternalModificationHeals: an out-of-band backend write must be
// detected on the next reconcile and reverted to the declared state with
// exactly one update; a follow-up run must be a pure no-op.
func TestExternalModificationHeals(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	d := decl("Topic", "t", "1")
	d.Spec = map[string]string{"partitions": "8"}
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d}); err != nil {
		t.Fatal(err)
	}

	// Operator edits the object directly on the backend.
	drifted := decl("Topic", "t", "1")
	drifted.Spec = map[string]string{"partitions": "1"}
	fx.fake.SeedWorld(drifted)
	t.Logf("INPUT external drift: backend partitions 8 -> 1, local record untouched")

	res, err := eng.Reconcile(ctx, 1, []v1.Declaration{d})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE heal round applied=%v noop=%v failed=%d", res.Applied, res.Noop, len(res.Failed))
	if len(res.Applied) != 1 || res.Applied[0] != "Topic/t" {
		t.Fatalf("drift must be corrected by exactly one update, got applied=%v", res.Applied)
	}
	got, _ := fx.fake.GetDeclaration("Topic/t")
	if got.Spec["partitions"] != "8" {
		t.Fatalf("backend not reverted to declared state: %v", got.Spec)
	}

	// Re-running after the heal must not write again.
	res2, err := eng.Reconcile(ctx, 1, []v1.Declaration{d})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-heal round applied=%v noop=%v", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 1 {
		t.Fatalf("post-heal run must be a pure no-op, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}
	c, u, del := fx.fake.Counts("Topic/t")
	t.Logf("JUDGE side effects c=%d u=%d d=%d (want 1/1/0)", c, u, del)
	if c != 1 || u != 1 || del != 0 {
		t.Fatalf("unexpected side effects c=%d u=%d d=%d", c, u, del)
	}
}

// TestExternalDeletionRecreates: an out-of-band backend delete must be
// detected even though the local record says Ready; the object is
// recreated exactly once and later runs stay no-op.
func TestExternalDeletionRecreates(t *testing.T) {
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

	// Someone deletes the dependent object directly on the backend.
	fx.fake.RemoveWorld("Topic/b")
	t.Logf("INPUT external delete of Topic/b; local record still says Ready")

	res, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE recreate round applied=%v noop=%v failed=%d", res.Applied, res.Noop, len(res.Failed))
	if !res.Succeeded() {
		t.Fatalf("recreate round must succeed: %+v", res.Failed)
	}
	if len(res.Applied) != 1 || res.Applied[0] != "Topic/b" {
		t.Fatalf("only the externally deleted resource must be recreated, got %v", res.Applied)
	}
	if len(res.Noop) != 1 || res.Noop[0] != "Bucket/a" {
		t.Fatalf("untouched dependency must stay no-op, got %v", res.Noop)
	}
	if !fx.fake.Exists("Topic/b") {
		t.Fatalf("recreated object missing from backend")
	}

	res2, err := eng.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-recreate round applied=%v noop=%v", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 2 {
		t.Fatalf("post-recreate run must be all no-op, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}
	c, u, del := fx.fake.Counts("Topic/b")
	t.Logf("JUDGE side effects Topic/b c=%d u=%d d=%d (want 2/0/0: initial create + recreate)", c, u, del)
	if c != 2 || u != 0 || del != 0 {
		t.Fatalf("unexpected side effects c=%d u=%d d=%d", c, u, del)
	}
	if c2, _, _ := fx.fake.Counts("Bucket/a"); c2 != 1 {
		t.Fatalf("untouched dependency must have exactly one create, got %d", c2)
	}
}

// TestUpdateFailureRetriesUntilEffective: a backend that keeps failing
// updates must be reported as failed every round (never as converged) and
// retried on every subsequent round — including rounds where other
// resources already succeed — until one update truly lands. That landing
// is the single real write; afterwards the resource is a stable no-op.
func TestUpdateFailureRetriesUntilEffective(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	// gen=1: both resources converge.
	d1 := decl("Topic", "t", "1")
	d1.Spec = map[string]string{"partitions": "1"}
	ok := decl("Bucket", "ok", "1")
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d1, ok}); err != nil {
		t.Fatal(err)
	}

	// gen=2: desired spec changes, but the backend fails the next two
	// update calls before healing.
	d2 := decl("Topic", "t", "1")
	d2.Spec = map[string]string{"partitions": "8"}
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Topic/t", Action: "update", Times: 2},
	}
	desired := []v1.Declaration{d2, ok}

	for round := 1; round <= 2; round++ {
		t.Logf("INPUT round=%d gen=2 desired partitions=8 (backend update failure #%d injected)", round, round)
		res, err := eng.Reconcile(ctx, 2, desired)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("JUDGE round=%d succeeded=%t applied=%v noop=%v failed=%v", round, res.Succeeded(), res.Applied, res.Noop, res.Failed)
		if res.Succeeded() {
			t.Fatalf("round %d: failed update must be reported as failure, not success", round)
		}
		if len(res.Failed) != 1 || res.Failed[0].Resource != "Topic/t" || res.Failed[0].Reason != v1.ReasonApplyFailed {
			t.Fatalf("round %d: want one ApplyFailed on Topic/t, got %+v", round, res.Failed)
		}
		if len(res.Noop) != 1 || res.Noop[0] != "Bucket/ok" {
			t.Fatalf("round %d: healthy resource must stay no-op, got %v", round, res.Noop)
		}
		// The local record must not claim the unapplied spec.
		stored, _ := fx.store.Get("Topic/t")
		if stored.Phase != v1.PhaseFailed {
			t.Fatalf("round %d: stored phase must be Failed while update has not landed, got %s", round, stored.Phase)
		}
		if got, _ := fx.fake.GetDeclaration("Topic/t"); got.Spec["partitions"] != "1" {
			t.Fatalf("round %d: backend must still hold the old spec, got %v", round, got.Spec)
		}
	}

	// Round 3: backend healed -> the pending update finally lands.
	t.Logf("INPUT round=3 gen=2 backend healed; Topic/t still not at desired state")
	res3, err := eng.Reconcile(ctx, 2, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE round=3 succeeded=%t applied=%v noop=%v", res3.Succeeded(), res3.Applied, res3.Noop)
	if !res3.Succeeded() || len(res3.Applied) != 1 || res3.Applied[0] != "Topic/t" {
		t.Fatalf("round 3 must apply the pending update exactly once: %+v", res3)
	}
	if got, _ := fx.fake.GetDeclaration("Topic/t"); got.Spec["partitions"] != "8" {
		t.Fatalf("backend must now hold the desired spec, got %v", got.Spec)
	}

	// Round 4: fully converged -> pure no-op, no further writes.
	res4, err := eng.Reconcile(ctx, 2, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE round=4 applied=%v noop=%v", res4.Applied, res4.Noop)
	if len(res4.Applied) != 0 || len(res4.Noop) != 2 {
		t.Fatalf("converged set must be all no-op, got applied=%v noop=%v", res4.Applied, res4.Noop)
	}

	c, u, del := fx.fake.Counts("Topic/t")
	t.Logf("JUDGE side effects Topic/t c=%d u=%d d=%d (want 1/1/0: one create, one effective update)", c, u, del)
	if c != 1 || u != 1 || del != 0 {
		t.Fatalf("the effective update must happen exactly once, got c=%d u=%d d=%d", c, u, del)
	}

	// Every failed round left a traceable audit record.
	var failedAudits int
	for _, ev := range fx.store.Audit() {
		if ev.Resource == "Topic/t" && ev.Reason == v1.ReasonApplyFailed {
			failedAudits++
			t.Logf("AUDIT #%d gen=%d action=%s reason=%s msg=%q", ev.Seq, ev.Generation, ev.Action, ev.Reason, ev.Message)
		}
	}
	if failedAudits != 2 {
		t.Fatalf("each failed round must be auditable, got %d ApplyFailed records", failedAudits)
	}
}

// TestRestartReconcilesFromLiveBackendState: after a process restart that
// reuses the durable state file, every resource is re-judged against the
// backend's live state — an object modified out-of-band is repaired and an
// object deleted out-of-band is recreated, even though the pre-restart
// records claimed everything was Ready. Untouched resources stay no-op.
func TestRestartReconcilesFromLiveBackendState(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	modified := decl("Topic", "drifted", "1")
	modified.Spec = map[string]string{"partitions": "8"}
	deleted := decl("Bucket", "gone", "1")
	untouched := decl("Bucket", "stable", "1")
	desired := []v1.Declaration{modified, deleted, untouched}
	if _, err := eng.Reconcile(ctx, 1, desired); err != nil {
		t.Fatal(err)
	}
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}

	// While the controller is down, an operator edits one object and
	// deletes another directly on the backend.
	drifted := decl("Topic", "drifted", "1")
	drifted.Spec = map[string]string{"partitions": "1"}
	fx.fake.SeedWorld(drifted)
	fx.fake.RemoveWorld("Bucket/gone")
	t.Logf("INPUT restart; pre-restart records all Ready, but backend drifted: Topic/drifted modified, Bucket/gone deleted")

	reloaded, err := storage.New(fx.dir + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reloaded.Close() })
	eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})

	res, err := eng2.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-restart applied=%v noop=%v failed=%d", res.Applied, res.Noop, len(res.Failed))
	if !res.Succeeded() {
		t.Fatalf("restart reconcile must succeed: %+v", res.Failed)
	}
	if len(res.Applied) != 2 {
		t.Fatalf("exactly the two drifted resources must be repaired, got %v", res.Applied)
	}
	if len(res.Noop) != 1 || res.Noop[0] != "Bucket/stable" {
		t.Fatalf("untouched resource must stay no-op, got %v", res.Noop)
	}
	if got, _ := fx.fake.GetDeclaration("Topic/drifted"); got.Spec["partitions"] != "8" {
		t.Fatalf("external modification not reverted: %v", got.Spec)
	}
	if !fx.fake.Exists("Bucket/gone") {
		t.Fatalf("externally deleted object not recreated")
	}

	// Side-effect ledger: the drifted object got exactly one repair update,
	// the deleted one exactly one recreate, the stable one nothing extra.
	if c, u, d := fx.fake.Counts("Topic/drifted"); c != 1 || u != 1 || d != 0 {
		t.Fatalf("Topic/drifted want c=1 u=1 d=0, got c=%d u=%d d=%d", c, u, d)
	}
	if c, u, d := fx.fake.Counts("Bucket/gone"); c != 2 || u != 0 || d != 0 {
		t.Fatalf("Bucket/gone want c=2 u=0 d=0 (create + recreate), got c=%d u=%d d=%d", c, u, d)
	}
	if c, u, d := fx.fake.Counts("Bucket/stable"); c != 1 || u != 0 || d != 0 {
		t.Fatalf("Bucket/stable want c=1 u=0 d=0, got c=%d u=%d d=%d", c, u, d)
	}

	// A further round after the repairs must be a pure no-op.
	res2, err := eng2.Reconcile(ctx, 1, desired)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-repair round applied=%v noop=%v", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 3 {
		t.Fatalf("post-repair run must be all no-op, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}
}

// TestRestartWithPendingFailureKeepsRetrying: an update that never landed
// (backend kept failing) must not be treated as satisfied after a restart,
// even though the failed record carries the desired hash. The restarted
// controller re-judges from the live backend, keeps retrying across rounds
// and persists the failure each time, until one update truly takes effect.
func TestRestartWithPendingFailureKeepsRetrying(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	d1 := decl("Topic", "t", "1")
	d1.Spec = map[string]string{"partitions": "1"}
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{d1}); err != nil {
		t.Fatal(err)
	}

	// gen=2 wants a new spec, but the backend update keeps failing.
	d2 := decl("Topic", "t", "1")
	d2.Spec = map[string]string{"partitions": "8"}
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Topic/t", Action: "update", Times: 2},
	}
	res, err := eng.Reconcile(ctx, 2, []v1.Declaration{d2})
	if err != nil || res.Succeeded() {
		t.Fatalf("expected reported failure, err=%v res=%+v", err, res)
	}
	t.Logf("JUDGE pre-restart failed=%v storedPhase=Failed (desired hash recorded but never effective)", res.Failed)
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: the durable record says Failed-with-desired-hash; the
	// backend still holds the old spec. The restarted engine must not
	// trust the record — round 1 after restart retries and still fails
	// (second injected failure), round 2 lands the update for real.
	reloaded, err := storage.New(fx.dir + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reloaded.Close() })
	eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})

	t.Logf("INPUT restart with pending ineffective update; backend still failing once more")
	res1, err := eng2.Reconcile(ctx, 2, []v1.Declaration{d2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-restart round=1 succeeded=%t failed=%v", res1.Succeeded(), res1.Failed)
	if res1.Succeeded() || len(res1.Failed) != 1 || res1.Failed[0].Reason != v1.ReasonApplyFailed {
		t.Fatalf("post-restart round must retry and report the failure, got %+v", res1)
	}
	if got, _ := fx.fake.GetDeclaration("Topic/t"); got.Spec["partitions"] != "1" {
		t.Fatalf("backend must still hold old spec, got %v", got.Spec)
	}

	t.Logf("INPUT post-restart round=2 backend healed")
	res2, err := eng2.Reconcile(ctx, 2, []v1.Declaration{d2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-restart round=2 applied=%v noop=%v", res2.Applied, res2.Noop)
	if !res2.Succeeded() || len(res2.Applied) != 1 || res2.Applied[0] != "Topic/t" {
		t.Fatalf("pending update must finally land exactly once, got %+v", res2)
	}
	if got, _ := fx.fake.GetDeclaration("Topic/t"); got.Spec["partitions"] != "8" {
		t.Fatalf("backend must now hold desired spec, got %v", got.Spec)
	}

	res3, err := eng2.Reconcile(ctx, 2, []v1.Declaration{d2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-restart round=3 applied=%v noop=%v", res3.Applied, res3.Noop)
	if len(res3.Applied) != 0 || len(res3.Noop) != 1 {
		t.Fatalf("after the update lands, further rounds must be no-op, got %+v", res3)
	}
	if c, u, d := fx.fake.Counts("Topic/t"); c != 1 || u != 1 || d != 0 {
		t.Fatalf("want exactly one effective update across restarts, got c=%d u=%d d=%d", c, u, d)
	}
}
