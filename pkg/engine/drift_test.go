package engine

import (
	"context"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

// specDecl builds a simple declaration with a spec payload.
func specDecl(kind, name, version, key, val string) v1.Declaration {
	d := decl(kind, name, version)
	d.Spec = map[string]string{key: val}
	return d
}

// assertCounts checks the exact backend side-effect counts for a resource.
func assertCounts(t *testing.T, fx *fixture, key string, want [3]int) {
	t.Helper()
	c, u, d := fx.fake.Counts(key)
	got := [3]int{c, u, d}
	t.Logf("JUDGE side effects %s creates=%d updates=%d deletes=%d want=%v", key, c, u, d, want)
	if got != want {
		t.Fatalf("%s side effects = %v, want %v", key, got, want)
	}
}

// TestExternalDriftSelfHeals: an operator mutates the backend object so it
// no longer matches the declaration. The next reconcile must observe the
// live difference, perform exactly one corrective update, and a follow-up
// reconcile must be a pure no-op with no further writes.
func TestExternalDriftSelfHeals(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := specDecl("Bucket", "b", "1", "region", "cn")
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired}); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, fx, "Bucket/b", [3]int{1, 0, 0})

	// Someone changes the backend directly, out of band.
	mutated := desired
	mutated.Spec = map[string]string{"region": "us"}
	fx.fake.SeedWorld(mutated)
	t.Logf("INPUT external drift on Bucket/b region cn -> us (store still says Ready)")

	res, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE drift run applied=%v noop=%v failed=%+v", res.Applied, res.Noop, res.Failed)
	if !res.Succeeded() || len(res.Applied) != 1 || res.Applied[0] != "Bucket/b" {
		t.Fatalf("drift must yield exactly one corrective update, got applied=%v failed=%+v", res.Applied, res.Failed)
	}
	assertCounts(t, fx, "Bucket/b", [3]int{1, 1, 0})
	if live, ok := fx.fake.GetDeclaration("Bucket/b"); !ok || live.Spec["region"] != "cn" {
		t.Fatalf("backend not restored to declared state: %+v", live.Spec)
	}
	stored, _ := fx.store.Get("Bucket/b")
	if stored.Phase != v1.PhaseReady {
		t.Fatalf("stored phase must be Ready after repair, got %s", stored.Phase)
	}

	// Follow-up run: live state now matches -> no more writes.
	res2, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE follow-up applied=%v noop=%v (expect noop)", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 1 {
		t.Fatalf("repaired resource must not be rewritten, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}
	assertCounts(t, fx, "Bucket/b", [3]int{1, 1, 0})
}

// TestExternalDeleteRecreated: an operator deletes the backend object while
// the store still records Ready. The next reconcile observes absence,
// recreates exactly once, and a follow-up is a no-op.
func TestExternalDeleteRecreated(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := specDecl("Topic", "t", "1", "partitions", "4")
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired}); err != nil {
		t.Fatal(err)
	}
	stored, _ := fx.store.Get("Topic/t")
	if stored.Phase != v1.PhaseReady {
		t.Fatalf("precondition: stored Ready, got %s", stored.Phase)
	}

	fx.fake.RemoveWorld("Topic/t")
	t.Logf("INPUT external deletion of Topic/t (store still says Ready, hash matches desired)")

	res, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE post-delete run applied=%v noop=%v failed=%+v", res.Applied, res.Noop, res.Failed)
	if !res.Succeeded() || len(res.Applied) != 1 || res.Applied[0] != "Topic/t" {
		t.Fatalf("external deletion must be repaired by one recreate, got applied=%v failed=%+v", res.Applied, res.Failed)
	}
	assertCounts(t, fx, "Topic/t", [3]int{2, 0, 0}) // initial create + recreate

	// Follow-up run must be a no-op.
	res2, _ := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	t.Logf("JUDGE follow-up applied=%v noop=%v (expect noop)", res2.Applied, res2.Noop)
	if len(res2.Applied) != 0 || len(res2.Noop) != 1 {
		t.Fatalf("recreated resource must not be rewritten, got applied=%v noop=%v", res2.Applied, res2.Noop)
	}
	assertCounts(t, fx, "Topic/t", [3]int{2, 0, 0})
}

// TestUpdateFailuresRetriedUntilActuallyEffective: an update keeps failing
// for several rounds (other resources succeed in between). Every failing
// round must honestly report failure and leave a trace; the moment the
// backend accepts the write there is exactly one successful update, the
// stored record must not lie about satisfaction meanwhile, and subsequent
// rounds are no-ops.
func TestUpdateFailuresRetriedUntilActuallyEffective(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	v1d := specDecl("Bucket", "drifting", "1", "size", "1")
	by := specDecl("Bucket", "bystander", "1", "size", "1")
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{v1d, by}); err != nil {
		t.Fatal(err)
	}

	// Desired state evolves; the update on drifting fails twice then heals.
	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Bucket/drifting", Action: "update", Times: 2},
	}
	want := specDecl("Bucket", "drifting", "1", "size", "9")

	for attempt := 1; attempt <= 2; attempt++ {
		t.Logf("INPUT attempt=%d gen=2 update expected to fail backend-side", attempt)
		res, err := eng.Reconcile(ctx, 2, []v1.Declaration{want, by})
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if res.Succeeded() {
			t.Fatalf("attempt %d must honestly report failure while the write has not landed", attempt)
		}
		var fe *v1.ReconcileError
		for _, f := range res.Failed {
			if f.Resource == "Bucket/drifting" {
				fe = f
			}
		}
		if fe == nil || fe.Reason != v1.ReasonApplyFailed {
			t.Fatalf("attempt %d: want ApplyFailed on Bucket/drifting, got %+v", attempt, res.Failed)
		}
		t.Logf("JUDGE attempt=%d honest failure reason=%s msg=%q applied=%v noop=%v",
			attempt, fe.Reason, fe.Message, res.Applied, res.Noop)
		// Backend must still hold the OLD content: nothing took effect.
		if live, ok := fx.fake.GetDeclaration("Bucket/drifting"); !ok || live.Spec["size"] != "1" {
			t.Fatalf("backend must be unchanged after a failed update, got %+v", live.Spec)
		}
		stored, _ := fx.store.Get("Bucket/drifting")
		if stored.Phase != v1.PhaseFailed {
			t.Fatalf("attempt %d stored phase must be Failed, got %s", attempt, stored.Phase)
		}
		if stored.SpecHash == want.SpecHash() {
			t.Fatalf("attempt %d stored hash must not claim the un-reached desired state", attempt)
		}
		// The healthy bystander must never be rewritten.
		if len(res.Noop) != 1 || res.Noop[0] != "Bucket/bystander" {
			t.Fatalf("attempt %d bystander must stay noop, got applied=%v noop=%v", attempt, res.Applied, res.Noop)
		}
	}

	// Third attempt: failure rule exhausted, update lands exactly once.
	t.Logf("INPUT attempt=3 gen=2 backend healed; update expected to land")
	res, err := eng.Reconcile(ctx, 2, []v1.Declaration{want, by})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE healed applied=%v noop=%v failed=%d succeeded=%t", res.Applied, res.Noop, len(res.Failed), res.Succeeded())
	if !res.Succeeded() || len(res.Applied) != 1 || res.Applied[0] != "Bucket/drifting" {
		t.Fatalf("only the previously failing resource should converge now, got applied=%v failed=%+v", res.Applied, res.Failed)
	}
	if live, _ := fx.fake.GetDeclaration("Bucket/drifting"); live.Spec["size"] != "9" {
		t.Fatalf("backend must now hold desired content size=9, got %+v", live.Spec)
	}
	assertCounts(t, fx, "Bucket/drifting", [3]int{1, 1, 0})  // one create, one update
	assertCounts(t, fx, "Bucket/bystander", [3]int{1, 0, 0}) // untouched

	// Any further round must be a pure no-op.
	res4, _ := eng.Reconcile(ctx, 2, []v1.Declaration{want, by})
	if !res4.Succeeded() || len(res4.Applied) != 0 || len(res4.Noop) != 2 {
		t.Fatalf("post-recovery round must be all noop, got applied=%v noop=%v failed=%+v",
			res4.Applied, res4.Noop, res4.Failed)
	}
	assertCounts(t, fx, "Bucket/drifting", [3]int{1, 1, 0})
}

// TestRestartRejudgesByLiveState: after a restart reusing the same state
// file, every resource is re-judged from backend reality, not from the
// pre-restart phase. Covers (a) external drift present at restart and
// (b) an update that never landed before the restart.
func TestRestartRejudgesByLiveState(t *testing.T) {
	t.Run("drift carried across restart", func(t *testing.T) {
		fx := newFixture(t, "2.0.0")
		eng := fx.newEngine(t, false)
		ctx := context.Background()

		desired := specDecl("Bucket", "b", "1", "region", "cn")
		if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired}); err != nil {
			t.Fatal(err)
		}
		// Operator mutates the backend, then the process restarts.
		mutated := desired
		mutated.Spec = map[string]string{"region": "eu"}
		fx.fake.SeedWorld(mutated)
		fx.store.Close()

		t.Logf("INPUT restart with drifted Bucket/b on the backend (store says Ready)")
		reloaded, err := storage.New(fx.dir + "/state.json")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { reloaded.Close() })
		eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})

		res, err := eng2.Reconcile(ctx, 1, []v1.Declaration{desired})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("JUDGE post-restart applied=%v noop=%v (expect one update)", res.Applied, res.Noop)
		if !res.Succeeded() || len(res.Applied) != 1 || res.Applied[0] != "Bucket/b" {
			t.Fatalf("restart must re-detect live drift and repair, got applied=%v failed=%+v", res.Applied, res.Failed)
		}
		assertCounts(t, fx, "Bucket/b", [3]int{1, 1, 0})

		// One more post-restart round must be fully no-op.
		res2, _ := eng2.Reconcile(ctx, 1, []v1.Declaration{desired})
		if len(res2.Applied) != 0 || len(res2.Noop) != 1 {
			t.Fatalf("repaired state must be stable after restart, got %+v", res2)
		}
		assertCounts(t, fx, "Bucket/b", [3]int{1, 1, 0})
	})

	t.Run("ineffective write carried across restart", func(t *testing.T) {
		fx := newFixture(t, "2.0.0")
		eng := fx.newEngine(t, false)
		ctx := context.Background()

		old := specDecl("Topic", "t", "1", "partitions", "1")
		if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{old}); err != nil {
			t.Fatal(err)
		}
		// New desired state, but the update never lands before the process
		// is killed: backend keeps old content while the store carries a
		// Failed attempt at generation 2.
		fx.fake.FailRules = []applier.FailRule{
			{Resource: "Topic/t", Action: "update", Times: 1},
		}
		want := specDecl("Topic", "t", "1", "partitions", "8")
		res, err := eng.Reconcile(ctx, 2, []v1.Declaration{want})
		if err != nil || res.Succeeded() {
			t.Fatalf("precondition: expect honest failure, err=%v res=%+v", err, res)
		}
		fx.store.Close()

		t.Logf("INPUT restart with Topic/t still holding partitions=1 in the backend (store says Failed)")
		reloaded, err := storage.New(fx.dir + "/state.json")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { reloaded.Close() })
		eng2 := New(reloaded, fx.fake, Options{Log: testLog(t)})

		res2, err := eng2.Reconcile(ctx, 2, []v1.Declaration{want})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("JUDGE post-restart applied=%v noop=%v (expect one update)", res2.Applied, res2.Noop)
		if !res2.Succeeded() || len(res2.Applied) != 1 || res2.Applied[0] != "Topic/t" {
			t.Fatalf("restart must retry the ineffective write, got applied=%v failed=%+v", res2.Applied, res2.Failed)
		}
		assertCounts(t, fx, "Topic/t", [3]int{1, 1, 0})

		res3, _ := eng2.Reconcile(ctx, 2, []v1.Declaration{want})
		if len(res3.Applied) != 0 || len(res3.Noop) != 1 {
			t.Fatalf("post-restart convergence must then be stable, got %+v", res3)
		}
		assertCounts(t, fx, "Topic/t", [3]int{1, 1, 0})
	})
}

// TestBackendUnobservableFailsAndRetries: when live state cannot be read
// (backend outage), the round must honestly report BackendUnavailable
// without any write; once observation works again, normal convergence
// resumes. Local Ready bookkeeping must never mask the outage.
func TestBackendUnobservableFailsAndRetries(t *testing.T) {
	fx := newFixture(t, "2.0.0")
	eng := fx.newEngine(t, false)
	ctx := context.Background()

	desired := specDecl("Bucket", "b", "1", "region", "cn")
	if _, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired}); err != nil {
		t.Fatal(err)
	}

	fx.fake.FailRules = []applier.FailRule{
		{Resource: "Bucket/b", Action: "get", Times: 1},
	}
	t.Logf("INPUT backend observation outage on Bucket/b (store says Ready)")
	res, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	if err != nil {
		t.Fatal(err)
	}
	if res.Succeeded() || len(res.Failed) != 1 || res.Failed[0].Reason != v1.ReasonBackendUnavailable {
		t.Fatalf("unreadable backend must yield BackendUnavailable, got %+v", res.Failed)
	}
	t.Logf("JUDGE outage round honest failure=%s msg=%q applied=%v", res.Failed[0].Reason, res.Failed[0].Message, res.Applied)
	if len(res.Applied) != 0 {
		t.Fatalf("no write allowed while the backend cannot be observed, got %v", res.Applied)
	}
	// No create/update side effect during the outage.
	assertCounts(t, fx, "Bucket/b", [3]int{1, 0, 0})

	// Observation recovers: live content matches -> noop, still no write.
	res2, err := eng.Reconcile(ctx, 1, []v1.Declaration{desired})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JUDGE recovered applied=%v noop=%v (expect noop)", res2.Applied, res2.Noop)
	if !res2.Succeeded() || len(res2.Noop) != 1 {
		t.Fatalf("after observation recovery the matching object must be noop, got %+v", res2)
	}
	assertCounts(t, fx, "Bucket/b", [3]int{1, 0, 0})
}
