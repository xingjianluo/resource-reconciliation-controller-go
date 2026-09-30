package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

func TestStoreRoundTripAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	r := &v1.Resource{
		Kind: "Bucket", Name: "a", Version: "1",
		Spec: map[string]string{"region": "cn"}, Phase: v1.PhaseReady,
		Generation: 7, Owner: v1.ControllerOwner,
	}
	t.Logf("INPUT put resource %s gen=7 and one audit event", r.Key())
	if err := st.Put(r); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendAudit(v1.AuditEvent{Generation: 7, Action: "reconciled", Resource: r.Key()}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetGeneration(7); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// reload from disk (simulates restart)
	st2, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	t.Logf("JUDGE reloaded gen=%d resources=%d audit=%d", st2.Generation(), len(st2.List()), len(st2.Audit()))
	if st2.Generation() != 7 {
		t.Fatalf("generation lost: %d", st2.Generation())
	}
	got, ok := st2.Get("Bucket/a")
	if !ok || got.Spec["region"] != "cn" || got.Phase != v1.PhaseReady {
		t.Fatalf("resource not restored correctly: %+v ok=%t", got, ok)
	}
	if len(st2.Audit()) != 1 || st2.Audit()[0].Seq != 1 {
		t.Fatalf("audit trail not restored: %+v", st2.Audit())
	}
}

func TestStoreDeepCopyIsolation(t *testing.T) {
	st, _ := New(filepath.Join(t.TempDir(), "s.json"))
	r := &v1.Resource{Kind: "Bucket", Name: "a", Spec: map[string]string{"k": "v"},
		DependsOn: []string{"x"}, Conditions: []v1.Condition{{Type: "Reconciled", Time: time.Now()}}}
	if err := st.Put(r); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get("Bucket/a")
	got.Spec["k"] = "mutated"
	got.DependsOn[0] = "mutated"
	again, _ := st.Get("Bucket/a")
	if again.Spec["k"] != "v" || again.DependsOn[0] != "x" {
		t.Fatal("Get must return defensive deep copies")
	}
}

func TestStoreGenerationMonotonic(t *testing.T) {
	st, _ := New(filepath.Join(t.TempDir(), "s.json"))
	if err := st.SetGeneration(5); err != nil {
		t.Fatal(err)
	}
	if err := st.SetGeneration(3); err == nil {
		t.Fatal("backwards generation move must be rejected")
	}
	t.Logf("JUDGE backwards generation rejected; current=%d", st.Generation())
}
