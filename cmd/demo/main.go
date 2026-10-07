// Command demo runs a small end-to-end reconciliation scenario against a
// temp file store and the fake backend, printing decisions and the audit
// trail.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/engine"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "rrc-demo-")
	must(err)
	defer os.RemoveAll(dir)

	st, err := storage.New(filepath.Join(dir, "state.json"))
	must(err)
	defer st.Close()

	ap := applier.NewFake("2.0.0")
	// Make the middle resource fail twice, then recover, to show retry
	// convergence without duplicate side effects on the healthy nodes.
	ap.FailRules = []applier.FailRule{
		{Resource: "Topic/events", Action: "create", Times: 2},
	}

	eng := engine.New(st, ap, engine.Options{Log: log.New(os.Stdout, "", log.Ltime)})

	desired := []v1.Declaration{
		{Kind: "Bucket", Name: "archive", Version: "1"},
		{Kind: "Topic", Name: "events", Version: "1",
			DependsOn:   []string{"Bucket/archive"},
			Spec:        map[string]string{"partitions": "4"},
			Annotations: map[string]string{v1.AnnotationMinApplierVersion: "1.5.0"},
		},
	}

	// Repeated triggers converge; the injected failure heals by the 3rd run.
	var res *v1.ReconcileResult
	for attempt := 1; attempt <= 3; attempt++ {
		fmt.Printf("\n===== reconcile attempt %d (generation 1) =====\n", attempt)
		res, err := eng.Reconcile(ctx, 1, desired)
		must(err)
		fmt.Printf("applied=%v noop=%v deleted=%v failed=%d succeeded=%t\n",
			res.Applied, res.Noop, res.Deleted, len(res.Failed), res.Succeeded())
	}

	c, u, d := ap.Counts("Topic/events")
	fmt.Printf("\nbackend side effects for Topic/events -> creates=%d updates=%d deletes=%d\n", c, u, d)

	// --- live-truth scenario: an operator mutates the backend out of band,
	// then the next reconcile must observe the drift and correct it once. ---
	fmt.Println("\n===== external drift scenario =====")
	drifted := desired[1]
	drifted.Spec = map[string]string{"partitions": "99"}
	ap.SeedWorld(drifted) // overwrite the backend object directly, no side effect
	fmt.Println("operator changed Topic/events partitions 4 -> 99 directly in the backend")
	res, err = eng.Reconcile(ctx, 1, desired)
	must(err)
	fmt.Printf("reconcile after external change -> applied=%v noop=%v failed=%d\n",
		res.Applied, res.Noop, len(res.Failed))
	res, err = eng.Reconcile(ctx, 1, desired)
	must(err)
	fmt.Printf("reconcile again (live state now matches) -> applied=%v noop=%v failed=%d\n",
		res.Applied, res.Noop, len(res.Failed))
	c, u, d = ap.Counts("Topic/events")
	fmt.Printf("backend side effects for Topic/events -> creates=%d updates=%d deletes=%d\n", c, u, d)

	fmt.Println("\n===== audit trail (last entries) =====")
	audit := st.Audit()
	start := len(audit) - 16
	if start < 0 {
		start = 0
	}
	for _, ev := range audit[start:] {
		fmt.Printf("#%d gen=%d %-10s %-18s reason=%s %s\n", ev.Seq, ev.Generation, ev.Action, ev.Resource, ev.Reason, ev.Message)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
