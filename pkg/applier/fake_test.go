package applier

import (
	"context"
	"errors"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

func TestFakeApplierDuplicateCreateRejected(t *testing.T) {
	f := NewFake("1.0.0")
	d := v1.Declaration{Kind: "Bucket", Name: "a"}
	if err := f.Create(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if err := f.Create(context.Background(), d); err == nil {
		t.Fatal("duplicate create must be rejected by the backend")
	}
	c, _, _ := f.Counts("Bucket/a")
	if c != 1 {
		t.Fatalf("failed duplicate create must not count, got %d", c)
	}
}

func TestFakeApplierFailRuleHeals(t *testing.T) {
	boom := errors.New("boom")
	f := NewFake("1.0.0")
	f.FailRules = []FailRule{{Resource: "Bucket/a", Action: "create", Times: 2, Err: boom}}
	d := v1.Declaration{Kind: "Bucket", Name: "a"}
	for i := 1; i <= 2; i++ {
		if err := f.Create(context.Background(), d); !errors.Is(err, boom) {
			t.Fatalf("attempt %d want boom, got %v", i, err)
		}
		t.Logf("JUDGE injected failure attempt=%d", i)
	}
	if err := f.Create(context.Background(), d); err != nil {
		t.Fatalf("third attempt must heal, got %v", err)
	}
	if c, _, _ := f.Counts("Bucket/a"); c != 1 {
		t.Fatalf("exactly one successful create expected, got %d", c)
	}
}
