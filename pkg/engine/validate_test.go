package engine

import (
	"errors"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

func TestValidateDeclarations(t *testing.T) {
	cases := []struct {
		name   string
		decls  []v1.Declaration
		reason v1.Reason
	}{
		{
			name: "valid chain",
			decls: []v1.Declaration{
				{Kind: "Bucket", Name: "a"},
				{Kind: "Topic", Name: "b", DependsOn: []string{"Bucket/a"}},
			},
		},
		{
			name:   "blank name is illegal",
			decls:  []v1.Declaration{{Kind: "Bucket", Name: ""}},
			reason: v1.ReasonInvalidDeclaration,
		},
		{
			name: "duplicate key is illegal",
			decls: []v1.Declaration{
				{Kind: "Bucket", Name: "a"},
				{Kind: "Bucket", Name: "a"},
			},
			reason: v1.ReasonInvalidDeclaration,
		},
		{
			name: "self dependency is illegal",
			decls: []v1.Declaration{
				{Kind: "Bucket", Name: "a", DependsOn: []string{"Bucket/a"}},
			},
			reason: v1.ReasonInvalidDeclaration,
		},
		{
			name: "malformed protected annotation is illegal",
			decls: []v1.Declaration{
				{Kind: "Bucket", Name: "a", Annotations: map[string]string{v1.AnnotationProtected: "yes"}},
			},
			reason: v1.ReasonInvalidDeclaration,
		},
		{
			name: "missing dependency",
			decls: []v1.Declaration{
				{Kind: "Topic", Name: "b", DependsOn: []string{"Bucket/ghost"}},
			},
			reason: v1.ReasonMissingDependency,
		},
		{
			name: "direct cycle",
			decls: []v1.Declaration{
				{Kind: "X", Name: "a", DependsOn: []string{"X/b"}},
				{Kind: "X", Name: "b", DependsOn: []string{"X/a"}},
			},
			reason: v1.ReasonCycleDetected,
		},
		{
			name: "three node cycle",
			decls: []v1.Declaration{
				{Kind: "X", Name: "a", DependsOn: []string{"X/c"}},
				{Kind: "X", Name: "b", DependsOn: []string{"X/a"}},
				{Kind: "X", Name: "c", DependsOn: []string{"X/b"}},
			},
			reason: v1.ReasonCycleDetected,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("INPUT declarations=%d", len(tc.decls))
			for _, d := range tc.decls {
				t.Logf("  INPUT %s dependsOn=%v annotations=%v", d.Key(), d.DependsOn, d.Annotations)
			}
			err := validateDeclarations(tc.decls)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				t.Logf("JUDGE accepted (no structural error)")
				return
			}
			if err == nil {
				t.Fatalf("expected rejection %s, got nil", tc.reason)
			}
			var re *v1.ReconcileError
			if !errors.As(err, &re) {
				t.Fatalf("expected *ReconcileError, got %T: %v", err, err)
			}
			t.Logf("JUDGE rejected reason=%s message=%q", re.Reason, re.Message)
			if re.Reason != tc.reason {
				t.Fatalf("expected reason %s, got %s", tc.reason, re.Reason)
			}
		})
	}
}

func TestTopoWavesOrdering(t *testing.T) {
	decls := []v1.Declaration{
		{Kind: "X", Name: "root"},
		{Kind: "X", Name: "mid", DependsOn: []string{"X/root"}},
		{Kind: "X", Name: "leaf", DependsOn: []string{"X/mid"}},
		{Kind: "X", Name: "sibling", DependsOn: []string{"X/root"}},
	}
	t.Logf("INPUT edges root->[mid,sibling] mid->leaf")
	waves, err := topoWaves(decls)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, w := range waves {
		t.Logf("JUDGE wave[%d]=%v", i, w)
	}
	if len(waves) != 3 {
		t.Fatalf("expected 3 waves, got %d: %v", len(waves), waves)
	}
	if len(waves[0]) != 1 || waves[0][0] != "X/root" {
		t.Fatalf("wave0 must be root only, got %v", waves[0])
	}
	if len(waves[1]) != 2 {
		t.Fatalf("wave1 must hold mid+sibling (parallel), got %v", waves[1])
	}
	if len(waves[2]) != 1 || waves[2][0] != "X/leaf" {
		t.Fatalf("wave2 must be leaf, got %v", waves[2])
	}
}

func TestCompareVersion(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.10.0", "1.9.0", 1},
		{"2.0.0", "2.0.1", -1},
		{"1.5", "1.5.0", 0},
	}
	for _, c := range cases {
		got := compareVersion(c.a, c.b)
		t.Logf("JUDGE compare(%q,%q)=%d want=%d", c.a, c.b, got, c.want)
		if got != c.want {
			t.Fatalf("compareVersion(%q,%q)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}
