package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// specHash computes a stable digest of the meaningful desired state:
// schema version, ordered dependency list, spec and annotations. Two
// declarations with equal meaning produce the same hash, which lets the
// engine skip no-op updates.
func specHash(d Declaration) string {
	deps := make([]string, len(d.DependsOn))
	copy(deps, d.DependsOn)
	sort.Strings(deps)

	canonical := struct {
		Kind        string            `json:"kind"`
		Version     string            `json:"version"`
		DependsOn   []string          `json:"dependsOn"`
		Spec        map[string]string `json:"spec"`
		Annotations map[string]string `json:"annotations"`
	}{
		Kind:        d.Kind,
		Version:     d.Version,
		DependsOn: deps,
		Spec:        d.Spec,
		Annotations: d.Annotations,
	}
	// encoding/json emits map keys in sorted order, giving stable output.
	b, err := json.Marshal(canonical)
	if err != nil {
		// Declarations only contain strings/slices/maps; marshal cannot fail.
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

func reconcileErrorString(e *ReconcileError) string {
	if e == nil {
		return "<nil>"
	}
	s := string(e.Reason)
	if e.Resource != "" {
		s += " on " + e.Resource
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}
