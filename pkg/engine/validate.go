package engine

import (
	"fmt"
	"sort"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/apis/v1"
)

// validateDeclarations performs whole-set, side-effect-free validation.
// It checks in this order and returns distinguishable errors:
//
//  1. InvalidDeclaration — blank kind/name, duplicate keys, self
//     dependency or malformed protection annotations.
//  2. MissingDependency  — a DependsOn entry names no declared resource.
//  3. CycleDetected      — the dependency graph contains a cycle (the
//     cycle members are listed in the message).
//
// Because this runs before any side effect, an invalid set can never leave
// a half-completed world.
func validateDeclarations(decls []v1.Declaration) error {
	keys := make(map[string]bool, len(decls))
	for _, d := range decls {
		key := d.Key()
		if d.Kind == "" || d.Name == "" {
			return &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonInvalidDeclaration,
				Message:  "kind and name must both be non-empty",
			}
		}
		if keys[key] {
			return &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonInvalidDeclaration,
				Message:  "duplicate resource declaration",
			}
		}
		keys[key] = true
		for _, dep := range d.DependsOn {
			if dep == key {
				return &v1.ReconcileError{
					Resource: key,
					Reason:   v1.ReasonInvalidDeclaration,
					Message:  "resource cannot depend on itself",
				}
			}
		}
		if v, ok := d.Annotations[v1.AnnotationProtected]; ok && v != "true" && v != "false" {
			return &v1.ReconcileError{
				Resource: key,
				Reason:   v1.ReasonInvalidDeclaration,
				Message:  fmt.Sprintf("annotation %s must be \"true\" or \"false\", got %q", v1.AnnotationProtected, v),
			}
		}
	}

	for _, d := range decls {
		for _, dep := range d.DependsOn {
			if !keys[dep] {
				return &v1.ReconcileError{
					Resource: d.Key(),
					Reason:   v1.ReasonMissingDependency,
					Message:  fmt.Sprintf("dependency %q is not declared in this set", dep),
				}
			}
		}
	}

	if cycle := findCycle(decls); len(cycle) > 0 {
		return &v1.ReconcileError{
			Reason:  v1.ReasonCycleDetected,
			Message: fmt.Sprintf("dependency cycle: %v", cycle),
		}
	}
	return nil
}

// findCycle returns the nodes participating in a dependency cycle, or nil.
// It uses iterative DFS with white/gray/black coloring.
func findCycle(decls []v1.Declaration) []string {
	deps := make(map[string][]string, len(decls))
	for _, d := range decls {
		d := d
		deps[d.Key()] = append([]string(nil), d.DependsOn...)
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	var cycle []string

	var visit func(key string) bool
	visit = func(key string) bool {
		color[key] = gray
		stack = append(stack, key)
		// deterministic edge order
		edges := append([]string(nil), deps[key]...)
		sort.Strings(edges)
		for _, dep := range edges {
			if color[dep] == white || color[dep] == 0 {
				if visit(dep) {
					return true
				}
			} else if color[dep] == gray {
				// found a back edge: extract the cycle from the stack
				for i, k := range stack {
					if k == dep {
						cycle = append([]string(nil), stack[i:]...)
						cycle = append(cycle, dep)
						return true
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[key] = black
		return false
	}

	all := make([]string, 0, len(deps))
	for k := range deps {
		all = append(all, k)
	}
	sort.Strings(all)
	for _, k := range all {
		if color[k] == white {
			if visit(k) {
				return cycle
			}
		}
	}
	return nil
}

// topoWaves groups declaration keys into dependency waves via Kahn's
// algorithm. A node is emitted once all its dependencies have been emitted;
// nodes released in the same iteration form one wave and are independent.
// Applying waves in order creates dependencies first; walking the waves in
// reverse deletes dependents before their dependencies.
func topoWaves(decls []v1.Declaration) ([][]string, error) {
	indeg := map[string]int{}
	dependents := map[string][]string{} // dep -> nodes that depend on it
	nodes := map[string]bool{}

	for _, d := range decls {
		key := d.Key()
		nodes[key] = true
		if _, ok := indeg[key]; !ok {
			indeg[key] = 0
		}
		for _, dep := range d.DependsOn {
			indeg[key]++
			dependents[dep] = append(dependents[dep], key)
		}
	}

	var waves [][]string
	emitted := 0
	for emitted < len(nodes) {
		var ready []string
		for key := range nodes {
			if indeg[key] == 0 {
				ready = append(ready, key)
			}
		}
		if len(ready) == 0 {
			// Should be unreachable: callers validate acyclicity first.
			if cycle := findCycle(decls); len(cycle) > 0 {
				return nil, &v1.ReconcileError{
					Reason:  v1.ReasonCycleDetected,
					Message: fmt.Sprintf("dependency cycle: %v", cycle),
				}
			}
			return nil, &v1.ReconcileError{Reason: v1.ReasonCycleDetected, Message: "unresolved dependency ordering"}
		}
		sort.Strings(ready)
		waves = append(waves, ready)
		for _, key := range ready {
			emitted++
			indeg[key] = -1 // mark emitted
			for _, child := range dependents[key] {
				indeg[child]--
			}
		}
	}
	return waves, nil
}
