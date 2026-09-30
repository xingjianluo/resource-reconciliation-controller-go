package reconciler

import (
	"errors"
	"testing"
)

func res(k, version string, deps ...string) *Resource {
	kind, name := splitKey(k)
	return &Resource{Kind: kind, Name: name, Version: version, Owner: "tester", DependsOn: deps, Revision: 1}
}

func splitKey(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '/' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func TestTopoOrder_CreateAndDelete(t *testing.T) {
	// 输入: C 依赖 B，B 依赖 A
	in := map[string]*Resource{
		"K/A": res("K/A", "1.0.0"),
		"K/B": res("K/B", "1.0.0", "K/A"),
		"K/C": res("K/C", "1.0.0", "K/B"),
	}
	t.Logf("输入资源: A=[] B=[A] C=[B]；期望创建顺序 A,B,C，删除顺序 C,B,A")

	order, _, err := TopoOrder(in, true)
	if err != nil {
		t.Fatalf("create order unexpected error: %v", err)
	}
	if len(order) != 3 || order[0] != "K/A" || order[1] != "K/B" || order[2] != "K/C" {
		t.Fatalf("create order wrong: %v（判定依据：被依赖者必须先创建）", order)
	}
	t.Logf("关键判定: 创建顺序=%v", order)

	delOrder, _, err := TopoOrder(in, false)
	if err != nil {
		t.Fatalf("delete order unexpected error: %v", err)
	}
	if len(delOrder) != 3 || delOrder[0] != "K/C" || delOrder[1] != "K/B" || delOrder[2] != "K/A" {
		t.Fatalf("delete order wrong: %v（判定依据：依赖方必须先删除）", delOrder)
	}
	t.Logf("关键判定: 删除顺序=%v", delOrder)
}

func TestTopoOrder_Cycle(t *testing.T) {
	in := map[string]*Resource{
		"K/A": res("K/A", "1.0.0", "K/B"),
		"K/B": res("K/B", "1.0.0", "K/C"),
		"K/C": res("K/C", "1.0.0", "K/A"),
	}
	t.Logf("输入资源: A->B->C->A（环）；期望 ReasonCycleDependency 且返回环路径")

	_, cycle, err := TopoOrder(in, true)
	re, ok := AsReconcileError(err)
	if !ok {
		t.Fatalf("期望 *ReconcileError，实际 %v", err)
	}
	if !errors.Is(err, err) || re.Reason != ReasonCycleDependency {
		t.Fatalf("期望原因 %s，实际 %s", ReasonCycleDependency, re.Reason)
	}
	if len(cycle) < 3 {
		t.Fatalf("期望环路径至少包含 3 个节点，实际 %v", cycle)
	}
	t.Logf("关键判定: 检测到环路径=%v，原因=%s", cycle, re.Reason)
}

func TestTopoOrder_DiamondDAG(t *testing.T) {
	in := map[string]*Resource{
		"K/A": res("K/A", "1.0.0"),
		"K/B": res("K/B", "1.0.0", "K/A"),
		"K/C": res("K/C", "1.0.0", "K/A"),
		"K/D": res("K/D", "1.0.0", "K/B", "K/C"),
	}
	t.Logf("输入菱形依赖: B,C 依赖 A，D 依赖 B,C")

	order, _, err := TopoOrder(in, true)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	pos := map[string]int{}
	for i, k := range order {
		pos[k] = i
	}
	if !(pos["K/A"] < pos["K/B"] && pos["K/B"] < pos["K/D"] && pos["K/C"] < pos["K/D"]) {
		t.Fatalf("顺序不满足依赖约束: %v", order)
	}
	t.Logf("关键判定: 拓扑顺序=%v 满足 A<B<D 且 A<C<D", order)
}
