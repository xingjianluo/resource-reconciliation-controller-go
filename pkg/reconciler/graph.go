package reconciler

import "sort"

// TopoOrder 对给定资源集合做拓扑排序。
// create=true 时返回创建/更新顺序（被依赖者在前）；
// create=false 时返回删除顺序（依赖者在前，即创建顺序的逆序）。
// 若存在环，返回 ReasonCycleDependency 错误，并在 cycle 中给出环上的 key 路径。
//
// 注意：本函数只做排序，不校验缺失依赖（缺失 key 会被直接忽略，
// 缺失依赖的拒绝由 PolicyEngine.ValidateDeclarations 负责）。
func TopoOrder(resources map[string]*Resource, create bool) (order []string, cycle []string, err error) {
	// indeg[k] = 指向 k 的边数，即 k 依赖了多少个集合内资源（创建视角）。
	indeg := map[string]int{}
	// dependents[k] = 所有依赖 k 的资源，即 k -> 依赖方 的反向邻接表。
	dependents := map[string][]string{}
	keys := make([]string, 0, len(resources))
	for k := range resources {
		keys = append(keys, k)
		indeg[k] = 0
	}
	sort.Strings(keys) // 保证同层级顺序确定，输出可解释、可复现

	for _, k := range keys {
		r := resources[k]
		seen := map[string]bool{}
		for _, dep := range r.DependsOn {
			if dep == k {
				continue // 自依赖由策略层拒绝；排序时忽略
			}
			if _, ok := resources[dep]; !ok {
				continue // 缺失依赖由策略层拒绝；排序时忽略
			}
			if seen[dep] {
				continue // 重复声明的依赖边只算一次
			}
			seen[dep] = true
			indeg[k]++
			dependents[dep] = append(dependents[dep], k)
		}
	}

	// Kahn 算法
	queue := make([]string, 0, len(keys))
	for _, k := range keys {
		if indeg[k] == 0 {
			queue = append(queue, k)
		}
	}
	createOrder := make([]string, 0, len(keys))
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		createOrder = append(createOrder, k)
		children := append([]string(nil), dependents[k]...)
		sort.Strings(children)
		for _, c := range children {
			indeg[c]--
			if indeg[c] == 0 {
				queue = append(queue, c)
			}
		}
	}

	if len(createOrder) != len(resources) {
		// 仍有入度非 0 的节点 => 存在环。提取一条具体环路径以便排障。
		cyclic := map[string]bool{}
		for k, d := range indeg {
			if d > 0 {
				cyclic[k] = true
			}
		}
		cyc := findCycle(resources, cyclic)
		return nil, cyc, Errorf(ReasonCycleDependency, "dependency cycle detected: %v", cyc)
	}

	if create {
		return createOrder, nil, nil
	}
	// 删除顺序：先删依赖方，即创建顺序整体逆序。
	deleteOrder := make([]string, len(createOrder))
	for i, k := range createOrder {
		deleteOrder[len(createOrder)-1-i] = k
	}
	return deleteOrder, nil, nil
}

// findCycle 在被标记为环上的节点集合中，沿依赖边找出一条具体环路径。
func findCycle(resources map[string]*Resource, cyclic map[string]bool) []string {
	start := ""
	for k := range cyclic {
		if start == "" || k < start {
			start = k
		}
	}
	if start == "" {
		return nil
	}
	// 从 start 出发，沿 "依赖" 边行走，直到回到已访问节点。
	path := []string{}
	index := map[string]int{}
	cur := start
	for {
		if pos, ok := index[cur]; ok {
			// 回到路径中的某点，截取环片段并闭合。
			cyc := append([]string(nil), path[pos:]...)
			cyc = append(cyc, cur)
			return cyc
		}
		index[cur] = len(path)
		path = append(path, cur)
		r := resources[cur]
		next := ""
		// 选择一个环上、且当前节点依赖的目标继续行走（取最小保证确定性）。
		for _, dep := range r.DependsOn {
			if cyclic[dep] && (next == "" || dep < next) {
				next = dep
			}
		}
		if next == "" {
			// 理论上不会发生（环上节点必有环上出边），稳妥返回当前路径。
			return path
		}
		cur = next
	}
}
