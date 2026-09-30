// Command demo 在本地端到端演示资源调和引擎：
// 依赖顺序创建、幂等收敛、瞬时故障重试、"进程重启"恢复。
//
// 运行: go run ./cmd/demo
package main

import (
	"context"
	"fmt"

	rcpkg "github.com/xingjianluo/resource-reconciliation-controller-go/pkg/reconciler"
)

func main() {
	ctx := context.Background()
	store := rcpkg.NewMemoryStore() // 本地演示；持久化到磁盘可换 NewFileStore("state.json")
	app := rcpkg.NewMemoryApplier()
	engine, err := rcpkg.NewReconciler(store, app, rcpkg.NewPolicyEngine("root"))
	must(err)

	// 注入：数据库第一次创建会瞬时失败，演示部分失败 + 重试。
	app.InjectTransientFailure("db/users", "create", 1)

	desired := []*rcpkg.Resource{
		{Kind: "db", Name: "users", Version: "1.0.0", Owner: "team-a", Revision: 1},
		{Kind: "db", Name: "orders", Version: "1.0.0", Owner: "team-a", Revision: 1, DependsOn: []string{"db/users"}},
		{Kind: "svc", Name: "api", Version: "2.3.0", Owner: "team-b", Revision: 1,
			DependsOn: []string{"db/users", "db/orders"}, ClaimKey: "port-8080"},
	}

	fmt.Println("== 第一次调和（db/users 注入了 1 次瞬时故障）==")
	res, err := engine.Declare(ctx, desired)
	must(err)
	printResult(res)

	fmt.Println("== 重试失败资源 ==")
	res, err = engine.RetryFailed(ctx)
	must(err)
	printResult(res)

	fmt.Println("== 重复提交相同目标（应全部跳过，零副作用）==")
	res, err = engine.Declare(ctx, desired)
	must(err)
	printResult(res)

	fmt.Println("== 模拟进程重启：从快照恢复后再调和一次 ==")
	engine2, err := rcpkg.NewReconciler(store, app, rcpkg.NewPolicyEngine("root"))
	must(err)
	res, err = engine2.ReconcileAll(ctx)
	must(err)
	printResult(res)

	fmt.Printf("== 审计记录共 %d 条（可追踪每次判定）==\n", len(engine2.Audit()))
	for _, a := range engine2.Audit() {
		fmt.Printf("  %-9s %-12s %-8s %s %s\n", a.Action, a.ResourceKey, a.Result, nz(a.Reason), a.Detail)
	}
	fmt.Printf("\n底层真实副作用次数: create=%d update=%d delete=%d\n",
		app.CountSideEffects("create"), app.CountSideEffects("update"), app.CountSideEffects("delete"))
}

func nz(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func printResult(r *rcpkg.Result) {
	if r.BatchRejected != nil {
		fmt.Printf("  整批拒绝: %s — %s\n", r.BatchRejected.Reason, r.BatchRejected.Detail)
	}
	fmt.Printf("  applied=%v skipped=%v\n", r.Applied, r.Skipped)
	for k, f := range r.Failed {
		fmt.Printf("  failed  %s: %s(retriable=%v, attempts=%d)\n", k, f.Reason, f.Retriable, f.Attempts)
	}
	for k, f := range r.Rejected {
		fmt.Printf("  reject  %s: %s — %s\n", k, f.Reason, f.Detail)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
