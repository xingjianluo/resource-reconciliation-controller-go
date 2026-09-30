package reconciler

import (
	"context"
	"reflect"
	"testing"
)

func newTestEngine(t *testing.T, admins ...string) (*Reconciler, *MemoryApplier) {
	t.Helper()
	store := NewMemoryStore()
	applier := NewMemoryApplier()
	rc, err := NewReconciler(store, applier, NewPolicyEngine(admins...))
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	return rc, applier
}

func specRes(k, version string, rev int64, spec map[string]any, deps ...string) *Resource {
	kind, name := splitKey(k)
	return &Resource{
		Kind: kind, Name: name, Version: version, Owner: "tester",
		Revision: rev, Spec: spec, DependsOn: deps,
	}
}

func TestReconcile_DependencyOrderCreateDelete(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	var seq []string
	app.SetAuditHook(func(k, action string) { seq = append(seq, action+":"+k) })

	desired := []*Resource{
		res("K/c", "1.0.0", "K/a", "K/b"),
		res("K/a", "1.0.0"),
		res("K/b", "1.0.0", "K/a"),
	}
	t.Logf("输入声明(乱序提交): c 依赖 a,b；b 依赖 a")
	r1, err := rc.Declare(ctx, desired)
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if r1.BatchRejected != nil || len(r1.Failed) > 0 || len(r1.Rejected) > 0 {
		t.Fatalf("期望全部成功，实际 rejected=%v failed=%v", r1.Rejected, r1.Failed)
	}
	wantSeq := []string{"create:K/a", "create:K/b", "create:K/c"}
	if !reflect.DeepEqual(seq, wantSeq) {
		t.Fatalf("创建顺序错误: %v，期望 %v（依据：被依赖者先建）", seq, wantSeq)
	}
	t.Logf("关键判定: 创建副作用顺序=%v", seq)

	// 删除 c：提交 a,b（c 消失）。期望先无删除冲突，c 被删除，a,b 保留。
	seq = nil
	t.Logf("第二次输入: 仅 a,b（c 应按依赖逆序删除）")
	r2, err := rc.Declare(ctx, []*Resource{desired[1], desired[2]})
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if len(r2.Failed) > 0 {
		t.Fatalf("删除不应失败: %v", r2.Failed)
	}
	if _, exist, _ := app.Read(ctx, "K/c"); exist {
		t.Fatal("K/c 应当已被物理删除")
	}
	t.Logf("关键判定: 删除后 c 存在=false，副作用序列=%v", seq)

	// 全量提交"仅 a"意味着 b 被删除（a 保留）。
	seq = nil
	t.Logf("第三次输入: 仅 a（全量语义 => 删除 b，保留 a）")
	r3, _ := rc.Declare(ctx, []*Resource{desired[1]})
	if len(r3.Failed) > 0 || len(r3.Rejected) > 0 {
		t.Fatalf("删除 b 不应有失败/拒绝: failed=%v rejected=%v", r3.Failed, r3.Rejected)
	}
	if !reflect.DeepEqual(seq, []string{"delete:K/b"}) {
		t.Fatalf("删除序列错误: %v，期望 [delete:K/b]", seq)
	}
	if _, exist, _ := app.Read(ctx, "K/b"); exist {
		t.Fatal("K/b 应已删除")
	}
	if _, exist, _ := app.Read(ctx, "K/a"); !exist {
		t.Fatal("K/a 应被保留")
	}
	t.Logf("关键判定: b 已删除、a 保留，副作用序列=%v", seq)

	// 空集合提交 => 最后的 a 也删除。
	seq = nil
	t.Log("第四次输入: 空集合（=> 删除剩余的 a）")
	r4, _ := rc.Declare(ctx, nil)
	if len(r4.Failed) > 0 || len(r4.Rejected) > 0 {
		t.Fatalf("删除 a 不应失败: failed=%v rejected=%v", r4.Failed, r4.Rejected)
	}
	if !reflect.DeepEqual(seq, []string{"delete:K/a"}) {
		t.Fatalf("删除序列错误: %v，期望 [delete:K/a]", seq)
	}
	if _, exist, _ := app.Read(ctx, "K/a"); exist {
		t.Fatal("K/a 应已删除")
	}
	t.Logf("关键判定: 空集合后 a 删除，副作用序列=%v", seq)
}

func TestReconcile_DeleteSetInfeasibleRejected(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	a := res("K/a", "1.0.0")
	b := res("K/b", "1.0.0", "K/a")
	rc.Declare(ctx, []*Resource{a, b})
	t.Log("初始: a 与 b（b 依赖 a）均 Applied")

	// 全量声明：删除 a，却保留依赖 a 的 b => 删除集不可行，必须整批拒绝、零副作用
	keepB := res("K/b", "1.0.1", "K/a")
	t.Log("非法输入: 声明集只有 b 且 b 仍依赖 a（a 被删除）")
	r, _ := rc.Declare(ctx, []*Resource{keepB})
	if r.BatchRejected == nil || r.BatchRejected.Reason != ReasonMissingDependency {
		t.Fatalf("期望整批 MissingDependency，实际 %+v", r.BatchRejected)
	}
	if app.CountSideEffects("delete") != 0 {
		t.Fatalf("整批拒绝不得产生删除副作用，实际 delete 次数=%d", app.CountSideEffects("delete"))
	}
	if _, exist, _ := app.Read(ctx, "K/a"); !exist {
		t.Fatal("被拒绝后 a 必须仍存在（无半完成状态）")
	}
	if _, exist, _ := app.Read(ctx, "K/b"); !exist {
		t.Fatal("被拒绝后 b 必须仍存在（无半完成状态）")
	}
	t.Logf("关键判定: 整批拒绝原因=%s，delete 副作用=0，a/b 均保留", r.BatchRejected.Reason)
}

func TestReconcile_IdempotentConvergence(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	r0 := specRes("K/a", "1.0.0", 1, map[string]any{"x": 1})
	res1, _ := rc.Declare(ctx, []*Resource{r0})
	t.Logf("首次输入: a@1.0.0 rev=1 spec={x:1}；applied=%d", len(res1.Applied))
	if app.CountSideEffects("") != 1 {
		t.Fatalf("期望首次 1 次副作用，实际 %d", app.CountSideEffects(""))
	}

	// 重复提交完全相同的目标 => 必须跳过，零新增副作用
	res2, _ := rc.Declare(ctx, []*Resource{specRes("K/a", "1.0.0", 1, map[string]any{"x": 1})})
	t.Logf("重复输入相同目标；skipped=%v applied=%v", res2.Skipped, res2.Applied)
	if len(res2.Skipped) != 1 || app.CountSideEffects("") != 1 {
		t.Fatalf("幂等失败: 副作用总数=%d（应保持 1），skipped=%v", app.CountSideEffects(""), res2.Skipped)
	}
	t.Log("关键判定: 重复目标 0 次新副作用（skip 而非 update）")

	// 无变化的 Spec 再次提交也不写
	res3, _ := rc.Declare(ctx, []*Resource{specRes("K/a", "1.0.0", 2, map[string]any{"x": 1})})
	t.Logf("rev=2 但内容相同；skipped=%v", res3.Skipped)
	if len(res3.Skipped) != 1 || app.CountSideEffects("update") != 0 {
		t.Fatalf("无变化不应触发 update，update 次数=%d", app.CountSideEffects("update"))
	}

	// 内容变化 => 恰好一次 update
	res4, _ := rc.Declare(ctx, []*Resource{specRes("K/a", "1.1.0", 3, map[string]any{"x": 2})})
	t.Logf("rev=3 内容变化到 1.1.0/{x:2}；applied=%v", res4.Applied)
	if app.CountSideEffects("update") != 1 {
		t.Fatalf("期望恰好 1 次 update，实际 %d", app.CountSideEffects("update"))
	}
	st := rc.State("K/a")
	if st.LiveVersion != "1.1.0" || st.Status != StatusApplied || st.ObservedRev != 3 {
		t.Fatalf("收敛后状态错误: %+v", st)
	}
	t.Log("关键判定: 差异触发恰好 1 次 update，状态收敛到 rev=3")
}

func TestReconcile_StaleRevisionRejected(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	rc.Declare(ctx, []*Resource{specRes("K/a", "1.2.0", 5, nil)})
	t.Log("已生效声明: a rev=5 version=1.2.0")

	// 旧 revision => 拒绝，不能覆盖
	old := specRes("K/a", "1.1.0", 4, nil)
	res, _ := rc.Declare(ctx, []*Resource{old})
	if res.Rejected["K/a"] == nil || res.Rejected["K/a"].Reason != ReasonStaleRevision {
		t.Fatalf("期望 StaleRevision，实际 %+v", res.Rejected["K/a"])
	}
	st := rc.State("K/a")
	if st.ObservedRev != 5 || st.LiveVersion != "1.2.0" {
		t.Fatalf("陈旧更新覆盖了新声明: rev=%d ver=%s", st.ObservedRev, st.LiveVersion)
	}
	t.Logf("关键判定: rev=4 被拒(%s)，仍保持 rev=5/1.2.0", res.Rejected["K/a"].Reason)
	if app.CountSideEffects("update") != 0 {
		t.Fatal("陈旧更新不得产生任何副作用")
	}

	// 同 revision 但内容被篡改 => 同样拒绝
	tampered := specRes("K/a", "1.3.0", 5, map[string]any{"evil": true})
	res2, _ := rc.Declare(ctx, []*Resource{tampered})
	if res2.Rejected["K/a"] == nil || res2.Rejected["K/a"].Reason != ReasonStaleRevision {
		t.Fatalf("同版本改内容应按 StaleRevision 拒绝，实际 %+v", res2.Rejected["K/a"])
	}
	t.Logf("关键判定: 同 rev=5 内容不同被拒原因=%s", res2.Rejected["K/a"].Reason)
}

func TestReconcile_BatchRejectNoSideEffects(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	batch := []*Resource{
		res("K/a", "1.0.0", "K/b"),
		res("K/b", "1.0.0", "K/a"), // 环
	}
	t.Log("输入: 含环批次 a<->b；期望整批拒绝且零副作用、零持久化目标")
	res, _ := rc.Declare(ctx, batch)
	if res.BatchRejected == nil || res.BatchRejected.Reason != ReasonCycleDependency {
		t.Fatalf("期望 BatchRejected=CycleDependency，实际 %+v", res.BatchRejected)
	}
	if app.CountSideEffects("") != 0 {
		t.Fatal("整批被拒绝时不得产生任何副作用")
	}
	snap := rc.Snapshot()
	if len(snap.Resources) != 0 {
		t.Fatalf("整批被拒绝时不得写入资源状态，实际 %d", len(snap.Resources))
	}
	t.Logf("关键判定: 整批拒绝原因=%s，副作用=0，状态项=0（无半完成状态）", res.BatchRejected.Reason)
}

func TestReconcile_ProtectedAndIncompatible(t *testing.T) {
	ctx := context.Background()
	rc, _ := newTestEngine(t, "root")

	first := specRes("K/db", "1.0.0", 1, nil)
	first.Protected = true
	first.Owner = "team-a"
	res, _ := rc.Declare(ctx, []*Resource{first})
	t.Log("首次创建受保护资源 db owner=team-a")
	if res.BatchRejected != nil || len(res.Rejected) != 0 {
		t.Fatalf("首次创建不应被拒绝: %+v", res)
	}

	// 他人跨主版本升级 => 越权与版本不兼容都必须被拒绝（原因可区分）
	intruder := specRes("K/db", "2.0.0", 2, nil)
	intruder.Protected = true
	intruder.Owner = "team-b"
	res2, _ := rc.Declare(ctx, []*Resource{intruder})
	if res2.Rejected["K/db"] == nil {
		t.Fatal("期望拒绝越权修改")
	}
	if res2.Rejected["K/db"].Reason != ReasonUnauthorized {
		t.Fatalf("期望先判定 Unauthorized，实际 %s", res2.Rejected["K/db"].Reason)
	}
	t.Logf("关键判定: team-b 修改被拒原因=%s（%s）", res2.Rejected["K/db"].Reason, res2.Rejected["K/db"].Detail)

	// owner 本人尝试跨主版本 => Unauthorized 通过，但 IncompatibleVersion 必须拒绝
	owner := specRes("K/db", "2.0.0", 2, nil)
	owner.Protected = true
	owner.Owner = "team-a"
	res3, _ := rc.Declare(ctx, []*Resource{owner})
	if res3.Rejected["K/db"] == nil || res3.Rejected["K/db"].Reason != ReasonIncompatibleVersion {
		t.Fatalf("期望 IncompatibleVersion，实际 %+v", res3.Rejected["K/db"])
	}
	t.Logf("关键判定: owner 跨主版本被拒原因=%s", res3.Rejected["K/db"].Reason)

	// owner 本人合规升级 => 成功
	ok := specRes("K/db", "1.5.0", 3, nil)
	ok.Protected = true
	ok.Owner = "team-a"
	res4, _ := rc.Declare(ctx, []*Resource{ok})
	if len(res4.Rejected) != 0 || rc.State("K/db").LiveVersion != "1.5.0" {
		t.Fatalf("合规升级应成功: rejected=%+v state=%+v", res4.Rejected, rc.State("K/db"))
	}
	t.Log("关键判定: owner 同主版本升级 1.0.0->1.5.0 成功")
}

func TestReconcile_PartialFailureRetryAndDependencyBlock(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	// b 依赖 a；a 前两次创建注入瞬时故障
	app.InjectTransientFailure("K/a", "create", 2)
	a := res("K/a", "1.0.0")
	b := res("K/b", "1.0.0", "K/a")
	t.Log("输入: a,b（b 依赖 a）；注入 a 前 2 次 create 失败")

	res1, _ := rc.Declare(ctx, []*Resource{a, b})
	if res1.Failed["K/a"] == nil || res1.Failed["K/a"].Reason != ReasonApplierFailure {
		t.Fatalf("期望 a 失败原因 ApplierFailure，实际 %+v", res1.Failed["K/a"])
	}
	if !res1.Failed["K/a"].Retriable {
		t.Fatal("瞬时故障必须标记为可重试")
	}
	if res1.Failed["K/b"] == nil || res1.Failed["K/b"].Reason != ReasonDependencyFailed {
		t.Fatalf("期望 b 被依赖阻塞 DependencyFailed，实际 %+v", res1.Failed["K/b"])
	}
	t.Logf("关键判定: 首次调和 a=%s（尝试%d），b=%s（依赖阻塞，不产生副作用）",
		res1.Failed["K/a"].Reason, res1.Failed["K/a"].Attempts, res1.Failed["K/b"].Reason)
	if app.CountSideEffects("create") != 0 {
		t.Fatal("两次失败都不算成功副作用，b 绝不能被创建")
	}
	if _, exist, _ := app.Read(ctx, "K/b"); exist {
		t.Fatal("依赖未就绪时 b 不得存在")
	}

	// 第一次重试：a 仍失败（第 2 次），b 继续阻塞
	res2, _ := rc.RetryFailed(ctx)
	t.Logf("第 1 次重试: failed=%v", failedKeys(res2))
	if res2.Failed["K/a"] == nil {
		t.Fatal("a 第 2 次仍应失败")
	}

	// 第二次重试：a 成功，随后 b 成功
	res3, _ := rc.RetryFailed(ctx)
	t.Logf("第 2 次重试: applied=%v failed=%v", res3.Applied, failedKeys(res3))
	if len(res3.Failed) != 0 {
		t.Fatalf("故障耗尽后应收敛，仍失败=%v", res3.Failed)
	}
	if rc.State("K/a").Status != StatusApplied || rc.State("K/b").Status != StatusApplied {
		t.Fatalf("期望 a,b 均 Applied，实际 a=%s b=%s", rc.State("K/a").Status, rc.State("K/b").Status)
	}
	if app.CountSideEffects("create") != 2 {
		t.Fatalf("期望恰好 a,b 各创建一次（共 2），实际 %d", app.CountSideEffects("create"))
	}
	t.Log("关键判定: 重试后 a 先建、b 后建；a 仅创建 1 次、b 仅创建 1 次（无重复副作用）")

	// 再触发一次完整调和：全部幂等跳过
	res4, _ := rc.RetryFailed(ctx)
	if len(res4.Failed) != 0 || len(res4.Applied) != 0 {
		t.Fatalf("收敛后重试应无失败无应用，applied=%v failed=%v", res4.Applied, res4.Failed)
	}
	t.Log("关键判定: 收敛后再调和零副作用零失败")
}

func failedKeys(r *Result) []string {
	ks := make([]string, 0, len(r.Failed))
	for k := range r.Failed {
		ks = append(ks, k)
	}
	return ks
}

func TestReconcile_AuditTrailPersists(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)
	app.InjectTransientFailure("K/a", "create", 1)
	rc.Declare(ctx, []*Resource{res("K/a", "1.0.0")})
	rc.RetryFailed(ctx)

	audit := rc.Audit()
	if len(audit) < 2 {
		t.Fatalf("期望至少含失败+成功两条审计，实际 %d", len(audit))
	}
	var sawFailed, sawOK bool
	for _, a := range audit {
		if a.Result == "failed" && a.Reason == ReasonApplierFailure {
			sawFailed = true
		}
		if a.Result == "ok" && a.Action == "create" {
			sawOK = true
		}
	}
	if !sawFailed || !sawOK {
		t.Fatalf("审计记录缺少失败或成功条目: %+v", audit)
	}
	t.Logf("关键判定: 审计链保留失败原因与成功记录，共 %d 条", len(audit))
}

func TestReconcile_ClaimConflictAcrossBatches(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	a := res("K/a", "1.0.0")
	a.ClaimKey = "port-8080"
	rc.Declare(ctx, []*Resource{a})
	t.Log("先存: a 占用 claim_key=port-8080")

	// 新批次用 b 抢占同一 claim（a 在全量语义下同时被删除）=> b 必须被拒绝
	b := res("K/b", "1.0.0")
	b.ClaimKey = "port-8080"
	r, _ := rc.Declare(ctx, []*Resource{b})
	if r.Rejected["K/b"] == nil || r.Rejected["K/b"].Reason != ReasonClaimConflict {
		t.Fatalf("期望 b 因 ClaimConflict 被拒，实际 %+v", r.Rejected["K/b"])
	}
	t.Logf("关键判定: b 抢占 claim 被拒原因=%s，审计可追踪", r.Rejected["K/b"].Reason)
	if app.CountSideEffects("create") != 1 {
		t.Fatalf("被拒绝的 b 不得创建，create 次数=%d（应仅 a 一次）", app.CountSideEffects("create"))
	}
	// a 正常随全量语义删除，不受 b 拒绝影响
	if _, exist, _ := app.Read(ctx, "K/a"); exist {
		t.Fatal("a 应已被删除")
	}
	// 审计链包含 claim 冲突拒绝记录
	found := false
	for _, au := range rc.Audit() {
		if au.ResourceKey == "K/b" && au.Reason == ReasonClaimConflict {
			found = true
		}
	}
	if !found {
		t.Fatal("审计链缺少 b 的 ClaimConflict 拒绝记录")
	}
	t.Log("关键判定: a 正常删除、b 未创建、冲突拒绝在审计链中可追踪")
}
