package reconciler

import (
	"context"
	"path/filepath"
	"testing"
)

// restart 用同一个持久化存储重建引擎（模拟进程重启），底层 applier 可替换。
func restart(t *testing.T, store Store, applier Applier, admins ...string) *Reconciler {
	t.Helper()
	rc, err := NewReconciler(store, applier, NewPolicyEngine(admins...))
	if err != nil {
		t.Fatalf("restart NewReconciler: %v", err)
	}
	return rc
}

func TestRecovery_CreateFailureAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := NewFileStore(path)
	app := NewMemoryApplier()

	// 第一轮：a 创建永久失败（进程死亡前快照已持久化失败状态与目标）
	app.InjectPermanentFailure("K/a", "create")
	rc1, _ := NewReconciler(store, app, NewPolicyEngine())
	r1, _ := rc1.Declare(ctx, []*Resource{res("K/a", "1.0.0")})
	t.Logf("重启前输入: a；调和结果 failed=%v（%s）", failedKeys(r1), r1.Failed["K/a"].Detail)
	if rc1.State("K/a").Status != StatusFailed {
		t.Fatal("期望 a 状态 Failed 已持久化")
	}
	if rc1.State("K/a").Failure.Retriable {
		t.Fatal("永久故障必须标记为不可重试（需要人工介入）")
	}
	t.Logf("关键判定: 永久故障 Retriable=false，可与瞬时故障区分")

	// 模拟进程重启 + 故障消除
	app.ClearFailures("")
	rc2 := restart(t, store, app)
	st := rc2.State("K/a")
	t.Logf("重启后恢复状态: status=%s target.version=%s attempts=%d", st.Status, st.Target.Version, st.Attempts)
	if st.Target == nil || st.Status != StatusFailed {
		t.Fatal("重启后应恢复出失败状态与目标声明，以便继续重试")
	}
	r2, _ := rc2.RetryFailed(ctx)
	if len(r2.Failed) != 0 {
		t.Fatalf("故障消除后重试应收敛: %v", r2.Failed)
	}
	if rc2.State("K/a").Status != StatusApplied {
		t.Fatalf("期望 Applied，实际 %s", rc2.State("K/a").Status)
	}
	if app.CountSideEffects("create") != 1 {
		t.Fatalf("恢复重试后 create 应恰好 1 次，实际 %d", app.CountSideEffects("create"))
	}
	t.Log("关键判定: 重启后从快照续跑，a 仅创建 1 次即收敛（无重复副作用）")
}

func TestRecovery_FileSnapshotSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := NewFileStore(path)
	app := NewMemoryApplier()

	rc1, _ := NewReconciler(store, app, NewPolicyEngine())
	rc1.Declare(ctx, []*Resource{
		res("K/a", "1.0.0"),
		res("K/b", "1.0.0", "K/a"),
	})
	t.Log("重启前: a,b 已 Applied，快照写入 JSON 文件")

	// 全新进程：新内存 applier 为空（模拟底层资源仍在，但新引擎只持有快照）。
	app2 := NewMemoryApplier()
	app2.seed(res("K/a", "1.0.0"))
	app2.seed(res("K/b", "1.0.0", "K/a"))
	rc2 := restart(t, store, app2)
	snap := rc2.Snapshot()
	if len(snap.Resources) != 2 {
		t.Fatalf("快照恢复资源数错误: %d", len(snap.Resources))
	}
	t.Logf("重启后状态: a=%s b=%s（从 %s 恢复）",
		rc2.State("K/a").Status, rc2.State("K/b").Status, path)

	// 恢复后做一次完整调和：底层与期望一致 => 全部 skip，无副作用
	r, _ := rc2.ReconcileAll(ctx)
	if len(r.Applied) != 0 || len(r.Failed) != 0 || len(r.Skipped) != 2 {
		t.Fatalf("恢复后调和应全部跳过: applied=%v failed=%v skipped=%v",
			r.Applied, r.Failed, r.Skipped)
	}
	if app2.CountSideEffects("") != 0 {
		t.Fatal("恢复后一致状态不得改写底层")
	}
	t.Logf("关键判定: 重启后全量调和 skipped=%v，副作用=0（幂等收敛）", r.Skipped)
}

func TestRecovery_DriftRepairAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := NewFileStore(path)
	app := NewMemoryApplier()

	rc1, _ := NewReconciler(store, app, NewPolicyEngine())
	rc1.Declare(ctx, []*Resource{specRes("K/a", "1.1.0", 1, map[string]any{"x": 1})})
	t.Log("重启前: a@1.1.0/{x:1} Applied")

	// 模拟"真实世界"在进程外被篡改：版本回退、spec 漂移
	drifted := specRes("K/a", "1.0.0", 1, map[string]any{"x": 999})
	app2 := NewMemoryApplier()
	app2.seed(drifted)
	t.Log("重启时底层实况: a@1.0.0/{x:999}（发生漂移）")

	rc2 := restart(t, store, app2)
	r, _ := rc2.ReconcileAll(ctx)
	if len(r.Failed) != 0 || len(r.Applied) != 1 {
		t.Fatalf("漂移应被修复: applied=%v failed=%v", r.Applied, r.Failed)
	}
	if app2.CountSideEffects("update") != 1 {
		t.Fatalf("期望恰好 1 次漂移修复 update，实际 %d", app2.CountSideEffects("update"))
	}
	live, exist, _ := app2.Read(ctx, "K/a")
	// 快照经 JSON 往返后数字为 float64，用数值容忍比较。
	x, _ := live.Spec["x"].(float64)
	if !exist || live.Version != "1.1.0" || x != 1 {
		t.Fatalf("漂移修复后实况错误: %+v", live)
	}
	t.Log("关键判定: 重启后检测到漂移并修复到 1.1.0/{x:1}，恰好 1 次 update")
}

func TestRecovery_DeleteInProgressContinuesAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := NewFileStore(path)
	app := NewMemoryApplier()

	// a 创建成功；删除时注入永久故障 => 留下删除中墓碑
	rc1, _ := NewReconciler(store, app, NewPolicyEngine())
	rc1.Declare(ctx, []*Resource{res("K/a", "1.0.0")})
	app.InjectPermanentFailure("K/a", "delete")
	r1, _ := rc1.Declare(ctx, nil) // 全量空集合 => 删除 a
	t.Logf("重启前: 删除 a 失败 failed=%v，墓碑 status=%s",
		failedKeys(r1), rc1.State("K/a").Status)
	if rc1.State("K/a").Status != StatusDeleting || !rc1.State("K/a").DeleteRequested {
		t.Fatal("期望删除中墓碑已持久化")
	}

	// 重启 + 故障消除 => 恢复时继续删除
	app.ClearFailures("")
	rc2 := restart(t, store, app)
	r2, _ := rc2.ReconcileAll(ctx)
	if len(r2.Failed) != 0 {
		t.Fatalf("重启后删除不应再失败: %v", r2.Failed)
	}
	if rc2.State("K/a") != nil {
		t.Fatal("删除完成后状态应被移除")
	}
	if _, exist, _ := app.Read(ctx, "K/a"); exist {
		t.Fatal("底层 a 应已删除")
	}
	t.Logf("关键判定: 重启后续做删除，audit 尾部=%+v", rc2.Audit()[len(rc2.Audit())-1].Action)
}

func TestRecovery_FailedDependencyChainAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewFileStore(pathOrTemp(t, dir))
	app := NewMemoryApplier()

	// b 依赖 a；a 前 2 次失败，其中第 1 次失败发生在"重启前"，重启后继续计数
	app.InjectTransientFailure("K/a", "create", 2)
	rc1, _ := NewReconciler(store, app, NewPolicyEngine())
	r1, _ := rc1.Declare(ctx, []*Resource{
		res("K/a", "1.0.0"),
		res("K/b", "1.0.0", "K/a"),
	})
	t.Logf("重启前: a 失败(尝试%d)，b 被依赖阻塞=%v", r1.Failed["K/a"].Attempts, failedKeys(r1))

	// 重启后第一次重试：a 第 2 次仍失败
	rc2 := restart(t, store, app)
	r2, _ := rc2.RetryFailed(ctx)
	t.Logf("重启后重试#1: failed=%v", failedKeys(r2))
	if r2.Failed["K/a"] == nil {
		t.Fatal("a 第 2 次仍应失败")
	}

	// 再重试：a 成功后 b 才创建
	r3, _ := rc2.RetryFailed(ctx)
	t.Logf("重启后重试#2: applied=%v failed=%v", r3.Applied, failedKeys(r3))
	if len(r3.Failed) != 0 {
		t.Fatalf("应全部收敛: %v", r3.Failed)
	}
	if app.CountSideEffects("create") != 2 {
		t.Fatalf("期望 a,b 各创建一次共 2 次，实际 %d", app.CountSideEffects("create"))
	}
	t.Log("关键判定: 跨重启的失败计数与依赖阻塞保持一致，最终幂等收敛")
}

func pathOrTemp(t *testing.T, dir string) string {
	t.Helper()
	return filepath.Join(dir, "state-chain.json")
}
