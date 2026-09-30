package reconciler

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Result 是一次调和的汇总结果。
type Result struct {
	// Applied 本次达成一致的资源 key（含新创建/更新/删除）。
	Applied []string
	// Failed 本次仍失败（可重试）的资源 key 及原因。
	Failed map[string]*FailureRecord
	// Rejected 本次被永久拒绝的资源 key 及原因。
	Rejected map[string]*FailureRecord
	// Skipped 已满足、无需改写的资源 key。
	Skipped []string
	// BatchRejected 整批声明在预校验阶段被拒绝时的错误（无任何副作用）。
	BatchRejected *ReconcileError
}

func newResult() *Result {
	return &Result{Failed: map[string]*FailureRecord{}, Rejected: map[string]*FailureRecord{}}
}

func (r *Result) addApplied(k string) { r.Applied = append(r.Applied, k) }
func (r *Result) addSkipped(k string) { r.Skipped = append(r.Skipped, k) }
func (r *Result) addFailed(k string, f *FailureRecord) {
	if f == nil {
		return
	}
	cp := *f
	r.Failed[k] = &cp
}
func (r *Result) addRejected(k string, f *FailureRecord) {
	if f == nil {
		return
	}
	cp := *f
	r.Rejected[k] = &cp
}

// Reconciler 是调和引擎，持有持久化状态与策略引擎。
// 所有公开方法都在互斥锁下串行执行，保证并发触发不会产生重复副作用。
type Reconciler struct {
	mu      sync.Mutex
	store   Store
	applier Applier
	policy  *PolicyEngine
	snap    *Snapshot
}

// NewReconciler 从 store 恢复状态并构造引擎。
func NewReconciler(store Store, applier Applier, policy *PolicyEngine) (*Reconciler, error) {
	snap, err := store.Load()
	if err != nil {
		return nil, err
	}
	if snap.Resources == nil {
		snap.Resources = map[string]*ResourceState{}
	}
	if snap.Audit == nil {
		snap.Audit = []AuditRecord{}
	}
	return &Reconciler{store: store, applier: applier, policy: policy, snap: snap}, nil
}

// nowTS 返回 UTC RFC3339 时间戳。
func nowTS() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// DeclareOption 配置一次声明提交。
type DeclareOption func(*declareOpts)

type declareOpts struct {
	requester string
	hasActor  bool
}

// WithRequester 指定本次提交的操作者（删除受保护资源时做越权校验）。
func WithRequester(owner string) DeclareOption {
	return func(o *declareOpts) { o.requester = owner; o.hasActor = true }
}

// Declare 提交一批资源声明（期望状态），等价于以资源 owner 身份操作。
func (rc *Reconciler) Declare(ctx context.Context, desired []*Resource) (*Result, error) {
	return rc.declare(ctx, desired, declareOpts{})
}

// DeclareAs 以指定操作者身份提交声明（用于删除受保护资源的越权校验）。
func (rc *Reconciler) DeclareAs(ctx context.Context, desired []*Resource, requester string) (*Result, error) {
	return rc.declare(ctx, desired, declareOpts{requester: requester, hasActor: true})
}

func (rc *Reconciler) declare(ctx context.Context, desired []*Resource, opts declareOpts) (*Result, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	res := newResult()

	// ---- 阶段 A：把入参组装成 map，并做纯只读的整批预校验 ----
	desiredMap := map[string]*Resource{}
	for _, r := range desired {
		if r == nil {
			res.BatchRejected = Errorf(ReasonIllegalDeclaration, "nil resource in declaration batch")
			return res, nil
		}
		k := r.Key()
		if _, dup := desiredMap[k]; dup {
			res.BatchRejected = Errorf(ReasonIllegalDeclaration, "duplicate resource key %q in declaration batch", k)
			return res, nil
		}
		desiredMap[k] = r
	}
	// 整批结构校验（非法字段 / 自依赖 / 缺失依赖 / 环 / 批内 Claim 冲突）。
	// 任何一项失败：直接拒绝整批，绝不触碰已有状态与底层资源（无半完成状态）。
	if err := rc.policy.ValidateDeclarations(desiredMap); err != nil {
		re, _ := AsReconcileError(err)
		res.BatchRejected = re
		rc.appendAudit(AuditRecord{
			Action: "reject", Result: "rejected",
			Reason: re.Reason, Detail: re.Detail, Timestamp: nowTS(),
		})
		_ = rc.persistLocked()
		return res, nil
	}

	// ---- 阶段 B：逐条判定（陈旧 / 冲突 / 越权 / 版本），合并目标集合 ----
	// accepted：本次将参与调和的最新有效声明（含未变化的旧声明，用于依赖分析）。
	accepted := map[string]*Resource{}
	rejectedKeys := map[string]bool{}
	keys := sortedKeys(desiredMap)

	// 期望集合与现有墓碑集合之间的 ClaimKey 占用分析。
	desiredClaim := map[string]string{} // claim -> owner key（本次有效新声明）
	for _, k := range keys {
		r := desiredMap[k]
		if r.ClaimKey != "" {
			desiredClaim[r.ClaimKey] = k
		}
	}

	for _, k := range keys {
		r := desiredMap[k]
		old := rc.snap.Resources[k]

		// B1. 陈旧更新 / 重复幂等提交
		if old != nil && old.Target != nil && !old.DeleteRequested {
			switch {
			case r.Revision < old.Target.Revision:
				rc.rejectOne(res, k, r, Errorf(ReasonStaleRevision,
					"stale revision %d for %q, observed revision is %d", r.Revision, k, old.Target.Revision))
				rejectedKeys[k] = true
				continue
			case r.Revision == old.Target.Revision && !sameResourceShape(old.Target, r):
				rc.rejectOne(res, k, r, Errorf(ReasonStaleRevision,
					"same revision %d for %q but declaration content differs", r.Revision, k))
				rejectedKeys[k] = true
				continue
			}
		}

		// B2. ClaimKey 与现存其他资源冲突
		if r.ClaimKey != "" {
			if holder, conflict := rc.claimHeldByOther(k, r.ClaimKey, rejectedKeys); conflict {
				rc.rejectOne(res, k, r, Errorf(ReasonClaimConflict,
					"claim_key %q already held by %q", r.ClaimKey, holder))
				rejectedKeys[k] = true
				continue
			}
		}

		// B3. 受保护资源的越权与版本兼容（针对已生效的旧声明）
		if old != nil && old.Target != nil && !old.DeleteRequested {
			if err := rc.policy.CheckChange(old.Target, r); err != nil {
				re, _ := AsReconcileError(err)
				rc.rejectOne(res, k, r, re)
				rejectedKeys[k] = true
				continue
			}
		}
		accepted[k] = r
	}

	// ---- 阶段 C：目标集合 = 本次接受的声明（全量期望状态语义）----
	// 未在本次声明中出现的存量资源将在阶段 D 进入删除流程；
	// 被拒绝的键（陈旧/越权/版本不兼容等）保持现状，绝不进入删除流程。
	targets := map[string]*Resource{}
	for k, r := range accepted {
		targets[k] = r
	}

	// ---- 阶段 D：标记删除墓碑 ----
	// 删除集 = 之前存在、现在既不在目标集合、本次也未被拒绝的资源。
	// 先做一次纯只读的可行性校验：若某个被删资源仍被保留资源依赖，
	// 则整批声明不可能成立（缺失依赖），在任何副作用前拒绝整批。
	toDelete := map[string]bool{}
	for k := range rc.snap.Resources {
		st := rc.snap.Resources[k]
		if _, keep := targets[k]; keep {
			continue
		}
		if rejectedKeys[k] {
			continue
		}
		if st.Target == nil {
			continue
		}
		toDelete[k] = true
	}
	for deleted := range toDelete {
		for other, r := range targets {
			for _, dep := range r.DependsOn {
				if dep == deleted {
					err := Errorf(ReasonMissingDependency,
						"cannot delete %q: resource %q in the desired set still depends on it", deleted, other)
					res.BatchRejected = err
					rc.appendAudit(AuditRecord{
						Action: "reject", Result: "rejected",
						Reason: err.Reason, Detail: err.Detail, Timestamp: nowTS(),
					})
					_ = rc.persistLocked()
					return res, nil
				}
			}
		}
	}
	// 删除越权检查同样在任何副作用之前完成：任一删除被拒则整批拒绝。
	for k := range toDelete {
		st := rc.snap.Resources[k]
		requester := opts.requester
		if !opts.hasActor {
			requester = st.Target.Owner
		}
		if err := rc.policy.CheckDelete(st.Target, requester); err != nil {
			re, _ := AsReconcileError(err)
			res.BatchRejected = re
			rc.appendAudit(AuditRecord{
				ResourceKey: k, Action: "reject", Result: "rejected",
				Reason: re.Reason, Detail: re.Detail, Timestamp: nowTS(),
			})
			_ = rc.persistLocked()
			return res, nil
		}
	}
	// 全部前置检查通过后，统一标记删除墓碑（意图持久化，崩溃后可继续删除）。
	tombstones := map[string]bool{}
	for k := range toDelete {
		st := rc.snap.Resources[k]
		requester := opts.requester
		if !opts.hasActor {
			requester = st.Target.Owner
		}
		st.DeleteRequested = true
		st.DeleteBy = requester
		st.Status = StatusDeleting
		st.Failure = nil
		st.UpdatedAt = nowTS()
		tombstones[k] = true
		rc.appendAudit(AuditRecord{
			ResourceKey: k, Action: "delete", Revision: st.ObservedRev,
			Result: "ok", Attempt: 0, Timestamp: nowTS(),
			Detail: "delete tombstone marked",
		})
	}

	// 对本次接受的声明：更新 Target 并持久化“意图”，使进程崩溃后仍可恢复继续。
	maxRev := rc.snap.Revision
	for k, r := range accepted {
		st := rc.snap.Resources[k]
		if st == nil {
			st = &ResourceState{CreatedAt: nowTS()}
			rc.snap.Resources[k] = st
		}
		st.Target = cloneResource(r)
		st.DeleteRequested = false
		st.DeleteBy = ""
		st.Status = StatusPending
		st.Failure = nil
		st.UpdatedAt = nowTS()
		if r.Revision > maxRev {
			maxRev = r.Revision
		}
	}
	rc.snap.Revision = maxRev
	if err := rc.persistLocked(); err != nil {
		return nil, err
	}

	// ---- 阶段 E：按依赖顺序执行收敛 ----
	// E1. 创建/更新顺序（只含本次有新声明或存量需要继续调和的存活资源）
	createOrder, _, err := TopoOrder(targets, true)
	if err != nil {
		// 理论上阶段 A 已排除环；防御性处理。
		re, _ := AsReconcileError(err)
		res.BatchRejected = re
		return res, nil
	}

	// 依赖阻塞状态表：key -> 阻塞原因（依赖失败/被拒绝）。
	blocked := map[string]*FailureRecord{}
	rejectSet := map[string]bool{}
	for k := range rejectedKeys {
		rejectSet[k] = true
	}

	for _, k := range createOrder {
		r := targets[k]
		if r == nil {
			continue
		}
		// 目标集合中的资源都参与本轮收敛；被拒绝的不驱动。
		st := rc.snap.Resources[k]
		if rejectSet[k] {
			continue
		}
		if blocker := depBlocker(k, r.DependsOn, blocked, rejectSet, rc.snap); blocker != nil {
			blocked[k] = blocker
			fail := *blocker
			st.Status = StatusFailed
			st.Failure = &fail
			st.UpdatedAt = nowTS()
			res.addFailed(k, &fail)
			rc.appendAudit(AuditRecord{
				ResourceKey: k, Action: "skip", Revision: r.Revision,
				Result: "failed", Reason: fail.Reason, Detail: fail.Detail,
				Attempt: st.Attempts, Timestamp: nowTS(),
			})
			_ = rc.persistLocked()
			continue
		}
		rc.applyOne(ctx, k, r, res, true)
	}

	// E2. 删除顺序（墓碑，按依赖逆序，先删依赖方）。
	// 既包含本次新标记的墓碑，也包含历史上残留（上次删除失败）的墓碑。
	tombTargets := map[string]*Resource{}
	for k, st := range rc.snap.Resources {
		if st.DeleteRequested && st.Target != nil {
			tombTargets[k] = st.Target
		}
	}
	for k := range tombstones {
		if st := rc.snap.Resources[k]; st != nil && st.Target != nil {
			tombTargets[k] = st.Target
		}
	}
	deleteOrder, _, err := TopoOrder(tombTargets, false)
	if err != nil {
		re, _ := AsReconcileError(err)
		res.BatchRejected = re
		return res, nil
	}
	for _, k := range deleteOrder {
		rc.deleteOne(ctx, k, res)
	}

	_ = rc.persistLocked()
	return res, nil
}

// applyOne 收敛单个存活资源到底层执行器。
func (rc *Reconciler) applyOne(ctx context.Context, key string, r *Resource, res *Result, inBatch bool) {
	st := rc.snap.Resources[key]
	st.Attempts++
	attempt := st.Attempts

	live, exist, rerr := rc.applier.Read(ctx, key)
	if rerr != nil {
		st.Exist = false
		rc.failOne(key, r, attempt, "read", rerr, true, st, res)
		return
	}

	switch {
	case !exist:
		// 底层缺失 => 创建
		if cerr := rc.applier.Create(ctx, r); cerr != nil {
			st.Exist = false // 创建失败：底层尚不存在，等待重试
			rc.failOne(key, r, attempt, "create", cerr, true, st, res)
			return
		}
		st.Exist = true
		st.LiveVersion = r.Version
		st.LiveSpec = cloneSpec(r.Spec)
		st.ObservedRev = r.Revision
		st.Status = StatusApplied
		st.Failure = nil
		st.Attempts = 0
		st.UpdatedAt = nowTS()
		res.addApplied(key)
		rc.appendAudit(AuditRecord{
			ResourceKey: key, Action: "create", ToVersion: r.Version,
			Revision: r.Revision, Result: "ok", Attempt: attempt, Timestamp: nowTS(),
		})
		_ = rc.persistLocked()
	case liveConverged(live, r):
		// 已满足幂等：实际状态与声明内容完全一致，不做任何底层改写。
		// 即便声明携带了更新的 revision（内容无差异），也只刷新已观察版本号。
		st.Status = StatusApplied
		st.Exist = true
		st.ObservedRev = r.Revision
		st.Failure = nil
		st.UpdatedAt = nowTS()
		res.addSkipped(key)
		rc.appendAudit(AuditRecord{
			ResourceKey: key, Action: "skip", FromVersion: live.Version, ToVersion: r.Version,
			Revision: r.Revision, Result: "ok", Attempt: attempt, Timestamp: nowTS(),
			Detail: "already converged; no side effect",
		})
		_ = rc.persistLocked()
	default:
		// 存在但与期望有差异 => 更新（含重启后发现的漂移修复）。
		from := live.Version
		if uerr := rc.applier.Update(ctx, live, r); uerr != nil {
			st.Exist = exist // 更新失败：底层资源仍在，保持实况标记
			rc.failOne(key, r, attempt, "update", uerr, true, st, res)
			return
		}
		st.Exist = true
		st.LiveVersion = r.Version
		st.LiveSpec = cloneSpec(r.Spec)
		st.ObservedRev = r.Revision
		st.Status = StatusApplied
		st.Failure = nil
		st.Attempts = 0
		st.UpdatedAt = nowTS()
		res.addApplied(key)
		rc.appendAudit(AuditRecord{
			ResourceKey: key, Action: "update", FromVersion: from, ToVersion: r.Version,
			Revision: r.Revision, Result: "ok", Attempt: attempt, Timestamp: nowTS(),
		})
		_ = rc.persistLocked()
	}
}

// deleteOne 物理删除一个墓碑资源。
func (rc *Reconciler) deleteOne(ctx context.Context, key string, res *Result) {
	st := rc.snap.Resources[key]
	if st == nil || !st.DeleteRequested {
		return
	}
	st.Attempts++
	attempt := st.Attempts
	target := st.Target
	if derr := rc.applier.Delete(ctx, target); derr != nil {
		re, ok := AsReconcileError(derr)
		reason := ReasonApplierFailure
		detail := derr.Error()
		retriable := true
		if ok {
			reason = re.Reason
			detail = re.Detail
			retriable = re.Retriable
		}
		fail := &FailureRecord{
			Reason: reason, Detail: detail, Attempts: attempt,
			Timestamp: nowTS(), Retriable: retriable,
		}
		st.Status = StatusDeleting
		st.Failure = fail
		st.UpdatedAt = nowTS()
		res.addFailed(key, fail)
		rc.appendAudit(AuditRecord{
			ResourceKey: key, Action: "delete", Revision: target.Revision,
			Result: "failed", Reason: reason, Detail: detail,
			Attempt: attempt, Timestamp: nowTS(),
		})
		_ = rc.persistLocked()
		return
	}
	// 删除成功：移除引擎状态，物理世界与期望一致。
	delete(rc.snap.Resources, key)
	res.addApplied(key)
	rc.appendAudit(AuditRecord{
		ResourceKey: key, Action: "delete", FromVersion: target.Version,
		Revision: target.Revision, Result: "ok", Attempt: attempt, Timestamp: nowTS(),
	})
	_ = rc.persistLocked()
}

// failOne 记录一次可重试/不可重试的执行失败。
func (rc *Reconciler) failOne(key string, r *Resource, attempt int, action string, cause error, retriable bool,
	st *ResourceState, res *Result) {
	re, ok := AsReconcileError(cause)
	reason := ReasonApplierFailure
	detail := cause.Error()
	if ok {
		reason = re.Reason
		detail = re.Detail
		// 执行器显式声明"永久不可重试"时遵从之；否则按调用点默认（瞬时故障可重试）。
		if !re.Retriable {
			retriable = false
		}
	}
	fail := &FailureRecord{
		Reason: reason, Detail: detail, Attempts: attempt,
		Timestamp: nowTS(), Retriable: retriable,
	}
	st.Status = StatusFailed
	st.Failure = fail
	st.UpdatedAt = nowTS()
	res.addFailed(key, fail)
	rc.appendAudit(AuditRecord{
		ResourceKey: key, Action: action, ToVersion: r.Version,
		Revision: r.Revision, Result: "failed", Reason: reason, Detail: detail,
		Attempt: attempt, Timestamp: nowTS(),
	})
}

// rejectOne 记录一次永久拒绝（不可重试，保留可追踪记录）。
func (rc *Reconciler) rejectOne(res *Result, key string, r *Resource, err *ReconcileError) {
	ts := nowTS()
	fail := &FailureRecord{
		Reason: err.Reason, Detail: err.Detail, Attempts: 0,
		Timestamp: ts, Retriable: false,
	}
	res.addRejected(key, fail)
	st := rc.snap.Resources[key]
	if st == nil {
		st = &ResourceState{CreatedAt: ts}
		rc.snap.Resources[key] = st
	}
	if st.Target == nil {
		st.Target = cloneResource(r)
	}
	st.Status = StatusRejected
	st.Failure = fail
	st.UpdatedAt = ts
	rc.appendAudit(AuditRecord{
		ResourceKey: key, Action: "reject", ToVersion: r.Version,
		Revision: r.Revision, Result: "rejected", Reason: err.Reason,
		Detail: err.Detail, Timestamp: ts,
	})
}

// RetryFailed 重新调和所有处于失败/删除中/缺失的资源（用于部分失败后的恢复）。
func (rc *Reconciler) RetryFailed(ctx context.Context) (*Result, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.reconcileAllLocked(ctx)
}

// reconcileAllLocked 对当前全部目标做一次完整收敛（恢复 / 漂移修复 / 重试）。
func (rc *Reconciler) reconcileAllLocked(ctx context.Context) (*Result, error) {
	res := newResult()
	targets := map[string]*Resource{}
	for k, st := range rc.snap.Resources {
		if !st.DeleteRequested && st.Target != nil {
			targets[k] = st.Target
		}
	}
	createOrder, _, err := TopoOrder(targets, true)
	if err != nil {
		re, _ := AsReconcileError(err)
		res.BatchRejected = re
		return res, nil
	}
	blocked := map[string]*FailureRecord{}
	rejectSet := map[string]bool{}
	for k, st := range rc.snap.Resources {
		if st.Status == StatusRejected {
			rejectSet[k] = true
			f := st.Failure
			res.addRejected(k, f)
		}
	}
	for _, k := range createOrder {
		r := targets[k]
		st := rc.snap.Resources[k]
		if st.Status == StatusRejected {
			continue
		}
		if blocker := depBlocker(k, r.DependsOn, blocked, rejectSet, rc.snap); blocker != nil {
			blocked[k] = blocker
			fail := *blocker
			st.Status = StatusFailed
			st.Failure = &fail
			st.UpdatedAt = nowTS()
			res.addFailed(k, &fail)
			_ = rc.persistLocked()
			continue
		}
		if st.Status == StatusApplied {
			// 恢复时仍校验一次底层是否漂移：漂移则修复，一致则跳过。
			rc.applyOne(ctx, k, r, res, false)
			if res.skippedContains(k) {
				// applyOne 内部已记 skip；保持 Applied。
			}
			continue
		}
		// Failed / Missing / Pending / Deleting(存活项不会是)
		rc.applyOne(ctx, k, r, res, true)
	}

	// 墓碑删除
	tombTargets := map[string]*Resource{}
	for k, st := range rc.snap.Resources {
		if st.DeleteRequested && st.Target != nil {
			tombTargets[k] = st.Target
		}
	}
	deleteOrder, _, err := TopoOrder(tombTargets, false)
	if err != nil {
		re, _ := AsReconcileError(err)
		res.BatchRejected = re
		return res, nil
	}
	for _, k := range deleteOrder {
		rc.deleteOne(ctx, k, res)
	}
	_ = rc.persistLocked()
	return res, nil
}

// claimHeldByOther 判断 claim 是否被"其他"资源占用。
// 优先看本次已拒绝的键（保持原占用），再看存量状态。
func (rc *Reconciler) claimHeldByOther(self, claim string, rejectedKeys map[string]bool) (string, bool) {
	for k, st := range rc.snap.Resources {
		if k == self || st.Target == nil || st.DeleteRequested {
			continue
		}
		if st.Target.ClaimKey == claim {
			return k, true
		}
	}
	return "", false
}

// depBlocker 检查依赖是否阻塞当前资源：依赖被拒绝 => 不可重试；依赖失败 => 可重试。
func depBlocker(key string, deps []string, blocked map[string]*FailureRecord, rejectSet map[string]bool,
	snap *Snapshot) *FailureRecord {
	var retriableBlocker *FailureRecord
	for _, dep := range deps {
		if rejectSet[dep] {
			return &FailureRecord{
				Reason:    ReasonDependencyRejected,
				Detail:    "dependency " + dep + " was rejected",
				Retriable: false, Timestamp: nowTS(),
			}
		}
		if f, ok := blocked[dep]; ok {
			retriableBlocker = &FailureRecord{
				Reason:    ReasonDependencyFailed,
				Detail:    "dependency " + dep + " is failed: " + f.Detail,
				Retriable: true, Timestamp: nowTS(),
			}
			continue
		}
		if st := snap.Resources[dep]; st != nil {
			switch st.Status {
			case StatusRejected:
				return &FailureRecord{
					Reason:    ReasonDependencyRejected,
					Detail:    "dependency " + dep + " was rejected",
					Retriable: false, Timestamp: nowTS(),
				}
			case StatusFailed, StatusMissing, StatusDeleting, StatusPending:
				retriableBlocker = &FailureRecord{
					Reason:    ReasonDependencyFailed,
					Detail:    "dependency " + dep + " is not converged (" + st.Status + ")",
					Retriable: true, Timestamp: nowTS(),
				}
			}
		}
	}
	return retriableBlocker
}

// liveConverged 判断底层实况是否已与目标声明一致（幂等跳过的关键判定）。
func liveConverged(live *Resource, target *Resource) bool {
	if live == nil || target == nil {
		return false
	}
	return live.Version == target.Version &&
		live.Owner == target.Owner &&
		live.Protected == target.Protected &&
		live.ClaimKey == target.ClaimKey &&
		reflect.DeepEqual(live.Spec, target.Spec) &&
		reflect.DeepEqual(live.DependsOn, target.DependsOn)
}

// sameResourceShape 判断同 revision 的两次声明内容是否一致（区分幂等重复 vs 同版本改内容）。
func sameResourceShape(a, b *Resource) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Version == b.Version &&
		a.Owner == b.Owner &&
		a.Protected == b.Protected &&
		a.ClaimKey == b.ClaimKey &&
		reflect.DeepEqual(a.Spec, b.Spec) &&
		reflect.DeepEqual(a.DependsOn, b.DependsOn)
}

func cloneResource(in *Resource) *Resource {
	if in == nil {
		return nil
	}
	out := *in
	if in.DependsOn != nil {
		out.DependsOn = append([]string(nil), in.DependsOn...)
	}
	out.Spec = cloneSpec(in.Spec)
	return &out
}

func sortedKeys(m map[string]*Resource) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (rc *Reconciler) appendAudit(a AuditRecord) {
	if a.Timestamp == "" {
		a.Timestamp = nowTS()
	}
	rc.snap.Audit = append(rc.snap.Audit, a)
}

func (rc *Reconciler) persistLocked() error {
	return rc.store.Save(rc.snap)
}

// Snapshot 返回当前状态快照的深拷贝。
func (rc *Reconciler) ReconcileAll(ctx context.Context) (*Result, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.reconcileAllLocked(ctx)
}

// Snapshot 返回当前状态快照的深拷贝。
func (rc *Reconciler) Snapshot() *Snapshot {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return cloneSnapshot(rc.snap)
}

// Audit 返回审计记录副本。
func (rc *Reconciler) Audit() []AuditRecord {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := make([]AuditRecord, len(rc.snap.Audit))
	copy(out, rc.snap.Audit)
	return out
}

// State 返回单个资源当前状态的深拷贝（不存在返回 nil）。
func (rc *Reconciler) State(key string) *ResourceState {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return cloneState(rc.snap.Resources[key])
}

func (r *Result) skippedContains(k string) bool {
	for _, s := range r.Skipped {
		if s == k {
			return true
		}
	}
	return false
}
