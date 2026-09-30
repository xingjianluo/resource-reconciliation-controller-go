package reconciler

import (
	"context"
	"sync"
)

// Applier 抽象底层资源的真实副作用（创建/更新/删除/读取）。
type Applier interface {
	// Create 物理创建资源。
	Create(ctx context.Context, r *Resource) error
	// Update 将资源从 old 变更到 next。
	Update(ctx context.Context, old, next *Resource) error
	// Delete 物理删除资源。
	Delete(ctx context.Context, r *Resource) error
	// Read 读取底层资源实况；不存在时 exist=false 且 err=nil。
	Read(ctx context.Context, key string) (r *Resource, exist bool, err error)
}

// MemoryApplier 是基于内存 map 的模拟执行器，
// 支持注入瞬时/永久故障以测试重试与拒绝。
type MemoryApplier struct {
	mu sync.Mutex
	// live 为底层"真实"资源世界。
	live map[string]*Resource
	// failTimes 对指定 key 注入故障。
	failTimes map[string]*failInject
	// auditHook 每次产生副作用时回调（供测试观测副作用次数，验证幂等）。
	auditHook func(key, action string)
	// opLog 记录全部副作用调用（含失败的），供测试断言。
	opLog []applierOp
}

// failInject 描述针对单个资源的故障注入。
type failInject struct {
	createRemaining int // Create 前 N 次调用失败
	updateRemaining int // Update 前 N 次调用失败
	deleteRemaining int // Delete 前 N 次调用失败
	permanent       bool
	message         string
}

// applierOp 记录一次底层副作用调用。
type applierOp struct {
	Key    string
	Action string
	Ok     bool
	Reason string
}

// NewMemoryApplier 创建空的模拟执行器。
func NewMemoryApplier() *MemoryApplier {
	return &MemoryApplier{
		live:      map[string]*Resource{},
		failTimes: map[string]*failInject{},
	}
}

// InjectTransientFailure 让资源 key 的指定动作前 times 次调用失败（之后恢复成功）。
// action 取值 "create"/"update"/"delete"/"*"（* 表示三类动作分别计数）。
func (a *MemoryApplier) InjectTransientFailure(key, action string, times int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.injectLocked(key, action, times, false, "injected transient failure")
}

// InjectPermanentFailure 让资源 key 的指定动作永久失败。
func (a *MemoryApplier) InjectPermanentFailure(key, action string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.injectLocked(key, action, 1<<30, true, "injected permanent failure")
}

func (a *MemoryApplier) injectLocked(key, action string, times int, permanent bool, msg string) {
	f := a.failTimes[key]
	if f == nil {
		f = &failInject{}
		a.failTimes[key] = f
	}
	f.permanent = permanent
	f.message = msg
	switch action {
	case "create", "*":
		f.createRemaining = times
	}
	switch action {
	case "update", "*":
		f.updateRemaining = times
	}
	switch action {
	case "delete", "*":
		f.deleteRemaining = times
	}
}

// ClearFailures 清除资源 key（或全部，key==""）的故障注入。
func (a *MemoryApplier) ClearFailures(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if key == "" {
		a.failTimes = map[string]*failInject{}
	} else {
		delete(a.failTimes, key)
	}
}

// SetAuditHook 安装副作用观测回调。
func (a *MemoryApplier) SetAuditHook(hook func(key, action string)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.auditHook = hook
}

// OpLog 返回副作用调用日志的副本。
func (a *MemoryApplier) OpLog() []applierOp {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]applierOp(nil), a.opLog...)
}

// CountSideEffects 返回指定动作（""/create/update/delete）的成功副作用次数。
func (a *MemoryApplier) CountSideEffects(action string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, op := range a.opLog {
		if op.Ok && (action == "" || op.Action == action) {
			n++
		}
	}
	return n
}

// seed 供恢复测试直接在"真实世界"放置一个资源。
func (a *MemoryApplier) seed(r *Resource) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := *r
	if r.DependsOn != nil {
		cp.DependsOn = append([]string(nil), r.DependsOn...)
	}
	cp.Spec = cloneSpec(r.Spec)
	a.live[r.Key()] = &cp
}

// LiveSnapshot 返回底层真实世界的快照副本（测试辅助）。
func (a *MemoryApplier) LiveSnapshot() map[string]*Resource {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]*Resource{}
	for k, v := range a.live {
		cp := *v
		out[k] = &cp
	}
	return out
}

func (a *MemoryApplier) Create(ctx context.Context, r *Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.live[r.Key()]; exists {
		// 幂等：已存在不重复创建，交由更新路径比较。
		a.recordOpLocked(r.Key(), "create", true, "")
		return nil
	}
	if f := a.failTimes[r.Key()]; f != nil && f.createRemaining > 0 {
		f.createRemaining--
		a.recordOpLocked(r.Key(), "create", false, f.message)
		return applierErr("create", r.Key(), f)
	}
	cp := *r
	if r.DependsOn != nil {
		cp.DependsOn = append([]string(nil), r.DependsOn...)
	}
	cp.Spec = cloneSpec(r.Spec)
	a.live[r.Key()] = &cp
	a.recordOpLocked(r.Key(), "create", true, "")
	if a.auditHook != nil {
		a.auditHook(r.Key(), "create")
	}
	return nil
}

func (a *MemoryApplier) Update(ctx context.Context, old, next *Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	cur, exists := a.live[next.Key()]
	if !exists {
		// 底层缺失：交由创建路径（可重试）。
		a.recordOpLocked(next.Key(), "update", false, "resource missing")
		return &ReconcileError{Reason: ReasonApplierFailure, Detail: "update " + next.Key() + ": resource not found", Retriable: true}
	}
	// 已满足则不产生实质改写：仍返回成功，由 reconciler 先比较后决定是否调用。
	if f := a.failTimes[next.Key()]; f != nil && f.updateRemaining > 0 {
		f.updateRemaining--
		a.recordOpLocked(next.Key(), "update", false, f.message)
		return applierErr("update", next.Key(), f)
	}
	_ = cur
	cp := *next
	if next.DependsOn != nil {
		cp.DependsOn = append([]string(nil), next.DependsOn...)
	}
	cp.Spec = cloneSpec(next.Spec)
	a.live[next.Key()] = &cp
	a.recordOpLocked(next.Key(), "update", true, "")
	if a.auditHook != nil {
		a.auditHook(next.Key(), "update")
	}
	return nil
}

func (a *MemoryApplier) Delete(ctx context.Context, r *Resource) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.live[r.Key()]; !exists {
		// 幂等：目标不存在视为删除成功。
		a.recordOpLocked(r.Key(), "delete", true, "")
		return nil
	}
	if f := a.failTimes[r.Key()]; f != nil && f.deleteRemaining > 0 {
		f.deleteRemaining--
		a.recordOpLocked(r.Key(), "delete", false, f.message)
		return applierErr("delete", r.Key(), f)
	}
	delete(a.live, r.Key())
	a.recordOpLocked(r.Key(), "delete", true, "")
	if a.auditHook != nil {
		a.auditHook(r.Key(), "delete")
	}
	return nil
}

func (a *MemoryApplier) Read(ctx context.Context, key string) (*Resource, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.live[key]
	if !ok {
		return nil, false, nil
	}
	cp := *r
	if r.DependsOn != nil {
		cp.DependsOn = append([]string(nil), r.DependsOn...)
	}
	cp.Spec = cloneSpec(r.Spec)
	return &cp, true, nil
}

// applierErr 根据注入类型返回可重试/不可重试的执行错误。
func applierErr(action, key string, f *failInject) error {
	return &ReconcileError{
		Reason:    ReasonApplierFailure,
		Detail:    action + " " + key + ": " + f.message,
		Retriable: !f.permanent,
	}
}

func (a *MemoryApplier) recordOpLocked(key, action string, ok bool, reason string) {
	a.opLog = append(a.opLog, applierOp{Key: key, Action: action, Ok: ok, Reason: reason})
}
