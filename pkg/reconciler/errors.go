package reconciler

import "fmt"

// 可区分的拒绝 / 失败原因常量。
const (
	// ReasonCycleDependency 依赖图中存在环。
	ReasonCycleDependency = "CycleDependency"
	// ReasonMissingDependency 声明依赖了不存在的资源。
	ReasonMissingDependency = "MissingDependency"
	// ReasonIllegalDeclaration 声明本身非法（空 Kind/Name、非法版本等）。
	ReasonIllegalDeclaration = "IllegalDeclaration"
	// ReasonSelfDependency 资源依赖自身。
	ReasonSelfDependency = "SelfDependency"
	// ReasonUnauthorized 越权修改受保护资源。
	ReasonUnauthorized = "Unauthorized"
	// ReasonIncompatibleVersion 版本不兼容（跨主版本或降级）。
	ReasonIncompatibleVersion = "IncompatibleVersion"
	// ReasonClaimConflict ClaimKey 冲突（同一占用键被多个资源争抢）。
	ReasonClaimConflict = "ClaimConflict"
	// ReasonStaleRevision 陈旧更新：声明 revision 不新于已观察版本。
	ReasonStaleRevision = "StaleRevision"
	// ReasonDependencyFailed 所依赖的资源处于失败状态，本资源无法继续。
	ReasonDependencyFailed = "DependencyFailed"
	// ReasonApplierFailure 底层执行器返回的运行时失败（可重试）。
	ReasonApplierFailure = "ApplierFailure"
	// ReasonDependencyRejected 所依赖的资源已被拒绝。
	ReasonDependencyRejected = "DependencyRejected"
)

// ReconcileError 携带可机器判别的失败原因与人类可读细节。
type ReconcileError struct {
	Reason    string
	Detail    string
	Retriable bool
}

func (e *ReconcileError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

// Errorf 构造一个 ReconcileError。
func Errorf(reason, format string, args ...any) *ReconcileError {
	return &ReconcileError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// AsReconcileError 从任意 error 中提取 *ReconcileError。
func AsReconcileError(err error) (*ReconcileError, bool) {
	if err == nil {
		return nil, false
	}
	if re, ok := err.(*ReconcileError); ok {
		return re, true
	}
	return nil, false
}
