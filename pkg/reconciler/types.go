// Package reconciler 实现一套本地资源编排与调和引擎：
// 按依赖顺序创建/更新/删除资源，支持幂等收敛、失败重试、
// 并发触发去重、进程重启恢复以及受保护资源约束。
package reconciler

// 资源声明状态 / 调和状态常量。
const (
	// StatusPending 声明已接收但尚未进入调和。
	StatusPending = "Pending"
	// StatusApplied 资源已与目标声明达成一致。
	StatusApplied = "Applied"
	// StatusFailed 最近一次调和失败（可重试）。
	StatusFailed = "Failed"
	// StatusRejected 声明非法，被永久拒绝（不可重试）。
	StatusRejected = "Rejected"
	// StatusDeleting 资源正在删除。
	StatusDeleting = "Deleting"
	// StatusMissing 期望存在，但底层资源缺失（等待重试创建）。
	StatusMissing = "Missing"
)

// Resource 是用户提交的资源声明（期望状态）。
type Resource struct {
	// Kind 资源种类，例如 "Database"、"Bucket"。
	Kind string `json:"kind"`
	// Name 资源名称，同一 Kind 内唯一；Key = Kind/Name。
	Name string `json:"name"`
	// Version 声明期望的版本号（语义化版本，如 "1.2.0"）。
	Version string `json:"version"`
	// DependsOn 该资源依赖的其他资源 Key 列表，
	// 被依赖者必须先创建；删除时顺序相反。
	DependsOn []string `json:"depends_on,omitempty"`
	// Spec 资源期望配置（任意 JSON 可序列化数据）。
	Spec map[string]any `json:"spec,omitempty"`
	// Owner 声明所有者（用于越权保护检查）。
	Owner string `json:"owner"`
	// Protected 为 true 时该资源受保护，
	// 未被授权的 owner 不得修改/删除。
	Protected bool `json:"protected,omitempty"`
	// ClaimKey 冲突检测键：同一 ClaimKey 同时只能被一个资源占用。
	ClaimKey string `json:"claim_key,omitempty"`
	// Revision 声明修订版本号，单调递增；
	// 较旧 revision 的更新为陈旧更新，必须被拒绝。
	Revision int64 `json:"revision"`
}

// Key 返回资源的全局唯一标识 "Kind/Name"。
func (r Resource) Key() string {
	return r.Kind + "/" + r.Name
}

// AuditRecord 是一次调和动作（或一次拒绝）的可追踪记录。
type AuditRecord struct {
	ResourceKey string `json:"resource_key"`
	Action      string `json:"action"` // create/update/delete/skip/reject
	FromVersion string `json:"from_version,omitempty"`
	ToVersion   string `json:"to_version,omitempty"`
	Revision    int64  `json:"revision"`
	Result      string `json:"result"` // ok/failed/rejected
	Reason      string `json:"reason,omitempty"`
	Detail      string `json:"detail,omitempty"`
	Attempt     int    `json:"attempt"`
	Timestamp   string `json:"timestamp"`
}

// FailureRecord 记录某个资源当前的失败信息，用于重试与可解释性。
type FailureRecord struct {
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
	Attempts  int    `json:"attempts"`
	Timestamp string `json:"timestamp"`
	// Retriable 表示该失败是否可在后续调和中重试。
	Retriable bool `json:"retriable"`
}

// ResourceState 是单个资源在引擎内的当前状态（持久化到快照）。
type ResourceState struct {
	Target      *Resource      `json:"target,omitempty"`       // 最新期望声明
	ObservedRev int64          `json:"observed_rev"`           // 已生效的声明 revision
	Status      string         `json:"status"`                 // 当前调和状态
	LiveVersion string         `json:"live_version,omitempty"` // 底层资源实际版本
	LiveSpec    map[string]any `json:"live_spec,omitempty"`    // 底层资源实际配置
	Exist       bool           `json:"exist"`                  // 底层资源是否存在
	Failure     *FailureRecord `json:"failure,omitempty"`      // 最近失败记录
	// DeleteRequested 为墓碑标记：该资源已不在期望集合中，待物理删除。
	DeleteRequested bool `json:"delete_requested,omitempty"`
	// DeleteBy 记录请求删除者（用于重启后继续做越权校验）。
	DeleteBy  string `json:"delete_by,omitempty"`
	Attempts  int    `json:"attempts,omitempty"` // 当前目标的累计尝试次数
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// Snapshot 是引擎状态快照，持久化后可用于进程重启恢复。
type Snapshot struct {
	Resources map[string]*ResourceState `json:"resources"`
	Audit     []AuditRecord             `json:"audit"`
	// Revision 是引擎接收过的最新声明修订（用于陈旧更新判断）。
	Revision int64 `json:"revision"`
}
