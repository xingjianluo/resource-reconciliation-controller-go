package reconciler

import (
	"sort"
	"strconv"
	"strings"
)

// PolicyEngine 负责受保护资源相关的策略判定：
// 非法声明、越权、版本兼容、ClaimKey 冲突。
type PolicyEngine struct {
	// admins 拥有所有受保护资源的修改权限。
	admins map[string]bool
}

// NewPolicyEngine 创建策略引擎，admins 为超级管理员集合。
func NewPolicyEngine(admins ...string) *PolicyEngine {
	m := map[string]bool{}
	for _, a := range admins {
		m[a] = true
	}
	return &PolicyEngine{admins: m}
}

// IsAdmin 判断 owner 是否为管理员。
func (p *PolicyEngine) IsAdmin(owner string) bool {
	return p.admins[owner]
}

// ValidateDeclarations 在产生任何副作用之前校验整批声明：
// 非法声明 / 自依赖 / 缺失依赖 / 环 / ClaimKey 冲突 一律拒绝。
// 纯只读检查：不修改任何状态，保证不会留下半完成状态。
func (p *PolicyEngine) ValidateDeclarations(resources map[string]*Resource) error {
	keys := make([]string, 0, len(resources))
	for k := range resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 1) 单条声明合法性
	for _, k := range keys {
		r := resources[k]
		if r.Kind == "" || r.Name == "" {
			return Errorf(ReasonIllegalDeclaration, "resource %q: kind and name are required", k)
		}
		if r.Key() != k {
			return Errorf(ReasonIllegalDeclaration, "resource map key %q does not match kind/name %q", k, r.Key())
		}
		if r.Owner == "" {
			return Errorf(ReasonIllegalDeclaration, "resource %q: owner is required", k)
		}
		if r.Revision < 0 {
			return Errorf(ReasonIllegalDeclaration, "resource %q: revision must be non-negative", k)
		}
		if _, err := parseSemver(r.Version); err != nil {
			return Errorf(ReasonIllegalDeclaration, "resource %q: %v", k, err)
		}
		// 依赖去重 / 自依赖
		seen := map[string]bool{}
		for _, dep := range r.DependsOn {
			if dep == k {
				return Errorf(ReasonSelfDependency, "resource %q must not depend on itself", k)
			}
			if dep == "" {
				return Errorf(ReasonIllegalDeclaration, "resource %q: empty dependency key", k)
			}
			if seen[dep] {
				return Errorf(ReasonIllegalDeclaration, "resource %q: duplicate dependency %q", k, dep)
			}
			seen[dep] = true
		}
	}

	// 2) 缺失依赖
	for _, k := range keys {
		r := resources[k]
		for _, dep := range r.DependsOn {
			if _, ok := resources[dep]; !ok {
				return Errorf(ReasonMissingDependency, "resource %q depends on missing resource %q", k, dep)
			}
		}
	}

	// 3) 环依赖（TopoOrder 同时返回具体环路径）
	if _, cycle, err := TopoOrder(resources, true); err != nil {
		return err
	} else if len(cycle) > 0 {
		return Errorf(ReasonCycleDependency, "dependency cycle detected: %v", cycle)
	}

	// 4) ClaimKey 冲突：同一非空 ClaimKey 同时只能被一个资源占用
	claimOwner := map[string]string{}
	for _, k := range keys {
		r := resources[k]
		if r.ClaimKey == "" {
			continue
		}
		if other, ok := claimOwner[r.ClaimKey]; ok {
			return Errorf(ReasonClaimConflict, "claim_key %q already claimed by %q, conflict with %q", r.ClaimKey, other, k)
		}
		claimOwner[r.ClaimKey] = k
	}
	return nil
}

// CheckChange 判定针对单个资源的变更是否被允许：
//   - 受保护资源只能由原 owner 或管理员修改/删除（越权拒绝）；
//   - 版本必须兼容（同主版本且不允许降级）。
//
// old 为 nil 表示首次创建（不受越权约束，但仍做版本格式校验）。
func (p *PolicyEngine) CheckChange(old, next *Resource) error {
	if old != nil && old.Protected {
		if next == nil {
			// 删除场景由调用方通过 CheckDelete 判定，这里不处理。
			return nil
		}
		if next.Owner != old.Owner && !p.admins[next.Owner] {
			return Errorf(ReasonUnauthorized, "owner %q is not allowed to modify protected resource %q owned by %q",
				next.Owner, old.Key(), old.Owner)
		}
	}
	if next != nil {
		if _, err := parseSemver(next.Version); err != nil {
			return Errorf(ReasonIllegalDeclaration, "resource %q: %v", next.Key(), err)
		}
		from := ""
		if old != nil {
			from = old.Version
		}
		if err := CompatibleVersion(from, next.Version); err != nil {
			return err
		}
	}
	return nil
}

// CheckDelete 判定删除一个受保护资源是否被允许。
func (p *PolicyEngine) CheckDelete(old *Resource, requester string) error {
	if old == nil {
		return nil
	}
	if old.Protected && requester != old.Owner && !p.admins[requester] {
		return Errorf(ReasonUnauthorized, "requester %q is not allowed to delete protected resource %q owned by %q",
			requester, old.Key(), old.Owner)
	}
	return nil
}

// semver 三段式版本号
type semver struct{ major, minor, patch int }

// parseSemver 解析 "X.Y.Z"，允许可选的 "v" 前缀与预发布/构建后缀。
func parseSemver(v string) (semver, error) {
	s := strings.TrimSpace(v)
	if s == "" {
		return semver{}, Errorf(ReasonIllegalDeclaration, "empty version")
	}
	if s[0] == 'v' || s[0] == 'V' {
		s = s[1:]
	}
	// 去掉构建与预发布后缀，比较只基于数字核心。
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semver{}, Errorf(ReasonIllegalDeclaration, "version %q must be in MAJOR.MINOR.PATCH form", v)
	}
	nums := [3]int{}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return semver{}, Errorf(ReasonIllegalDeclaration, "version %q has invalid numeric part %q", v, p)
		}
		nums[i] = n
	}
	return semver{nums[0], nums[1], nums[2]}, nil
}

// compareSemver 返回 -1/0/1。
func compareSemver(a, b semver) int {
	switch {
	case a.major != b.major:
		return boolCmp(a.major < b.major)
	case a.minor != b.minor:
		return boolCmp(a.minor < b.minor)
	case a.patch != b.patch:
		return boolCmp(a.patch < b.patch)
	default:
		return 0
	}
}

func boolCmp(less bool) int {
	if less {
		return -1
	}
	return 1
}

// CompatibleVersion 判断 from -> to 的版本升级是否兼容：
// 要求语义化版本、主版本相同且不允许降级（to >= from）。
// from 为空表示首次创建，只要 to 本身合法即兼容。
func CompatibleVersion(from, to string) error {
	tv, err := parseSemver(to)
	if err != nil {
		return err
	}
	if from == "" {
		return nil
	}
	fv, err := parseSemver(from)
	if err != nil {
		return err
	}
	if tv.major != fv.major {
		return Errorf(ReasonIncompatibleVersion, "incompatible major version: %s -> %s (major versions differ)", from, to)
	}
	if compareSemver(tv, fv) < 0 {
		return Errorf(ReasonIncompatibleVersion, "downgrade is not allowed: %s -> %s", from, to)
	}
	return nil
}
