package reconciler

import "testing"

func expectReason(t *testing.T, err error, want string) {
	t.Helper()
	re, ok := AsReconcileError(err)
	if !ok {
		t.Fatalf("期望 *ReconcileError(reason=%s)，实际 err=%v", want, err)
	}
	if re.Reason != want {
		t.Fatalf("期望原因 %s，实际 %s（%s）", want, re.Reason, re.Detail)
	}
	t.Logf("关键判定: 拒绝原因=%s 符合预期；细节=%s", re.Reason, re.Detail)
}

func TestValidate_IllegalDeclarations(t *testing.T) {
	p := NewPolicyEngine("root")

	t.Run("空Kind/Name", func(t *testing.T) {
		bad := &Resource{Kind: "", Name: "", Version: "1.0.0", Owner: "u"}
		t.Logf("输入: %+v", bad)
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"/": bad}), ReasonIllegalDeclaration)
	})
	t.Run("非法版本", func(t *testing.T) {
		bad := res("K/a", "1.0")
		t.Logf("输入: key=K/a version=1.0（非三段式）")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": bad}), ReasonIllegalDeclaration)
	})
	t.Run("缺少Owner", func(t *testing.T) {
		bad := res("K/a", "1.0.0")
		bad.Owner = ""
		t.Logf("输入: key=K/a owner 为空")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": bad}), ReasonIllegalDeclaration)
	})
	t.Run("自依赖", func(t *testing.T) {
		bad := res("K/a", "1.0.0", "K/a")
		t.Logf("输入: key=K/a 依赖自身")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": bad}), ReasonSelfDependency)
	})
	t.Run("重复依赖边", func(t *testing.T) {
		b := res("K/b", "1.0.0")
		a := res("K/a", "1.0.0", "K/b", "K/b")
		t.Logf("输入: a 重复依赖 b 两次")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": a, "K/b": b}), ReasonIllegalDeclaration)
	})
	t.Run("缺失依赖", func(t *testing.T) {
		a := res("K/a", "1.0.0", "K/ghost")
		t.Logf("输入: a 依赖不存在的 K/ghost")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": a}), ReasonMissingDependency)
	})
	t.Run("环", func(t *testing.T) {
		a := res("K/a", "1.0.0", "K/b")
		b := res("K/b", "1.0.0", "K/a")
		t.Logf("输入: a<->b 互相依赖")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": a, "K/b": b}), ReasonCycleDependency)
	})
	t.Run("Claim冲突", func(t *testing.T) {
		a := res("K/a", "1.0.0")
		a.ClaimKey = "port-8080"
		b := res("K/b", "1.0.0")
		b.ClaimKey = "port-8080"
		t.Logf("输入: a 与 b 同时占用 claim_key=port-8080")
		expectReason(t, p.ValidateDeclarations(map[string]*Resource{"K/a": a, "K/b": b}), ReasonClaimConflict)
	})
	t.Run("合法批次", func(t *testing.T) {
		a := res("K/a", "1.0.0")
		a.ClaimKey = "port-8080"
		b := res("K/b", "1.0.0", "K/a")
		t.Logf("输入: 合法批次 a,b；期望校验通过")
		if err := p.ValidateDeclarations(map[string]*Resource{"K/a": a, "K/b": b}); err != nil {
			t.Fatalf("合法批次被拒绝: %v", err)
		}
		t.Log("关键判定: 合法批次校验通过")
	})
}

func TestCheckChange_Unauthorized(t *testing.T) {
	p := NewPolicyEngine("root")
	old := res("K/db", "1.0.0")
	old.Protected = true
	old.Owner = "team-a"

	t.Run("非owner非admin修改被拒", func(t *testing.T) {
		next := res("K/db", "1.1.0")
		next.Protected = true
		next.Owner = "team-b"
		t.Logf("输入: 受保护资源 owner=team-a，修改者=team-b")
		expectReason(t, p.CheckChange(old, next), ReasonUnauthorized)
	})
	t.Run("owner本人可修改", func(t *testing.T) {
		next := res("K/db", "1.1.0")
		next.Protected = true
		next.Owner = "team-a"
		t.Logf("输入: 修改者=team-a（owner 本人）")
		if err := p.CheckChange(old, next); err != nil {
			t.Fatalf("owner 本人应可修改: %v", err)
		}
		t.Log("关键判定: owner 本人修改通过")
	})
	t.Run("admin可修改", func(t *testing.T) {
		next := res("K/db", "1.1.0")
		next.Protected = true
		next.Owner = "root"
		t.Logf("输入: 修改者=root（admin）")
		if err := p.CheckChange(old, next); err != nil {
			t.Fatalf("admin 应可修改: %v", err)
		}
		t.Log("关键判定: admin 修改通过")
	})
	t.Run("非owner删除受保护资源被拒", func(t *testing.T) {
		t.Logf("输入: 受保护资源 owner=team-a，删除请求者=intruder")
		expectReason(t, p.CheckDelete(old, "intruder"), ReasonUnauthorized)
	})
}

func TestCompatibleVersion(t *testing.T) {
	cases := []struct {
		name     string
		from, to string
		wantOK   bool
		reason   string
	}{
		{"首次创建", "", "2.0.0", true, ""},
		{"补丁升级", "1.0.0", "1.0.1", true, ""},
		{"次版本升级", "1.0.0", "1.2.0", true, ""},
		{"跨主版本", "1.0.0", "2.0.0", false, ReasonIncompatibleVersion},
		{"降级", "1.5.0", "1.4.9", false, ReasonIncompatibleVersion},
		{"同版本", "1.2.3", "1.2.3", true, ""},
		{"非法目标版本", "1.0.0", "bad", false, ReasonIllegalDeclaration},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Logf("输入: from=%q to=%q，期望 ok=%v", c.from, c.to, c.wantOK)
			err := CompatibleVersion(c.from, c.to)
			if c.wantOK {
				if err != nil {
					t.Fatalf("期望兼容，实际被拒: %v", err)
				}
				t.Log("关键判定: 版本兼容")
			} else {
				expectReason(t, err, c.reason)
			}
		})
	}
}
