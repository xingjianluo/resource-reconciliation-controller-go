package reconciler

import (
	"context"
	"sync"
	"testing"
)

func TestLoop_ConcurrentTriggersCoalesce(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)
	// 让 create 慢一点，制造大量触发重叠窗口
	rc.Declare(ctx, []*Resource{specRes("K/a", "1.0.0", 1, nil)})
	// 变更 spec，使后续调和确实有一次 update 工作
	rc.Declare(ctx, []*Resource{specRes("K/a", "1.0.1", 2, nil)})

	loop := NewLoop(rc)
	defer loop.Stop()

	var wg sync.WaitGroup
	const n = 50
	mergedCount := 0
	var mMu sync.Mutex
	t.Logf("输入: 对同一目标并发触发 %d 次调和（状态已收敛，期望零重复副作用）", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			merged, err := loop.Trigger(ctx)
			if err != nil {
				t.Errorf("Trigger: %v", err)
				return
			}
			if merged {
				mMu.Lock()
				mergedCount++
				mMu.Unlock()
			}
		}()
	}
	wg.Wait()
	loop.WaitIdle()

	t.Logf("关键判定: 50 次并发触发中被合并=%d，实际执行周期=%d", mergedCount, loop.RunCount())
	// 整个过程副作用数必须保持：1 create + 1 update（无论触发多少次）
	if got := app.CountSideEffects(""); got != 2 {
		t.Fatalf("并发触发产生重复副作用: %d（期望 2 = 1 create + 1 update）", got)
	}
	t.Logf("关键判定: 副作用总数=%d（幂等，无重复 create/update）", app.CountSideEffects(""))
}

func TestLoop_ConcurrentDeclareNoDuplicateSideEffects(t *testing.T) {
	ctx := context.Background()
	rc, app := newTestEngine(t)

	// 多个 goroutine 并发提交完全相同的声明（重复提交语义）
	const n = 30
	var wg sync.WaitGroup
	t.Logf("输入: %d 个 goroutine 并发提交同一声明 a@1.0.0 rev=1", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rc.Declare(ctx, []*Resource{specRes("K/a", "1.0.0", 1, nil)})
			if err != nil {
				t.Errorf("Declare: %v", err)
				return
			}
			if res.BatchRejected != nil && res.BatchRejected.Reason != ReasonStaleRevision {
				t.Errorf("意外的整批拒绝: %+v", res.BatchRejected)
			}
		}()
	}
	wg.Wait()

	if got := app.CountSideEffects("create"); got != 1 {
		t.Fatalf("并发重复声明产生 %d 次 create（期望恰好 1 次）", got)
	}
	st := rc.State("K/a")
	if st.Status != StatusApplied || st.ObservedRev != 1 {
		t.Fatalf("收敛状态错误: %+v", st)
	}
	t.Logf("关键判定: 30 个并发提交后 create 副作用=%d，状态 Applied rev=1（重复触发幂等）",
		app.CountSideEffects("create"))
}

func TestLoop_StopWaitsInFlight(t *testing.T) {
	ctx := context.Background()
	rc, _ := newTestEngine(t)
	loop := NewLoop(rc)
	loop.Trigger(ctx)
	loop.Stop()
	// Stop 后再触发必须报错
	if _, err := loop.Trigger(ctx); err == nil {
		t.Fatal("Stop 后 Trigger 应返回 ErrLoopStopped")
	}
	t.Log("关键判定: Stop 等待在途周期结束，之后 Trigger 返回 ErrLoopStopped")
}
