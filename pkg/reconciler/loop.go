package reconciler

import (
	"context"
	"runtime"
	"sync"
)

// Loop 是带触发合并的后台调和循环：
// 同一时刻只运行一个调和周期；周期进行中到达的多个触发会被合并为一次。
// 因此对同一目标的并发触发最多排队一次，配合 Reconciler 的串行锁与
// 幂等判定，不会产生重复副作用。
type Loop struct {
	rc *Reconciler

	mu      sync.Mutex
	running bool // 是否有调和周期正在执行
	queued  bool // 是否有触发在当前周期期间被合并
	stopped bool
	doneCh  chan struct{}

	// historyMu 保护 lastResult/lastErr/runCount
	historyMu  sync.Mutex
	lastResult *Result
	lastErr    error
	runCount   int
}

// NewLoop 构造调和循环。
func NewLoop(rc *Reconciler) *Loop {
	return &Loop{rc: rc, doneCh: make(chan struct{})}
}

// Trigger 请求一次调和。
// 返回值 merged 表示本次触发是否被合并（已有周期在跑且已存在排队请求）。
// 注意：是否"实际执行"由调度决定；被合并的触发会在当前周期结束后再跑一次完整收敛，
// 而幂等收敛保证第二次不会重复产生副作用。
func (l *Loop) Trigger(ctx context.Context) (merged bool, err error) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return false, ErrLoopStopped
	}
	if l.running {
		wasQueued := l.queued
		l.queued = true
		l.mu.Unlock()
		// 首个排队触发不算"被合并丢弃"，它保证会再跑一轮；
		// 之后的触发才真正合并到同一个排队标记上。
		return wasQueued, nil
	}
	l.running = true
	l.mu.Unlock()

	go l.run(ctx)
	return false, nil
}

func (l *Loop) run(ctx context.Context) {
	for {
		res, err := l.rc.ReconcileAll(ctx)
		l.historyMu.Lock()
		l.lastResult = res
		l.lastErr = err
		l.runCount++
		l.historyMu.Unlock()

		l.mu.Lock()
		if l.queued && !l.stopped {
			// 期间有触发到达：消费标记并立即再跑一轮。
			l.queued = false
			l.mu.Unlock()
			continue
		}
		l.running = false
		if l.stopped {
			close(l.doneCh)
		}
		l.mu.Unlock()
		return
	}
}

// Stop 等待正在运行的周期（含合并轮次）结束后停止。
func (l *Loop) Stop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		<-l.doneCh
		return
	}
	l.stopped = true
	if !l.running {
		l.running = true // 防止 Stop 与 Trigger 竞争
		close(l.doneCh)
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	<-l.doneCh
}

// LastResult 返回最近一次调和的结果与错误（测试可观测）。
func (l *Loop) LastResult() (*Result, error) {
	l.historyMu.Lock()
	defer l.historyMu.Unlock()
	return l.lastResult, l.lastErr
}

// RunCount 返回已执行的调和周期数。
func (l *Loop) RunCount() int {
	l.historyMu.Lock()
	defer l.historyMu.Unlock()
	return l.runCount
}

// WaitIdle 阻塞直到没有正在运行/排队的调和周期。
func (l *Loop) WaitIdle() {
	for {
		l.mu.Lock()
		idle := !l.running && !l.queued
		l.mu.Unlock()
		if idle {
			return
		}
		runtime.Gosched()
	}
}

// loopStoppedError ...
type loopStoppedError struct{}

func (loopStoppedError) Error() string { return "reconcile loop stopped" }

// ErrLoopStopped 在已停止的 Loop 上触发时返回。
var ErrLoopStopped error = loopStoppedError{}
