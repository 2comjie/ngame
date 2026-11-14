package actor

import (
	"container/heap"
	"time"
)

type actorTimer struct {
	owner    *runner
	next     time.Time
	interval time.Duration
	task     Task
	index    int
	canceled bool
}

var _ Timer = (*actorTimer)(nil)

func (timer *actorTimer) Cancel() {
	r := timer.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if !timer.canceled {
		if timer.index >= 0 {
			heap.Remove(&r.timers, timer.index)
		}
		timer.canceled = true
		timer.task = nil
		r.wakeLoop()
	}
}

func (r *runner) After(delay time.Duration, task Task) (Timer, error) {
	if delay < 0 {
		return nil, ErrInvalidTimer
	}
	return r.addTimer(delay, 0, task)
}

func (r *runner) Every(interval time.Duration, task Task) (Timer, error) {
	if interval <= 0 {
		return nil, ErrInvalidTimer
	}
	return r.addTimer(interval, interval, task)
}

func (r *runner) addTimer(delay, interval time.Duration, task Task) (Timer, error) {
	if task == nil {
		panic("actor: 必须提供定时任务")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state >= stateStopping {
		return nil, ErrStopping
	}
	timer := &actorTimer{
		owner:    r,
		next:     time.Now().Add(delay),
		interval: interval,
		task:     task,
		index:    -1,
	}
	heap.Push(&r.timers, timer)
	r.wakeLoop()
	return timer, nil
}

// timerHeap 由 runner.mu 保护
type timerHeap []*actorTimer

func (h timerHeap) Len() int { return len(h) }

func (h timerHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *timerHeap) Push(value any) {
	timer := value.(*actorTimer)
	timer.index = len(*h)
	*h = append(*h, timer)
}

func (h *timerHeap) Pop() any {
	old := *h
	last := len(old) - 1
	timer := old[last]
	old[last] = nil
	*h = old[:last]
	timer.index = -1
	return timer
}
