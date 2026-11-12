package actor

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
