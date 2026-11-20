package utils

import (
	"errors"
	"sync"
	"time"
)

type roundItem[T comparable] struct {
	rounds int
	item   T
}

type slot[T comparable] struct {
	sync.Mutex
	items []*roundItem[T]
}

func (s *slot[T]) AddItem(item *roundItem[T]) {
	s.Lock()
	defer s.Unlock()
	s.items = append(s.items, item)
}

func (s *slot[T]) CollectZeroRoundItems() []T {
	s.Lock()
	defer s.Unlock()
	var remainIndex int
	var ids []T
	for i := 0; i < len(s.items); i++ {
		t := s.items[i]
		if t.rounds > 0 {
			t.rounds--
			s.items[remainIndex] = t
			remainIndex++
		} else {
			ids = append(ids, t.item)
			s.items[i] = nil // 解除引用 使其可被垃圾回收
		}
	}
	s.items = s.items[:remainIndex]
	return ids
}

type itemsProcessor[T comparable] func(items []T)

type TimeWheel[T comparable] struct {
	intervalInSeconds int
	slots             []*slot[T]
	lastTriggeredSlot int
	ticker            *time.Ticker
	numSlots          int // slot数量
	stopCh            chan struct{}
	stopOnce          sync.Once

	_totalSecondsPerRound int

	// 批量item 的处理函数，当每一个slot通过 CollectZeroRoundItems 收集了一批items 后的处理逻辑
	batchItemProcessor itemsProcessor[T]
}

func NewTimeWheel[T comparable](
	name string,
	intervalInSeconds int,
	numSlots int,
	batchItemProcessor itemsProcessor[T]) *TimeWheel[T] {
	tw := &TimeWheel[T]{
		intervalInSeconds:     intervalInSeconds,
		slots:                 make([]*slot[T], numSlots),
		ticker:                time.NewTicker(time.Second * time.Duration(intervalInSeconds)),
		numSlots:              numSlots,
		stopCh:                make(chan struct{}),
		_totalSecondsPerRound: intervalInSeconds * numSlots,

		batchItemProcessor: batchItemProcessor,
	}
	for i := 0; i < numSlots; i++ {
		tw.slots[i] = &slot[T]{}
	}
	tw.lastTriggeredSlot = tw.slotByTime(time.Now().Unix() - int64(tw.intervalInSeconds))
	go tw.startLoop()
	return tw
}

// 触发时间已经超过当前时间
var ErrTriggerTimeOutdated = errors.New("trigger time outdated")

// AddItem 添加任务
func (tw *TimeWheel[T]) AddItem(triggerTime time.Time, item T) error {
	now := time.Now().Unix()
	triggerSec := triggerTime.Unix()
	// 如果触发时间已经过去，在下一个 slot 触发
	if triggerSec < now {
		triggerSec = now + int64(tw.intervalInSeconds)
	}
	rounds := int(triggerSec-now) / tw._totalSecondsPerRound
	slot := tw.slotByTime(triggerSec)
	task := &roundItem[T]{rounds: rounds, item: item}
	tw.slots[slot].AddItem(task)
	return nil
}

func (tw *TimeWheel[T]) slotByTime(ts int64) int {
	return int(ts/int64(tw.intervalInSeconds)) % tw.numSlots
}

func (tw *TimeWheel[T]) slot(index int) int {
	if index < tw.numSlots {
		return index
	}
	return index % tw.numSlots
}

func (tw *TimeWheel[T]) startLoop() {
loop:
	for {
		select {
		case <-tw.stopCh:
			break loop
		case <-tw.ticker.C:
			end := tw.slotByTime(time.Now().Unix())
			start := tw.lastTriggeredSlot + 1
			if end < start {
				end += tw.numSlots
			}
			for i := start; i < end; i++ {
				slot := tw.slot(i)
				items := tw.slots[slot].CollectZeroRoundItems()
				if len(items) > 0 {
					tw.batchItemProcessor(items)
				}
				tw.lastTriggeredSlot = slot
			}
		}
	}
}

// Stop 停止时间轮：释放 Ticker，并关闭 stopCh 使 startLoop 退出。可多次调用，仅首次生效。
func (tw *TimeWheel[T]) Stop() {
	tw.stopOnce.Do(func() {
		if tw.ticker != nil {
			tw.ticker.Stop()
		}
		close(tw.stopCh)
	})
}
