package index

import (
	"sync"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/google/btree"
)

// ========================= Item =========================
type Item struct {
	field    schema.FieldValue
	valueSet IndexValueSet
}

func (it *Item) Less(other btree.Item) bool {
	o, ok := other.(*Item)
	if !ok {
		return false
	}
	return it.field.Less(it.field.Type(), o.field)
}

// probeItemPool 缓存 doInsert / doDelete 里 tree.Get 用的临时探针 Item。
// 探针只在调用方栈上短暂存在；典型 row.update（OpSet 触发 N 个 indexed 字段
// 各做一次 Delete+Insert）下，每条 row 直接省掉 2N 次 Item 分配。
//
// 命中 / 未命中两种走法：
//   - tree.Get 返回 hit：探针未被 tree 接管，直接 Put 回池。
//   - tree.Get 未命中：doInsert 把探针留在 tree 里作为新 entry——这条路径不能 Put。
//     此时下次需要从池里再取一个新对象（pool 自动 New），不影响正确性。
var probeItemPool = sync.Pool{
	New: func() any { return &Item{} },
}

func acquireProbe(field schema.FieldValue) *Item {
	p := probeItemPool.Get().(*Item)
	p.field = field
	// valueSet 不在探针场景使用——保持上次释放时的 nil。
	return p
}

func releaseProbe(p *Item) {
	p.field = schema.FieldValue{}
	p.valueSet = nil
	probeItemPool.Put(p)
}

type fieldName struct {
	name      string
	fieldType schema.FieldType
}

// ======================= IndexTree ======================
type IndexTree struct {
	mu         sync.RWMutex
	fieldNames []fieldName
	tree       *btree.BTreeG[*Item]
}

// NewIndexTree 新建一个IndexTree
func NewIndexTree(degree int) *IndexTree {
	return &IndexTree{
		tree: btree.NewG(degree, func(a, b *Item) bool {
			return a.Less(b)
		}),
	}
}

// Insert 插入索引
func (t *IndexTree) Insert(field schema.FieldValue, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.doInsert(field, value)
}

func (t *IndexTree) doInsert(field schema.FieldValue, value string) {
	probe := acquireProbe(field)
	item, ok := t.tree.Get(probe)
	if !ok || item == nil {
		// miss：把探针自身升格为新 entry，不归还池（已被 tree 持有）。
		probe.valueSet = make(IndexValueSet)
		t.tree.ReplaceOrInsert(probe)
		item = probe
	} else {
		releaseProbe(probe)
	}
	item.valueSet[value] = struct{}{}
}

// Delete 删除索引
func (t *IndexTree) Delete(field schema.FieldValue, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.doDelete(field, value)
}

func (t *IndexTree) doDelete(field schema.FieldValue, value string) {
	probe := acquireProbe(field)
	item, ok := t.tree.Get(probe)
	releaseProbe(probe)
	if !ok || item == nil {
		return
	}
	delete(item.valueSet, value)
	if len(item.valueSet) == 0 {
		t.tree.Delete(item)
	}
}

// Update 更新索引
func (t *IndexTree) Update(old, new schema.FieldValue, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.doDelete(old, value)
	t.doInsert(new, value)
}

// Size 返回索引数量
func (t *IndexTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tree.Len()
}

// Lookup 查找索引
func (t *IndexTree) Lookup(ranges Ranges) (IndexValueSet, error) {
	var result IndexValueSet
	var btreeWrapper = NewBTreeWrapper(t.tree, func(a, b *Item) bool {
		return a.Less(b)
	})
	itemBuilder := func(field schema.FieldValue) *Item {
		return &Item{field: field}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, rng := range ranges {
		var tmp = IndexValueSet{}
		collect := func(item *Item) {
			for m := range item.valueSet {
				tmp[m] = struct{}{}
			}
		}
		err := btreeWrapper.Iterate(rng, itemBuilder, collect)
		if err != nil {
			return nil, err
		}
		result = result.Merge(tmp)
	}
	return result, nil
}

// ======================= BTreeWrapper =====================
type BTreeWrapper[T any] struct {
	tree *btree.BTreeG[T]
	less btree.LessFunc[T]
}

func NewBTreeWrapper[T any](
	tree *btree.BTreeG[T],
	less btree.LessFunc[T],
) *BTreeWrapper[T] {
	return &BTreeWrapper[T]{
		tree: tree,
		less: less,
	}
}

func (w BTreeWrapper[T]) Iterate(
	rng Range,
	itemBuilder func(schema.FieldValue) T,
	iter func(T)) error {
	itemEquals := func(a, b T) bool {
		return !w.less(a, b) && !w.less(b, a)
	}
	// 有左边界
	if rng.Left != nil {
		leftItem := itemBuilder(rng.Left.Field)
		switch rng.Left.Comparator {
		case GreaterThan:
			if rng.Right == nil {
				w.tree.DescendGreaterThan(leftItem, func(item T) bool {
					iter(item)
					return true
				})
			} else {
				rightItem := itemBuilder(rng.Right.Field)
				switch rng.Right.Comparator {
				case LessThan, LessThanOrEqualsTo:
					w.tree.DescendRange(rightItem, leftItem, func(item T) bool {
						// 不包含右边界时，不收集数据
						if itemEquals(item, rightItem) && rng.Right.Comparator == LessThan {
							return true
						}
						iter(item)
						return true
					})
				default:
					return ErrInvalidRange
				}
			}
		case GreaterThanOrEqualsTo:
			if rng.Right == nil {
				w.tree.AscendGreaterOrEqual(leftItem, func(item T) bool {
					iter(item)
					return true
				})
			} else {
				rightItem := itemBuilder(rng.Right.Field)
				switch rng.Right.Comparator {
				case LessThan:
					w.tree.AscendRange(leftItem, rightItem, func(item T) bool {
						iter(item)
						return true
					})
				case LessThanOrEqualsTo:
					w.tree.AscendGreaterOrEqual(leftItem, func(item T) bool {
						// 当发现超过right时，终止迭代
						if w.less(rightItem, item) {
							return false
						}
						iter(item)
						return true
					})
				default:
					return ErrInvalidRange
				}
			}
		default:
			return ErrInvalidRange
		}
	} else if rng.Right != nil { // 无左边界，有右边界
		rightItem := itemBuilder(rng.Right.Field)
		switch rng.Right.Comparator {
		case LessThan:
			w.tree.AscendLessThan(rightItem, func(item T) bool {
				iter(item)
				return true
			})
		case LessThanOrEqualsTo:
			w.tree.DescendLessOrEqual(rightItem, func(item T) bool {
				iter(item)
				return true
			})
		default:
			return ErrInvalidRange
		}
	}
	return nil
}
