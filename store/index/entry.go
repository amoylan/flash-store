package index

// type IndexEntryType int

// const (
// 	IndexEntryTypeInt64 IndexEntryType = iota
// 	IndexEntryTypeString
// )

// 索引Key 需要支持Less方法 用于比较大小
// type IndexEntry interface {
// 	Type() IndexEntryType
// 	btree.Item
// }

// ======================== Int64Entry ====================
// type Int64Entry struct {
// 	value int64
// }

// func (i Int64Entry) Less(other btree.Item) bool {
// 	o, ok := other.(Int64Entry)
// 	if !ok {
// 		return false
// 	}
// 	return i.lessThan(o)
// }

// func (i Int64Entry) Type() IndexEntryType {
// 	return IndexEntryTypeInt64
// }

// func (i Int64Entry) lessThan(other Int64Entry) bool {
// 	return i.value < other.value
// }

// ======================== StringEntry ====================
// type StringEntry struct {
// 	value string
// }

// func (i StringEntry) Less(other btree.Item) bool {
// 	o, ok := other.(StringEntry)
// 	if !ok {
// 		return false
// 	}
// 	return i.lessThan(o)
// }

// func (i StringEntry) Type() IndexEntryType {
// 	return IndexEntryTypeString
// }

// func (i StringEntry) lessThan(other StringEntry) bool {
// 	return i.value < other.value
// }
