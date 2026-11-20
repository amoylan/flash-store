package schema

import "fmt"

// FieldDescriptor 是字段级 schema 定义：身份（Num + Name）、类型、
// 索引意图、默认值以及类型相关约束（Decimal 精度、String 最大长度）。
//
// 同时承担两个角色：
//  1. **Builder 内部表示**：Def[T].fields 的元素类型；FieldOption
//     （schema.Indexed / Default / DecimalPrec / StringMax）在注册期修改它。
//  2. **可序列化表示**：KindDescriptor.Fields 携带它，snapshot Header 和
//     WAL broadcast 里流转的就是这份字节。
//
// JSON tag 使 FieldDescriptor 能直接 marshal；registry 的 Builder 链不使用 JSON
// 路径，tag 只是"需要时可用"，平时零开销。
//
// 身份以 Num 为准；Name 是业务可读标签（允许跨版本重命名）。
// Default / Type 是演进规则里的"承诺"，一旦设定不可改（compat check 严格比对）。
//
// Default 不可改的原因：Default 不存在 Row 里，每次 Fill / indexedFieldValue
// 都从当前 Def 拿。如果 master 改了 Default，历史 Row 在下一次 replay 时被
// Fill 出来的字段值就会和之前那次 replay 不一样，下游跨重启漂移。业务要改
// Default 的语义，应该加新字段（新 Num）。
type FieldDescriptor struct {
	Num              int32      `json:"num"`
	Name             FieldName  `json:"name"`
	Type             FieldType  `json:"type"`
	Default          FieldValue `json:"default"`
	Indexed          bool       `json:"indexed,omitempty"`
	DecimalPrecision int32      `json:"decimal_precision,omitempty"`
	StringMaxLen     int        `json:"string_max_len,omitempty"`
}

// KindDescriptor 是某个 Kind 在某一刻的 schema 快照，用于跨进程 / 跨重启传递。
//
// 两个使用场景：
//   - **WAL Schema broadcast**：master 启动时把当前注册的 alive Fields 广播给 slave。
//     此场景下 DroppedFields 恒空——RetiredField 不再喂入 descriptor。
//   - **Snapshot Header.Schemas**：snapshot 写入时刻把 Store.schemasByKind 里
//     **alive + tombstone** 一并落盘，restore 时通过 UpsertSchema 把 tombstone
//     完整恢复——让 fresh 节点也能拿到完整的 Num 历史。
//
// 字段集合：
//   - Fields：当前 schema 里还 alive 的字段（Num→FieldDescriptor）
//   - DroppedFields：历史上曾经存在、现已删除的字段（tombstone）。仅 snapshot Header
//     场景非空；用于兜住"跨版本 Num 复用"的 compat 检查（某 Num 被删除后以不同
//     语义重新加回 → 拒绝）
//
// Version 是 Fields 对应的版本号；DroppedFields 里各字段的原始版本信息不单
// 独保留（只需要 Type + Default 足够做 compat，不做更细粒度回放）。
//
// tombstone 的累积来源（仅 Store 运行期，不来自源码）：
//   - "vanished from alive" diff：每次 upsert 把"老 alive 中消失的 Num"自动迁入 tombstone
//   - snapshot Header restore：把上一代 Store 累积的 tombstone 一次性载入
type KindDescriptor struct {
	Kind          string            `json:"kind"` // Kind.String() 形式："domain.name"
	Version       uint32            `json:"version"`
	Fields        []FieldDescriptor `json:"fields"`
	DroppedFields []FieldDescriptor `json:"dropped_fields,omitempty"`
}

// CheckSchemaCompat 判定从 `old` schema 演进到 `cur`（当前代码注册的）是否兼容。
// 基于 Num 身份做比对——Go struct 字段顺序、Row 位置都与此无关，只有 Num
// 是字段身份锚点。
//
// 允许：
//   - 新增字段（Num 新分配）——读路径走 Default
//   - 删除字段（Num 在 cur 中消失）——历史 Row 里的该 Num 在 Fill 时被丢弃
//   - 改字段的 Name（Num 不变即可，真 rename）
//   - 改字段的 Indexed / DecimalPrecision / StringMaxLen
//
// 禁止：
//   - 同一 Num 的 Type 变化（老 Row 里该 Num 存的是旧类型，新 schema 读出来会
//     类型错配→索引腐败 / Fill 失败）
//   - 同一 Num 的 Default 变化（Default 不存 Row，改了会让历史 Row 跨重启漂移）
//   - 同一 schema 内 Num 重复（业务注册 bug）
//
// 注：Num 复用（某 Num 曾存在、之后被删、再给另一个不同语义字段）由 store
// 级别的多版本缓存联动校验（checkSchemasCompat 枚举全历史），本函数只管相邻
// 两版本。
func CheckSchemaCompat(old, cur KindDescriptor) error {
	if old.Kind != cur.Kind {
		return fmt.Errorf("kind mismatch: snapshot=%s current=%s", old.Kind, cur.Kind)
	}
	oldByNum := make(map[int32]FieldDescriptor, len(old.Fields))
	for _, f := range old.Fields {
		oldByNum[f.Num] = f
	}
	curByNum := make(map[int32]FieldDescriptor, len(cur.Fields))
	for _, f := range cur.Fields {
		if prior, dup := curByNum[f.Num]; dup {
			return fmt.Errorf(
				"kind %s: duplicate num=%d in current schema (%q vs %q)",
				cur.Kind, f.Num, prior.Name, f.Name)
		}
		curByNum[f.Num] = f
	}
	for num, oldF := range oldByNum {
		curF, stillHere := curByNum[num]
		if !stillHere {
			continue // field dropped — allowed
		}
		if oldF.Type != curF.Type {
			return fmt.Errorf(
				"kind %s: num=%d (name %q→%q) type changed: %s → %s (use a new num for the new type)",
				cur.Kind, num, oldF.Name, curF.Name, oldF.Type, curF.Type)
		}
		if !oldF.Default.Equal(curF.Default) {
			return fmt.Errorf(
				"kind %s: num=%d (name %q) default changed: %+v → %+v "+
					"(Default is retroactive; add a new num for new default semantics)",
				cur.Kind, num, curF.Name, oldF.Default, curF.Default)
		}
	}
	return nil
}
