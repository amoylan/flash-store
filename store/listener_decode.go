package store

import (
	"fmt"

	"github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/wal"
)

// Listener 路径上的便利函数：把 wal.Cmd 里的 Row 解回业务形态。
// WalSentListener / WalReplayedListener 的回调里，每个 Cmd 携带 Kind + Row
// （OpSet 时）；业务通常需要把 Row 还原成具体 struct 或 RowView 再做推送/补发。
// 这两个 helper 把"按 Kind 找 Def → Fill / 建 RowView"的 3 行收敛成 1 行，
// 对齐 GetObject / GetView 的写法。
//
// 语义约束：两者只处理 OpSet。遇到 OpDelete（Row 为空）返回 ErrCmdNotSetOp，
// 业务应在外层 switch cmd.Op 时自行走 delete 分支（cmd.Key 仍然有效）。

// DecodeCmd 把 Cmd.Row 通过 Def[T].Fill 回填到 out。
// 仅在 cmd.Op == wal.OpSet 时成功；其他情况返回 ErrCmdNotSetOp。
// kind 未注册 Def[T] 或 T 与注册类型不符时返回 ErrKindNotRegistered。
func DecodeCmd[T any](cmd wal.Cmd, out *T) error {
	if !cmd.IsSet() {
		return errors.ErrCmdNotSetOp
	}
	def, ok := registry.GetDef[T](cmd.Kind)
	if !ok {
		return errors.ErrKindNotRegistered
	}
	if err := def.Fill(cmd.Row, out); err != nil {
		return fmt.Errorf("decode cmd %s/%s: %w", cmd.Kind, cmd.Key, err)
	}
	return nil
}

// DecodeCmdAsView 返回一个只读 RowView，用于只关心若干字段、不想重建完整 struct 的场景
// （撮合/推送消息构造等）。
// kind 只要通过 RegisterDef 注册过即可，不需要 T 匹配——RowView 走 TypedDef。
func DecodeCmdAsView(cmd wal.Cmd) (*registry.RowView, error) {
	if !cmd.IsSet() {
		return nil, errors.ErrCmdNotSetOp
	}
	def, ok := registry.GetTypedDef(cmd.Kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	return registry.NewRowView(def, cmd.Row), nil
}
