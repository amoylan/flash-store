package errors

import (
	"errors"
	"fmt"
)

type StoreError struct {
	err error
	msg string
}

func newError(err error) *StoreError {
	return &StoreError{err, ""}
}

func WrapError(err *StoreError, format string, a ...any) *StoreError {
	return &StoreError{err, fmt.Sprintf(format, a...)}
}

func (e *StoreError) Error() string {
	return fmt.Sprintf("%s, reason: %s", e.msg, e.err.Error())
}

func (e *StoreError) Unwrap() error {
	return e.err
}

// ErrKindNotRegistered 当领域名称未注册时返回该错误
var ErrKindNotRegistered = newError(errors.New("kind not registered"))

// ErrCodecIDNotRegistered 当类型ID未注册时返回该错误
var ErrCodecIDNotRegistered = newError(errors.New("codecID not registered"))

// ErrTypeAssertionFailed 当类型断言失败时返回该错误
var ErrTypeAssertionFailed = newError(errors.New("type assertion failed"))

// ErrTypeNotMatch 当尝试get的对象，kind 与实际存储的不匹配时返回该错误
var ErrTypeNotMatch = newError(errors.New("type not match"))

// ErrKeyNotFound 当尝试get的对象不存在时返回该错误
var ErrKeyNotFound = newError(errors.New("key not found"))

// ErrShardNotFound 当尝试get的shard不存在时返回该错误
var ErrShardNotFound = newError(errors.New("shard not found"))

// ErrReplayApplyToStoreAlreadyStarted 在 ApplyToStore==true 时，StartReplay 仅允许成功调用一次，再次调用返回该错误。
var ErrReplayApplyToStoreAlreadyStarted = errors.New("StartReplay with ApplyToStore already started")

// ErrStoreAlreadyStarted 在 Store 启动后（StartReplay / RestoreAndStartReplay 调用过）
// 再次 AddWalSentListener 返回该错误——listener 注册必须在 Start 之前。
var ErrStoreAlreadyStarted = errors.New("store already started, listener registration not allowed")

// ErrTxAlreadyCommitted 同一个 *Tx 第二次调用 Commit 返回该错误。
// Tx 不是 reusable 对象——一旦 Commit 成功就不应再操作；Rollback 后亦不应再 Commit。
var ErrTxAlreadyCommitted = errors.New("tx already committed")

// ErrReplayInvalidModeForApplyToStore 在 ApplyToStore==true 时，当前 Role 所要求的 Replay Mode 不满足（Slave 须 ReplayForever；Master/RoleUndefined 须 ReplayToLatest）。
var ErrReplayInvalidModeForApplyToStore = errors.New("invalid replay Mode with ApplyToStore for store role")

// ErrSnapshotNotSnapshotSlave CreateSnapshot 仅 RoleSnapshotSlave 可调用。
var ErrSnapshotNotSnapshotSlave = errors.New("CreateSnapshot is only allowed on RoleSnapshotSlave")

// ErrSnapshotReplayRunning CreateSnapshot 时 replay 仍在运行，必须先停止。
var ErrSnapshotReplayRunning = errors.New("cannot create snapshot while replay is running")

// ErrSnapshotReplayErrored 上一轮 replay 中至少有一个 worker 异常退出（非 ctx 取消），
// 该 partition 的状态已停滞，不允许直接打 snapshot——必须 StopReplay 重置后重启 replay。
var ErrSnapshotReplayErrored = errors.New("cannot create snapshot: previous replay errored, partition state stale")

// ErrSnapshotShardCountMismatch 恢复时 snapshot 的 shard 数量与当前 Store 不匹配。
var ErrSnapshotShardCountMismatch = errors.New("snapshot shard count does not match store")

// ErrSnapshotWorkerCountMismatch 恢复时 snapshot 的 worker 数量与当前 Store 不匹配。
var ErrSnapshotWorkerCountMismatch = errors.New("snapshot worker count does not match store")

// ErrCmdNotSetOp 当 DecodeCmd / DecodeCmdAsView 遇到非 OpSet 的 cmd（如 OpDelete）时返回。
// 业务侧看到这个错误通常意味着应走对应的 delete 分支处理，而不是尝试解码 Row。
var ErrCmdNotSetOp = errors.New("cmd op is not OpSet (no Row to decode)")
