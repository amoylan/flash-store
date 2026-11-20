package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/amoylan/flash-store/store/config"
	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/log"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal"
	walconfig "github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
)

// validateSchemaConfig 做 replay-time 的第二道 compat 检查：
// 新广播的 schema 相对 store 累积的 alive+tombstone 是否为兼容演进。
// slave 没有代码 Def，这是在 slave 上能做的唯一防御；master 启动时 snapshot
// header 的检查是第一道（master 自有代码 Def 作为对比目标）。
//
// 广播只带 alive（master 端 DescriptorOf 不导 DroppedFields），所以这里能做的
// 就是 newDesc.Fields 自身的 compat：
//   - vs alive：拦截同 Num 改 Type/Default。
//   - vs tombstone：拦截"某 Num 曾在历史里被删、现在又以不同 Type/Default 加回"。
func validateSchemaConfig(st *Store, newSchemas []schema.KindDescriptor) error {
	for _, newDesc := range newSchemas {
		kind, err := registry.ParseKindFromString(newDesc.Kind)
		if err != nil {
			return fmt.Errorf("invalid kind %q: %w", newDesc.Kind, err)
		}
		st.schemasByKindMu.RLock()
		reg := st.schemasByKind[kind]
		st.schemasByKindMu.RUnlock()
		if reg == nil {
			continue // 新增 Kind，无对比基准
		}
		if err := checkCompatAgainstRegistry(reg, newDesc); err != nil {
			return err
		}
	}
	return nil
}

// replayer 与 worker 一一绑定，负责从 wal 中读取事件并应用到 store 中
type replayer struct {
	worker *worker
	reader wal.Reader
}

func newReplayer(worker *worker, walConfig walconfig.Config) (*replayer, error) {
	// partition / Dir 子目录的处理已经下沉到各 Source.NewReader 实现内部，
	// replayer 只把 worker.id 当 partition 透传——与 newWorker 透传 partition
	// 给 NewWriter 对称。

	readerSource, err := source.GetSource(walConfig.Reader.Source)
	if err != nil {
		return nil, err
	}

	reader, err := readerSource.NewReader(walConfig.Reader, worker.id)
	if err != nil {
		return nil, err
	}
	return &replayer{
		worker: worker,
		reader: reader,
	}, nil
}

// replayApplier 包装 replayer，实现 wal.Applier 接口。
type replayApplier struct {
	*replayer
	applyFunc ApplyFunc // 业务 WAL 的 apply 链（含 applyToStore + listener + controller）
}

func (r *replayApplier) Apply(ctx context.Context, w wal.Wal, meta wal.Meta) (err error) {
	// Seq 计数器无论 apply 成功与否都不能回退——本角色将来升 Master 后写新 WAL 必须 > 所有已读 seq。
	// Seq 和 WalType 从 meta.Header 读取，不再存在于 Wal struct 里。
	r.replayer.worker.advanceSeq(meta.Header.Seq)

	// maxWalOffset 仅在 case 分支成功后推进——它是 snapshot.WalOffset 的来源，
	// 若 apply 失败也推进会导致下次 snapshot 把"声明处理但实际丢失"的 offset 写下去，
	// restore 后从 (offset+1) 起 replay 就永久跳过了那条 WAL。
	defer func() {
		if err == nil {
			r.replayer.worker.advanceMaxWalOffset(int64(meta.Offset))
		}
	}()

	switch meta.Header.WalType {
	case wal.WalTypeSchemaConfig:
		// Schema 广播 WAL：master 广播"当前版本"（每 Kind 一条），slave 把新
		// 版本增量追加到本地历史缓存（AddSchema）。仅由 worker 0 传递（全局配置）。
		// 新 schema 相对 **全部历史版本** 做第二道 compat 检查——如果 master 把
		// 某字段类型改错 or 跨版本复用 Num（compat 不允许），这里 panic 阻止 slave
		// 继续把坏 schema 写进下次 snapshot；master 启动时的 snapshot 侧检查
		// 是第一道防线，这里是 replay-time 的补充。
		st := r.replayer.worker.store
		if err := validateSchemaConfig(st, w.Schemas); err != nil {
			// schema 演进不兼容是代码级 bug，不是数据问题 → panic 立即暴露。
			panic(fmt.Sprintf("schema broadcast incompatible with history: %v", err))
		}
		for _, d := range w.Schemas {
			st.UpsertSchema(d)
		}

		return nil

	default: // WalTypeBusiness
		if r.applyFunc == nil {
			return nil
		}
		return r.applyFunc(meta, w.Events)
	}
}

// start 在本 worker 对应分区上启动 WAL 回放。ctx 取消时 ReplayForever 模式会结束。
// startOffsets 由 Store 层注入（snapshot 恢复后的断点 offsets），为 nil 时回退到 ack 水位。
//
// reader 由 newReplayer 创建并归本方法管理；start 返回（无论正常结束还是 ctx 取消）时
// 通过 defer 关闭 reader——避免 StopReplay → CreateSnapshot → StartReplay 循环每次创建
// 新 reader 而旧 reader 的 FetchMessage goroutine + 连接 + 文件句柄全部泄漏。
func (r *replayer) start(ctx context.Context, opts ReplayOptions, startOffsets map[int]wal.Offset) error {
	defer func() {
		if err := r.reader.Close(); err != nil {
			log.Warn("replayer: reader close failed",
				log.Int("worker_id", r.worker.id),
				log.NamedError("err", err))
		}
	}()
	partition := r.worker.id
	store := r.worker.store

	// 确定 replay 起始 offset：优先使用 snapshot 注入的 startOffsets；否则从 0 开始。
	var startOffset wal.Offset
	if startOffsets != nil {
		if off, ok := startOffsets[partition]; ok {
			startOffset = off
		}
	}

	// --- 业务 WAL apply 链 ---
	// 所有 WAL 直接 apply；快照任意时刻安全。
	var apply ApplyFunc = func(meta wal.Meta, events []wal.Event) error {
		if opts.ApplyToStore {
			if err := store.applyEvents(events); err != nil {
				return err
			}
		}
		return nil
	}

	if opts.Listener != nil {
		listener := opts.Listener
		prev := apply
		// 按 WAL 顺序 apply 成功后立即触发 OnWalReplayed（无去重/补发判定）。
		wm := r.worker.metrics
		apply = func(meta wal.Meta, events []wal.Event) error {
			if err := prev(meta, events); err != nil {
				return err
			}
			lstart := time.Now()
			listener.OnWalReplayed(ctx, WalRecord{
				Partition: partition,
				Events:    events,
				Offset:    meta.Offset,
			})
			wm.ObserveListenerWalReplayed(time.Since(lstart))
			return nil
		}
	}

	if opts.Controller != nil {
		apply = opts.Controller(partition, apply)
	}

	applier := &replayApplier{
		replayer:  r,
		applyFunc: apply,
	}
	walReplayer := wal.NewReplayer(r.reader, applier)
	// Run 是阻塞方法，在调用方 goroutine 中直接执行回放循环；
	// Store.StartReplay 已为每个 worker 启动独立 goroutine，此处无需再起。
	seqDedup := r.worker.seqDedup
	runErr := walReplayer.Run(ctx, wal.ReplayOptions{
		StartOffset: startOffset,
		Mode:        opts.Mode,
		SeqDedup:    seqDedup,
	})
	return runErr
}

type ApplyFunc func(
	meta wal.Meta,
	events []wal.Event,
) error

// ReplayController 可按 partition 包装 apply（例如加锁、暂停回放）。
type ReplayController func(partition int, apply ApplyFunc) ApplyFunc

// ReplayOptions Store 层回放选项，映射到 wal.ReplayOptions。
type ReplayOptions struct {
	// Mode：ReplayToLatest 追到当前日志末尾即退出；ReplayForever 持续跟随直到 ctx 取消。
	Mode wal.ReplayMode
	// ApplyToStore 为 true 时，将回放的 Events 经 applyEvents 写入本机 shard（含 TTL 时间轮注册）；为 false 时仅走下方 Listener/Controller，不修改内存。
	// 为 true 时 StartReplay 仅允许首次成功：再次调用返回 ErrReplayApplyToStoreAlreadyStarted。
	// 且 Slave 必须 Mode==ReplayForever；Master 与 RoleUndefined 必须 Mode==ReplayToLatest，否则返回 ErrReplayInvalidModeForApplyToStore。
	ApplyToStore bool
	// Controller 可选，在 apply 链最外层包装（含 Listener 与内存 apply）。
	Controller ReplayController
	// Listener 可选；在 apply 链内层成功之后调用（含 ApplyToStore 写内存成功后）。
	Listener WalReplayedListener
}

// StartReplay 为每个 worker（WAL 分区）启动一条独立回放协程，共享同一份 ReplayOptions；
// 各分区起始 offset 为 0（或 snapshot 注入的断点）。
// 启动期"snapshot 恢复后接续 replay"的场景请使用 RestoreAndStartReplay，不要直接调本方法。
func (s *Store) StartReplay(ctx context.Context, options ReplayOptions) error {
	return s.startReplay(ctx, options, nil)
}

// startReplay 内部入口：startOffsets 由调用方注入（snapshot 断点 offsets），为 nil 时走 ack 水位逻辑。
// ApplyToStore 时先 CAS 再创建 replayer，避免并发两次调用都建出一批 Reader 再竞争失败；若 CAS 之后 newReplayer 失败，标志位仍为已占用（通常启动期应保证 Reader 可用）。
func (s *Store) startReplay(ctx context.Context, options ReplayOptions, startOffsets map[int]wal.Offset) error {
	if options.ApplyToStore {
		if err := s.validateReplayApplyOptions(options); err != nil {
			return err
		}
		if !s.replayApplyToStoreStarted.CompareAndSwap(false, true) {
			return storeerrors.ErrReplayApplyToStoreAlreadyStarted
		}
	}

	// 第一次 Start 时 freeze WalSentListener 注册——此后 AddWalSentListener 返回错误。
	// 多次 Start（StopReplay 后再 Start）安全：freezeListeners 是幂等的。
	s.freezeListeners()

	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	replayCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.replayCancel = cancel
	s.replayDone = done
	// 清空上一轮 replay 残留的 err 标志，本轮 worker 失败再 CAS 写入。
	s.replayErr.Store(nil)

	var wg sync.WaitGroup
	for i := range s.workers {
		worker := s.workers[i]
		rp, err := newReplayer(worker, s.config.Wal)
		if err != nil {
			cancel()
			log.Error("failed to create replayer", log.Int("worker_id", worker.id), log.NamedError("err", err))
			return err
		}
		wid := worker.id
		wg.Add(1)
		go func(rpl *replayer, id int) {
			defer wg.Done()
			er := rpl.start(replayCtx, options, startOffsets)
			if er == nil || errors.Is(er, context.Canceled) {
				return
			}
			// partition 在追日志的过程中异常退出（解码失败 / apply 失败 / reader 故障 等）。
			// 记录首个 err 并主动 cancel 全局 ctx，让其他 worker 一并退出——
			// 否则 caller 看到的 isReplayRunning 仍为 true（其他分区还在跑），
			// 一直要等 StopReplay 才停，期间该分区状态已停滞。
			log.Error("replay failed for worker", log.Int("worker_id", id), log.NamedError("err", er))
			s.replayErr.CompareAndSwap(nil, &er)
			cancel()
		}(rp, wid)
	}

	// 监控所有 replay goroutine 退出后关闭 done channel
	go func() {
		wg.Wait()
		close(done)
	}()

	return nil
}

// WaitReplay 阻塞直到当前所有 replay goroutine 退出（ReplayToLatest 追完即退出）。
// 若 replay 未运行则立即返回。不会取消 replay，仅等待其自然结束。
func (s *Store) WaitReplay() {
	s.replayMu.Lock()
	done := s.replayDone
	s.replayMu.Unlock()
	if done != nil {
		<-done
	}
}

// StopReplay 停止当前正在运行的 replay，阻塞直到所有 replay goroutine 退出。
// 若 replay 未运行则立即返回。
func (s *Store) StopReplay() {
	s.replayMu.Lock()
	cancel := s.replayCancel
	done := s.replayDone
	s.replayMu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}

	s.replayMu.Lock()
	s.replayCancel = nil
	s.replayDone = nil
	// 重置标志位，允许下次 StartReplay with ApplyToStore
	s.replayApplyToStoreStarted.Store(false)
	s.replayMu.Unlock()
	// StopReplay 是 caller 主动停止 → 清掉 err，避免 ctx.Canceled 之外的失败
	// 在重启 replay 前继续阻挡 CreateSnapshot。新 startReplay 会重新设 nil；
	// 若用户在 stop 后未 start 就 CreateSnapshot，由 isReplayRunning 路径接管。
	s.replayErr.Store(nil)
}

// ReplayErr 返回任一 worker goroutine 在最近一轮 replay 中遇到的首个非 ctx-canceled 错误。
// 健康停止（追到末尾 / ctx 正常取消）返回 nil。SnapshotSlave 的 CreateSnapshot 用它在
// 前置检查里区分"replay 已结束、可以打 snapshot"和"replay 出错、partition 状态停滞"。
func (s *Store) ReplayErr() error {
	if p := s.replayErr.Load(); p != nil {
		return *p
	}
	return nil
}

// isReplayRunning 检查 replay 是否正在运行（用于 CreateSnapshot 前置检查）。
func (s *Store) isReplayRunning() bool {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if s.replayDone == nil {
		return false
	}
	select {
	case <-s.replayDone:
		return false // goroutine 已全部退出
	default:
		return true
	}
}

func (s *Store) validateReplayApplyOptions(o ReplayOptions) error {
	switch s.config.Role {
	case config.RoleSnapshotSlave, config.RoleReadOnlySlave:
		if o.Mode != wal.ReplayForever {
			return fmt.Errorf("slave with ApplyToStore requires wal.ReplayForever: %w", storeerrors.ErrReplayInvalidModeForApplyToStore)
		}
	default:
		// Master、RoleUndefined 及其它：按 Master 策略要求追到最新即停
		if o.Mode != wal.ReplayToLatest {
			return fmt.Errorf("master with ApplyToStore requires wal.ReplayToLatest: %w", storeerrors.ErrReplayInvalidModeForApplyToStore)
		}
	}
	return nil
}
