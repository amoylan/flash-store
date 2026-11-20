package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/log"
)

type Applier interface {
	Apply(ctx context.Context, wal Wal, meta Meta) error
}

type Replayer struct {
	reader  Reader
	applier Applier
}

func NewReplayer(reader Reader, applier Applier) *Replayer {
	return &Replayer{
		reader:  reader,
		applier: applier,
	}
}

type ReplayMode int

const (
	ReplayToLatest ReplayMode = iota // 重放到最新的offset
	ReplayForever                    // 重放到当前WAL末尾后继续监听新记录并重放，直到上下文被取消
)

type ReplayOptions struct {
	StartOffset Offset
	Mode        ReplayMode
	// DedupWindowSize 控制去重滑动窗口的大小，0 表示使用默认值（DefaultDedupWindowSize）。
	// 去重默认开启，无法关闭——相同 seq 的消息（Kafka 客户端重试写入导致）会被自动跳过。
	DedupWindowSize int64
	// SeqDedup 可选，从 snapshot 恢复的去重器实例。
	// 非 nil 时直接使用，忽略 DedupWindowSize；nil 时按 DedupWindowSize 新建。
	SeqDedup *SeqDeduplicator
}

// Run 执行回放循环，阻塞直到回放结束（追到最新 / ctx 取消 / 遇到不可恢复错误）。
// 调用方应在独立 goroutine 中调用 Run，以便通过 ctx 控制生命周期。
func (r *Replayer) Run(ctx context.Context, opts ReplayOptions) error {
	err := r.reader.Seek(ctx, opts.StartOffset)
	if err != nil {
		return err
	}

	// 优先使用外部注入的去重器（从 snapshot 恢复），否则新建空白实例
	dedup := opts.SeqDedup
	if dedup == nil {
		dedup = NewSeqDeduplicator(opts.DedupWindowSize)
	}

	defer func() {
		log.Info("replay finished")
	}()

	for {
		raw, err := r.reader.Next(ctx)
		if err != nil {
			if err == ErrEndOfWAL {
				// 当前无数据可读（分区为空 / Seek 超过 HWM）
				if opts.Mode == ReplayToLatest {
					return nil
				}
				// ReplayForever：短暂等待后重试，同时响应 ctx 取消
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			if err == ErrReaderClosed {
				log.Info("reader closed, stopping replay")
				return nil
			}
			log.Error("reader.Next error, stopping replay", log.NamedError("err", err))
			return err
		}
		if raw == nil {
			continue
		}

		meta := Meta{
			Time:   raw.Time,
			Offset: raw.Offset,
			Header: raw.Header,
		}

		// 去重检查：利用 header 中已解析的 seq，无需提前 decode
		if dedup.CheckAndMark(raw.Header.Seq) {
			log.Warn("duplicate wal seq detected, skipping",
				log.Int64("seq", raw.Header.Seq),
				log.Int64("offset", raw.Offset))
			// 重复记录若恰好是分区最后一条（如多 master 并发写产生的队尾重复），
			// 同样要在这里结束 ReplayToLatest——否则跳过 AtLatest 判断后回到 Next，
			// 会在 HWM 处阻塞等新消息直到 ctx 超时。
			if reachedLatest(raw, opts.Mode) {
				return nil
			}
			continue
		}

		// 解码与 apply 的失败是数据完整性级别的问题：
		//   - SnapshotSlave 跳过坏 WAL 会让内存与 WAL 永久错位，下次 CreateSnapshot 会把空洞固化进新 snapshot
		//   - Master + Listener 跳过会导致 OnWalReplayed 永不触发，下游 resend 永久缺失
		// 因此这些错误必须冒泡给 caller，由 caller 决定是 fatal、retry 还是回滚——不能在这里静默 continue。
		codec, ok := GetWalCodec(raw.Header.Format)
		if !ok {
			log.Error("no codec registered for format, stopping replay",
				log.String("format", string(raw.Header.Format)),
				log.Int64("offset", raw.Offset))
			return fmt.Errorf("wal replay: no codec registered for format %q at offset %d", raw.Header.Format, raw.Offset)
		}
		var wal *Wal
		wal, err = codec.Decode(raw.Data)
		if err != nil {
			log.Error("failed to decode wal record, stopping replay",
				log.Int64("offset", raw.Offset),
				log.NamedError("err", err))
			return fmt.Errorf("wal replay: decode at offset %d: %w", raw.Offset, err)
		}

		err = r.applier.Apply(ctx, *wal, meta)
		if err != nil {
			log.Error("failed to apply wal record, stopping replay",
				log.Int64("offset", raw.Offset),
				log.NamedError("err", err))
			return fmt.Errorf("wal replay: apply at offset %d: %w", raw.Offset, err)
		}

		// 已追上最新记录。ReplayForever 不在此结束——继续调用 Next，
		// 底层 FetchMessage 会自动阻塞等待新消息。
		if reachedLatest(raw, opts.Mode) {
			return nil
		}
	}
}

// reachedLatest 判断是否应结束本轮 replay：仅 ReplayToLatest 模式下、且当前记录
// 已是 WAL 末尾时为 true。业务 apply 路径与 dup-skip 路径共用——两条路径都必须在
// 追到末尾时收尾，否则 reader.Next 会在 HWM 处阻塞到 ctx 超时。
func reachedLatest(raw *ReadRecord, mode ReplayMode) bool {
	return raw.AtLatest && mode == ReplayToLatest
}
