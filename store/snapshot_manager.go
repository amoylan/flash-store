package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amoylan/flash-store/store/config"
	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/log"
	"github.com/amoylan/flash-store/store/metrics"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/snapshot"
	snapconfig "github.com/amoylan/flash-store/store/snapshot/config"
	"github.com/amoylan/flash-store/store/snapshot/source"
	"github.com/amoylan/flash-store/store/wal"
)

// ======================== CreateSnapshot ========================

// CreateSnapshot 创建全量快照并写入底层存储。仅 Slave 可调用，且 replay 必须已停止。
//
// 调用方需自行保证：
//  1. 调用前已通过 StopReplay 停止回放（本方法会校验，未停止则报错）
//  2. 调用期间无并发写入（Slave 不写 WAL，停止 replay 后无内存变更）
//  3. 调用完成后按需重新 StartReplay / RestoreAndStartReplay
//
// Writer、Codec 从 s.config.Snapshot 自动构造，无需调用方传入。
// 返回生成的 version 标识，供日志/审计使用。
func (s *Store) CreateSnapshot(ctx context.Context) (version string, err error) {
	totalStart := time.Now()
	// 通过 named return err 在 defer 中判定结果分类——"aborted" 暂时与 "error" 合并，
	// 业务关心的"是否产出可用版本"用返回值即可区分。
	defer func() {
		s.metrics.ObserveSnapshotPhase(metrics.SnapshotPhaseTotal, time.Since(totalStart))
		result := metrics.SnapshotResultOK
		if err != nil {
			result = metrics.SnapshotResultError
		}
		s.metrics.IncSnapshotCreate(result)
	}()
	// --- 前置检查 ---
	if s.config.Role != config.RoleSnapshotSlave {
		return "", storeerrors.ErrSnapshotNotSnapshotSlave
	}
	if s.isReplayRunning() {
		return "", storeerrors.ErrSnapshotReplayRunning
	}
	// 上一轮 replay 是否有 worker 异常退出。即便 isReplayRunning==false（所有 worker 都已退出），
	// 只要其中一个是异常退出而非 ctx 正常取消，partition 状态就已停滞，
	// 此时打 snapshot 等于把空洞固化下来——必须先 StopReplay 重置后重启 replay。
	if err := s.ReplayErr(); err != nil {
		return "", fmt.Errorf("%w: %v", storeerrors.ErrSnapshotReplayErrored, err)
	}

	snapConf := s.config.Snapshot
	name := snapConf.Name

	// --- 构造 writer ---
	writer, err := newSnapshotWriter(snapConf)
	if err != nil {
		return "", fmt.Errorf("snapshot: create writer: %w", err)
	}
	defer writer.Close()

	// --- 构造 codec ---
	codec, ok := snapshot.GetCodec(snapConf.Writer.Format)
	if !ok {
		return "", fmt.Errorf("snapshot: codec %q not registered", snapConf.Writer.Format)
	}

	// --- 0. 创建新版本 ---
	version, err = writer.BeginWrite(ctx, name)
	if err != nil {
		return "", fmt.Errorf("snapshot: begin write: %w", err)
	}

	log.Info("snapshot: create started", log.String("name", name), log.String("version", version))

	seg := func(kind string, id int) snapshot.Segment {
		return snapshot.Segment{Name: name, Version: version, Kind: kind, ID: id}
	}

	// totalBytes 在并发协程里累加，atomic 即可。
	var totalBytes int64

	headerStart := time.Now()
	// --- 1. 写 Header（始终 JSON 编码，包含 format 字段供 restore 时选择 codec） ---
	header := snapshot.Header{
		Version:    1,
		Format:     snapConf.Writer.Format,
		CreatedAt:  time.Now(),
		NumShards:  len(s.shards),
		NumWorkers: len(s.workers),
		// schemasByKind 来源：Master 从 registry 自动初始化、Slave 从 Master
		// 广播接收。SnapshotSlave 创建 snapshot 时从这里拿到 Master 广播过来的
		// schema 写入 Header——不走 registry.AllDescriptors()，因为 Slave 没
		// 业务 struct 注册、registry 上是空的。
		Schemas: s.KnownSchemas(),
	}
	headerBytes, encErr := json.Marshal(&header)
	if encErr != nil {
		_ = writer.Abort(ctx, name, version)
		return "", fmt.Errorf("snapshot: encode header: %w", encErr)
	}
	if wErr := writer.WriteSegment(ctx, seg("header", 0), headerBytes); wErr != nil {
		_ = writer.Abort(ctx, name, version)
		return "", fmt.Errorf("snapshot: write header: %w", wErr)
	}
	atomic.AddInt64(&totalBytes, int64(len(headerBytes)))
	s.metrics.ObserveSnapshotPhase(metrics.SnapshotPhaseHeader, time.Since(headerStart))

	// --- 2. 并发写 Shard + Worker ---
	shardStart := time.Now()
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(e error) {
		errOnce.Do(func() { firstErr = e })
	}

	// 写 Shards
	for _, sh := range s.shards {
		wg.Add(1)
		go func(sh *shard) {
			defer wg.Done()
			snap, snapErr := s.snapshotShard(sh)
			if snapErr != nil {
				setErr(fmt.Errorf("snapshot: dump shard %d: %w", sh.id, snapErr))
				return
			}
			data, encErr := codec.EncodeShard(snap)
			if encErr != nil {
				setErr(fmt.Errorf("snapshot: encode shard %d: %w", sh.id, encErr))
				return
			}
			if wErr := writer.WriteSegment(ctx, seg("shard", sh.id), data); wErr != nil {
				setErr(fmt.Errorf("snapshot: write shard %d: %w", sh.id, wErr))
				return
			}
			atomic.AddInt64(&totalBytes, int64(len(data)))
		}(sh)
	}

	// 写 Workers
	for _, w := range s.workers {
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			snap := s.snapshotWorker(w)
			data, encErr := codec.EncodeWorker(snap)
			if encErr != nil {
				setErr(fmt.Errorf("snapshot: encode worker %d: %w", w.id, encErr))
				return
			}
			if wErr := writer.WriteSegment(ctx, seg("worker", w.id), data); wErr != nil {
				setErr(fmt.Errorf("snapshot: write worker %d: %w", w.id, wErr))
				return
			}
			atomic.AddInt64(&totalBytes, int64(len(data)))
		}(w)
	}

	wg.Wait()
	s.metrics.ObserveSnapshotPhase(metrics.SnapshotPhaseShardsWorkers, time.Since(shardStart))
	if firstErr != nil {
		_ = writer.Abort(ctx, name, version)
		return "", firstErr
	}

	// --- 3. 原子提交 ---
	finishStart := time.Now()
	if err := writer.Finish(ctx, name, version); err != nil {
		return "", fmt.Errorf("snapshot: finish: %w", err)
	}
	s.metrics.ObserveSnapshotPhase(metrics.SnapshotPhaseFinish, time.Since(finishStart))
	s.metrics.ObserveSnapshotSize(atomic.LoadInt64(&totalBytes))

	// --- 4. 更新 lastSnapshotWalOffsets：下次 CreateSnapshot 安全检查以本次为基准 ---
	s.lastSnapshotWalOffsets = make(map[int]int64, len(s.workers))
	for _, w := range s.workers {
		s.lastSnapshotWalOffsets[w.id] = w.LoadMaxWalOffset()
	}

	log.Info("snapshot: create finished", log.String("name", name), log.String("version", version))
	return version, nil
}

// snapshotShard 委托 shard 自身导出快照数据。
func (s *Store) snapshotShard(sh *shard) (snapshot.ShardSnapshot, error) {
	return sh.snapshotData()
}

// snapshotWorker 从 worker 内存中导出 WorkerSnapshot。
func (s *Store) snapshotWorker(w *worker) snapshot.WorkerSnapshot {
	// SeqDedup — 由 worker 持有，replay 停止后状态仍在，可直接导出完整的滑动窗口。
	dedupState := w.SnapshotSeqDedup()

	return snapshot.WorkerSnapshot{
		WorkerID:  w.id,
		Seq:       w.LoadSeq(),
		WalOffset: w.LoadMaxWalOffset(),
		SeqDedup:  dedupState,
	}
}

// ======================== RestoreAndStartReplay ========================

// RestoreAndStartReplay 是"启动恢复"场景的对外入口：先从 snapshot 恢复全量状态，
// 再以各 worker 的 snapshot.WalOffset+1 作为 replay 起点启动回放，两步原子化，
// 业务方无需（也无法）手动传递 snapshot 内部 offsets，避免错配。
//
// version 为空时自动使用最新版本。
//
// 无可用 snapshot（ErrNoSnapshot）时静默降级为等价于直接调用 StartReplay（从 0 开始），
// 此时返回的 Header 为 nil。
func (s *Store) RestoreAndStartReplay(ctx context.Context, version string, opts ReplayOptions) (*snapshot.Header, error) {
	header, err := s.restoreFromSnapshot(ctx, version)
	if err != nil {
		if errors.Is(err, snapshot.ErrNoSnapshot) {
			// 无 snapshot：静默降级为普通 StartReplay。
			if rErr := s.startReplay(ctx, opts, nil); rErr != nil {
				return nil, rErr
			}
			if err := s.afterReplayStarted(ctx, opts); err != nil {
				return nil, err
			}
			return nil, nil
		}
		return nil, err
	}

	// 基于 snapshot 中各 worker 的 WalOffset 构造 replay 起点：WalOffset+1（跳过已在 snapshot 中的最后一条）。
	// WalOffset<0 表示该 worker 在 snapshot 中无有效 offset，此时不注入、让 startReplay 走默认水位逻辑。
	offsets := s.workerWalOffsets()
	startOffsets := make(map[int]wal.Offset, len(offsets))
	for p, off := range offsets {
		if off >= 0 {
			startOffsets[p] = off + 1
		}
	}

	if err := s.startReplay(ctx, opts, startOffsets); err != nil {
		return header, err
	}
	if err := s.afterReplayStarted(ctx, opts); err != nil {
		return header, err
	}
	return header, nil
}

// afterReplayStarted 在 startReplay 之后执行后续操作：
//   - ReplayToLatest 模式：同步等待所有 replay goroutine 完成
//   - Master 角色：PublishSchemas 广播 schema
func (s *Store) afterReplayStarted(ctx context.Context, opts ReplayOptions) error {
	if opts.Mode == wal.ReplayToLatest {
		s.WaitReplay()
	}
	if s.config.Role == config.RoleMaster {
		// Schema 启动期三件套（顺序关键）：
		//  1. resolveSchemaVersions：按 cached vs current 自动赋 Def.Version
		//     （全新 Kind=1、结构等价保留、演进递增）
		//  2. checkSchemasCompat：用 resolveSchemaVersions 赋完值后的 AllDescriptors
		//     对比 cache，拦截非法演进
		//  3. PublishSchemas：写 WAL + 刷新 master 自己的 schemasByKind
		s.resolveSchemaVersions()
		if err := s.checkSchemasCompat(registry.AllDescriptors()); err != nil {
			return fmt.Errorf("schema compat: %w", err)
		}
		if err := s.PublishSchemas(ctx); err != nil {
			return fmt.Errorf("broadcast schemas: %w", err)
		}
	}
	return nil
}

// ======================== restoreFromSnapshot ========================

// restoreFromSnapshot 从快照恢复全量数据到 Store。Master 和 Slave 均可使用。
// 必须在 startReplay 之前调用（Store 刚创建、内存为空时）。外部调用请走 RestoreAndStartReplay。
//
// version 为空时自动使用最新版本（Reader.LatestVersion），非空时使用指定版本。
//
// Reader、Codec 从 s.config.Snapshot 自动构造，Codec 格式从 snapshot 内的 meta segment 读取。
//
// 恢复流程：
//  1. 解析 version（空则取最新）
//  2. 读 meta segment 获取 format，选择 codec
//  3. 读 Header，校验 shard/worker 数量
//  4. 并发读取并恢复各 Shard（写入内存 + 注册 TTL）
//  5. 并发读取并恢复各 Worker（seq、SeqDedup）
//  6. 返回 Header
func (s *Store) restoreFromSnapshot(ctx context.Context, version string) (header *snapshot.Header, err error) {
	restoreStart := time.Now()
	defer func() {
		// "no_snapshot" 走静默降级路径，单独区分；其余非 nil error 计 "error"。
		result := metrics.SnapshotResultOK
		if err != nil {
			if errors.Is(err, snapshot.ErrNoSnapshot) {
				result = metrics.RestoreResultNoSnapshot
			} else {
				result = metrics.SnapshotResultError
			}
		}
		s.metrics.ObserveSnapshotRestore(result, time.Since(restoreStart))
	}()

	snapConf := s.config.Snapshot
	name := snapConf.Name

	// --- 构造 reader ---
	reader, err := newSnapshotReader(snapConf)
	if err != nil {
		return nil, fmt.Errorf("snapshot: create reader: %w", err)
	}
	defer reader.Close()

	// --- 0. 解析 version ---
	if version == "" {
		v, err := reader.LatestVersion(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("snapshot: resolve latest version: %w", err)
		}
		version = v
	}

	log.Info("snapshot: restore started", log.String("name", name), log.String("version", version))

	seg := func(kind string, id int) snapshot.Segment {
		return snapshot.Segment{Name: name, Version: version, Kind: kind, ID: id}
	}

	// --- 1. 读 Header（始终 JSON 编码），从中获取 format 选择 codec ---
	headerBytes, err := reader.ReadSegment(ctx, seg("header", 0))
	if err != nil {
		return nil, fmt.Errorf("snapshot: read header: %w", err)
	}
	var hdr snapshot.Header
	if err := json.Unmarshal(headerBytes, &hdr); err != nil {
		return nil, fmt.Errorf("snapshot: decode header: %w", err)
	}

	// 从 header 获取 format；若为空（旧版 snapshot），回退读 meta-00.dat
	format := hdr.Format
	if format == "" {
		metaBytes, mErr := reader.ReadSegment(ctx, snapshot.Segment{Name: name, Version: version, Kind: "meta", ID: 0})
		if mErr != nil {
			return nil, fmt.Errorf("snapshot: header missing format and no meta segment: %w", mErr)
		}
		var legacy struct {
			Format snapconfig.SnapshotFormat `json:"format"`
		}
		if jErr := json.Unmarshal(metaBytes, &legacy); jErr != nil {
			return nil, fmt.Errorf("snapshot: decode legacy meta: %w", jErr)
		}
		format = legacy.Format
	}

	codec, ok := snapshot.GetCodec(format)
	if !ok {
		return nil, fmt.Errorf("snapshot: codec %q not registered", format)
	}

	if hdr.NumShards != len(s.shards) {
		return nil, fmt.Errorf("snapshot: %w: snapshot=%d store=%d", storeerrors.ErrSnapshotShardCountMismatch, hdr.NumShards, len(s.shards))
	}
	if hdr.NumWorkers != len(s.workers) {
		return nil, fmt.Errorf("snapshot: %w: snapshot=%d store=%d", storeerrors.ErrSnapshotWorkerCountMismatch, hdr.NumWorkers, len(s.workers))
	}

	// --- 2. 加载 snapshot 里的 schema 到 schemasByKind（仅做加载，不做校验） ---
	// Header.Schemas 是 snapshot 时刻每个 Kind 的"alive + tombstone 摘要"：
	// KindDescriptor.Fields 是当时 alive 的字段，DroppedFields 是 tombstone。
	// 逐条 UpsertSchema 重建 fieldsRegistry；之后 replay 里的 WalTypeSchemaConfig
	// 会继续增量更新。
	//
	// 真正的 compat 检查延后到 afterReplayStarted——那时 replay 已经把
	// post-snapshot 的 WalTypeSchemaConfig 全部消费，schemasByKind 反映的是
	// "master 在 WAL 末尾最后声明的 schema"。对 snapshot 本身做校验会拿一份可能
	// 早已过时的 schema 去对比当前代码，误拦/漏拦都不准确。
	for _, d := range hdr.Schemas {
		s.UpsertSchema(d)
	}

	// --- 3. 并发恢复 Shards + Workers ---
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(e error) {
		errOnce.Do(func() { firstErr = e })
	}

	// 恢复 Shards
	for i := 0; i < hdr.NumShards; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			data, rErr := reader.ReadSegment(ctx, seg("shard", shardID))
			if rErr != nil {
				setErr(fmt.Errorf("snapshot: read shard %d: %w", shardID, rErr))
				return
			}
			shardSnap, dErr := codec.DecodeShard(data)
			if dErr != nil {
				setErr(fmt.Errorf("snapshot: decode shard %d: %w", shardID, dErr))
				return
			}
			if aErr := s.restoreShard(shardSnap); aErr != nil {
				setErr(fmt.Errorf("snapshot: restore shard %d: %w", shardID, aErr))
			}
		}(i)
	}

	// 恢复 Workers
	for i := 0; i < hdr.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			data, rErr := reader.ReadSegment(ctx, seg("worker", workerID))
			if rErr != nil {
				setErr(fmt.Errorf("snapshot: read worker %d: %w", workerID, rErr))
				return
			}
			workerSnap, dErr := codec.DecodeWorker(data)
			if dErr != nil {
				setErr(fmt.Errorf("snapshot: decode worker %d: %w", workerID, dErr))
				return
			}
			if aErr := s.restoreWorker(workerSnap); aErr != nil {
				setErr(fmt.Errorf("snapshot: restore worker %d: %w", workerID, aErr))
			}
		}(i)
	}

	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	// --- 4. 记录 lastSnapshotWalOffsets（用于可观测性指标） ---
	s.lastSnapshotWalOffsets = make(map[int]int64, len(s.workers))
	for _, w := range s.workers {
		s.lastSnapshotWalOffsets[w.id] = w.LoadMaxWalOffset()
	}

	log.Info("snapshot: restore finished",
		log.String("name", name),
		log.String("version", version),
		log.Int("shards", hdr.NumShards),
		log.Int("workers", hdr.NumWorkers))

	return &hdr, nil
}

// restoreShard 委托 shard 自身恢复快照数据，然后在 Store 层注册 TTL。
func (s *Store) restoreShard(snap *snapshot.ShardSnapshot) error {
	if snap.ShardID < 0 || snap.ShardID >= len(s.shards) {
		return storeerrors.ErrShardNotFound
	}
	sh := s.shards[snap.ShardID]

	ttlJobs, err := sh.restoreFromSnapshot(snap)
	if err != nil {
		return err
	}

	s.scheduleTTLJobs(ttlJobs)
	return nil
}

// restoreWorker 将 WorkerSnapshot 的状态恢复到对应 worker。
func (s *Store) restoreWorker(snap *snapshot.WorkerSnapshot) error {
	if snap.WorkerID < 0 || snap.WorkerID >= len(s.workers) {
		return fmt.Errorf("snapshot: invalid worker id %d", snap.WorkerID)
	}
	w := s.workers[snap.WorkerID]

	// 恢复 seq
	w.StoreSeq(snap.Seq)

	// 恢复 SeqDeduplicator
	w.RestoreSeqDedup(snap.SeqDedup)

	// 恢复 maxWalOffset（用于 replay 起点计算）
	w.StoreMaxWalOffset(snap.WalOffset)

	log.Info("snapshot: worker restored",
		log.Int("worker_id", snap.WorkerID),
		log.Int64("seq", snap.Seq),
		log.Int64("wal_offset", snap.WalOffset))

	return nil
}

// checkSchemasCompat 校验 newSchemas（本次重启代码里当前的 schema）相对
// store.schemasByKind 里累积的 alive + tombstone 是否为兼容演进。
// 调用时机：master 启动 replay 全部完成、即将 PublishSchemas 之前。
// 那时 schemasByKind 含：snapshot header 里 restore 来的 alive/tombstone +
// replay 期间 SchemaConfig 的增量合并结果。
//
// 语义：
//   - 对 newDesc 里每个字段 f：
//     · 若 alive[f.Num] 存在 → checkFieldsEquivalent（Type + Default）
//     · 若 tombstone[f.Num] 存在 → checkFieldsEquivalent（拦跨版本 Num 复用：某 Num
//     曾在历史上有 int32，被删，现在加回作 string 应拒绝）
//   - 无 registry 项（全新 Kind）：放行。
//
// 复杂度从旧版 O(V*F) 降到 O(F+D)；D = tombstone 大小。
func (s *Store) checkSchemasCompat(newSchemas []schema.KindDescriptor) error {
	for _, newDesc := range newSchemas {
		kind, err := registry.ParseKindFromString(newDesc.Kind)
		if err != nil {
			return fmt.Errorf("invalid kind %q: %w", newDesc.Kind, err)
		}
		s.schemasByKindMu.RLock()
		reg := s.schemasByKind[kind]
		s.schemasByKindMu.RUnlock()
		if reg == nil {
			continue // 全新 Kind，无对比基准
		}
		if err := checkCompatAgainstRegistry(reg, newDesc); err != nil {
			return err
		}
		if reg.latestVersion != newDesc.Version && reg.latestVersion != 0 {
			log.Info("kind version evolved (allowed compat evolution)",
				log.String("kind", newDesc.Kind),
				log.Uint32("from", reg.latestVersion),
				log.Uint32("to", newDesc.Version))
		}
	}
	return nil
}

// checkCompatAgainstRegistry 校验 newDesc 相对 reg（alive + tombstone）是否兼容。
//
// 两道检查，都是**语义正确性**防御，**不**强制业务对 schema 历史做完整声明——
// 回滚到旧代码、老代码不知道后加的字段都是合法的 ops 动作，静默接受即可
// （alive 消失的字段自动迁到 tombstone，见 fieldsRegistry.upsert）。
//
//  1. newDesc 内部 Num 唯一
//  2. vs alive：同 Num 不能改 Type/Default
//  3. vs tombstone：newDesc.Fields 里的 Num 若在 tombstone 中出现 → Type/Default
//     必须完全一致（"同语义复活"OK；"借 Num 做别的语义字段"拒绝）
//
// newDesc.DroppedFields 这条路不再做对账：DroppedFields 是 Store 运行期从
// "vanished from alive" diff 累积出来的，源码里的 RetiredField 不再喂入 descriptor。
// 唯一会带 DroppedFields 的入口是 snapshot Header restore，那条路走 UpsertSchema
// 直接合并到 tombstone，不需要在这里二次对账。
func checkCompatAgainstRegistry(reg *fieldsRegistry, newDesc schema.KindDescriptor) error {
	// 1. newDesc 内部自洽：Num 唯一（走 CheckSchemaCompat 的 duplicate-num 检测）
	if err := schema.CheckSchemaCompat(
		schema.KindDescriptor{Kind: newDesc.Kind, Fields: nil},
		newDesc,
	); err != nil {
		return err
	}
	// 2. vs alive：同 Num 不能改 Type/Default
	if len(reg.alive) > 0 {
		aliveDesc := schema.KindDescriptor{Kind: newDesc.Kind}
		aliveDesc.Fields = make([]schema.FieldDescriptor, 0, len(reg.alive))
		for _, f := range reg.alive {
			aliveDesc.Fields = append(aliveDesc.Fields, f)
		}
		if err := schema.CheckSchemaCompat(aliveDesc, newDesc); err != nil {
			return fmt.Errorf("vs alive: %w", err)
		}
	}
	// 3. newDesc.Fields vs tombstone：alive 里的 Num 若是从 tombstone "复活"
	//    的，必须是同 Type/Default，否则就是借 Num 做别的字段——拒绝。
	for _, nf := range newDesc.Fields {
		tf, dropped := reg.tombstone[nf.Num]
		if !dropped {
			continue
		}
		if tf.Type != nf.Type {
			return fmt.Errorf(
				"kind %s: num=%d was dropped with Type=%s, now reintroduced as Type=%s (Num reuse with different type forbidden; pick a new num)",
				newDesc.Kind, nf.Num, tf.Type, nf.Type)
		}
		if !tf.Default.Equal(nf.Default) {
			return fmt.Errorf(
				"kind %s: num=%d was dropped with Default=%+v, now reintroduced with Default=%+v (Default is retroactive; pick a new num for new default semantics)",
				newDesc.Kind, nf.Num, tf.Default, nf.Default)
		}
	}
	return nil
}

// ======================== helpers ========================

func newSnapshotWriter(conf snapconfig.Config) (snapshot.Writer, error) {
	src, err := source.GetSource(conf.Writer.Source)
	if err != nil {
		return nil, err
	}
	return src.NewWriter(conf.Writer)
}

func newSnapshotReader(conf snapconfig.Config) (snapshot.Reader, error) {
	src, err := source.GetSource(conf.Reader.Source)
	if err != nil {
		return nil, err
	}
	return src.NewReader(conf.Reader)
}
