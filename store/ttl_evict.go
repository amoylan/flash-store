package store

import (
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/utils"
)

// ttlEvictionKey 时间轮任务键；到期时在本地 shard 上删除数据，不写 WAL。
// expireAt 为写入时与 objectKeyExpires / kValue.expiredAt 一致的 Unix 秒，用于忽略已被更新 TTL 的旧定时。
type ttlEvictionKey struct {
	shardID  int
	isKV     bool
	kind     registry.Kind
	key      string
	expireAt int64
}

func (s *Store) initTTLWheel() {
	const slotSec = 1
	const numSlots = 3600 // 每圈 1h，更长 TTL 走 rounds

	s.ttlWheel = utils.NewTimeWheel[ttlEvictionKey]("store-ttl", slotSec, numSlots, func(items []ttlEvictionKey) {
		s.processTTLBatch(items)
	})
}

func (s *Store) scheduleTTLJobs(jobs []ttlEvictionKey) {
	if len(jobs) == 0 || s.ttlWheel == nil {
		return
	}
	for _, j := range jobs {
		_ = s.ttlWheel.AddItem(time.Unix(j.expireAt, 0), j)
	}
}

func (s *Store) processTTLBatch(items []ttlEvictionKey) {
	byShard := make(map[int][]ttlEvictionKey)
	for _, it := range items {
		byShard[it.shardID] = append(byShard[it.shardID], it)
	}
	for sid, batch := range byShard {
		if sid < 0 || sid >= len(s.shards) || s.shards[sid] == nil {
			continue
		}
		s.shards[sid].processTTLExpirations(batch)
	}
}

func (s *shard) processTTLExpirations(jobs []ttlEvictionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	for _, job := range jobs {
		if job.isKV {
			s.evictKVIfMatch(job.key, job.expireAt, now)
		} else {
			s.evictObjectIfMatch(job.kind, job.key, job.expireAt, now)
		}
	}
}

func (s *shard) evictKVIfMatch(key string, expectedExpire, now int64) {
	v, ok := s.kv[key]
	if !ok {
		return
	}
	if v.expiredAt == 0 || v.expiredAt != expectedExpire {
		return
	}
	if now < v.expiredAt {
		return
	}
	delete(s.kv, key)
}

func (s *shard) evictObjectIfMatch(kind registry.Kind, key string, expectedExpire, now int64) {
	tbl, ok := s.tables[kind]
	if !ok {
		return
	}
	exp, has := tbl.objectKeyExpires[key]
	if !has || exp == 0 || exp != expectedExpire {
		return
	}
	if now < exp {
		return
	}
	o, ok := tbl.objects[key]
	if !ok {
		delete(tbl.objectKeyExpires, key)
		return
	}
	if def, ok := registry.GetTypedDef(kind); ok {
		tbl.removeRowFromIndexes(def, o.row, key)
	}
	delete(tbl.objects, key)
	delete(tbl.objectKeyExpires, key)
}
