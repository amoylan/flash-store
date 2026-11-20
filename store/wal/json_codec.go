package wal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal/config"
)

type jsonWalCodec struct{}

// jsonWalWire 是 WAL payload 的 JSON wire 表达。Type/Seq 由 Header 携带，不在 payload 中。
// Cmd.Row 序列化为 Row 的 binary bytes，JSON 里直接作为 []byte（json 默认 base64）。
type jsonWalWire struct {
	Events  []jsonEventWire         `json:"events,omitempty"`
	Schemas []schema.KindDescriptor `json:"schemas,omitempty"`
}

type jsonEventWire struct {
	EventType string        `json:"event_type"`
	Identity  uint64        `json:"identity"`
	Cmds      []jsonCmdWire `json:"cmds"`
	KvCmds    []KvCmd       `json:"kv_cmds"`
	BizData   *jsonBizWire  `json:"biz_data,omitempty"`
}

type jsonCmdWire struct {
	Op   Operation     `json:"op"`
	Key  string        `json:"key"`
	Kind registry.Kind `json:"kind"`
	// Row 是 schema.Row 的 binary 字节（自描述），json 里自动 base64。OpDelete 时为 nil。
	Row []byte        `json:"row,omitempty"`
	Ttl time.Duration `json:"ttl"`
}

type jsonBizWire struct {
	Version uint8  `json:"version"`
	Data    []byte `json:"data"`
}

func (jsonWalCodec) Encode(w *Wal) ([]byte, error) {
	wire := jsonWalWire{
		Events:  make([]jsonEventWire, 0, len(w.Events)),
		Schemas: w.Schemas,
	}
	for _, ev := range w.Events {
		jev, err := encodeEventWire(ev)
		if err != nil {
			return nil, err
		}
		wire.Events = append(wire.Events, jev)
	}
	return json.Marshal(&wire)
}

func encodeEventWire(ev Event) (jsonEventWire, error) {
	jev := jsonEventWire{
		EventType: ev.EventType,
		Identity:  ev.Identity,
		KvCmds:    ev.KvCmds,
	}
	if len(ev.Cmds) > 0 {
		jev.Cmds = make([]jsonCmdWire, 0, len(ev.Cmds))
		for _, c := range ev.Cmds {
			jc, err := encodeCmdWire(c)
			if err != nil {
				return jsonEventWire{}, err
			}
			jev.Cmds = append(jev.Cmds, jc)
		}
	}
	if ev.BizData != nil {
		jev.BizData = &jsonBizWire{
			Version: ev.BizData.Version(),
			Data:    ev.BizData.Bytes(),
		}
	}
	return jev, nil
}

func encodeCmdWire(c Cmd) (jsonCmdWire, error) {
	jc := jsonCmdWire{
		Op:   c.Op,
		Key:  c.Key,
		Kind: c.Kind,
		Ttl:  c.Ttl,
	}
	if c.Op == OpSet {
		rowBytes, err := schema.EncodeRow(c.Row)
		if err != nil {
			return jsonCmdWire{}, fmt.Errorf("wal json encode Row: %w", err)
		}
		jc.Row = rowBytes
	}
	return jc, nil
}

func (c jsonWalCodec) Decode(data []byte) (*Wal, error) {
	var wire jsonWalWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	out := &Wal{
		Events:  make([]Event, 0, len(wire.Events)),
		Schemas: wire.Schemas,
	}
	for _, jev := range wire.Events {
		ev, err := decodeEventWire(jev)
		if err != nil {
			return nil, err
		}
		out.Events = append(out.Events, ev)
	}
	return out, nil
}

func decodeEventWire(jev jsonEventWire) (Event, error) {
	ev := Event{
		EventType: jev.EventType,
		Identity:  jev.Identity,
		KvCmds:    jev.KvCmds,
	}
	if len(jev.Cmds) > 0 {
		ev.Cmds = make([]Cmd, 0, len(jev.Cmds))
		for _, jc := range jev.Cmds {
			c, err := decodeCmdWire(jc)
			if err != nil {
				return Event{}, err
			}
			ev.Cmds = append(ev.Cmds, c)
		}
	}
	if jev.BizData != nil {
		raw := make([]byte, 1+len(jev.BizData.Data))
		raw[0] = jev.BizData.Version
		copy(raw[1:], jev.BizData.Data)
		ev.BizData = RawBizData(raw)
	}
	return ev, nil
}

func decodeCmdWire(jc jsonCmdWire) (Cmd, error) {
	c := Cmd{
		Op:   jc.Op,
		Key:  jc.Key,
		Kind: jc.Kind,
		Ttl:  jc.Ttl,
	}
	if jc.Op == OpSet {
		if len(jc.Row) == 0 {
			return Cmd{}, fmt.Errorf("wal json decode: OpSet missing row for key %q", jc.Key)
		}
		row, err := schema.DecodeRow(jc.Row)
		if err != nil {
			return Cmd{}, fmt.Errorf("wal json decode Row key %q: %w", jc.Key, err)
		}
		c.Row = row
	}
	return c, nil
}

func init() {
	RegisterWalCodec(config.WalFormatJson, jsonWalCodec{})
}
