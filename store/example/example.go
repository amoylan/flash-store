package example

import (
	"context"
	"strconv"
	"time"

	"github.com/amoylan/flash-store/store"
	"github.com/amoylan/flash-store/store/config"
	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// DomainFuture 领域名称（或者叫 namespace），比如合约、现货。
const DomainFuture = "future"
const KindOrder = "order"

var (
	// OrderKind 是本示例注册的业务对象 Kind（"future.order"），demo 与文档引用它。
	OrderKind registry.Kind
	orderDef  *registry.Def[Order]
)

func init() {
	var err error
	OrderKind, err = registry.NewKind(DomainFuture, KindOrder)
	if err != nil {
		panic(err)
	}

	// 单一真相源：tag 直接写在 struct 字段上，RegisterStruct 反射一次构建 Def。
	// 热路径走 unsafe.Pointer + offset，和手写 Builder 同速。
	// Status 是 OrderStatus（~int32），RegisterStruct 按底层类型处理，不需要任何手动转换。
	orderDef, err = registry.RegisterStruct[Order](OrderKind)
	if err != nil {
		panic(err)
	}
}

// OrderStatus 业务枚举——演示 ~int32 定制类型在 RegisterStruct 下直接可用。
type OrderStatus int32

const (
	StatusOpen   OrderStatus = 1
	StatusFilled OrderStatus = 2
)

// Order 用 `store:"..."` tag 声明 schema。
//
// tag 格式：`store:"<name>,num=<int>[,<opt>[=<val>]...]"`
//
//	无 tag / `store:"-"`                  跳过
//	`store:"name,num=1"`                  最小形态——name + num 必填
//	`store:"name,num=2,index"`            进入索引
//	`store:"name,num=3,prec=8"`           Decimal 精度
//	`store:"name,num=4,max=N"`            String 最大长度（元数据）
//	`store:"name,num=5,default=<literal>"` 默认值（schema 演进补位）
//	`store:"name,num=6,custom"`           强制 Custom（一般由 Value/Scan 自动识别）
//
// Num 是字段的**稳定身份**。重排 Go struct 字段、rename 字段、在 schema 中
// reorder、删除字段——只要 Num 不变都是向后兼容的；删过的 Num 不可被不同语义的
// 字段复用（由 compat 检查兜底）。
type Order struct {
	AccountId  int64           `store:"account_id,num=1"`
	OrderId    int64           `store:"order_id,num=2"`
	Status     OrderStatus     `store:"status,num=3,index,type=int32"` // 自定义命名 scalar 必须显式 type=
	ContractId int64           `store:"contract_id,num=4,index"`
	Price      decimal.Decimal `store:"price,num=5,index,prec=8"`
	Vol        decimal.Decimal `store:"vol,num=6,prec=8"`
	CreatedAt  time.Time       `store:"created_at,num=7"`
	UpdatedAt  time.Time       `store:"updated_at,num=8"`
}

func (o *Order) StrOrderId() string {
	return strconv.FormatInt(o.OrderId, 10)
}

// FieldNameXxx 供查询 IndexHint / RowView.Get 使用。名字与 tag 里声明的一致。
const (
	FieldNameAccountId  schema.FieldName = "account_id"
	FieldNameOrderId    schema.FieldName = "order_id"
	FieldNameStatus     schema.FieldName = "status"
	FieldNameContractId schema.FieldName = "contract_id"
	FieldNamePrice      schema.FieldName = "price"
	FieldNameVol        schema.FieldName = "vol"
	FieldNameCreatedAt  schema.FieldName = "created_at"
	FieldNameUpdatedAt  schema.FieldName = "updated_at"
)

// OrderView 是业务方自定义的对象视图封装（和 registry.RowView 是两种东西）。
type OrderView struct {
	order *Order
}

func NewOrderView(order *Order) *OrderView   { return &OrderView{order: order} }
func (ov *OrderView) OrderId() int64         { return ov.order.OrderId }
func (ov *OrderView) Price() decimal.Decimal { return ov.order.Price }
func (ov *OrderView) Vol() decimal.Decimal   { return ov.order.Vol }

var EventTypeApprovalSubmitOrder store.EventType = "ApprovalSubmitOrder"

// WriteDemo 展示如何写入数据到 Store。
func WriteDemo() {
	var s, _ = store.NewStore(config.DefaultConfigForTest(), nil)

	order := &Order{
		AccountId: 1001,
		OrderId:   123,
		Status:    StatusOpen,
	}
	identity := uint64(order.AccountId)

	err := s.Transaction(context.Background(), func(tx *store.Tx) error {
		event := tx.BeginEvent(EventTypeApprovalSubmitOrder, nil)

		if err := store.SetObject(event, identity, OrderKind, order.StrOrderId(), order, time.Duration(0)); err != nil {
			return err
		}
		if err := store.SetKV(event, identity, "some_key", []byte("some_value"), time.Duration(0)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// ReadObjectDemo 展示如何从 Store 读取对象。读路径的 filter 收到 *registry.RowView。
func ReadObjectDemo() {
	var s, _ = store.NewStore(config.DefaultConfigForTest(), nil)

	identity := uint64(1001)

	orderId := "123"
	order, err := store.GetObject[Order](s, identity, OrderKind, orderId)
	if err != nil {
	}
	_ = order

	orders, err := store.GetObjects[Order](s, identity, OrderKind, []string{"123", "456", "789"})
	if err != nil {
	}
	_ = orders

	// 按索引 + 过滤查询，指定身份标识。
	// index.NewEqualRange 期望 FieldValue；用 schema.NewInt32 构造（零分配）。
	orders2, err := store.QueryObjectsByIdentity[Order](s, identity, OrderKind, []store.IndexHint{
		{
			Name: FieldNameStatus,
			Ranges: []index.Range{
				index.NewEqualRange(schema.NewInt32(int32(StatusOpen))),
			},
		},
	}, func(view *registry.RowView) bool {
		price, err := view.GetResult(FieldNamePrice).Decimal()
		if err != nil {
			return false
		}
		return price.GreaterThan(decimal.NewFromInt(1000))
	})
	if err != nil {
	}
	_ = orders2

	// 全局跨 shard 查询。
	orders3, err := store.QueryObjects[Order](s, OrderKind, []store.IndexHint{
		{
			Name: FieldNameStatus,
			Ranges: []index.Range{
				index.NewEqualRange(schema.NewInt32(int32(StatusOpen))),
			},
		},
	}, func(view *registry.RowView) bool {
		price, err := view.GetResult(FieldNamePrice).Decimal()
		if err != nil {
			return false
		}
		return price.GreaterThan(decimal.NewFromInt(1000))
	})
	if err != nil {
	}
	_ = orders3
}

// ReadViewDemo 展示只读 View 路径——不重建业务 struct，直接按字段名取值。
func ReadViewDemo() {
	var s, _ = store.NewStore(config.DefaultConfigForTest(), nil)

	identity := uint64(1001)

	orderId := "123"
	orderView, err := store.GetView[Order](s, identity, OrderKind, orderId)
	if err != nil {
	}
	_ = orderView

	orderViews, err := store.GetViews[Order](s, identity, OrderKind, []string{"123", "456", "789"})
	if err != nil {
	}
	_ = orderViews

	orderViews2, err := store.QueryViewsByIdentity[Order](s, identity, OrderKind, []store.IndexHint{
		{
			Name: FieldNameStatus,
			Ranges: []index.Range{
				index.NewEqualRange(schema.NewInt32(int32(StatusOpen))),
			},
		},
	}, func(view *registry.RowView) bool {
		price, err := view.GetResult(FieldNamePrice).Decimal()
		if err != nil {
			return false
		}
		return price.GreaterThan(decimal.NewFromInt(1000))
	})
	if err != nil {
	}
	_ = orderViews2

	orderViews3, err := store.QueryViews[Order](s, OrderKind, []store.IndexHint{
		{
			Name: FieldNameStatus,
			Ranges: []index.Range{
				index.NewEqualRange(schema.NewInt32(int32(StatusOpen))),
			},
		},
	}, func(view *registry.RowView) bool {
		price, err := view.GetResult(FieldNamePrice).Decimal()
		if err != nil {
			return false
		}
		return price.GreaterThan(decimal.NewFromInt(1000))
	})
	if err != nil {
	}
	_ = orderViews3
}
