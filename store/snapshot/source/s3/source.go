/*
Package s3 实现了基于 S3（兼容 MinIO / COS 等）的 snapshot source。

# 存储结构

所有 snapshot 数据以对象形式存储在 S3 bucket 中，路径布局：

	s3://<bucket>/<prefix>/<name>/<version>/
	├── header-00.dat    # 快照头部（JSON）：版本号、编码格式、创建时间、shard/worker 数量
	├── shard-00.dat     # shard 0 的全量用户数据
	├── shard-01.dat
	├── ...
	├── worker-00.dat    # worker 0 的状态（seq、ack 水位、去重器）
	├── worker-01.dat
	├── ...
	└── _SUCCESS         # 完成标记，存在即表示该版本写入完整

# 文件命名规则

	<kind>-<id>.dat

	kind    含义                        id 说明
	──────  ──────────────────────────  ──────────────────────
	header  快照头部（Header，JSON）     固定为 00
	shard   单个 shard 的用户数据        shard ID（00, 01, ...）
	worker  单个 worker 的运行状态       worker ID（00, 01, ...）

# 版本号

	由 BeginWrite 生成，格式为 UTC 时间戳 "YYYYMMDD-HHmmss"，如 "20260409-153000"。
	字典序即时间序，LatestVersion 据此排序选取最新已完成版本。

# 写入流程（CreateSnapshot）

 1. BeginWrite  → 生成版本号 "20260409-153000"

 2. WriteSegment → PutObject 逐个写入 header、shard、worker 文件

 3. Finish       → PutObject 写入空的 _SUCCESS 标记

 4. 若出错 Abort → ListObjects + DeleteObjects 清理该版本下所有对象

    写入期间各 segment 直接存放在正式路径，不使用临时目录。
    _SUCCESS 标记确保 LatestVersion 不会选取未完成的版本。

# 读取流程（RestoreFromSnapshot）

 1. LatestVersion → ListObjectsV2（Delimiter="/"）列出版本目录，
    从最新向前逐个 HeadObject 检查 _SUCCESS 标记
 2. ReadSegment   → GetObject 读取对应 segment 的全部字节

# 示例路径

假设配置 Bucket="trade-snapshots", Prefix="prod", Name="order-store"，
某次快照生成的版本为 "20260409-153000"，Store 有 4 个 shard、2 个 worker：

	s3://trade-snapshots/prod/order-store/20260409-153000/header-00.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/shard-00.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/shard-01.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/shard-02.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/shard-03.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/worker-00.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/worker-01.dat
	s3://trade-snapshots/prod/order-store/20260409-153000/_SUCCESS

# 自定义 Endpoint

通过 Config.Endpoint 可连接 MinIO、腾讯 COS 等 S3 兼容存储，
设置后自动启用 path-style 访问（而非 virtual-hosted-style）。

# 使用方式

在应用入口 import 本包触发 init() 自动注册：

	import _ "github.com/amoylan/flash-store/store/snapshot/source/s3"
*/
package s3

import (
	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/snapshot/config"
	"github.com/amoylan/flash-store/store/snapshot/source"
)

type s3Source struct{}

func (s *s3Source) NewReader(conf config.Reader) (snapshot.Reader, error) {
	client, err := newS3Client(conf.S3)
	if err != nil {
		return nil, err
	}
	return &s3Reader{
		client: client,
		bucket: conf.S3.Bucket,
		prefix: conf.S3.Prefix,
	}, nil
}

func (s *s3Source) NewWriter(conf config.Writer) (snapshot.Writer, error) {
	client, err := newS3Client(conf.S3)
	if err != nil {
		return nil, err
	}
	return &s3Writer{
		client: client,
		bucket: conf.S3.Bucket,
		prefix: conf.S3.Prefix,
	}, nil
}

func init() {
	source.RegisterSource(config.SourceTypeS3, &s3Source{})
}
