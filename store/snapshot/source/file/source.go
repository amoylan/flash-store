/*
Package file 实现了基于本地文件系统的 snapshot source。

# 存储结构

所有 snapshot 数据按目录层级存储在本地文件系统中，布局：

	<dir>/<name>/<version>/
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

 1. BeginWrite  → 生成版本号，创建 <dir>/<name>/<version>/ 目录
 2. WriteSegment → os.WriteFile 逐个写入 header、shard、worker 文件
 3. Finish       → 写入空的 _SUCCESS 标记文件
 4. 若出错 Abort → os.RemoveAll 删除整个版本目录

# 读取流程（RestoreFromSnapshot）

 1. LatestVersion → os.ReadDir 列出版本子目录，从最新向前检查 _SUCCESS 标记
 2. ReadSegment   → os.ReadFile 读取对应 segment 文件的全部字节

# 示例路径

假设配置 Dir="/data/snapshots", Name="order-store"，
某次快照生成的版本为 "20260409-153000"，Store 有 4 个 shard、2 个 worker：

	/data/snapshots/order-store/20260409-153000/header-00.dat
	/data/snapshots/order-store/20260409-153000/shard-00.dat
	/data/snapshots/order-store/20260409-153000/shard-01.dat
	/data/snapshots/order-store/20260409-153000/shard-02.dat
	/data/snapshots/order-store/20260409-153000/shard-03.dat
	/data/snapshots/order-store/20260409-153000/worker-00.dat
	/data/snapshots/order-store/20260409-153000/worker-01.dat
	/data/snapshots/order-store/20260409-153000/_SUCCESS

# 使用方式

在应用入口 import 本包触发 init() 自动注册：

	import _ "github.com/amoylan/flash-store/store/snapshot/source/file"
*/
package file

import (
	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/snapshot/config"
	"github.com/amoylan/flash-store/store/snapshot/source"
)

type fileSource struct{}

func (s *fileSource) NewReader(conf config.Reader) (snapshot.Reader, error) {
	return &fileReader{dir: conf.File.Dir}, nil
}

func (s *fileSource) NewWriter(conf config.Writer) (snapshot.Writer, error) {
	return &fileWriter{dir: conf.File.Dir}, nil
}

func init() {
	source.RegisterSource(config.SourceTypeFile, &fileSource{})
}
