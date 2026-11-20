package config

// Config 是本地文件 snapshot source 的配置。
type Config struct {
	// Dir 快照根目录，版本数据存储在 <Dir>/<name>/<version>/ 下
	Dir string
}
