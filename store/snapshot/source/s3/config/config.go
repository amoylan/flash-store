package config

// Config 是 S3 snapshot source 的配置，Reader 和 Writer 共用。
type Config struct {
	// Bucket S3 桶名
	Bucket string
	// Prefix 对象键前缀（如 "snapshots"），末尾不含 "/"
	Prefix string
	// Region AWS Region（如 "ap-southeast-1"）
	Region string
	// Endpoint 自定义 S3 endpoint（兼容 MinIO / COS 等），为空时使用 AWS 默认
	Endpoint string
}
