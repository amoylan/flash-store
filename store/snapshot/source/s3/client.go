package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	s3conf "github.com/amoylan/flash-store/store/snapshot/source/s3/config"
)

// newS3Client 根据配置构建 S3 client，支持自定义 endpoint（MinIO/COS 等兼容存储）。
func newS3Client(conf s3conf.Config) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error

	if conf.Region != "" {
		opts = append(opts, awsconfig.WithRegion(conf.Region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	var s3Opts []func(*s3.Options)
	if conf.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(conf.Endpoint)
			o.UsePathStyle = true // 自定义 endpoint 通常需要 path-style
		})
	}

	return s3.NewFromConfig(cfg, s3Opts...), nil
}
