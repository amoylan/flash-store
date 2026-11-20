package s3

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/amoylan/flash-store/store/snapshot"
)

type s3Writer struct {
	client *s3.Client
	bucket string
	prefix string
}

// BeginWrite 生成时间戳版本号。
// name 参数未使用：S3 source 不需要在 BeginWrite 阶段为 name 创建任何资源，
// 版本目录结构在 WriteSegment 时由 PutObject 隐式创建。
func (w *s3Writer) BeginWrite(_ context.Context, _ string) (string, error) {
	return time.Now().UTC().Format("20060102-150405"), nil
}

// WriteSegment 将 segment 数据直接 PUT 到正式路径。
func (w *s3Writer) WriteSegment(ctx context.Context, seg snapshot.Segment, data []byte) error {
	key := segmentKey(w.prefix, seg.Name, seg.Version, seg)

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// Finish 写入 _SUCCESS 标记文件，标志该版本写入完成。
func (w *s3Writer) Finish(ctx context.Context, name string, version string) error {
	key := successMarkerKey(w.prefix, name, version)

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte{}),
	})
	return err
}

// Abort 删除该版本下所有已写入的对象（含可能的部分 segment）。
func (w *s3Writer) Abort(ctx context.Context, name string, version string) error {
	prefix := versionPrefix(w.prefix, name, version)

	// 列出该版本下所有对象
	paginator := s3.NewListObjectsV2Paginator(w.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucket),
		Prefix: aws.String(prefix),
	})

	var objects []types.ObjectIdentifier
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("snapshot s3: abort list objects: %w", err)
		}
		for _, obj := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
		}
	}

	if len(objects) == 0 {
		return nil
	}

	// 批量删除（每批最多 1000 个）
	for i := 0; i < len(objects); i += 1000 {
		end := i + 1000
		if end > len(objects) {
			end = len(objects)
		}
		_, err := w.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(w.bucket),
			Delete: &types.Delete{
				Objects: objects[i:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("snapshot s3: abort delete objects: %w", err)
		}
	}

	return nil
}

func (w *s3Writer) Close() error {
	return nil
}
