package s3

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/amoylan/flash-store/store/snapshot"
)

type s3Reader struct {
	client *s3.Client
	bucket string
	prefix string
}

// LatestVersion 列出指定 name 下所有已完成的版本（含 _SUCCESS 标记），返回最新版本。
func (r *s3Reader) LatestVersion(ctx context.Context, name string) (string, error) {
	prefix := namePrefix(r.prefix, name)

	// 用 Delimiter "/" 列出 version 级别的 common prefixes
	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(r.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	var versions []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", err
		}
		for _, cp := range page.CommonPrefixes {
			// cp.Prefix 形如 "prefix/name/20260409-153000/"
			ver := extractVersion(prefix, aws.ToString(cp.Prefix))
			if ver == "" {
				continue
			}
			versions = append(versions, ver)
		}
	}

	if len(versions) == 0 {
		return "", snapshot.ErrNoSnapshot
	}

	// 按字典序排序，时间戳格式保证字典序 == 时间序
	sort.Strings(versions)

	// 从最新往前找，第一个有 _SUCCESS 标记的就是最新完成版本
	for i := len(versions) - 1; i >= 0; i-- {
		ver := versions[i]
		markerKey := successMarkerKey(r.prefix, name, ver)
		_, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(r.bucket),
			Key:    aws.String(markerKey),
		})
		if err == nil {
			return ver, nil
		}
		// HeadObject 失败说明该版本未完成，跳过
	}

	return "", snapshot.ErrNoSnapshot
}

// ReadSegment 从 S3 读取指定 segment 的全部字节。
func (r *s3Reader) ReadSegment(ctx context.Context, seg snapshot.Segment) ([]byte, error) {
	key := segmentKey(r.prefix, seg.Name, seg.Version, seg)

	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	var buf bytes.Buffer
	if out.ContentLength != nil && *out.ContentLength > 0 {
		buf.Grow(int(*out.ContentLength))
	}
	if _, err := io.Copy(&buf, out.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *s3Reader) Close() error {
	return nil
}

// extractVersion 从 common prefix 中提取版本名。
// cpPrefix 形如 "snapshots/order-store/20260409-153000/"，namePrefix 为 "snapshots/order-store/"
func extractVersion(namePfx, cpPrefix string) string {
	trimmed := strings.TrimPrefix(cpPrefix, namePfx)
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" || strings.Contains(trimmed, "/") {
		return ""
	}
	return trimmed
}
