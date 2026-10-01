package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/flyteorg/stow"
	"github.com/pkg/errors"
)

var (
	// copyObjectMaxSize is the largest object S3 copies with one CopyObject
	// request. Bigger objects are copied in parts.
	copyObjectMaxSize int64 = 5 << 30
	// copyPartSize is the size of the parts of a multipart copy.
	copyPartSize int64 = 1 << 30
)

var _ stow.Copier = (*container)(nil)

// Copy copies src into the container on the S3 side.
func (c *container) Copy(ctx context.Context, src stow.Item, name string) (stow.Item, error) {
	srcItem, ok := src.(*item)
	if !ok {
		return nil, stow.ErrCopyNotSupported
	}
	size, err := srcItem.Size()
	if err != nil {
		return nil, errors.Wrap(err, "Copy, getting the source size")
	}
	source := copySource(srcItem.container.name, srcItem.ID())

	var etag string
	if size <= copyObjectMaxSize {
		res, err := c.client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(c.name),
			Key:        aws.String(name),
			CopySource: aws.String(source),
		})
		if err != nil {
			return nil, errors.Wrap(err, "Copy, copying the object")
		}
		if res.CopyObjectResult != nil && res.CopyObjectResult.ETag != nil {
			etag = cleanEtag(*res.CopyObjectResult.ETag)
		}
	} else {
		etag, err = c.multipartCopy(ctx, srcItem, source, name, size)
		if err != nil {
			return nil, err
		}
	}

	return &item{
		container: c,
		client:    c.client,
		properties: properties{
			ETag: &etag,
			Key:  &name,
			Size: &size,
		},
	}, nil
}

// multipartCopy copies an object that is too big for CopyObject. A multipart
// upload does not inherit anything from the source, so the metadata and the
// content headers are carried over explicitly.
func (c *container) multipartCopy(ctx context.Context, srcItem *item, source, name string, size int64) (string, error) {
	head, err := srcItem.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcItem.container.name),
		Key:    aws.String(srcItem.ID()),
	})
	if err != nil {
		return "", errors.Wrap(err, "Copy, getting the source object")
	}

	upload, err := c.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(c.name),
		Key:                aws.String(name),
		Metadata:           head.Metadata,
		ContentType:        head.ContentType,
		ContentEncoding:    head.ContentEncoding,
		ContentDisposition: head.ContentDisposition,
		ContentLanguage:    head.ContentLanguage,
		CacheControl:       head.CacheControl,
	})
	if err != nil {
		return "", errors.Wrap(err, "Copy, creating the multipart upload")
	}

	abort := func() {
		// The caller's context may be the reason for the failure, and the
		// parts are billed until the upload is aborted.
		_, _ = c.client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(c.name),
			Key:      aws.String(name),
			UploadId: upload.UploadId,
		})
	}

	var parts []*s3.CompletedPart
	for start, number := int64(0), int64(1); start < size; start, number = start+copyPartSize, number+1 {
		end := start + copyPartSize - 1
		if end >= size {
			end = size - 1
		}
		res, err := c.client.UploadPartCopyWithContext(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(c.name),
			Key:             aws.String(name),
			UploadId:        upload.UploadId,
			PartNumber:      aws.Int64(number),
			CopySource:      aws.String(source),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		})
		if err != nil {
			abort()
			return "", errors.Wrapf(err, "Copy, copying part %d", number)
		}
		if res.CopyPartResult == nil {
			abort()
			return "", errors.Errorf("Copy, copying part %d: empty result", number)
		}
		parts = append(parts, &s3.CompletedPart{
			ETag:       res.CopyPartResult.ETag,
			PartNumber: aws.Int64(number),
		})
	}

	res, err := c.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(c.name),
		Key:             aws.String(name),
		UploadId:        upload.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		abort()
		return "", errors.Wrap(err, "Copy, completing the multipart upload")
	}
	if res.ETag == nil {
		return "", nil
	}
	return cleanEtag(*res.ETag), nil
}

// copySource builds the URL-encoded "bucket/key" S3 expects as a copy source.
// S3 reads a literal "+" in it as a space, so it is escaped as well.
func copySource(bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, segment := range segments {
		segments[i] = strings.ReplaceAll(url.PathEscape(segment), "+", "%2B")
	}
	return url.PathEscape(bucket) + "/" + strings.Join(segments, "/")
}
