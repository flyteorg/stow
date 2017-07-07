package s3

import (
	"io"
	"time"

	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/graymeta/stow"
	"github.com/pkg/errors"
)

type item struct {
	id           string
	container    *container
	client       *s3.S3
	eTag         string
	lastModified *time.Time
	size         int64
	metadata     map[string]interface{}
}

func (i *item) ID() string {
	if i == nil {
		return ""
	}
	return i.id
}

func (i *item) Name() string {
	if i == nil {
		return ""
	}
	return i.id
}

func (i *item) Size() (int64, error) {
	if i == nil {
		return 0, stow.ErrNotFound
	}
	return i.size, nil
}

func (i *item) URL() *url.URL {
	if i == nil {
		return &url.URL{}
	}

	u := fmt.Sprintf("https://s3-%s.amazonaws.com/%s/%s", i.container.region, i.container.name, i.id)

	return &url.URL{Scheme: "s3", Path: u}
}

func (i *item) Open() (io.ReadCloser, error) {
	if i == nil {
		return nil, stow.ErrNotFound
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(i.container.name),
		Key:    aws.String(i.id),
	}

	res, err := i.client.GetObject(params)
	if err != nil {
		return nil, errors.Wrap(err, "opening the item")
	}

	return res.Body, nil
}

func (i *item) LastMod() (time.Time, error) {
	if i == nil {
		return time.Time{}, stow.ErrNotFound
	}
	return *i.lastModified, nil
}

func (i *item) ETag() (string, error) {
	if i == nil {
		return "", stow.ErrNotFound
	}
	return i.eTag, nil
}

func (i *item) Metadata() (map[string]interface{}, error) {
	if i == nil {
		return nil, stow.ErrNotFound
	}
	return i.metadata, nil
}