package s3

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/aws/smithy-go"
	"github.com/flyteorg/stow"
)

// getItem looks up the object stored under id. If there is none and id is not
// in the form aws-sdk-go v1 sent it (see legacyKey), it looks up the legacy
// key instead, so objects written before v0.5.0 can still be read.
//
// The returned item carries the key it was found under, so reading it opens
// that object.
func (c *container) getItem(ctx context.Context, id string) (*item, error) {
	i, err := c.headItem(ctx, id)
	if err == nil {
		return i, nil
	}

	legacy, ok := legacyKey(id)
	if !ok || !mayBeLegacyKey(err) {
		return nil, err
	}

	legacyItem, legacyErr := c.headItem(ctx, legacy)
	if legacyErr != nil {
		// The legacy key is only a fallback: report what happened to id.
		return nil, err
	}
	return legacyItem, nil
}

// legacyKey returns the key that the aws-sdk-go v1 client used for key, and
// whether it differs from key.
//
// Up to v0.4, stow's S3 provider used aws-sdk-go v1, which cleans the path of
// every request with path.Clean and keeps a trailing slash: an object written
// as "a//b" was stored as "a/b". aws-sdk-go-v2 sends the key as given, so it
// no longer finds that object.
func legacyKey(key string) (string, bool) {
	cleaned := path.Clean("/" + key)
	if strings.HasSuffix(key, "/") && !strings.HasSuffix(cleaned, "/") {
		cleaned += "/"
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	return cleaned, cleaned != key && cleaned != ""
}

// mayBeLegacyKey reports whether err from a lookup of a key that v1 would have
// cleaned can mean the object is stored under its legacy key. S3 answers
// NotFound. MinIO rejects keys with empty or dot segments with 400, which
// HeadObject reports as BadRequest since the response has no body.
func mayBeLegacyKey(err error) bool {
	if errors.Is(err, stow.ErrNotFound) {
		return true
	}
	if aerr, ok := errors.AsType[smithy.APIError](err); ok {
		switch aerr.ErrorCode() {
		case "NotFound", "NoSuchKey", "BadRequest", "XMinioInvalidObjectName":
			return true
		}
	}
	return false
}
