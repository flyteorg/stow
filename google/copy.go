package google

import (
	"context"

	"github.com/flyteorg/stow"
)

var _ stow.Copier = (*Container)(nil)

// Copy copies src into the container on the Google Cloud Storage side. The
// metadata of the source object is kept. An item from another location is
// streamed instead.
func (c *Container) Copy(ctx context.Context, src stow.Item, name string) (stow.Item, error) {
	// An item of another client may live in another service, e.g. an emulator,
	// where the same bucket and name are a different object.
	srcItem, ok := src.(*Item)
	if !ok || srcItem.client != c.client {
		return stow.StreamCopy(ctx, c, src, name)
	}

	source := c.client.Bucket(srcItem.container.name).Object(srcItem.name)
	attrs, err := c.Bucket().Object(name).CopierFrom(source).Run(ctx)
	if err != nil {
		return nil, err
	}

	return c.convertToStowItem(attrs)
}
