package google

import (
	"context"

	"github.com/flyteorg/stow"
)

var _ stow.Copier = (*Container)(nil)

// Copy copies src into the container on the Google Cloud Storage side. The
// metadata of the source object is kept.
func (c *Container) Copy(ctx context.Context, src stow.Item, name string) (stow.Item, error) {
	srcItem, ok := src.(*Item)
	if !ok {
		return nil, stow.ErrCopyNotSupported
	}

	source := c.client.Bucket(srcItem.container.name).Object(srcItem.name)
	attrs, err := c.Bucket().Object(name).CopierFrom(source).Run(ctx)
	if err != nil {
		return nil, err
	}

	return c.convertToStowItem(attrs)
}
