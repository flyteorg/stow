package unionmeta

import (
	"github.com/flyteorg/stow"
	"net/url"
)

type item struct {
	stow.Item
}

func (i item) URL() *url.URL {
	u := i.Item.URL()
	u.Scheme = Kind
	return u
}
