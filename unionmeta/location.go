package unionmeta

import (
	"github.com/flyteorg/stow"
	"net/url"
)

type location struct {
	stow.Location
	targetScheme string
}

func (l location) ItemByURL(url *url.URL) (stow.Item, error) {
	url.Scheme = l.targetScheme
	i, err := l.Location.ItemByURL(url)
	if err != nil {
		return nil, err
	}

	return item{Item: i}, nil
}
