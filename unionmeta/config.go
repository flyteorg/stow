package unionmeta

import (
	"fmt"
	"github.com/flyteorg/stow"
	"github.com/flyteorg/stow/s3"
	"net/url"
)

// Kind represents the name of the location/storage type.
const Kind = "unionmeta"

func MakeFunc(config stow.Config) (stow.Location, error) {
	s3Loc, err := s3.MakeFunc(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the underlying location: %w", err)
	}

	return &location{
		Location:     s3Loc,
		targetScheme: "s3",
	}, nil
}

func init() {
	kindfn := func(u *url.URL) bool {
		return u.Scheme == Kind
	}

	stow.Register(Kind, MakeFunc, kindfn, s3.ValidateFunc)
}
