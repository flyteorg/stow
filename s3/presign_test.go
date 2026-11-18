package s3

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flyteorg/stow"
)

// Presigning works with a custom endpoint: the Accept-Encoding middleware stays out of the presign
// client's stack, which has no Signing step to place it around.
func TestPreSignRequestWithEndpoint(t *testing.T) {
	c := fakeContainer(t, newFakeS3(t))
	for _, method := range []stow.ClientMethod{stow.ClientMethodPut, stow.ClientMethodGet} {
		resp, err := c.(*container).PreSignRequest(context.Background(), method, "dir/k", stow.PresignRequestParams{ExpiresIn: time.Minute})
		require.NoError(t, err, method)
		u, err := url.Parse(resp.Url)
		require.NoError(t, err)
		assert.Equal(t, "/bucket/dir/k", u.Path)
		assert.NotEmpty(t, u.Query().Get("X-Amz-Signature"))
	}
}
