package google

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/flyteorg/stow"
)

// TestCopyEmulator runs against a Google Cloud Storage emulator, e.g.
// STORAGE_EMULATOR_HOST=127.0.0.1:4443 with fake-gcs-server.
func TestCopyEmulator(t *testing.T) {
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("skipping test because missing STORAGE_EMULATOR_HOST")
	}
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	defer client.Close()

	newContainer := func(prefix string) *Container {
		name := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
		require.NoError(t, client.Bucket(name).Create(ctx, "stow", nil))
		return &Container{name: name, client: client, ctx: ctx}
	}
	srcContainer, dstContainer := newContainer("stow-copy-src"), newContainer("stow-copy-dst")

	content := make([]byte, 9<<20)
	_, err = rand.Read(content)
	require.NoError(t, err)
	metadata := map[string]interface{}{"stow": "copy"}
	_, err = srcContainer.Put("dir/src item.pb", bytes.NewReader(content), int64(len(content)), metadata)
	require.NoError(t, err)

	src, err := srcContainer.Item("dir/src item.pb")
	require.NoError(t, err)

	copied, err := dstContainer.Copy(ctx, src, "dir/dst.pb")
	require.NoError(t, err)
	assert.Equal(t, "dir/dst.pb", copied.ID())
	size, err := copied.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), size)

	dst, err := dstContainer.Item("dir/dst.pb")
	require.NoError(t, err)
	r, err := dst.Open()
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, got), "copied content differs")
	md, err := dst.Metadata()
	require.NoError(t, err)
	assert.Equal(t, metadata, md)

	t.Run("missing source", func(t *testing.T) {
		require.NoError(t, srcContainer.RemoveItem(src.ID()))
		_, err := dstContainer.Copy(ctx, src, "dir/missing.pb")
		assert.Error(t, err)
	})
}

func TestCopyForeignItem(t *testing.T) {
	_, err := (&Container{}).Copy(context.Background(), nil, "name")
	assert.Equal(t, stow.ErrCopyNotSupported, err)
}
