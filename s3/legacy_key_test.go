package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flyteorg/stow"
)

func TestLegacyKey(t *testing.T) {
	for _, tc := range []struct {
		key, want string
		differs   bool
	}{
		{"a/b", "a/b", false},
		{"a//b", "a/b", true},
		{"a///b", "a/b", true},
		{"configuration/ymlJ//Vut5=", "configuration/ymlJ/Vut5=", true},
		{"a//b/", "a/b/", true},
		{"a/./b", "a/b", true},
		{"a/x/../b", "a/b", true},
		{"/a", "a", true},
		{"dir/", "dir/", false},
		{"", "", false},
		{"//", "", false}, // nothing left to look up
	} {
		got, differs := legacyKey(tc.key)
		assert.Equal(t, tc.differs, differs, tc.key)
		if differs {
			assert.Equal(t, tc.want, got, tc.key)
		}
	}
}

// legacyS3 serves HeadObject and GetObject on one bucket, path-style. With
// rejectEmptySegments set it answers keys with "//" the way MinIO does: 400.
type legacyS3 struct {
	objects             map[string]string
	rejectEmptySegments bool
	heads               []string
}

func (f *legacyS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method == http.MethodHead {
		f.heads = append(f.heads, key)
	}
	if f.rejectEmptySegments && strings.Contains(key, "//") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	obj, ok := f.objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", `"etag"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj)))
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, obj)
	}
}

func legacyContainer(t *testing.T, f *legacyS3) stow.Container {
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	location, err := stow.Dial(Kind, stow.ConfigMap{
		ConfigAccessKeyID: "access-key",
		ConfigSecretKey:   "secret-key",
		ConfigRegion:      "us-east-1",
		ConfigEndpoint:    server.URL,
	})
	require.NoError(t, err)
	container, err := location.Container("bucket")
	require.NoError(t, err)
	return container
}

func readItem(t *testing.T, item stow.Item) string {
	r, err := item.Open()
	require.NoError(t, err)
	defer r.Close()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

func TestItemFallsBackToLegacyKey(t *testing.T) {
	for _, tc := range []struct {
		name  string
		minio bool
	}{
		{"s3 answers not found", false},
		{"minio rejects the key", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &legacyS3{objects: map[string]string{"config/ab/cd=": "legacy"}, rejectEmptySegments: tc.minio}
			item, err := legacyContainer(t, f).Item("config/ab//cd=")
			require.NoError(t, err)
			assert.Equal(t, "config/ab/cd=", item.ID())
			assert.Equal(t, "legacy", readItem(t, item))
			assert.Equal(t, []string{"config/ab//cd=", "config/ab/cd="}, f.heads)
		})
	}
}

func TestItemPrefersExactKey(t *testing.T) {
	f := &legacyS3{objects: map[string]string{"config/ab//cd=": "exact", "config/ab/cd=": "legacy"}}
	item, err := legacyContainer(t, f).Item("config/ab//cd=")
	require.NoError(t, err)
	assert.Equal(t, "config/ab//cd=", item.ID())
	assert.Equal(t, "exact", readItem(t, item))
	assert.Equal(t, []string{"config/ab//cd="}, f.heads)
}

func TestItemNotFoundUnderEitherKey(t *testing.T) {
	f := &legacyS3{objects: map[string]string{}}
	_, err := legacyContainer(t, f).Item("config/ab//cd=")
	assert.ErrorIs(t, err, stow.ErrNotFound)
	assert.Equal(t, []string{"config/ab//cd=", "config/ab/cd="}, f.heads)
}

func TestItemCleanKeyHasNoFallback(t *testing.T) {
	f := &legacyS3{objects: map[string]string{}}
	_, err := legacyContainer(t, f).Item("config/ab/cd=")
	assert.ErrorIs(t, err, stow.ErrNotFound)
	assert.Equal(t, []string{"config/ab/cd="}, f.heads)
}
