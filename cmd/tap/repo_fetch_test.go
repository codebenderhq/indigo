package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	repolib "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/cmd/tap/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadRepoFixture(t *testing.T) (identity.Identity, []byte) {
	t.Helper()
	docBytes, err := os.ReadFile("../../testing/testdata/greenground.didDoc.json")
	require.NoError(t, err)
	var doc identity.DIDDocument
	require.NoError(t, json.Unmarshal(docBytes, &doc))
	carBytes, err := os.ReadFile("../../testing/testdata/greenground.repo.car")
	require.NoError(t, err)
	return identity.ParseIdentity(&doc), carBytes
}

func fixtureRepoClient(t *testing.T, body []byte, contentLength int64, limit int64) *http.Client {
	t.Helper()
	return newCandidateHTTPClientWithTransport(time.Second, limit, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "/xrpc/com.atproto.sync.getRepo", req.URL.Path)
		assert.Equal(t, "did:plc:wqgdnqlv2mwiio6pfchwtrff", req.URL.Query().Get("did"))
		assert.Equal(t, userAgent(), req.Header.Get("User-Agent"))
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: contentLength,
			Body:          io.NopCloser(bytes.NewReader(body)),
			Request:       req,
		}, nil
	}))
}

func TestFetchRepoCARExactLimitStreamsToDisk(t *testing.T) {
	_, carBytes := loadRepoFixture(t)
	tempDir := t.TempDir()
	r := &Resyncer{
		repoHTTPClient: fixtureRepoClient(t, carBytes, int64(len(carBytes)), int64(len(carBytes))),
		repoMaxBytes:   int64(len(carBytes)),
		repoTempDir:    tempDir,
	}
	file, size, err := r.fetchRepoCAR(t.Context(), "https://bsky.social", "did:plc:wqgdnqlv2mwiio6pfchwtrff")
	require.NoError(t, err)
	assert.Equal(t, int64(len(carBytes)), size)
	stat, err := file.Stat()
	require.NoError(t, err)
	assert.Equal(t, int64(len(carBytes)), stat.Size())
	assertTempDirEmpty(t, tempDir)
	_, err = os.Stat(file.Name())
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, cleanupRepoFile(file))
	assertTempDirEmpty(t, tempDir)
}

func TestFetchRepoCARChunkedExactLimitIsAccepted(t *testing.T) {
	_, carBytes := loadRepoFixture(t)
	tempDir := t.TempDir()
	r := &Resyncer{
		repoHTTPClient: fixtureRepoClient(t, carBytes, -1, int64(len(carBytes))),
		repoMaxBytes:   int64(len(carBytes)),
		repoTempDir:    tempDir,
	}
	file, size, err := r.fetchRepoCAR(t.Context(), "https://bsky.social", "did:plc:wqgdnqlv2mwiio6pfchwtrff")
	require.NoError(t, err)
	assert.Equal(t, int64(len(carBytes)), size)
	assertTempDirEmpty(t, tempDir)
	require.NoError(t, cleanupRepoFile(file))
	assertTempDirEmpty(t, tempDir)
}

type cancelingChunkReader struct {
	reader io.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *cancelingChunkReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	n, err := r.reader.Read(p)
	r.reads++
	if r.reads == 8 {
		r.cancel()
	}
	return n, err
}

func TestRepoCARParseStopsOnContextCancellation(t *testing.T) {
	_, carBytes := loadRepoFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	reader := &cancelingChunkReader{reader: bytes.NewReader(carBytes), cancel: cancel}
	_, _, err := repolib.LoadRepoFromCAR(ctx, &contextReader{ctx: ctx, reader: reader})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), err)
}

func TestFetchRepoCARRejectsDeclaredAndChunkedOverLimitAndCleansTemp(t *testing.T) {
	for _, contentLength := range []int64{33, -1} {
		t.Run(httpContentLengthName(contentLength), func(t *testing.T) {
			tempDir := t.TempDir()
			body := &countingReadCloser{reader: bytes.NewReader(bytes.Repeat([]byte("x"), 64))}
			r := &Resyncer{
				repoMaxBytes: 32,
				repoTempDir:  tempDir,
			}
			r.repoHTTPClient = newCandidateHTTPClientWithTransport(time.Second, 32, roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: contentLength, Body: body, Request: req}, nil
			}))

			_, _, err := r.fetchRepoCAR(t.Context(), "https://pds.example", "did:plc:wqgdnqlv2mwiio6pfchwtrff")
			var limitErr *HTTPBodyTooLargeError
			require.ErrorAs(t, err, &limitErr)
			assert.LessOrEqual(t, body.read, int64(33))
			assertTempDirEmpty(t, tempDir)
		})
	}
}

func TestFetchRepoCARContextCancellationCleansTemp(t *testing.T) {
	tempDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	r := &Resyncer{repoMaxBytes: 1024, repoTempDir: tempDir}
	r.repoHTTPClient = newCandidateHTTPClientWithTransport(time.Minute, 1024, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			_, _ = writer.Write([]byte("partial"))
			<-req.Context().Done()
			_ = writer.CloseWithError(req.Context().Err())
		}()
		return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: reader, Request: req}, nil
	}))

	done := make(chan error, 1)
	go func() {
		_, _, err := r.fetchRepoCAR(ctx, "https://pds.example", "did:plc:wqgdnqlv2mwiio6pfchwtrff")
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("repo fetch did not stop after context cancellation")
	}
	assertTempDirEmpty(t, tempDir)
}

func TestCARBlockPreflightExactAndTooManySmallBlocks(t *testing.T) {
	_, carBytes := loadRepoFixture(t)
	file, err := os.CreateTemp(t.TempDir(), "blocks-*.car")
	require.NoError(t, err)
	defer cleanupRepoFile(file)
	_, err = file.Write(carBytes)
	require.NoError(t, err)

	count, err := preflightCARBlocks(t.Context(), file, maxRepoMaxBlocks)
	require.NoError(t, err)
	require.Greater(t, count, int64(1))
	exact, err := preflightCARBlocks(t.Context(), file, count)
	require.NoError(t, err)
	assert.Equal(t, count, exact)
	_, err = preflightCARBlocks(t.Context(), file, count-1)
	var blockLimitErr *CARBlockLimitError
	require.ErrorAs(t, err, &blockLimitErr)
}

func TestResyncDidTooManyBlocksIsTerminalAndCleansTemp(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ident, carBytes := loadRepoFixture(t)
	te.idDir.Insert(ident)
	te.insertRepo(ident.DID.String(), models.RepoStateResyncing, "", "", "")
	r.repoHTTPClient = fixtureRepoClient(t, carBytes, int64(len(carBytes)), int64(len(carBytes)))
	r.repoMaxBytes = int64(len(carBytes))
	r.repoMaxBlocks = 3
	r.repoTempDir = t.TempDir()

	err := r.resyncDid(te.ctx, ident.DID.String())
	var blockLimitErr *CARBlockLimitError
	require.ErrorAs(t, err, &blockLimitErr)
	var tracked models.Repo
	require.NoError(t, te.db.First(&tracked, "did = ?", ident.DID.String()).Error)
	assert.Equal(t, models.RepoStateTerminal, tracked.State)
	assertTempDirEmpty(t, r.repoTempDir)
}

func TestDoResyncRejectsWrongPDSServiceType(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ident, _ := loadRepoFixture(t)
	service := ident.Services["atproto_pds"]
	service.Type = "NotAPersonalDataServer"
	ident.Services["atproto_pds"] = service
	te.idDir.Insert(ident)
	te.insertRepo(ident.DID.String(), models.RepoStatePending, "", "", "")

	success, err := r.doResync(te.ctx, ident.DID.String())
	assert.False(t, success)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid type")
}

func TestCandidatePDSEndpointCanonicalizesTrailingSlash(t *testing.T) {
	ident, _ := loadRepoFixture(t)
	service := ident.Services["atproto_pds"]
	service.URL += "/"
	ident.Services["atproto_pds"] = service
	endpoint, err := candidatePDSEndpoint(&ident)
	require.NoError(t, err)
	assert.Equal(t, "https://bsky.social", endpoint)
}

func TestDoResyncStillRejectsInvalidSignature(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ident, carBytes := loadRepoFixture(t)
	key := ident.Keys["atproto"]
	key.PublicKeyMultibase = "zRUTFuPmtaLAcwxS8GGhyTy2y9CmbNLRJtvqNuYXZ5SCU2hRvsJHpiZzDWafLhBx6YiN2NEE7QHen1pokhd7Ptsnb"
	ident.Keys["atproto"] = key
	te.idDir.Insert(ident)
	te.insertRepo(ident.DID.String(), models.RepoStatePending, "", "", "")
	r.repoHTTPClient = fixtureRepoClient(t, carBytes, int64(len(carBytes)), int64(len(carBytes)))
	r.repoMaxBytes = int64(len(carBytes))
	r.repoMaxBlocks = defaultRepoMaxBlocks
	r.repoTempDir = t.TempDir()

	success, err := r.doResync(te.ctx, ident.DID.String())
	assert.False(t, success)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify signature")
	assertTempDirEmpty(t, r.repoTempDir)
}

func TestDoResyncStillRejectsAlteredCAR(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ident, carBytes := loadRepoFixture(t)
	altered := append([]byte(nil), carBytes...)
	altered[len(altered)-1] ^= 0xff
	te.idDir.Insert(ident)
	te.insertRepo(ident.DID.String(), models.RepoStatePending, "", "", "")
	r.repoHTTPClient = fixtureRepoClient(t, altered, int64(len(altered)), int64(len(altered)))
	r.repoMaxBytes = int64(len(altered))
	r.repoMaxBlocks = defaultRepoMaxBlocks
	r.repoTempDir = t.TempDir()

	success, err := r.doResync(te.ctx, ident.DID.String())
	assert.False(t, success)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "content integrity") || strings.Contains(err.Error(), "CID"), err.Error())
	assertTempDirEmpty(t, r.repoTempDir)
}

func assertTempDirEmpty(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	assert.Empty(t, entries)
}
