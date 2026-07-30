package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/tap/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type failingIdentityDirectory struct {
	err error
}

func (d *failingIdentityDirectory) LookupHandle(context.Context, syntax.Handle) (*identity.Identity, error) {
	return nil, d.err
}

func (d *failingIdentityDirectory) LookupDID(context.Context, syntax.DID) (*identity.Identity, error) {
	return nil, d.err
}

func (d *failingIdentityDirectory) Lookup(context.Context, syntax.AtIdentifier) (*identity.Identity, error) {
	return nil, d.err
}

func (d *failingIdentityDirectory) Purge(context.Context, syntax.AtIdentifier) error {
	return nil
}

func newTestResyncer(te *testEnv) *Resyncer {
	config := &TapConfig{
		ResyncParallelism: 1,
		RepoFetchTimeout:  30 * time.Second,
		EventCacheSize:    1000,
	}
	return NewResyncer(te.server.logger, te.db, te.repos, te.events, config)
}

// --- claimResyncJob tests ---

func TestClaimResyncJob_Priority(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	te.insertRepo("did:example:pending", models.RepoStatePending, "", "", "")
	te.insertRepo("did:example:desynced", models.RepoStateDesynchronized, "", "", "")
	te.insertRepo("did:example:errored", models.RepoStateError, "", "", "")

	// First claim should get pending
	did1, found1, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found1 || did1 != "did:example:pending" {
		t.Fatalf("expected pending repo first, got did=%q found=%v", did1, found1)
	}

	// Second claim should get desynchronized
	did2, found2, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found2 || did2 != "did:example:desynced" {
		t.Fatalf("expected desynchronized repo second, got did=%q found=%v", did2, found2)
	}

	// Third claim should get error
	did3, found3, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found3 || did3 != "did:example:errored" {
		t.Fatalf("expected error repo third, got did=%q found=%v", did3, found3)
	}
}

func TestClaimResyncJob_RetryAfterRespected(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:retry-after"
	te.insertRepo(did, models.RepoStateError, "", "", "")

	// Set retry_after to the future
	futureTs := time.Now().Add(1 * time.Hour).Unix()
	te.db.Model(&models.Repo{}).Where("did = ?", did).Update("retry_after", futureTs)

	// Should not be claimable
	_, found, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected no job found when retry_after is in the future")
	}

	// Set retry_after to the past
	pastTs := time.Now().Add(-1 * time.Minute).Unix()
	te.db.Model(&models.Repo{}).Where("did = ?", did).Update("retry_after", pastTs)

	// Should now be claimable
	got, found, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found || got != did {
		t.Fatalf("expected errored repo to be claimable, got did=%q found=%v", got, found)
	}
}

func TestClaimResyncJob_NoneAvailable(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	// Empty DB — no repos
	_, found, err := r.claimResyncJob(te.ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected no job found in empty DB")
	}
}

// --- handleResyncError tests ---

func TestHandleResyncError_WithError(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:resync-err"
	te.insertRepo(did, models.RepoStateResyncing, "3jzfcijpj2z2a", "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454", "alice.test")

	err := r.handleResyncError(te.ctx, did, fmt.Errorf("something went wrong"))
	if err == nil || err.Error() != "something went wrong" {
		t.Fatalf("expected original error returned, got: %v", err)
	}

	var repo models.Repo
	te.db.First(&repo, "did = ?", did)

	if repo.State != models.RepoStateError {
		t.Fatalf("expected state=error, got %s", repo.State)
	}
	if repo.ErrorMsg != "something went wrong" {
		t.Fatalf("expected error_msg='something went wrong', got %q", repo.ErrorMsg)
	}
	if repo.RetryCount != 1 {
		t.Fatalf("expected retry_count=1, got %d", repo.RetryCount)
	}
	// retry_after should be in the future (roughly 60s from now, with jitter)
	retryAfterTime := time.Unix(repo.RetryAfter, 0)
	if retryAfterTime.Before(time.Now()) {
		t.Fatalf("expected retry_after in the future, got %v", retryAfterTime)
	}
}

func TestHandleResyncError_WithNilError(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:resync-nil"
	te.insertRepo(did, models.RepoStateResyncing, "3jzfcijpj2z2a", "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454", "")

	err := r.handleResyncError(te.ctx, did, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var repo models.Repo
	te.db.First(&repo, "did = ?", did)

	if repo.State != models.RepoStateDesynchronized {
		t.Fatalf("expected state=desynchronized, got %s", repo.State)
	}
	if repo.ErrorMsg != "" {
		t.Fatalf("expected empty error_msg, got %q", repo.ErrorMsg)
	}
}

func TestHandleResyncError_ExponentialBackoff(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:backoff"
	te.insertRepo(did, models.RepoStatePending, "", "", "")

	var prevRetryAfter int64

	for i := 0; i < 4; i++ {
		te.db.Model(&models.Repo{}).Where("did = ?", did).Update("state", models.RepoStateResyncing)

		r.handleResyncError(te.ctx, did, fmt.Errorf("err"))

		var repo models.Repo
		te.db.First(&repo, "did = ?", did)

		if repo.RetryCount != i+1 {
			t.Fatalf("iteration %d: expected retry_count=%d, got %d", i, i+1, repo.RetryCount)
		}

		if i > 0 && repo.RetryAfter <= prevRetryAfter {
			t.Fatalf("iteration %d: expected retry_after to increase, got %d <= %d", i, repo.RetryAfter, prevRetryAfter)
		}
		prevRetryAfter = repo.RetryAfter
	}
}

func TestHandleResyncError_OversizeIsTerminal(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:oversized"
	te.insertRepo(did, models.RepoStateResyncing, "", "", "")
	limitErr := &HTTPBodyTooLargeError{Limit: 1024, Declared: 1025}

	err := r.handleResyncError(te.ctx, did, fmt.Errorf("failed to get repo: %w", limitErr))
	require.ErrorIs(t, err, limitErr)

	var repo models.Repo
	require.NoError(t, te.db.First(&repo, "did = ?", did).Error)
	assert.Equal(t, models.RepoStateTerminal, repo.State)
	assert.Zero(t, repo.RetryAfter)
	assert.Equal(t, 1, repo.RetryCount)

	_, found, err := r.claimResyncJob(te.ctx)
	require.NoError(t, err)
	assert.False(t, found, "terminal repos must not be retried")

	require.NoError(t, deleteRepo(te.db, did))
	te.insertRepo(did, models.RepoStatePending, "", "", "")
	claimed, found, err := r.claimResyncJob(te.ctx)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, did, claimed)
}

func TestHandleResyncErrorCannotOverwriteTerminalState(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	did := "did:example:absorbing-terminal"
	te.insertRepo(did, models.RepoStateTerminal, "", "", "")
	require.NoError(t, te.db.Model(&models.Repo{}).Where("did = ?", did).Updates(map[string]interface{}{
		"error_msg":   "poison identity",
		"retry_count": 7,
	}).Error)

	resyncErr := errors.New("late resync failure")
	err := r.handleResyncError(te.ctx, did, resyncErr)
	require.ErrorIs(t, err, resyncErr)
	require.ErrorIs(t, err, errResyncStateChanged)
	var tracked models.Repo
	require.NoError(t, te.db.First(&tracked, "did = ?", did).Error)
	assert.Equal(t, models.RepoStateTerminal, tracked.State)
	assert.Equal(t, "poison identity", tracked.ErrorMsg)
	assert.Equal(t, 7, tracked.RetryCount)
}

func TestResyncDidOversizedIdentityIsTerminal(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	did := "did:plc:wqgdnqlv2mwiio6pfchwtrff"
	te.insertRepo(did, models.RepoStateResyncing, "", "", "")
	limitErr := &HTTPBodyTooLargeError{Limit: 1024, Declared: -1}
	te.repos.idDir = &failingIdentityDirectory{err: fmt.Errorf("identity response: %w", limitErr)}

	err := r.resyncDid(te.ctx, did)
	require.ErrorIs(t, err, limitErr)
	var repo models.Repo
	require.NoError(t, te.db.First(&repo, "did = ?", did).Error)
	assert.Equal(t, models.RepoStateTerminal, repo.State)
}

func TestResyncDidOversizedCARIsTerminal(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	docBytes, err := os.ReadFile("../../testing/testdata/greenground.didDoc.json")
	require.NoError(t, err)
	var doc identity.DIDDocument
	require.NoError(t, json.Unmarshal(docBytes, &doc))
	te.idDir.Insert(identity.ParseIdentity(&doc))
	did := doc.DID.String()
	te.insertRepo(did, models.RepoStateResyncing, "", "", "")
	r.repoHTTPClient = newCandidateHTTPClientWithTransport(time.Second, 32, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: -1,
			Body:          io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 33))),
			Request:       req,
		}, nil
	}))

	err = r.resyncDid(te.ctx, did)
	var limitErr *HTTPBodyTooLargeError
	require.ErrorAs(t, err, &limitErr)
	var repo models.Repo
	require.NoError(t, te.db.First(&repo, "did = ?", did).Error)
	assert.Equal(t, models.RepoStateTerminal, repo.State)
}

func TestDoResyncSignedCARStillVerifiesAndWalksMST(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	docBytes, err := os.ReadFile("../../testing/testdata/greenground.didDoc.json")
	require.NoError(t, err)
	var doc identity.DIDDocument
	require.NoError(t, json.Unmarshal(docBytes, &doc))
	ident := identity.ParseIdentity(&doc)
	te.idDir.Insert(ident)

	carBytes, err := os.ReadFile("../../testing/testdata/greenground.repo.car")
	require.NoError(t, err)
	var requestCount int
	r.repoMaxBytes = int64(len(carBytes))
	r.repoMaxBlocks = defaultRepoMaxBlocks
	r.repoTempDir = t.TempDir()
	r.repoHTTPClient = newCandidateHTTPClientWithTransport(time.Second, int64(len(carBytes)), roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestCount++
		assert.Equal(t, "https", req.URL.Scheme)
		assert.Equal(t, "bsky.social", req.URL.Host)
		assert.Equal(t, "/xrpc/com.atproto.sync.getRepo", req.URL.Path)
		assert.Equal(t, userAgent(), req.Header.Get("User-Agent"))
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: int64(len(carBytes)),
			Body:          io.NopCloser(bytes.NewReader(carBytes)),
			Request:       req,
		}, nil
	}))

	te.insertRepo(doc.DID.String(), models.RepoStateResyncing, "", "", "")
	success, err := r.doResync(te.ctx, doc.DID.String())
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, 1, requestCount)

	var repo models.Repo
	require.NoError(t, te.db.First(&repo, "did = ?", doc.DID.String()).Error)
	assert.Equal(t, models.RepoStateActive, repo.State)
	assert.NotEmpty(t, repo.Rev)
	assert.NotEmpty(t, repo.PrevData)
	assertTempDirEmpty(t, r.repoTempDir)
}

func TestConcurrentTerminalStateWinsOverResyncSuccess(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ident, carBytes := loadRepoFixture(t)
	te.idDir.Insert(ident)
	did := ident.DID.String()
	te.insertRepo(did, models.RepoStateResyncing, "", "", "")
	r.repoHTTPClient = fixtureRepoClient(t, carBytes, int64(len(carBytes)), int64(len(carBytes)))
	r.repoMaxBytes = int64(len(carBytes))
	r.repoMaxBlocks = defaultRepoMaxBlocks
	r.repoTempDir = t.TempDir()

	activationStarted := make(chan struct{})
	releaseActivation := make(chan struct{})
	require.NoError(t, te.db.Callback().Update().Before("gorm:update").Register("test:block_resync_activation", func(tx *gorm.DB) {
		updates, ok := tx.Statement.Dest.(map[string]interface{})
		if ok && fmt.Sprint(updates["state"]) == string(models.RepoStateActive) {
			close(activationStarted)
			<-releaseActivation
		}
	}))
	defer te.db.Callback().Update().Remove("test:block_resync_activation")
	type result struct {
		success bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		success, err := r.doResync(te.ctx, did)
		done <- result{success: success, err: err}
	}()
	select {
	case <-activationStarted:
	case res := <-done:
		t.Fatalf("resync completed before activation hook: %v", res.err)
	case <-time.After(time.Second):
		t.Fatal("resync did not reach conditional activation")
	}
	terminalErr := errors.New("concurrent poison identity")
	require.NoError(t, te.repos.MarkRepoTerminal(te.ctx, did, terminalErr))
	close(releaseActivation)
	res := <-done
	assert.False(t, res.success)
	require.ErrorIs(t, res.err, errResyncStateChanged)
	var tracked models.Repo
	require.NoError(t, te.db.First(&tracked, "did = ?", did).Error)
	assert.Equal(t, models.RepoStateTerminal, tracked.State)
	assert.Equal(t, terminalErr.Error(), tracked.ErrorMsg)
}

// --- resetPartiallyResynced tests ---

func TestResetPartiallyResynced(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	te.insertRepo("did:example:resyncing-1", models.RepoStateResyncing, "", "", "")
	te.insertRepo("did:example:resyncing-2", models.RepoStateResyncing, "", "", "")
	te.insertRepo("did:example:active-repo", models.RepoStateActive, "3jzfcijpj2z2a", "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454", "")

	if err := r.resetPartiallyResynced(te.ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var r1, r2, r3 models.Repo
	te.db.First(&r1, "did = ?", "did:example:resyncing-1")
	te.db.First(&r2, "did = ?", "did:example:resyncing-2")
	te.db.First(&r3, "did = ?", "did:example:active-repo")

	if r1.State != models.RepoStateDesynchronized {
		t.Fatalf("expected resyncing1 state=desynchronized, got %s", r1.State)
	}
	if r2.State != models.RepoStateDesynchronized {
		t.Fatalf("expected resyncing2 state=desynchronized, got %s", r2.State)
	}
	if r3.State != models.RepoStateActive {
		t.Fatalf("expected active repo unchanged, got %s", r3.State)
	}
}

func TestResyncWorkersJoinPromptlyOnShutdown(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		r.run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("resync workers did not join after cancellation")
	}
}

// --- drainResyncBuffer tests ---

func TestDrainResyncBuffer_Basic(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:drain-basic"
	cidA := "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
	cidB := "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"
	rev1 := "3jzfcijpj2z2a"
	rev2 := "3jzfcijpj2z2b"
	recordCid := "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"

	te.insertRepo(did, models.RepoStateActive, rev1, cidA, "")

	commit := Commit{
		Did:      did,
		Rev:      rev2,
		DataCid:  cidB,
		PrevData: cidA,
		Ops: []CommitOp{
			{Collection: "app.bsky.feed.post", Rkey: "3jzfcijpj2z3a", Action: "create", Cid: recordCid},
		},
	}
	commitJSON, _ := json.Marshal(commit)

	te.db.Create(&models.ResyncBuffer{
		Did:  did,
		Data: string(commitJSON),
	})

	if err := r.drainResyncBuffer(te.ctx, did); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Buffer should be empty
	var bufCount int64
	te.db.Model(&models.ResyncBuffer{}).Where("did = ?", did).Count(&bufCount)
	if bufCount != 0 {
		t.Fatalf("expected buffer to be drained, got %d entries", bufCount)
	}

	// Repo should be updated
	var repo models.Repo
	te.db.First(&repo, "did = ?", did)
	if repo.Rev != rev2 {
		t.Fatalf("expected rev=%s, got %s", rev2, repo.Rev)
	}
	if repo.PrevData != cidB {
		t.Fatalf("expected prev_data=%s, got %s", cidB, repo.PrevData)
	}

	// Record should exist
	var recCount int64
	te.db.Model(&models.RepoRecord{}).Where("did = ?", did).Count(&recCount)
	if recCount != 1 {
		t.Fatalf("expected 1 record, got %d", recCount)
	}
}

func TestDrainResyncBuffer_SkipsMismatchedPrevData(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:mismatch"
	cidA := "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
	cidX := "bafyreigdm6oz2tlydvfmq4ch7tfaicrfqiwob3ux3xsh6lv5n4va5d5rem"
	cidB := "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"
	rev1 := "3jzfcijpj2z2a"
	rev2 := "3jzfcijpj2z2b"

	te.insertRepo(did, models.RepoStateActive, rev1, cidA, "")

	commit := Commit{
		Did:      did,
		Rev:      rev2,
		DataCid:  cidB,
		PrevData: cidX, // doesn't match repo's cidA
		Ops: []CommitOp{
			{Collection: "app.bsky.feed.post", Rkey: "3jzfcijpj2z3a", Action: "create", Cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"},
		},
	}
	commitJSON, _ := json.Marshal(commit)

	te.db.Create(&models.ResyncBuffer{
		Did:  did,
		Data: string(commitJSON),
	})

	if err := r.drainResyncBuffer(te.ctx, did); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Buffer entry should still exist (not processed)
	var bufCount int64
	te.db.Model(&models.ResyncBuffer{}).Where("did = ?", did).Count(&bufCount)
	if bufCount != 1 {
		t.Fatalf("expected buffer entry to remain, got %d", bufCount)
	}

	// Repo should be unchanged
	var repo models.Repo
	te.db.First(&repo, "did = ?", did)
	if repo.Rev != rev1 {
		t.Fatalf("expected rev unchanged at %s, got %s", rev1, repo.Rev)
	}
}

func TestDrainResyncBuffer_ChainedCommits(t *testing.T) {
	te := newTestEnv(t, testEnvOpts{})
	r := newTestResyncer(te)

	did := "did:example:chained"
	cidA := "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
	cidB := "bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu"
	cidC := "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm"
	rev1 := "3jzfcijpj2z2a"
	rev2 := "3jzfcijpj2z2b"
	rev3 := "3jzfcijpj2z2c"
	recCid1 := "bafyreigdm6oz2tlydvfmq4ch7tfaicrfqiwob3ux3xsh6lv5n4va5d5rem"
	recCid2 := "bafyreifhm3zcrp65kcjmv3ke6j3v7yio5vqxdmgpnlrji77ty6mfzselm"

	te.insertRepo(did, models.RepoStateActive, rev1, cidA, "")

	// Two chained commits: A->B, B->C
	commit1 := Commit{
		Did:      did,
		Rev:      rev2,
		DataCid:  cidB,
		PrevData: cidA,
		Ops: []CommitOp{
			{Collection: "app.bsky.feed.post", Rkey: "3jzfcijpj2z3a", Action: "create", Cid: recCid1},
		},
	}
	commit2 := Commit{
		Did:      did,
		Rev:      rev3,
		DataCid:  cidC,
		PrevData: cidB,
		Ops: []CommitOp{
			{Collection: "app.bsky.feed.post", Rkey: "3jzfcijpj2z3b", Action: "create", Cid: recCid2},
		},
	}

	c1JSON, _ := json.Marshal(commit1)
	c2JSON, _ := json.Marshal(commit2)

	te.db.Create(&models.ResyncBuffer{Did: did, Data: string(c1JSON)})
	te.db.Create(&models.ResyncBuffer{Did: did, Data: string(c2JSON)})

	if err := r.drainResyncBuffer(te.ctx, did); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var repo models.Repo
	te.db.First(&repo, "did = ?", did)

	// Both chained commits should be applied
	var bufCount int64
	te.db.Model(&models.ResyncBuffer{}).Where("did = ?", did).Count(&bufCount)

	if repo.Rev != rev3 {
		t.Fatalf("expected rev=%s after both commits, got %s", rev3, repo.Rev)
	}
	if repo.PrevData != cidC {
		t.Fatalf("expected prev_data=%s after both commits, got %s", cidC, repo.PrevData)
	}
	if bufCount != 0 {
		t.Fatalf("expected buffer to be fully drained, got %d entries", bufCount)
	}
}
