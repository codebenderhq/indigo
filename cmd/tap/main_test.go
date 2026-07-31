package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCandidateByteLimitValidation(t *testing.T) {
	require.NoError(t, validateByteLimit("identity-max-bytes", defaultIdentityMaxBytes, maxIdentityMaxBytes))
	require.NoError(t, validateByteLimit("repo-max-bytes", defaultRepoMaxBytes, maxRepoMaxBytes))
	require.NoError(t, validateCountLimit("repo-max-blocks", defaultRepoMaxBlocks, maxRepoMaxBlocks))
	require.Error(t, validateByteLimit("identity-max-bytes", 0, maxIdentityMaxBytes))
	require.Error(t, validateByteLimit("identity-max-bytes", -1, maxIdentityMaxBytes))
	require.Error(t, validateByteLimit("identity-max-bytes", maxIdentityMaxBytes+1, maxIdentityMaxBytes))
	require.Error(t, validateByteLimit("repo-max-bytes", maxRepoMaxBytes+1, maxRepoMaxBytes))
	require.Error(t, validateCountLimit("repo-max-blocks", maxRepoMaxBlocks+1, maxRepoMaxBlocks))
}

func TestCandidateByteLimitFlagsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "zero identity", args: []string{"tap", "run", "--identity-max-bytes", "0"}, want: "must be a positive"},
		{name: "negative repo", args: []string{"tap", "run", "--repo-max-bytes", "-1"}, want: "must be a positive"},
		{name: "identity ceiling", args: []string{"tap", "run", "--identity-max-bytes", fmt.Sprint(maxIdentityMaxBytes + 1)}, want: "must not exceed"},
		{name: "repo ceiling", args: []string{"tap", "run", "--repo-max-bytes", fmt.Sprint(maxRepoMaxBytes + 1)}, want: "must not exceed"},
		{name: "zero blocks", args: []string{"tap", "run", "--repo-max-blocks", "0"}, want: "must be a positive"},
		{name: "blocks ceiling", args: []string{"tap", "run", "--repo-max-blocks", fmt.Sprint(maxRepoMaxBlocks + 1)}, want: "must not exceed"},
		{name: "resync parallelism fixed", args: []string{"tap", "run", "--resync-parallelism", "2"}, want: "must be 1"},
		{name: "zero outbox parallelism", args: []string{"tap", "run", "--outbox-parallelism", "0"}, want: "outbox-parallelism must be positive"},
		{name: "outbox parallelism ceiling", args: []string{"tap", "run", "--outbox-parallelism", fmt.Sprint(maxOutboxParallelism + 1)}, want: "outbox-parallelism must not exceed"},
		{name: "zero firehose parallelism", args: []string{"tap", "run", "--firehose-parallelism", "0"}, want: "firehose-parallelism must be positive"},
		{name: "zero outbox capacity", args: []string{"tap", "run", "--outbox-capacity", "0"}, want: "outbox-capacity must be positive"},
		{name: "zero identity cache", args: []string{"tap", "run", "--ident-cache-size", "0"}, want: "ident-cache-size must be positive"},
		{name: "zero database connections", args: []string{"tap", "run", "--max-db-conn", "0"}, want: "max-db-conn must be positive"},
		{name: "not numeric", args: []string{"tap", "run", "--repo-max-bytes", "64MiB"}, want: "invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(tt.args)
			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.want))
		})
	}
}

func TestCandidateByteLimitEnvironmentRejectsInvalidValue(t *testing.T) {
	t.Setenv("TAP_IDENTITY_MAX_BYTES", "0")
	err := run([]string{"tap", "run"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity-max-bytes must be a positive")
}

func TestRepoBlockLimitEnvironmentRejectsInvalidValue(t *testing.T) {
	t.Setenv("TAP_REPO_MAX_BLOCKS", "0")
	err := run([]string{"tap", "run"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repo-max-blocks must be a positive")
}

func TestNewTapRejectsUnsafePLCBeforeOpeningDatabase(t *testing.T) {
	_, err := NewTap(TapConfig{PLCURL: "http://127.0.0.1", DatabaseURL: "unsupported://database"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid PLC URL")
}

func TestNewTapRejectsZeroOutboxParallelismBeforeOpeningDatabase(t *testing.T) {
	config := TapConfig{
		DatabaseURL:         "unsupported://database",
		PLCURL:              "https://plc.directory",
		DBMaxConns:          1,
		FirehoseParallelism: 1,
		ResyncParallelism:   1,
		OutboxParallelism:   0,
		IdentityCacheSize:   1,
		EventCacheSize:      1,
	}
	_, err := NewTap(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outbox-parallelism must be positive")
}
