package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/cmd/tap/models"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Tap struct {
	db     *gorm.DB
	logger *slog.Logger

	firehose *FirehoseProcessor
	events   *EventManager
	repos    *RepoManager
	resyncer *Resyncer
	crawler  *Crawler

	server *TapServer
	outbox *Outbox

	outboxOnly bool
}

type TapConfig struct {
	DatabaseURL                string
	DBMaxConns                 int
	PLCURL                     string
	RelayUrl                   string
	FirehoseParallelism        int
	ResyncParallelism          int
	OutboxParallelism          int
	FirehoseCursorSaveInterval time.Duration
	NoReplay                   bool
	RepoFetchTimeout           time.Duration
	IdentityMaxBytes           int64
	RepoMaxBytes               int64
	RepoMaxBlocks              int64
	IdentityCacheSize          int
	EventCacheSize             int
	FullNetworkMode            bool
	SignalCollection           string
	DisableAcks                bool
	WebhookURL                 string
	CollectionFilters          []string // e.g., ["app.bsky.feed.post", "app.bsky.graph.*"]
	OutboxOnly                 bool
	AdminPassword              string
	RetryTimeout               time.Duration
}

const (
	maxDBConnections       = 1_024
	maxFirehoseParallelism = 1_024
	maxOutboxParallelism   = 128
	maxCacheEntries        = 10_000_000
)

func validateIntSetting(name string, value, maximum int) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	if value > maximum {
		return fmt.Errorf("%s must not exceed %d", name, maximum)
	}
	return nil
}

func validateTapRuntimeSettings(config TapConfig) error {
	for _, setting := range []struct {
		name    string
		value   int
		maximum int
	}{
		{name: "max-db-conn", value: config.DBMaxConns, maximum: maxDBConnections},
		{name: "firehose-parallelism", value: config.FirehoseParallelism, maximum: maxFirehoseParallelism},
		{name: "outbox-parallelism", value: config.OutboxParallelism, maximum: maxOutboxParallelism},
		{name: "ident-cache-size", value: config.IdentityCacheSize, maximum: maxCacheEntries},
		{name: "outbox-capacity", value: config.EventCacheSize, maximum: maxCacheEntries},
	} {
		if err := validateIntSetting(setting.name, setting.value, setting.maximum); err != nil {
			return err
		}
	}
	return nil
}

func NewTap(config TapConfig) (*Tap, error) {
	if config.PLCURL == "" {
		config.PLCURL = identity.DefaultPLCURL
	}
	plcURL, err := canonicalCandidateOrigin(config.PLCURL)
	if err != nil {
		return nil, fmt.Errorf("invalid PLC URL: %w", err)
	}
	config.PLCURL = plcURL
	if config.IdentityMaxBytes == 0 {
		config.IdentityMaxBytes = defaultIdentityMaxBytes
	}
	if err := validateByteLimit("identity-max-bytes", config.IdentityMaxBytes, maxIdentityMaxBytes); err != nil {
		return nil, err
	}
	if config.RepoMaxBytes == 0 {
		config.RepoMaxBytes = defaultRepoMaxBytes
	}
	if err := validateByteLimit("repo-max-bytes", config.RepoMaxBytes, maxRepoMaxBytes); err != nil {
		return nil, err
	}
	if config.RepoMaxBlocks == 0 {
		config.RepoMaxBlocks = defaultRepoMaxBlocks
	}
	if err := validateCountLimit("repo-max-blocks", config.RepoMaxBlocks, maxRepoMaxBlocks); err != nil {
		return nil, err
	}
	if config.ResyncParallelism == 0 {
		config.ResyncParallelism = 1
	}
	if config.ResyncParallelism != 1 {
		return nil, fmt.Errorf("resync-parallelism must be 1")
	}
	if err := validateTapRuntimeSettings(config); err != nil {
		return nil, err
	}

	db, err := SetupDatabase(config.DatabaseURL, config.DBMaxConns)
	if err != nil {
		return nil, err
	}

	bdir := identity.BaseDirectory{
		PLCURL:                config.PLCURL,
		HTTPClient:            *newCandidateHTTPClient(identityHTTPTimeout, config.IdentityMaxBytes),
		TryAuthoritativeDNS:   false,
		SkipDNSDomainSuffixes: []string{".bsky.social"},
		UserAgent:             userAgent(),
	}
	cdir := identity.NewCacheDirectory(&bdir, config.IdentityCacheSize, time.Hour*24, time.Minute*2, time.Minute*5)

	logger := slog.Default().With("system", "tap")

	evtMngr := NewEventManager(logger, db, &config)

	repoMngr := NewRepoManager(logger, db, evtMngr, cdir)

	resyncer := NewResyncer(logger, db, repoMngr, evtMngr, &config)

	firehose := NewFirehoseProcessor(logger, db, evtMngr, repoMngr, &config)

	crawler := NewCrawler(logger, db, &config)

	outbox := NewOutbox(logger, evtMngr, &config)

	server := NewTapServer(logger, db, outbox, cdir, firehose, crawler, &config)

	t := &Tap{
		db:     db,
		logger: slog.Default().With("system", "tap"),

		firehose:   firehose,
		events:     evtMngr,
		repos:      repoMngr,
		resyncer:   resyncer,
		crawler:    crawler,
		server:     server,
		outbox:     outbox,
		outboxOnly: config.OutboxOnly,
	}

	if err := t.resyncer.resetPartiallyResynced(context.Background()); err != nil {
		return nil, err
	}

	return t, nil
}

// Run starts internal background workers for resync, cursor saving, and outbox delivery.
func (t *Tap) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		t.events.LoadEvents(ctx)
	}()

	if !t.outboxOnly {
		workers.Add(1)
		go func() {
			defer workers.Done()
			t.resyncer.run(ctx)
		}()
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		t.outbox.Run(ctx)
	}()
	workers.Wait()
}

func (t *Tap) CloseDb(ctx context.Context) error {
	t.logger.Info("shutting down tap")

	sqlDB, err := t.db.DB()
	if err != nil {
		t.logger.Error("error getting sql db", "error", err)
		return err
	}

	if err := sqlDB.Close(); err != nil {
		t.logger.Error("error closing sqlite db", "error", err)
		return err
	}

	return nil
}

func SetupDatabase(dbUrl string, maxConns int) (*gorm.DB, error) {
	// Setup database connection (supports both SQLite and Postgres)
	var dialector gorm.Dialector
	isSqlite := false

	if strings.HasPrefix(dbUrl, "sqlite://") {
		sqlitePath := dbUrl[len("sqlite://"):]
		dialector = sqlite.Open(sqlitePath)
		isSqlite = true
	} else if strings.HasPrefix(dbUrl, "postgresql://") || strings.HasPrefix(dbUrl, "postgres://") {
		dialector = postgres.Open(dbUrl)
	} else {
		return nil, fmt.Errorf("unsupported database URL scheme: must start with sqlite://, postgres://, or postgresql://")
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	if isSqlite {
		db.Exec("PRAGMA journal_mode=WAL;")
		db.Exec("PRAGMA synchronous=NORMAL;")
		db.Exec("PRAGMA busy_timeout=10000;")
		db.Exec("PRAGMA temp_store=MEMORY;")
		db.Exec("PRAGMA cache_size=8000;")
		db.Exec("PRAGMA wal_autocheckpoint=3000;")
	} else {
		// Configure connection pool
		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}
		sqlDB.SetMaxOpenConns(maxConns)
		sqlDB.SetMaxIdleConns(maxConns)
		sqlDB.SetConnMaxIdleTime(time.Hour)

	}

	if err := db.AutoMigrate(&models.Repo{}, &models.RepoRecord{}, &models.OutboxBuffer{}, &models.OutboxDeadLetter{}, &models.ResyncBuffer{}, &models.FirehoseCursor{}, &models.ListReposCursor{}, &models.CollectionCursor{}); err != nil {
		return nil, err
	}
	payloadLengthExpr := "OCTET_LENGTH(data)"
	if isSqlite {
		payloadLengthExpr = "LENGTH(CAST(data AS BLOB))"
	}
	if err := db.Exec("UPDATE outbox_dead_letters SET payload_bytes = " + payloadLengthExpr + " WHERE payload_bytes = 0 AND data <> ''").Error; err != nil {
		return nil, err
	}

	return db, nil
}
