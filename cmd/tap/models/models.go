package models

import "time"

type RepoState string

const (
	RepoStatePending        RepoState = "pending"
	RepoStateDesynchronized RepoState = "desynchronized"
	RepoStateResyncing      RepoState = "resyncing"
	RepoStateActive         RepoState = "active"
	RepoStateTakendown      RepoState = "takendown"
	RepoStateSuspended      RepoState = "suspended"
	RepoStateDeactivated    RepoState = "deactivated"
	RepoStateError          RepoState = "error"
	RepoStateTerminal       RepoState = "terminal"
)

type AccountStatus string

const (
	AccountStatusActive      AccountStatus = "active"
	AccountStatusTakendown   AccountStatus = "takendown"
	AccountStatusSuspended   AccountStatus = "suspended"
	AccountStatusDeactivated AccountStatus = "deactivated"
	AccountStatusDeleted     AccountStatus = "deleted"
)

type Repo struct {
	Did        string        `gorm:"primaryKey"`
	State      RepoState     `gorm:"not null;default:'pending';index:idx_repos_state_retry"`
	Status     AccountStatus `gorm:"not null;default:'active'"`
	Handle     string        `gorm:"type:text"`
	Rev        string        `gorm:"type:text"`
	PrevData   string        `gorm:"type:text"`
	ErrorMsg   string        `gorm:"type:text"`
	RetryCount int           `gorm:"not null;default:0"`
	RetryAfter int64         `gorm:"not null;default:0;index:idx_repos_state_retry"` // Unix timestamp (seconds)
}

type OutboxBuffer struct {
	ID         uint   `gorm:"primaryKey"`
	Did        string `gorm:"not null"`
	Live       bool   `gorm:"not null"`
	Data       string `gorm:"type:text;not null"` // JSON-encoded operations
	Generation uint64 `gorm:"not null;default:1"`
}

type OutboxDeadLetter struct {
	ID                uint       `gorm:"primaryKey"`
	OriginalEventID   uint       `gorm:"not null;uniqueIndex:idx_dead_letter_event_generation"`
	Generation        uint64     `gorm:"not null;uniqueIndex:idx_dead_letter_event_generation"`
	Did               string     `gorm:"not null;index"`
	Live              bool       `gorm:"not null"`
	Data              string     `gorm:"type:text;not null"`
	SHA256            string     `gorm:"type:char(64);not null"`
	PayloadBytes      int64      `gorm:"not null;default:0"`
	Reason            string     `gorm:"type:text;not null"`
	HTTPStatus        int        `gorm:"not null"`
	DeadLetteredAt    time.Time  `gorm:"not null;index"`
	Attempts          int        `gorm:"not null"`
	RequeuedAt        *time.Time `gorm:"index"`
	RequeueGeneration uint64     `gorm:"not null;default:0"`
}

type ResyncBuffer struct {
	ID   uint   `gorm:"primaryKey"`
	Did  string `gorm:"not null;index"`
	Data string `gorm:"type:text;not null"` // JSON-encoded Commit
}

type RepoRecord struct {
	Did        string `gorm:"primaryKey"`
	Collection string `gorm:"primaryKey"`
	Rkey       string `gorm:"primaryKey"`
	Cid        string `gorm:"not null"`
}

type FirehoseCursor struct {
	Url    string `gorm:"primaryKey"`
	Cursor int64  `gorm:"not null"`
}

type ListReposCursor struct {
	Url    string `gorm:"primaryKey"`
	Cursor string `gorm:"not null"`
}

type CollectionCursor struct {
	Url        string `gorm:"primaryKey"`
	Collection string `gorm:"primaryKey"`
	Cursor     string `gorm:"not null"`
}
