package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bluesky-social/indigo/cmd/tap/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errDeadLetterNotFound  = errors.New("outbox dead letter not found")
	errDeadLetterConflict  = errors.New("outbox dead-letter transition conflict")
	errActiveEventConflict = errors.New("active outbox event ID already exists")
	errDeadLetterTooLarge  = errors.New("dead-letter payload exceeds the 3 MiB delivery limit")
)

type deadLetterResult struct {
	deadLetter *models.OutboxDeadLetter
	created    bool
}

type requeueResult struct {
	event          *OutboxEvt
	shouldSend     bool
	compactedBytes int64
}

func eventSHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func outboxEventMatchesBuffer(evt *OutboxEvt, active *models.OutboxBuffer) bool {
	return evt.ID == active.ID &&
		evt.Did == active.Did &&
		evt.Live == active.Live &&
		string(evt.Event) == active.Data &&
		evt.Generation == active.Generation
}

func outboxEventsMatch(a, b *OutboxEvt) bool {
	return a.ID == b.ID &&
		a.Did == b.Did &&
		a.Live == b.Live &&
		string(a.Event) == string(b.Event) &&
		a.Generation == b.Generation
}

func deadLetterMatches(deadLetter *models.OutboxDeadLetter, evt *OutboxEvt, reason string, httpStatus int) bool {
	bodyMatches := deadLetter.Data == string(evt.Event)
	if deadLetter.RequeuedAt != nil && deadLetter.Data == "" {
		bodyMatches = deadLetter.PayloadBytes == int64(len(evt.Event))
	}
	return deadLetter.OriginalEventID == evt.ID &&
		deadLetter.Generation == evt.Generation &&
		deadLetter.Did == evt.Did &&
		deadLetter.Live == evt.Live &&
		bodyMatches &&
		deadLetter.SHA256 == eventSHA256(evt.Event) &&
		deadLetter.Reason == reason &&
		deadLetter.HTTPStatus == httpStatus
}

func (em *EventManager) DeadLetterEvent(ctx context.Context, evt *OutboxEvt, reason string, httpStatus, attempts int) (*deadLetterResult, error) {
	if cached, ok := em.GetEvent(evt.ID); ok && !outboxEventsMatch(cached, evt) {
		return nil, errDeadLetterConflict
	}

	generation := evt.Generation
	var deadLetter models.OutboxDeadLetter
	created := false
	err := em.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var active models.OutboxBuffer
		err := tx.First(&active, "id = ?", evt.ID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if err := tx.Where("original_event_id = ? AND generation = ?", evt.ID, generation).
				Order("id DESC").First(&deadLetter).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errDeadLetterNotFound
				}
				return err
			}
			if !deadLetterMatches(&deadLetter, evt, reason, httpStatus) {
				return errDeadLetterConflict
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !outboxEventMatchesBuffer(evt, &active) {
			return errDeadLetterConflict
		}

		deadLetter = models.OutboxDeadLetter{
			OriginalEventID: active.ID,
			Generation:      active.Generation,
			Did:             active.Did,
			Live:            active.Live,
			Data:            active.Data,
			SHA256:          eventSHA256([]byte(active.Data)),
			PayloadBytes:    int64(len(active.Data)),
			Reason:          reason,
			HTTPStatus:      httpStatus,
			DeadLetteredAt:  time.Now().UTC(),
			Attempts:        attempts,
		}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&deadLetter)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			if err := tx.Where("original_event_id = ? AND generation = ?", evt.ID, generation).
				First(&deadLetter).Error; err != nil {
				return err
			}
			if !deadLetterMatches(&deadLetter, evt, reason, httpStatus) {
				return errDeadLetterConflict
			}
		} else {
			created = true
		}

		result = tx.Where("id = ? AND generation = ?", evt.ID, generation).Delete(&models.OutboxBuffer{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errDeadLetterConflict
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	em.cacheLk.Lock()
	if cached, ok := em.cache[evt.ID]; ok && outboxEventsMatch(cached, evt) {
		delete(em.cache, evt.ID)
	}
	eventCacheSize.Set(float64(len(em.cache)))
	em.cacheLk.Unlock()

	return &deadLetterResult{deadLetter: &deadLetter, created: created}, nil
}

func (em *EventManager) reconcileRequeue(ctx context.Context, deadLetterID uint) (*models.OutboxDeadLetter, *models.OutboxBuffer, bool, error) {
	var deadLetter models.OutboxDeadLetter
	if err := em.db.WithContext(ctx).First(&deadLetter, "id = ?", deadLetterID).Error; err != nil {
		return nil, nil, false, err
	}
	if deadLetter.RequeuedAt == nil || deadLetter.RequeueGeneration == 0 {
		return nil, nil, false, errActiveEventConflict
	}
	var active models.OutboxBuffer
	err := em.db.WithContext(ctx).First(&active, "id = ?", deadLetter.OriginalEventID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &deadLetter, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	if active.Did != deadLetter.Did || active.Live != deadLetter.Live || active.Generation != deadLetter.RequeueGeneration ||
		eventSHA256([]byte(active.Data)) != deadLetter.SHA256 || int64(len(active.Data)) != deadLetter.PayloadBytes {
		return nil, nil, false, errActiveEventConflict
	}
	return &deadLetter, &active, true, nil
}

func (em *EventManager) RequeueDeadLetter(ctx context.Context, deadLetterID uint) (*requeueResult, error) {
	var deadLetter models.OutboxDeadLetter
	var active models.OutboxBuffer
	activeFound := false
	var compactedBytes int64
	err := em.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&deadLetter, "id = ?", deadLetterID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errDeadLetterNotFound
			}
			return err
		}
		if deadLetter.Generation == math.MaxUint64 {
			return errActiveEventConflict
		}

		nextGeneration := deadLetter.Generation + 1
		payloadBytes := deadLetter.PayloadBytes
		if payloadBytes == 0 && deadLetter.Data != "" {
			payloadBytes = int64(len(deadLetter.Data))
		}
		if deadLetter.RequeuedAt == nil {
			if payloadBytes != int64(len(deadLetter.Data)) || deadLetter.SHA256 != eventSHA256([]byte(deadLetter.Data)) {
				return errActiveEventConflict
			}
			if payloadBytes > maxWebhookEventBytes {
				return errDeadLetterTooLarge
			}
		} else if deadLetter.RequeueGeneration != nextGeneration {
			return errActiveEventConflict
		}

		active = models.OutboxBuffer{
			ID:         deadLetter.OriginalEventID,
			Did:        deadLetter.Did,
			Live:       deadLetter.Live,
			Generation: nextGeneration,
		}
		if deadLetter.RequeuedAt == nil {
			active.Data = deadLetter.Data
		}

		var existing models.OutboxBuffer
		err := tx.First(&existing, "id = ?", active.ID).Error
		if err == nil {
			activeFound = true
			if existing.Did != active.Did || existing.Live != active.Live || existing.Generation != active.Generation ||
				eventSHA256([]byte(existing.Data)) != deadLetter.SHA256 || int64(len(existing.Data)) != payloadBytes {
				return errActiveEventConflict
			}
			active = existing
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if deadLetter.RequeuedAt != nil {
			return nil
		}

		if !activeFound {
			if err := tx.Create(&active).Error; err != nil {
				return err
			}
			activeFound = true
		}
		now := time.Now().UTC()
		result := tx.Model(&models.OutboxDeadLetter{}).
			Where("id = ? AND requeued_at IS NULL", deadLetter.ID).
			Updates(map[string]interface{}{
				"requeued_at":        now,
				"requeue_generation": nextGeneration,
				"payload_bytes":      payloadBytes,
				"data":               "",
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errActiveEventConflict
		}
		deadLetter.RequeuedAt = &now
		deadLetter.RequeueGeneration = nextGeneration
		compactedBytes = payloadBytes
		return nil
	})
	if err == nil && em.afterRequeueCommitHook != nil {
		err = em.afterRequeueCommitHook()
	}
	if err != nil {
		reconcileCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reconciledDeadLetter, reconciledActive, reconciledActiveFound, reconcileErr := em.reconcileRequeue(reconcileCtx, deadLetterID)
		cancel()
		if reconcileErr != nil {
			return nil, err
		}
		deadLetter = *reconciledDeadLetter
		if reconciledActiveFound {
			active = *reconciledActive
		}
		activeFound = reconciledActiveFound
	}

	evt := &OutboxEvt{
		ID:         active.ID,
		Did:        active.Did,
		Live:       active.Live,
		Event:      []byte(active.Data),
		Generation: active.Generation,
	}
	shouldSend := false
	if activeFound {
		em.cacheLk.Lock()
		cached, exists := em.cache[evt.ID]
		if !exists || !outboxEventsMatch(cached, evt) {
			em.cache[evt.ID] = evt
			shouldSend = true
		}
		eventCacheSize.Set(float64(len(em.cache)))
		em.cacheLk.Unlock()
		em.advanceNextID(evt.ID)
	}

	return &requeueResult{
		event:          evt,
		shouldSend:     shouldSend,
		compactedBytes: compactedBytes,
	}, nil
}
