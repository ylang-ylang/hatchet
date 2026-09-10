package v1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/pgoutbox"
	outboxsqlc "github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/internal/services/shared/timeout_lock"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	concurrencystrategy "github.com/hatchet-dev/hatchet/pkg/scheduling/v1/concurrency"
)

type continuationTestRepo struct {
	updateCalls int
}

func (r *continuationTestRepo) ReadConcurrencySlotsForIndexing(context.Context, uuid.UUID, int64, chan<- *sqlcv1.ListConcurrencySlotsForIndexingRow) error {
	return nil
}

func (r *continuationTestRepo) UpdateConcurrencySlotsTx(context.Context, pgx.Tx, uuid.UUID, int64, []repository.TaskIdInsertedAtRetryCount, []repository.CancelledSlotInput) (*repository.RunConcurrencyResult, error) {
	r.updateCalls++
	return &repository.RunConcurrencyResult{NextConcurrencyStrategies: []int64{int64(r.updateCalls)}}, nil
}

func (r *continuationTestRepo) UpdateConcurrencySlots(ctx context.Context, tenantID uuid.UUID, strategyID int64, filled []repository.TaskIdInsertedAtRetryCount, cancelled []repository.CancelledSlotInput) (*repository.RunConcurrencyResult, error) {
	return r.UpdateConcurrencySlotsTx(ctx, nil, tenantID, strategyID, filled, cancelled)
}

func (*continuationTestRepo) UpdateConcurrencyStrategyIsActive(context.Context, uuid.UUID, *sqlcv1.V1StepConcurrency) error {
	return nil
}

func (*continuationTestRepo) CheckAndDeactivateTenantConcurrency(context.Context, uuid.UUID, int64) error {
	return nil
}

func (*continuationTestRepo) RunConcurrencyStrategy(context.Context, uuid.UUID, *sqlcv1.V1StepConcurrency) (*repository.RunConcurrencyResult, error) {
	return &repository.RunConcurrencyResult{}, nil
}

func (*continuationTestRepo) DeactivateStaleStepConcurrency(context.Context, uuid.UUID) error {
	return nil
}

func (*continuationTestRepo) ListTenantsWithManyStepConcurrencies(context.Context, int64) ([]*sqlcv1.ListTenantsWithManyStepConcurrenciesRow, error) {
	return nil, nil
}

type continuationTestOutbox struct {
	flusher pgoutbox.Flusher
	batches [][]*outboxsqlc.Message
	next    int
}

func (o *continuationTestOutbox) AddFlusher(_ string, flusher pgoutbox.Flusher) {
	o.flusher = flusher
}

func (*continuationTestOutbox) AddMessages(context.Context, pgx.Tx, string, []pgoutbox.MessageOpts, ...pgoutbox.AddOpt) error {
	return nil
}

func (o *continuationTestOutbox) ProcessMessages(ctx context.Context, _ string, _ ...pgoutbox.ProcessOpt) ([]*outboxsqlc.Message, error) {
	if o.next >= len(o.batches) {
		return nil, nil
	}
	batch := o.batches[o.next]
	o.next++
	if len(batch) > 0 {
		if err := o.flusher.Flush(continuationFlushContext{Context: ctx}, batch); err != nil {
			return nil, err
		}
	}
	return batch, nil
}

func (*continuationTestOutbox) Subscribe(context.Context, string, ...pgoutbox.SubscribeOpt) error {
	return nil
}

func (*continuationTestOutbox) AcquireTopic(context.Context, string) error {
	return nil
}

func (*continuationTestOutbox) ReleaseTopic(context.Context, string) error {
	return nil
}

type continuationFlushContext struct {
	context.Context
}

func (continuationFlushContext) Tx() pgx.Tx {
	return nil
}

func continuationMessage(t *testing.T, id int64) *outboxsqlc.Message {
	t.Helper()
	now := time.Now().UTC()
	payload, err := json.Marshal(map[string]interface{}{
		"operation":           "INSERT",
		"key":                 uuid.NewString(),
		"taskId":              id,
		"taskInsertedAt":      now,
		"scheduleTimeoutAtMs": now.Add(time.Hour).UnixMilli(),
		"priority":            1,
		"taskRetryCount":      0,
	})
	if err != nil {
		t.Fatalf("marshal WAL: %v", err)
	}
	return &outboxsqlc.Message{ID: id, Payload: payload}
}

func TestConcurrencyManagerSelfWakesAfterBoundedRun(t *testing.T) {
	tenantID := uuid.New()
	descriptor := &sqlcv1.V1StepConcurrency{
		ID:             1,
		TenantID:       tenantID,
		MaxConcurrency: 100,
		Strategy:       sqlcv1.V1ConcurrencyStrategyGROUPROUNDROBIN,
	}
	repo := &continuationTestRepo{}
	outbox := &continuationTestOutbox{batches: [][]*outboxsqlc.Message{
		{continuationMessage(t, 1)},
		{continuationMessage(t, 2)},
		{continuationMessage(t, 3)},
		{continuationMessage(t, 4)},
		{continuationMessage(t, 5)},
		{},
	}}
	logger := zerolog.Nop()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	strategy := concurrencystrategy.NewConcurrencyStrategy(ctx, repo, descriptor, outbox, &logger)
	results := make(chan *ConcurrencyResults, 2)
	manager := &ConcurrencyManager{
		l:                    &logger,
		strategy:             descriptor,
		concurrencyStrategy:  strategy,
		tenantId:             tenantID,
		notifyConcurrencyCh:  make(chan map[string]string, 2),
		resultsCh:            results,
		minPollingInterval:   time.Hour,
		maxPollingInterval:   time.Hour + time.Second,
		advisoryLock:         timeout_lock.NewKeyedTimeoutLock[int64](time.Second),
		advisoryParentLock:   timeout_lock.NewKeyedTimeoutLock[int64](time.Second),
	}

	go manager.loopConcurrency(ctx)
	manager.notify(ctx)

	for wantCount := 4; wantCount >= 1; wantCount -= 3 {
		select {
		case result := <-results:
			if len(result.NextConcurrencyStrategies) != wantCount {
				t.Fatalf("delivered strategies = %v, want count %d", result.NextConcurrencyStrategies, wantCount)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for result slice with %d notifications", wantCount)
		}
	}
}
