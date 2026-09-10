package concurrency

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/pgoutbox"
	outboxsqlc "github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/pkg/repository"
)

type scriptedOutboxStep struct {
	messages       []*outboxsqlc.Message
	failAfterFlush error
}

type scriptedOutbox struct {
	flusher pgoutbox.Flusher
	steps   []scriptedOutboxStep
	calls   int
}

func (o *scriptedOutbox) AddFlusher(_ string, flusher pgoutbox.Flusher) {
	o.flusher = flusher
}

func (o *scriptedOutbox) AddMessages(context.Context, pgx.Tx, string, []pgoutbox.MessageOpts, ...pgoutbox.AddOpt) error {
	return nil
}

func (o *scriptedOutbox) ProcessMessages(ctx context.Context, _ string, _ ...pgoutbox.ProcessOpt) ([]*outboxsqlc.Message, error) {
	if o.calls >= len(o.steps) {
		o.calls++
		return nil, nil
	}

	step := o.steps[o.calls]
	o.calls++
	if len(step.messages) > 0 {
		if err := o.flusher.Flush(testFlushContext{Context: ctx}, step.messages); err != nil {
			return nil, err
		}
	}
	if step.failAfterFlush != nil {
		return nil, step.failAfterFlush
	}

	return step.messages, nil
}

func (o *scriptedOutbox) Subscribe(context.Context, string, ...pgoutbox.SubscribeOpt) error {
	return nil
}

func (o *scriptedOutbox) AcquireTopic(context.Context, string) error {
	return nil
}

func (o *scriptedOutbox) ReleaseTopic(context.Context, string) error {
	return nil
}

type testFlushContext struct {
	context.Context
}

func (testFlushContext) Tx() pgx.Tx {
	return nil
}

type sequencedConcurrencyRepo struct {
	mockConcurrencyRepo
	results []*repository.RunConcurrencyResult
}

func (r *sequencedConcurrencyRepo) UpdateConcurrencySlotsTx(
	_ context.Context,
	_ pgx.Tx,
	_ uuid.UUID,
	_ int64,
	filled []repository.TaskIdInsertedAtRetryCount,
	cancelled []repository.CancelledSlotInput,
) (*repository.RunConcurrencyResult, error) {
	r.lastFilled = filled
	r.lastCancelled = cancelled
	call := r.updateCalls
	r.updateCalls++
	if call < len(r.results) {
		return r.results[call], nil
	}
	return &repository.RunConcurrencyResult{}, nil
}

func (r *sequencedConcurrencyRepo) UpdateConcurrencySlots(
	ctx context.Context,
	tenantID uuid.UUID,
	strategyID int64,
	filled []repository.TaskIdInsertedAtRetryCount,
	cancelled []repository.CancelledSlotInput,
) (*repository.RunConcurrencyResult, error) {
	return r.UpdateConcurrencySlotsTx(ctx, nil, tenantID, strategyID, filled, cancelled)
}

func resultWithNextStrategy(id int64) *repository.RunConcurrencyResult {
	return &repository.RunConcurrencyResult{NextConcurrencyStrategies: []int64{id}}
}

func messageBatch(t *testing.T, firstID int64, count int) []*outboxsqlc.Message {
	t.Helper()

	now := time.Now().UTC()
	messages := make([]*outboxsqlc.Message, count)
	for i := range count {
		payload, err := json.Marshal(walInsert(
			"key-"+uuid.NewString(),
			firstID+int64(i),
			1,
			now,
			now.Add(time.Hour),
		))
		if err != nil {
			t.Fatalf("marshal WAL: %v", err)
		}
		messages[i] = &outboxsqlc.Message{ID: firstID + int64(i), Payload: payload}
	}

	return messages
}

func runnableStrategy(repo repository.ConcurrencyRepository, outbox pgoutbox.Outbox) *ConcurrencyStrategy {
	strategy := newTestStrategy(repo, maxOutboxMessagesPerRun+1)
	strategy.outbox = outbox
	strategy.topic = "bounded-run-test"
	strategy.initialQueued = true
	close(strategy.built)
	return strategy
}

func TestRunStopsAtDeterministicLimitAndRequestsContinuation(t *testing.T) {
	results := make([]*repository.RunConcurrencyResult, maxOutboxBatchesPerRun+1)
	steps := make([]scriptedOutboxStep, maxOutboxBatchesPerRun+2)
	for i := 0; i < maxOutboxBatchesPerRun+1; i++ {
		results[i] = resultWithNextStrategy(int64(i + 1))
		steps[i] = scriptedOutboxStep{messages: messageBatch(t, int64(i*outboxMessagesPerBatch+1), outboxMessagesPerBatch)}
	}
	// The second Run observes the empty topic after committing the remaining batch.
	steps[len(steps)-1] = scriptedOutboxStep{}

	repo := &sequencedConcurrencyRepo{results: results}
	outbox := &scriptedOutbox{steps: steps}
	strategy := runnableStrategy(repo, outbox)
	outbox.flusher = strategy

	first, shouldContinue, err := strategy.Run(t.Context())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if !shouldContinue {
		t.Fatalf("Run did not request continuation after reaching its slice limit")
	}
	if outbox.calls != maxOutboxBatchesPerRun {
		t.Fatalf("ProcessMessages calls = %d, want %d", outbox.calls, maxOutboxBatchesPerRun)
	}
	if len(first.NextConcurrencyStrategies) != maxOutboxBatchesPerRun {
		t.Fatalf("first delivered results = %d, want %d", len(first.NextConcurrencyStrategies), maxOutboxBatchesPerRun)
	}

	second, shouldContinue, err := strategy.Run(t.Context())
	if err != nil {
		t.Fatalf("continuation Run: %v", err)
	}
	if shouldContinue {
		t.Fatalf("continuation requested after observing an empty topic")
	}
	if len(second.NextConcurrencyStrategies) != 1 || second.NextConcurrencyStrategies[0] != int64(maxOutboxBatchesPerRun+1) {
		t.Fatalf("continuation delivered results = %v", second.NextConcurrencyStrategies)
	}
}

func TestRunDeliversCommittedBatchesWhenLaterBatchFails(t *testing.T) {
	repo := &sequencedConcurrencyRepo{results: []*repository.RunConcurrencyResult{
		resultWithNextStrategy(11),
		resultWithNextStrategy(22),
	}}
	outbox := &scriptedOutbox{steps: []scriptedOutboxStep{
		{messages: messageBatch(t, 1, 1)},
		{messages: messageBatch(t, 2, 1), failAfterFlush: errors.New("commit failed")},
	}}
	strategy := runnableStrategy(repo, outbox)
	outbox.flusher = strategy

	res, shouldContinue, err := strategy.Run(t.Context())
	if err == nil {
		t.Fatalf("expected later batch failure")
	}
	if shouldContinue {
		t.Fatalf("failed Run must not busy-loop via continuation")
	}
	if res == nil {
		t.Fatalf("committed first-batch result was discarded")
	}
	if len(res.NextConcurrencyStrategies) != 1 || res.NextConcurrencyStrategies[0] != 11 {
		t.Fatalf("delivered results = %v, want only committed batch [11]", res.NextConcurrencyStrategies)
	}
	if len(strategy.pending) != 0 {
		t.Fatalf("failed batch left %d pending results", len(strategy.pending))
	}
	if len(strategy.openScopes) != 0 {
		t.Fatalf("failed batch left %d open undo scopes", len(strategy.openScopes))
	}
}
