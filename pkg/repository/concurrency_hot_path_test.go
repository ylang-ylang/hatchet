//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/hatchet-dev/hatchet/pkg/repository/cache"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type countingDBTX struct {
	execCalls      int
	queryCalls     int
	queryRowCalls  int
	copyFromCalls  int
	sendBatchCalls int
}

func (d *countingDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	d.execCalls++
	return pgconn.CommandTag{}, errors.New("unexpected Exec")
}

func (d *countingDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	d.queryCalls++
	return nil, errors.New("unexpected Query")
}

func (d *countingDBTX) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	d.queryRowCalls++
	return nil
}

func (d *countingDBTX) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	d.copyFromCalls++
	return 0, errors.New("unexpected CopyFrom")
}

func (d *countingDBTX) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	d.sendBatchCalls++
	return nil
}

func (d *countingDBTX) totalCalls() int {
	return d.execCalls + d.queryCalls + d.queryRowCalls + d.copyFromCalls + d.sendBatchCalls
}

func TestConcurrencyEmptyWritesIssueNoSQL(t *testing.T) {
	db := &countingDBTX{}
	queueCache := cache.New(5 * time.Minute)
	defer queueCache.Stop()
	shared := &sharedRepository{queries: sqlcv1.New(), queueCache: queueCache}

	released, err := shared.releaseTasks(t.Context(), db, uuid.New(), nil)
	if err != nil {
		t.Fatalf("releaseTasks empty input: %v", err)
	}
	if len(released) != 0 {
		t.Fatalf("releaseTasks returned %d rows for empty input", len(released))
	}

	tenantID := uuid.New()
	queueCache.Set(getQueueCacheKey(tenantID, "already-known"), true)
	save, err := shared.upsertQueues(t.Context(), db, tenantID, []string{"already-known"})
	if err != nil {
		t.Fatalf("upsertQueues cached input: %v", err)
	}
	save()

	rows, err := sqlcv1.New().UpdateConcurrencySlotIsFilledBatch(t.Context(), db, nil)
	if err != nil {
		t.Fatalf("UpdateConcurrencySlotIsFilledBatch empty input: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("empty slot update returned %d rows", len(rows))
	}
	if db.totalCalls() != 0 {
		t.Fatalf("empty operations issued %d SQL calls", db.totalCalls())
	}
}

func TestUpdateConcurrencySlotIsFilledBatchUsesOneOrderedSetUpdate(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := t.Context()
	baseTaskID := time.Now().UnixNano()
	insertedAt := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := uuid.New()
	workflowID := uuid.New()
	workflowVersionID := uuid.New()
	workflowRunID := uuid.New()
	strategyID := int64(40001)

	_, err := pool.Exec(ctx, `
INSERT INTO v1_concurrency_slot (
    task_id, task_inserted_at, task_retry_count, external_id, tenant_id,
    workflow_id, workflow_version_id, workflow_run_id, strategy_id,
    priority, key, is_filled, queue_to_notify, schedule_timeout_at
) VALUES
	($1, $2, 0, $7,  $8, $9, $10, $11, $12, 1, 'same-task-old-retry', FALSE, 'queue-a', $13),
	($1, $2, 1, $14, $8, $9, $10, $11, $12, 1, 'same-task-current-retry', FALSE, 'queue-b', $13),
	($3, $4, 0, $15, $8, $9, $10, $11, $12, 1, 'second-task', FALSE, 'queue-c', $13),
	($5, $6, 0, $16, $8, $9, $10, $11, $12, 1, 'already-filled', TRUE, 'queue-d', $13)
`,
		baseTaskID, insertedAt,
		baseTaskID+1, insertedAt.Add(time.Microsecond),
		baseTaskID+2, insertedAt.Add(2*time.Microsecond),
		uuid.New(), tenantID, workflowID, workflowVersionID, workflowRunID, strategyID,
		insertedAt.Add(time.Hour),
		uuid.New(), uuid.New(), uuid.New(),
	)
	if err != nil {
		t.Fatalf("insert concurrency slots: %v", err)
	}
	defer pool.Exec(context.Background(), `DELETE FROM v1_concurrency_slot WHERE task_id BETWEEN $1 AND $2`, baseTaskID, baseTaskID+2) //nolint:errcheck

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin update transaction: %v", err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE slot_update_statement_count (n integer NOT NULL)`); err != nil {
		t.Fatalf("create statement counter: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO slot_update_statement_count VALUES (0)`); err != nil {
		t.Fatalf("seed statement counter: %v", err)
	}
	if _, err := tx.Exec(ctx, `
CREATE FUNCTION pg_temp.count_slot_update_statements() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE slot_update_statement_count SET n = n + 1;
    RETURN NULL;
END
$$`); err != nil {
		t.Fatalf("create statement trigger function: %v", err)
	}
	if _, err := tx.Exec(ctx, `
CREATE TRIGGER count_slot_update_statements
AFTER UPDATE ON v1_concurrency_slot
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.count_slot_update_statements()`); err != nil {
		t.Fatalf("create statement trigger: %v", err)
	}

	queries := sqlcv1.New()
	updated, err := queries.UpdateConcurrencySlotIsFilledBatch(ctx, tx, []sqlcv1.UpdateConcurrencySlotIsFilledParams{
		{
			IsFilled:       true,
			TaskID:         baseTaskID + 2,
			TaskInsertedAt: pgtype.Timestamptz{Time: insertedAt.Add(2 * time.Microsecond), Valid: true},
			TaskRetryCount: 0,
			StrategyID:     strategyID,
		},
		{
			IsFilled:       true,
			TaskID:         baseTaskID + 1,
			TaskInsertedAt: pgtype.Timestamptz{Time: insertedAt.Add(time.Microsecond), Valid: true},
			TaskRetryCount: 0,
			StrategyID:     strategyID,
		},
		{
			IsFilled:       true,
			TaskID:         baseTaskID,
			TaskInsertedAt: pgtype.Timestamptz{Time: insertedAt, Valid: true},
			TaskRetryCount: 1,
			StrategyID:     strategyID,
		},
	})
	if err != nil {
		t.Fatalf("set update: %v", err)
	}

	updatedRetries := make(map[[2]int64]bool, len(updated))
	for _, row := range updated {
		updatedRetries[[2]int64{row.TaskID, int64(row.TaskRetryCount)}] = true
	}
	if len(updated) != 2 || !updatedRetries[[2]int64{baseTaskID, 1}] || !updatedRetries[[2]int64{baseTaskID + 1, 0}] {
		t.Fatalf("updated identities = %v, want current retry and second task only", updatedRetries)
	}
	if updated[0].TaskID != baseTaskID+1 || updated[1].TaskID != baseTaskID {
		t.Fatalf("returned task order = [%d %d], want input order [%d %d]", updated[0].TaskID, updated[1].TaskID, baseTaskID+1, baseTaskID)
	}

	var statements int
	if err := tx.QueryRow(ctx, `SELECT n FROM slot_update_statement_count`).Scan(&statements); err != nil {
		t.Fatalf("read statement count: %v", err)
	}
	if statements != 1 {
		t.Fatalf("UPDATE statement count = %d, want 1", statements)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback set update: %v", err)
	}

	var filledCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM v1_concurrency_slot WHERE task_id BETWEEN $1 AND $2 AND is_filled`, baseTaskID, baseTaskID+2).Scan(&filledCount); err != nil {
		t.Fatalf("read rolled-back state: %v", err)
	}
	if filledCount != 1 {
		t.Fatalf("filled rows after rollback = %d, want only the originally-filled row", filledCount)
	}
}
