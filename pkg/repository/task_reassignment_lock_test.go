//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestListTasksToReassignSkipsTaskLockedByFinalizer(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := t.Context()
	queries := sqlcv1.New()
	tenantID := uuid.New()
	_, err := queries.CreateTenant(ctx, pool, sqlcv1.CreateTenantParams{
		ID:      tenantID,
		Name:    "reassignment-lock-order",
		Slug:    "reassignment-lock-order-" + tenantID.String(),
		Version: sqlcv1.NullTenantMajorEngineVersion{TenantMajorEngineVersion: sqlcv1.TenantMajorEngineVersionV1, Valid: true},
	})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	dispatcherID := uuid.New()
	if _, err := queries.CreateDispatcher(ctx, pool, dispatcherID); err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}
	worker, err := queries.CreateWorker(ctx, pool, sqlcv1.CreateWorkerParams{
		Tenantid:     tenantID,
		Name:         "stale-worker",
		Dispatcherid: dispatcherID,
		Type: sqlcv1.NullWorkerType{
			WorkerType: sqlcv1.WorkerTypeSELFHOSTED,
			Valid:      true,
		},
		Language: sqlcv1.NullWorkerSDKS{
			WorkerSDKS: sqlcv1.WorkerSDKSGO,
			Valid:      true,
		},
	})
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE "Worker" SET "lastHeartbeatAt" = now() - interval '1 hour' WHERE id = $1`, worker.ID); err != nil {
		t.Fatalf("age worker heartbeat: %v", err)
	}

	type taskIdentity struct {
		id         int64
		insertedAt pgtype.Timestamptz
		retryCount int32
	}
	tasks := make([]taskIdentity, 3)
	for i := range tasks {
		err := pool.QueryRow(ctx, `
INSERT INTO v1_task (
    tenant_id, queue, action_id, step_id, step_readable_id, workflow_id,
    workflow_version_id, workflow_run_id, schedule_timeout, step_timeout,
    sticky, external_id, display_name, input, step_index, is_durable
) VALUES ($1, 'default', 'test:run', $2, 'step', $3, $4, $5, '5m', '5m',
          'NONE', $6, $7, '{}'::jsonb, 0, false)
RETURNING id, inserted_at, retry_count`,
			tenantID, uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), "task").Scan(
			&tasks[i].id,
			&tasks[i].insertedAt,
			&tasks[i].retryCount,
		)
		if err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO v1_task_runtime (
    task_id, task_inserted_at, retry_count, worker_id, tenant_id, timeout_at
) VALUES ($1, $2, $3, $4, $5, now() + interval '1 hour')`,
			tasks[i].id,
			tasks[i].insertedAt,
			tasks[i].retryCount,
			worker.ID,
			tenantID,
		); err != nil {
			t.Fatalf("create task runtime %d: %v", i, err)
		}
	}

	finalizedTaskIDs := []int64{tasks[0].id}
	finalizedInsertedAts := []pgtype.Timestamptz{tasks[0].insertedAt}
	finalizedRetryCounts := []int32{tasks[0].retryCount}

	finalizerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin finalizer transaction: %v", err)
	}
	defer finalizerTx.Rollback(context.Background()) //nolint:errcheck

	updated, err := queries.FailTaskInternalFailure(ctx, finalizerTx, sqlcv1.FailTaskInternalFailureParams{
		Maxinternalretries: 3,
		Taskids:            finalizedTaskIDs,
		Taskinsertedats:    finalizedInsertedAts,
		Taskretrycounts:    finalizedRetryCounts,
		Tenantid:           tenantID,
	})
	if err != nil {
		t.Fatalf("lock task rows through failure finalizer: %v", err)
	}
	if len(updated) != 1 {
		t.Fatalf("finalizer updated %d tasks, want 1", len(updated))
	}

	reassignTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reassignment transaction: %v", err)
	}
	defer reassignTx.Rollback(context.Background()) //nolint:errcheck

	started := time.Now()
	selected, err := queries.ListTasksToReassign(ctx, reassignTx, sqlcv1.ListTasksToReassignParams{
		Tenantid: tenantID,
		Limit:    pgtype.Int4{Int32: 2, Valid: true},
	})
	if err != nil {
		t.Fatalf("list tasks while finalizer holds task locks: %v", err)
	}
	if len(selected) != 2 || selected[0].ID != tasks[1].id || selected[1].ID != tasks[2].id {
		t.Fatalf("selected tasks = %v, want the two unlocked identities [%d %d]", selected, tasks[1].id, tasks[2].id)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("reassignment waited behind finalizer for %s instead of skipping", time.Since(started))
	}

	probeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin runtime lock probe: %v", err)
	}
	defer probeTx.Rollback(context.Background()) //nolint:errcheck
	if _, err := probeTx.Exec(ctx, `
SELECT task_id
FROM v1_task_runtime
WHERE task_id = ANY($1::bigint[])
ORDER BY task_id, task_inserted_at, retry_count
FOR UPDATE NOWAIT`, finalizedTaskIDs); err != nil {
		t.Fatalf("reassignment locked runtimes before skipping locked tasks: %v", err)
	}
}
