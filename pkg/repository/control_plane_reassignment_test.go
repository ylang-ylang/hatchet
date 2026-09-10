//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestControlPlaneDegradationSuppressesFalseReassignmentThenRecovers(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := t.Context()
	queries := sqlcv1.New()
	tasks := newBatchTestRepository(pool)
	tasks.reassignLimit = 1000
	workers := newWorkerRepository(tasks.sharedRepository)

	clock := time.Now().UTC()
	tasks.controlPlaneHealth.now = func() time.Time { return clock }

	tenantID := uuid.New()
	_, err := queries.CreateTenant(ctx, pool, sqlcv1.CreateTenantParams{
		ID:      tenantID,
		Name:    "control-plane-degradation",
		Slug:    "control-plane-degradation-" + tenantID.String(),
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
		Name:         "live-worker-with-blocked-heartbeat",
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
	if _, err := pool.Exec(ctx, `UPDATE "Worker" SET "lastHeartbeatAt" = now() - interval '1 minute' WHERE id = $1`, worker.ID); err != nil {
		t.Fatalf("set stale stored heartbeat: %v", err)
	}

	var taskID int64
	var insertedAt pgtype.Timestamptz
	var retryCount int32
	if err := pool.QueryRow(ctx, `
INSERT INTO v1_task (
    tenant_id, queue, action_id, step_id, step_readable_id, workflow_id,
    workflow_version_id, workflow_run_id, schedule_timeout, step_timeout,
    sticky, external_id, display_name, input, step_index, is_durable
) VALUES ($1, 'default', 'test:run', $2, 'step', $3, $4, $5, '5m', '5m',
          'NONE', $6, 'task', '{}'::jsonb, 0, false)
RETURNING id, inserted_at, retry_count`,
		tenantID, uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
	).Scan(&taskID, &insertedAt, &retryCount); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO v1_task_runtime (
    task_id, task_inserted_at, retry_count, worker_id, tenant_id, timeout_at
) VALUES ($1, $2, $3, $4, $5, now() + interval '1 hour')`,
		taskID, insertedAt, retryCount, worker.ID, tenantID,
	); err != nil {
		t.Fatalf("create task runtime: %v", err)
	}

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin heartbeat blocker: %v", err)
	}
	defer blocker.Rollback(context.Background()) //nolint:errcheck
	if _, err := blocker.Exec(ctx, `SELECT id FROM "Worker" WHERE id = $1 FOR UPDATE`, worker.ID); err != nil {
		t.Fatalf("lock worker heartbeat row: %v", err)
	}

	for attempt := range controlPlaneFailureThreshold {
		heartbeatCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		err := workers.UpdateWorkerHeartbeat(heartbeatCtx, tenantID, worker.ID, time.Now().UTC())
		cancel()
		if err == nil {
			t.Fatalf("heartbeat attempt %d unexpectedly succeeded through row lock", attempt)
		}
	}

	// This is the pre-fix behavior: the stale database timestamp alone still selects the task,
	// even though the worker is alive and its fresh heartbeat writes just failed.
	rawTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin raw reassignment query: %v", err)
	}
	raw, err := queries.ListTasksToReassign(ctx, rawTx, sqlcv1.ListTasksToReassignParams{
		Tenantid: tenantID,
		Limit:    pgtype.Int4{Int32: 1000, Valid: true},
	})
	if err != nil {
		t.Fatalf("raw reassignment query: %v", err)
	}
	if len(raw) != 1 || raw[0].ID != taskID {
		t.Fatalf("raw heartbeat-only selection = %v, want live task %d", raw, taskID)
	}
	if err := rawTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback raw reassignment query: %v", err)
	}

	suppressed, more, err := tasks.ProcessTaskReassignments(ctx, tenantID)
	if err != nil {
		t.Fatalf("suppressed reassignment: %v", err)
	}
	if more || len(suppressed.ReleasedTasks) != 0 || len(suppressed.RetriedTasks) != 0 {
		t.Fatalf("degraded control plane reassigned task: %+v", suppressed)
	}
	assertTaskAttemptState(t, pool, taskID, insertedAt, 0, 1)

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release heartbeat blocker: %v", err)
	}
	for range controlPlaneRecoverySuccessThreshold {
		if err := workers.UpdateWorkerHeartbeat(ctx, tenantID, worker.ID, time.Now().UTC()); err != nil {
			t.Fatalf("recovery heartbeat: %v", err)
		}
	}

	// Successful writes prove database recovery, but the grace period must still allow every live
	// worker to refresh its formerly-untrustworthy stored timestamp.
	stillSuppressed, _, err := tasks.ProcessTaskReassignments(ctx, tenantID)
	if err != nil {
		t.Fatalf("recovery-grace reassignment: %v", err)
	}
	if len(stillSuppressed.ReleasedTasks) != 0 {
		t.Fatalf("task reassigned during heartbeat recovery grace")
	}

	clock = clock.Add(controlPlaneRecoveryGrace + time.Second)
	if _, err := pool.Exec(ctx, `UPDATE "Worker" SET "lastHeartbeatAt" = now() - interval '1 minute' WHERE id = $1`, worker.ID); err != nil {
		t.Fatalf("simulate worker death after database recovery: %v", err)
	}

	reassigned, more, err := tasks.ProcessTaskReassignments(ctx, tenantID)
	if err != nil {
		t.Fatalf("healthy-database reassignment: %v", err)
	}
	if more || len(reassigned.ReleasedTasks) != 1 || len(reassigned.RetriedTasks) != 1 {
		t.Fatalf("dead worker was not reassigned after recovery grace: %+v", reassigned)
	}
	assertTaskAttemptState(t, pool, taskID, insertedAt, 1, 0)
}

func assertTaskAttemptState(t *testing.T, db sqlcv1.DBTX, taskID int64, insertedAt pgtype.Timestamptz, wantRetry int32, wantRuntimeCount int) {
	t.Helper()
	var retry int32
	var runtimes int
	if err := db.QueryRow(t.Context(), `SELECT retry_count FROM v1_task WHERE id = $1 AND inserted_at = $2`, taskID, insertedAt).Scan(&retry); err != nil {
		t.Fatalf("read task retry: %v", err)
	}
	if err := db.QueryRow(t.Context(), `SELECT count(*) FROM v1_task_runtime WHERE task_id = $1 AND task_inserted_at = $2`, taskID, insertedAt).Scan(&runtimes); err != nil {
		t.Fatalf("read task runtimes: %v", err)
	}
	if retry != wantRetry || runtimes != wantRuntimeCount {
		t.Fatalf("task state = retry %d, runtimes %d; want retry %d, runtimes %d", retry, runtimes, wantRetry, wantRuntimeCount)
	}
}

func TestControlPlaneHealthRequiresFailureBurst(t *testing.T) {
	clock := time.Now().UTC()
	health := controlPlaneHealth{now: func() time.Time { return clock }}
	failure := errors.New("statement timeout")

	health.observe("heartbeat", failure)
	if suppress, _ := health.suppression(); suppress {
		t.Fatalf("one isolated failure degraded the control plane")
	}

	clock = clock.Add(controlPlaneFailureWindow + time.Second)
	health.observe("heartbeat", failure)
	if suppress, _ := health.suppression(); suppress {
		t.Fatalf("failures outside the burst window degraded the control plane")
	}

	health.observe("lease", failure)
	suppress, snapshot := health.suppression()
	if !suppress || snapshot.causeID == uuid.Nil || snapshot.state != controlPlaneDegraded {
		t.Fatalf("failure burst did not enter causal degraded state: %+v", snapshot)
	}
}
