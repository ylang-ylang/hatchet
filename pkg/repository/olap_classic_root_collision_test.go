//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// classicCollisionRun models a classic engine run whose DAG row and root task row share the same
// numeric id and inserted_at: the identity sequences for v1_dag and v1_task both start at 1, so the
// very first classic runs create their root task in the same transaction as the DAG, with
// (id, inserted_at) identical to the DAG's. The AFTER INSERT trigger on v1_tasks_olap then writes
// a v1_dag_to_task_olap junction row that is byte-identical to the self-mapping row the engine
// writes for operator-orchestrated DAGs (dag-as-orchestrator-task). The run is otherwise fully
// classic: every task — root included — is a real v1_tasks_olap row whose dag_id/dag_inserted_at
// point back at the DAG, and no core v1_dag row is seeded (none of the queries under test read
// it).
type classicCollisionRun struct {
	tenantId      uuid.UUID
	dagId         int64
	dagInsertedAt pgtype.Timestamptz
	dagExternalId uuid.UUID // workflow-run external id; keys the kind='DAG' run and its lookup row
	workflowId    uuid.UUID

	root     replayStatusFixture // id == dagId, inserted_at == dagInsertedAt, dag_id == dagId
	children []replayStatusFixture
}

// seedClassicCollisionRun writes the DAG row first (the engine's created-dag message), then the
// root task — with the DAG's own (id, inserted_at) and dag columns pointing at the DAG — and two
// ordinary children, all through the same OLAP write paths the engine uses.
func seedClassicCollisionRun(t *testing.T, ctx context.Context, repo *OLAPRepositoryImpl, dagId int64) classicCollisionRun {
	t.Helper()

	tenantId := uuid.New()
	dagInsertedAt := pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}
	dagExternalId := uuid.New()
	workflowId := uuid.New()

	// No tasks exist yet, so the insert-time status computation lands on QUEUED. total_tasks
	// covers the root task plus the two children.
	dag := &DAGWithData{
		V1Dag: &sqlcv1.V1Dag{
			ID:                dagId,
			InsertedAt:        dagInsertedAt,
			TenantID:          tenantId,
			ExternalID:        dagExternalId,
			DisplayName:       "classic-collision-dag",
			WorkflowID:        workflowId,
			WorkflowVersionID: uuid.New(),
		},
		Input:              []byte(`{}`),
		AdditionalMetadata: []byte(`{}`),
		TotalTasks:         3,
	}

	locksNotAcquired, err := repo.CreateDAGs(ctx, tenantId, []*DAGWithData{dag})
	require.NoError(t, err)
	require.Empty(t, locksNotAcquired)

	f := classicCollisionRun{
		tenantId:      tenantId,
		dagId:         dagId,
		dagInsertedAt: dagInsertedAt,
		dagExternalId: dagExternalId,
		workflowId:    workflowId,
	}

	f.root = newClassicCollisionTask(f, dagId, dagInsertedAt)
	f.children = []replayStatusFixture{
		newClassicCollisionTask(f, dagId+10000, pgtype.Timestamptz{Time: dagInsertedAt.Time.Add(time.Microsecond), Valid: true}),
		newClassicCollisionTask(f, dagId+20000, pgtype.Timestamptz{Time: dagInsertedAt.Time.Add(2 * time.Microsecond), Valid: true}),
	}

	tasks := []*V1TaskWithPayload{
		classicTaskWithDAGLink(f, f.root, "test:classic-collision-root", "classic-root"),
		classicTaskWithDAGLink(f, f.children[0], "test:classic-collision-child", "classic-child-1"),
		classicTaskWithDAGLink(f, f.children[1], "test:classic-collision-child", "classic-child-2"),
	}

	_, locksNotAcquired, err = repo.CreateTasks(ctx, tenantId, tasks)
	require.NoError(t, err)
	require.Empty(t, locksNotAcquired)

	return f
}

func (f classicCollisionRun) allTasks() []replayStatusFixture {
	return append([]replayStatusFixture{f.root}, f.children...)
}

// newClassicCollisionTask builds a task identity (external id, worker id, timestamps) for a run.
func newClassicCollisionTask(f classicCollisionRun, taskId int64, insertedAt pgtype.Timestamptz) replayStatusFixture {
	return replayStatusFixture{
		tenantId:   f.tenantId,
		taskId:     taskId,
		insertedAt: insertedAt,
		externalId: uuid.New(),
		workflowId: f.workflowId,
		workerId:   uuid.New(),
	}
}

// classicTaskWithDAGLink builds the task row payload the engine would write: a genuine task whose
// dag columns point back at the DAG it belongs to.
func classicTaskWithDAGLink(f classicCollisionRun, task replayStatusFixture, actionId, displayName string) *V1TaskWithPayload {
	return &V1TaskWithPayload{
		V1Task: &sqlcv1.V1Task{
			ID:                 task.taskId,
			InsertedAt:         task.insertedAt,
			TenantID:           task.tenantId,
			Queue:              "default",
			ActionID:           actionId,
			StepID:             uuid.New(),
			WorkflowID:         task.workflowId,
			WorkflowVersionID:  uuid.New(),
			WorkflowRunID:      f.dagExternalId,
			ScheduleTimeout:    "5m",
			StepTimeout:        pgtype.Text{String: "60s", Valid: true},
			Priority:           pgtype.Int4{Int32: 1, Valid: true},
			Sticky:             sqlcv1.V1StickyStrategyNONE,
			ExternalID:         task.externalId,
			DisplayName:        displayName,
			Input:              []byte(`{}`),
			AdditionalMetadata: []byte(`{}`),
			DagID:              pgtype.Int8{Int64: f.dagId, Valid: true},
			DagInsertedAt:      f.dagInsertedAt,
		},
		Payload: []byte(`{}`),
	}
}

// classicLifecycleEvents returns the four ordinary engine lifecycle events (QUEUED → ASSIGNED →
// STARTED → FINISHED) for one task, the event stream the archive shows for the stuck classic
// runs (no orchestrator events exist for them).
func classicLifecycleEvents(task replayStatusFixture) []sqlcv1.CreateTaskEventsOLAPParams {
	return []sqlcv1.CreateTaskEventsOLAPParams{
		task.event(sqlcv1.V1EventTypeOlapQUEUED, sqlcv1.V1ReadableStatusOlapQUEUED, 0),
		task.event(sqlcv1.V1EventTypeOlapASSIGNED, sqlcv1.V1ReadableStatusOlapRUNNING, 0),
		task.event(sqlcv1.V1EventTypeOlapSTARTED, sqlcv1.V1ReadableStatusOlapRUNNING, 0),
		task.event(sqlcv1.V1EventTypeOlapFINISHED, sqlcv1.V1ReadableStatusOlapCOMPLETED, 0),
	}
}

// applyClassicLifecycle feeds one task's lifecycle through the MQ event path (writeTaskEventBatch,
// which drives UpdateTaskStatusesFromMQ and then UpdateDAGStatusesFromMQ in one transaction).
func applyClassicLifecycle(t *testing.T, ctx context.Context, repo *OLAPRepositoryImpl, task replayStatusFixture, runExternalId uuid.UUID) {
	t.Helper()

	eventExternalIdToWorkflowRunId := map[uuid.UUID]uuid.UUID{task.externalId: runExternalId}

	_, locksNotAcquired, err := repo.CreateTaskEvents(ctx, task.tenantId, classicLifecycleEvents(task), eventExternalIdToWorkflowRunId, nil, nil)
	require.NoError(t, err)
	require.Empty(t, locksNotAcquired)
}

func (f classicCollisionRun) assertStatuses(t *testing.T, ctx context.Context, pool *pgxpool.Pool, wantStatus string) {
	t.Helper()

	var status string

	err := pool.QueryRow(ctx, `
		SELECT readable_status::text
		FROM v1_dags_olap
		WHERE tenant_id = $1 AND id = $2
	`, f.tenantId, f.dagId).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, wantStatus, status, "v1_dags_olap.readable_status")

	err = pool.QueryRow(ctx, `
		SELECT readable_status::text
		FROM v1_runs_olap
		WHERE tenant_id = $1 AND external_id = $2
	`, f.tenantId, f.dagExternalId).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, wantStatus, status, "v1_runs_olap.readable_status")
}

func assertClassicTaskStatuses(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f classicCollisionRun, wantStatus string) {
	t.Helper()

	for _, task := range f.allTasks() {
		assertOLAPTaskStatus(t, ctx, pool, task, wantStatus, 0)
	}
}

// TestClassicDAGRootIdentityCollision_RollsUpToCompleted is the regression for runs whose classic
// root task shares the DAG's (id, inserted_at): the trigger-written junction row is byte-identical
// to the operator orchestrator's self-mapping, and the operator-exclusion heuristic in both DAG
// rollup queries used to strand such a DAG at QUEUED forever (a classic run has no orchestrator
// event stream to update it through the operator path). Both rollup paths must promote the DAG —
// and its mirrored v1_runs_olap row — to COMPLETED once every real task of the DAG completes.
func TestClassicDAGRootIdentityCollision_RollsUpToCompleted(t *testing.T) {
	basePool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool := createEnumAwarePool(t, basePool)
	repo := createOLAPRepositoryWithPayloadStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, repo.UpdateTablePartitions(ctx))

	t.Run("mq_path", func(t *testing.T) {
		f := seedClassicCollisionRun(t, ctx, repo, 7100)
		f.assertStatuses(t, ctx, pool, "QUEUED")

		// the root task completes first while the children are still queued: a tracked DAG must
		// be RUNNING at this point (pre-fix it never leaves QUEUED because the rollup excludes it)
		applyClassicLifecycle(t, ctx, repo, f.root, f.dagExternalId)
		assertOLAPTaskStatus(t, ctx, pool, f.root, "COMPLETED", 0)
		f.assertStatuses(t, ctx, pool, "RUNNING")

		applyClassicLifecycle(t, ctx, repo, f.children[0], f.dagExternalId)
		assertOLAPTaskStatus(t, ctx, pool, f.children[0], "COMPLETED", 0)
		f.assertStatuses(t, ctx, pool, "RUNNING")

		applyClassicLifecycle(t, ctx, repo, f.children[1], f.dagExternalId)
		assertOLAPTaskStatus(t, ctx, pool, f.children[1], "COMPLETED", 0)
		f.assertStatuses(t, ctx, pool, "COMPLETED")
	})

	t.Run("tmp_table_path", func(t *testing.T) {
		f := seedClassicCollisionRun(t, ctx, repo, 7200)
		f.assertStatuses(t, ctx, pool, "QUEUED")

		// Complete the tasks through the periodic task-status consumer: their lifecycle events
		// land in the tmp events table, and repo.UpdateTaskStatuses applies them. No dag rollup
		// runs on this path, so the dag row stays QUEUED.
		for _, task := range f.allTasks() {
			for _, e := range classicLifecycleEvents(task) {
				_, err := pool.Exec(ctx, `
					INSERT INTO v1_task_events_olap_tmp (
						tenant_id, task_id, task_inserted_at, event_type, readable_status, retry_count, worker_id
					) VALUES ($1, $2, $3, $4::v1_event_type_olap, $5::v1_readable_status_olap, $6, $7)
				`, e.TenantID, e.TaskID, e.TaskInsertedAt, string(e.EventType), string(e.ReadableStatus), e.RetryCount, e.WorkerID)
				require.NoError(t, err)
			}
		}

		_, _, err := repo.UpdateTaskStatuses(ctx, []uuid.UUID{f.tenantId})
		require.NoError(t, err)
		assertClassicTaskStatuses(t, ctx, pool, f, "COMPLETED")

		// Enqueue the DAG for the periodic DAG-status consumer: the same
		// (tenant_id, dag_id, dag_inserted_at) v1_task_status_updates_tmp row the task-status
		// update path feeds it with (the trigger that wrote these rows was removed in migration
		// v1_0_104, so the staging table is seeded directly, exactly as the task staging table
		// is seeded above). repo.UpdateDAGStatuses then rolls the dag up from this queue.
		_, err = pool.Exec(ctx, `
			INSERT INTO v1_task_status_updates_tmp (tenant_id, dag_id, dag_inserted_at)
			VALUES ($1, $2, $3)
		`, f.tenantId, f.dagId, f.dagInsertedAt)
		require.NoError(t, err)

		// pre-fix the DAG consumer excludes the DAG (self junction row, no orchestrator events
		// exist for a classic run), requeues its status update, and leaves it QUEUED
		_, _, err = repo.UpdateDAGStatuses(ctx, []uuid.UUID{f.tenantId})
		require.NoError(t, err)
		f.assertStatuses(t, ctx, pool, "COMPLETED")
	})
}

// runEventListKey identifies one aggregated run-event row for set comparison between listings.
type runEventListKey struct {
	taskID     int64
	eventType  sqlcv1.V1EventTypeOlap
	retryCount int32
}

func runEventListKeys(events []*TaskEventWithPayloads) map[runEventListKey]struct{} {
	keys := make(map[runEventListKey]struct{}, len(events))
	for _, e := range events {
		keys[runEventListKey{taskID: e.TaskID, eventType: e.EventType, retryCount: e.RetryCount}] = struct{}{}
	}
	return keys
}

// TestClassicDAGRootIdentityCollision_RootEventsIncludedInRunEventList is the regression for the
// run-event listing hiding rule: the classic root task's junction row is a real task (backed by a
// v1_tasks_olap row whose dag columns point at the DAG), so the default listing of the run's
// events must include the root's events even though genuine child tasks exist. Only true operator
// orchestrator self-mappings may be hidden, and the include-orchestrator flag must not change
// what a classic run shows.
func TestClassicDAGRootIdentityCollision_RootEventsIncludedInRunEventList(t *testing.T) {
	basePool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool := createEnumAwarePool(t, basePool)
	repo := createOLAPRepositoryWithPayloadStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, repo.UpdateTablePartitions(ctx))

	f := seedClassicCollisionRun(t, ctx, repo, 7300)

	for _, task := range f.allTasks() {
		applyClassicLifecycle(t, ctx, repo, task, f.dagExternalId)
	}

	// pre-fix the root's rows are missing from the default listing (it looks like the
	// orchestrator self-mapping and real children exist), but present when the flag is set
	defaultEvents, err := repo.ListTaskRunEventsByWorkflowRunId(ctx, f.tenantId, f.dagExternalId, false)
	require.NoError(t, err)
	explicitEvents, err := repo.ListTaskRunEventsByWorkflowRunId(ctx, f.tenantId, f.dagExternalId, true)
	require.NoError(t, err)

	// one aggregated row per lifecycle event type per task (root + two children)
	require.Len(t, defaultEvents, 12, "default run-event listing must include the classic root's events")
	require.Len(t, explicitEvents, 12)

	rootKey := runEventListKey{taskID: f.root.taskId, eventType: sqlcv1.V1EventTypeOlapFINISHED, retryCount: 0}
	assert.Contains(t, runEventListKeys(defaultEvents), rootKey, "root task FINISHED event")

	childKeys := map[runEventListKey]struct{}{}
	for _, child := range f.children {
		childKeys[runEventListKey{taskID: child.taskId, eventType: sqlcv1.V1EventTypeOlapFINISHED, retryCount: 0}] = struct{}{}
	}
	for key := range childKeys {
		assert.Contains(t, runEventListKeys(defaultEvents), key, "child task FINISHED event")
	}

	// the classic root is a genuine task, not an orchestrator: the flag must not change the listing
	assert.Equal(t, runEventListKeys(defaultEvents), runEventListKeys(explicitEvents))
}

// TestOperatorDAG_OrchestratorEventsHiddenByDefaultInRunEventList locks in the true-operator side
// of the same hiding rule: for an operator DAG the orchestrator's events (task identity == dag
// identity, with no v1_tasks_olap row backing that identity) stay hidden from the default
// run-event listing once real child tasks exist, and are included only when
// includeOrchestratorEvents is set. Together with the classic-root test above, this pins the
// discriminator between a true operator self-mapping and a numerically colliding classic root.
func TestOperatorDAG_OrchestratorEventsHiddenByDefaultInRunEventList(t *testing.T) {
	basePool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool := createEnumAwarePool(t, basePool)
	repo := createOLAPRepositoryWithPayloadStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, repo.UpdateTablePartitions(ctx))

	f := seedOperatorDag(t, ctx, repo, 7400)

	childA := f.createChild(t, ctx, repo, 7401)
	childB := f.createChild(t, ctx, repo, 7402)

	childEvents := append(classicLifecycleEvents(childA), classicLifecycleEvents(childB)...)
	f.applyChildEvents(t, ctx, repo, childEvents)

	// orchestrator events are addressed to the DAG's own (id, inserted_at); no task row ever
	// backs them (the operator root is never emitted to v1_tasks_olap)
	f.applyOrchestratorMonitoringEvents(t, ctx, repo,
		f.orchestratorEvent(sqlcv1.V1EventTypeOlapQUEUED, sqlcv1.V1ReadableStatusOlapQUEUED, 0),
		f.orchestratorEvent(sqlcv1.V1EventTypeOlapASSIGNED, sqlcv1.V1ReadableStatusOlapRUNNING, 0),
		f.orchestratorEvent(sqlcv1.V1EventTypeOlapSTARTED, sqlcv1.V1ReadableStatusOlapRUNNING, 0),
		f.orchestratorEvent(sqlcv1.V1EventTypeOlapFINISHED, sqlcv1.V1ReadableStatusOlapCOMPLETED, 0),
	)

	defaultEvents, err := repo.ListTaskRunEventsByWorkflowRunId(ctx, f.tenantId, f.dagExternalId, false)
	require.NoError(t, err)
	explicitEvents, err := repo.ListTaskRunEventsByWorkflowRunId(ctx, f.tenantId, f.dagExternalId, true)
	require.NoError(t, err)

	// the two children's four lifecycle events each — and nothing addressed to the orchestrator
	defaultKeys := runEventListKeys(defaultEvents)
	require.Len(t, defaultEvents, 8, "orchestrator events must be hidden by default")
	for _, child := range []replayStatusFixture{childA, childB} {
		_, ok := defaultKeys[runEventListKey{taskID: child.taskId, eventType: sqlcv1.V1EventTypeOlapFINISHED, retryCount: 0}]
		assert.True(t, ok, "child task FINISHED event must be listed by default")
	}

	explicitKeys := runEventListKeys(explicitEvents)
	require.Len(t, explicitEvents, 12, "orchestrator events must be included on request")
	_, ok := explicitKeys[runEventListKey{taskID: f.dagId, eventType: sqlcv1.V1EventTypeOlapFINISHED, retryCount: 0}]
	assert.True(t, ok, "orchestrator FINISHED event must be included on request")

	for key := range defaultKeys {
		_, ok := explicitKeys[key]
		assert.True(t, ok, "default listing rows must be a subset of the explicit listing")
	}
}

// TestStandaloneTaskRun_EventsListedByWorkflowRunId is the regression for task-keyed standalone
// runs: their lookup row carries task_id with dag_id NULL (no dag-to-task junction to expand), so
// listing the run's task events by its run id used to return zero rows. The events of the task
// itself must be returned directly.
func TestStandaloneTaskRun_EventsListedByWorkflowRunId(t *testing.T) {
	basePool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool := createEnumAwarePool(t, basePool)
	repo := createOLAPRepositoryWithPayloadStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, repo.UpdateTablePartitions(ctx))

	// standalone task: dag_id NULL on the task and on its lookup row; run external id == task
	// external id, so the run's lookup row is the task's own
	f := seedReplayTask(t, ctx, repo, 8000)
	applyClassicLifecycle(t, ctx, repo, f, f.externalId)

	events, err := repo.ListTaskRunEventsByWorkflowRunId(ctx, f.tenantId, f.externalId, false)
	require.NoError(t, err)

	// pre-fix this is empty: the lookup row's NULL dag_id drops it from the inner join to
	// v1_dag_to_task_olap, so no task ever reaches the event aggregation
	require.Len(t, events, 4, "standalone task run must return its own native events")

	foundFinished := false
	for _, e := range events {
		assert.Equal(t, f.taskId, e.TaskID, "all rows must belong to the standalone task")
		if e.EventType == sqlcv1.V1EventTypeOlapFINISHED && e.ReadableStatus == sqlcv1.V1ReadableStatusOlapCOMPLETED {
			foundFinished = true
		}
	}
	assert.True(t, foundFinished, "task FINISHED/COMPLETED event must be listed")
}
