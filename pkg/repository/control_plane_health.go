package repository

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	controlPlaneFailureThreshold        = 2
	controlPlaneRecoverySuccessThreshold = 3
	controlPlaneFailureWindow           = 10 * time.Second
	controlPlaneRecoveryGrace           = 30 * time.Second
)

type controlPlaneState uint8

const (
	controlPlaneHealthy controlPlaneState = iota
	controlPlaneDegraded
	controlPlaneRecovering
)

func (s controlPlaneState) String() string {
	switch s {
	case controlPlaneDegraded:
		return "degraded"
	case controlPlaneRecovering:
		return "recovering"
	default:
		return "healthy"
	}
}

type controlPlaneSnapshot struct {
	state          controlPlaneState
	causeID        uuid.UUID
	degradedAt     time.Time
	resumeAt       time.Time
	lastFailureAt  time.Time
	lastSource     string
	lastError      string
	failureCount   int
	becameDegraded bool
	becameHealthy  bool
	beganRecovery  bool
}

type controlPlaneHealth struct {
	mu sync.Mutex

	state             controlPlaneState
	causeID           uuid.UUID
	degradedAt        time.Time
	resumeAt          time.Time
	lastFailureAt     time.Time
	lastSource        string
	lastError         string
	failureCount      int
	recoverySuccesses int
	now               func() time.Time
}

func (h *controlPlaneHealth) currentTime() time.Time {
	if h.now != nil {
		return h.now().UTC()
	}
	return time.Now().UTC()
}

func (h *controlPlaneHealth) snapshot() controlPlaneSnapshot {
	return controlPlaneSnapshot{
		state:         h.state,
		causeID:       h.causeID,
		degradedAt:    h.degradedAt,
		resumeAt:      h.resumeAt,
		lastFailureAt: h.lastFailureAt,
		lastSource:    h.lastSource,
		lastError:     h.lastError,
		failureCount:  h.failureCount,
	}
}

func (h *controlPlaneHealth) observe(source string, err error) controlPlaneSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.currentTime()
	if err != nil {
		if h.lastFailureAt.IsZero() || now.Sub(h.lastFailureAt) > controlPlaneFailureWindow {
			h.failureCount = 0
		}
		h.failureCount++
		h.lastFailureAt = now
		h.lastSource = source
		h.lastError = err.Error()
		h.recoverySuccesses = 0
		h.resumeAt = time.Time{}

		becameDegraded := h.state != controlPlaneDegraded && h.failureCount >= controlPlaneFailureThreshold
		if becameDegraded {
			h.state = controlPlaneDegraded
			h.causeID = uuid.New()
			h.degradedAt = now
		}

		snapshot := h.snapshot()
		snapshot.becameDegraded = becameDegraded
		return snapshot
	}

	if h.state == controlPlaneDegraded {
		h.recoverySuccesses++
		if h.recoverySuccesses >= controlPlaneRecoverySuccessThreshold {
			h.state = controlPlaneRecovering
			h.resumeAt = now.Add(controlPlaneRecoveryGrace)
			snapshot := h.snapshot()
			snapshot.beganRecovery = true
			return snapshot
		}
	} else if h.state == controlPlaneHealthy && !h.lastFailureAt.IsZero() && now.Sub(h.lastFailureAt) > controlPlaneFailureWindow {
		h.failureCount = 0
		h.lastFailureAt = time.Time{}
		h.lastSource = ""
		h.lastError = ""
	}

	return h.snapshot()
}

func (h *controlPlaneHealth) suppression() (bool, controlPlaneSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state == controlPlaneRecovering && !h.currentTime().Before(h.resumeAt) {
		h.state = controlPlaneHealthy
		h.failureCount = 0
		h.recoverySuccesses = 0
		h.lastFailureAt = time.Time{}
		h.lastSource = ""
		h.lastError = ""
		h.resumeAt = time.Time{}
		snapshot := h.snapshot()
		snapshot.becameHealthy = true
		return false, snapshot
	}

	return h.state != controlPlaneHealthy, h.snapshot()
}

func (r *sharedRepository) observeControlPlaneWrite(ctx context.Context, source string, err error) {
	if err != nil && (errors.Is(err, pgx.ErrNoRows) || (errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled))) {
		return
	}

	snapshot := r.controlPlaneHealth.observe(source, err)
	if r.l == nil {
		return
	}

	switch {
	case snapshot.becameDegraded:
		r.l.Error().Ctx(ctx).
			Err(err).
			Str("control_plane_cause_id", snapshot.causeID.String()).
			Str("control_plane_source", source).
			Int("control_plane_failure_count", snapshot.failureCount).
			Time("control_plane_degraded_at", snapshot.degradedAt).
			Msg("control-plane writes degraded; suppressing heartbeat-based task reassignment")
	case snapshot.beganRecovery:
		r.l.Warn().Ctx(ctx).
			Str("control_plane_cause_id", snapshot.causeID.String()).
			Time("task_reassignment_resume_at", snapshot.resumeAt).
			Msg("control-plane writes recovered; holding reassignment through heartbeat recovery grace")
	}
}

func (r *sharedRepository) shouldSuppressTaskReassignment(ctx context.Context, tenantID uuid.UUID) bool {
	suppress, snapshot := r.controlPlaneHealth.suppression()
	if r.l == nil {
		return suppress
	}

	if snapshot.becameHealthy {
		r.l.Info().Ctx(ctx).
			Str("control_plane_cause_id", snapshot.causeID.String()).
			Msg("control-plane recovery grace elapsed; resuming heartbeat-based task reassignment")
	}
	if suppress {
		event := r.l.Warn().Ctx(ctx).
			Str("tenant_id", tenantID.String()).
			Str("control_plane_cause_id", snapshot.causeID.String()).
			Str("control_plane_state", snapshot.state.String()).
			Str("control_plane_last_failure_source", snapshot.lastSource).
			Str("control_plane_last_error", snapshot.lastError).
			Time("control_plane_degraded_at", snapshot.degradedAt)
		if !snapshot.resumeAt.IsZero() {
			event = event.Time("task_reassignment_resume_at", snapshot.resumeAt)
		}
		event.Msg("task reassignment suppressed because worker heartbeat freshness is not trustworthy")
	}

	return suppress
}
