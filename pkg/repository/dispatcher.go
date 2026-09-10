package repository

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlchelpers"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type CreateDispatcherOpts struct {
	ID uuid.UUID `validate:"required"`
}

type UpdateDispatcherOpts struct {
	LastHeartbeatAt *time.Time
}

type DispatcherRepository interface {
	// CreateNewDispatcher creates a new dispatcher for a given tenant.
	CreateNewDispatcher(ctx context.Context, opts *CreateDispatcherOpts) (*sqlcv1.Dispatcher, error)

	// UpdateDispatcher updates a dispatcher for a given tenant.
	UpdateDispatcher(ctx context.Context, dispatcherId uuid.UUID, opts *UpdateDispatcherOpts) (*sqlcv1.Dispatcher, error)

	Delete(ctx context.Context, dispatcherId uuid.UUID) error
}

type dispatcherRepository struct {
	*sharedRepository
}

func newDispatcherRepository(shared *sharedRepository) DispatcherRepository {
	return &dispatcherRepository{
		sharedRepository: shared,
	}
}

func (d *dispatcherRepository) CreateNewDispatcher(ctx context.Context, opts *CreateDispatcherOpts) (*sqlcv1.Dispatcher, error) {
	if err := d.v.Validate(opts); err != nil {
		return nil, err
	}

	return d.queries.CreateDispatcher(ctx, d.pool, opts.ID)
}

func (d *dispatcherRepository) UpdateDispatcher(ctx context.Context, dispatcherId uuid.UUID, opts *UpdateDispatcherOpts) (*sqlcv1.Dispatcher, error) {
	if err := d.v.Validate(opts); err != nil {
		return nil, err
	}

	dispatcher, err := d.queries.UpdateDispatcher(ctx, d.pool, sqlcv1.UpdateDispatcherParams{
		ID:              dispatcherId,
		LastHeartbeatAt: sqlchelpers.TimestampFromTime(opts.LastHeartbeatAt.UTC()),
	})
	d.observeControlPlaneWrite(ctx, "dispatcher-heartbeat", err)

	return dispatcher, err
}

func (d *dispatcherRepository) Delete(ctx context.Context, dispatcherId uuid.UUID) error {
	_, err := d.queries.DeleteDispatcher(ctx, d.pool, dispatcherId)
	return err
}
