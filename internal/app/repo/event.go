package repo

import (
	"github.com/ozonmp/est-rent-api/internal/model"
)

type EventRepo interface {
	Lock(n uint64) ([]model.RentEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []model.RentEvent) error
	Remove(eventIDs []uint64) error
}

