package sender

import (
	"github.com/ozonmp/est-rent-api/internal/model"
)

type EventSender interface {
	Send(subdomain *model.RentEvent) error
}
