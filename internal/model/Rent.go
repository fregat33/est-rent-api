package model

import (
	"github.com/shopspring/decimal"
	"time"
)

type Rent struct {
	ID         uint64
	RenterId   uint64          `json:"renter_id"`
	ObjectType RentObjectType  `json:"object_type"`
	ObjectInfo string          `json:"object_info"`
	Period     time.Duration   `json:"period"`
	Price      decimal.Decimal `json:"price"`
}
