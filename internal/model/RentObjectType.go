package model

import "fmt"

const (
	House RentObjectType = "house"
	Car   RentObjectType = "car"
)

type RentObjectType string

func (r RentObjectType) String() string {
	return string(r)
}

func (r RentObjectType) Icon() string {
	switch r {
	case Car:
		return "🚗"
	case House:
		return "🏚"
	default:
		panic(fmt.Errorf("RentObjectType: error unexpected value %s", r))
	}
	return r.String()
}

func (r RentObjectType) Full() string {
	switch r {
	case Car:
		return "🚗 car"
	case House:
		return "🏚 house"
	default:
		panic(fmt.Errorf("RentObjectType: error unexpected value %s", r))
	}
	return r.String()
}
