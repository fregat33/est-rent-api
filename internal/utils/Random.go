package utils

import (
	"math/rand"
	"sync"
	"time"
)

var once sync.Once
func GetRandomInt(min, max int) int {
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
	})
	return rand.Intn(max - min) + min
}