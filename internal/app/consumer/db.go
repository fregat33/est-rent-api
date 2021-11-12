package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ozonmp/est-rent-api/internal/app/repo"
	"github.com/ozonmp/est-rent-api/internal/model"
)

type Consumer interface {
	Start()
	Close()
}

type consumer struct {
	n      uint64
	events chan<- model.RentEvent

	repo repo.EventRepo

	batchSize uint64
	timeout   time.Duration

	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func NewDbConsumer(
	n uint64,
	batchSize uint64,
	consumeTimeout time.Duration,
	repo repo.EventRepo,
	events chan<- model.RentEvent,
) Consumer {
	wg := &sync.WaitGroup{}

	return &consumer{
		n:         n,
		batchSize: batchSize,
		timeout:   consumeTimeout,
		repo:      repo,
		events:    events,
		wg:        wg,
		cancel:    func() {},
	}
}

func (c *consumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	for i := uint64(0); i < c.n; i++ {
		c.wg.Add(1)
		go c.workerExec(i, ctx)
	}
}

func (c *consumer) workerExec(workerIdx uint64, ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.timeout)
	for {
		select {
		case <-ticker.C:
			events, err := c.repo.Lock(c.batchSize)
			if err != nil {
				fmt.Printf("Consumer.Start.Lock: error in worker: %v, message: %s\n",
					workerIdx, err.Error())
				continue
			}
			for _, event := range events {
				c.events <- event
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *consumer) Close() {
	c.cancel()
	c.wg.Wait()
}
