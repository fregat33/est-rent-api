package producer

import (
	"context"
	"fmt"
	"github.com/ozonmp/est-rent-api/internal/app/repo"
	"sync"
	"time"

	"github.com/ozonmp/est-rent-api/internal/app/sender"
	"github.com/ozonmp/est-rent-api/internal/model"

	"github.com/gammazero/workerpool"
)

type Producer interface {
	Start()
	Close()
}

type producer struct {
	n       uint64
	timeout time.Duration

	sender sender.EventSender
	events <-chan model.RentEvent
	repo   repo.EventRepo

	workerPool *workerpool.WorkerPool

	wg     *sync.WaitGroup
	cancel context.CancelFunc
}

func NewKafkaProducer(
	n uint64,
	sender sender.EventSender,
	repo repo.EventRepo,
	events <-chan model.RentEvent,
	workerPool *workerpool.WorkerPool,
) Producer {

	wg := &sync.WaitGroup{}

	return &producer{
		n:          n,
		sender:     sender,
		events:     events,
		repo:       repo,
		workerPool: workerPool,
		wg:         wg,
		cancel:     func() {},
	}
}

func (p *producer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go p.workerExec(i, ctx)
	}
}

func (p *producer) workerExec(workerIdx uint64, ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case event := <-p.events:
			if event.Type != model.Created {
				continue //TODO handle all types with guarantee
			}

			if err := p.sender.Send(&event); err != nil {
				p.workerPool.Submit(func() {
					if errInner := p.repo.Unlock([]uint64{event.ID}); errInner != nil {
						fmt.Printf("Producer.procEvents.Unlock: error in worker: %v, in eventID: %d, eventType: %v, message: %s\n",
							workerIdx, event.ID, event.Type, errInner.Error())
					}
				})
			} else {
				p.workerPool.Submit(func() {
					if errInner := p.repo.Remove([]uint64{event.ID}); errInner != nil {
						fmt.Printf("Producer.procEvents.Remove: error in worker: %v, eventID: %d, eventType: %v, message: %s\n",
							workerIdx, event.ID, event.Type, errInner.Error())
					}
				})
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *producer) Close() {
	p.cancel()
	p.wg.Wait()
}
