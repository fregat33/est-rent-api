package retranslator

import (
	"fmt"
	"github.com/ozonmp/est-rent-api/internal/model"
	"github.com/ozonmp/est-rent-api/internal/utils"
	"github.com/shopspring/decimal"
	"math/big"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ozonmp/est-rent-api/internal/mocks"
)

func TestStartLockStop(t *testing.T) {
	t.Parallel()

	cfg := given(t)

	rtr := when(cfg)

	then(cfg, func(repo *mocks.MockEventRepo, sender *mocks.MockEventSender) {
		repo.EXPECT().Lock(gomock.Any()).AnyTimes()
	})

	rtr.Close()
}

func TestPositiveWorkflow(t *testing.T) {
	t.Parallel()

	cfg := given(t)

	rtr := when(cfg)
	defer rtr.Close()

	then(cfg, func(repo *mocks.MockEventRepo, sender *mocks.MockEventSender) {
		for _, event := range getEvents(cfg.ChannelSize) {
			repo.EXPECT().Lock(cfg.ConsumeSize).Return([]model.RentEvent{event}, nil).MinTimes(1).MaxTimes(1)
			sender.EXPECT().Send(&event).Return(nil).MinTimes(1).MaxTimes(1)
			repo.EXPECT().Remove([]uint64{event.ID}).Return(nil).MinTimes(1).MaxTimes(1)
		}
	})
}

func TestUnlockErrorWorkflow(t *testing.T) {
	t.Parallel()

	cfg := given(t)

	rtr := when(cfg)
	defer rtr.Close()

	then(cfg, func(repo *mocks.MockEventRepo, sender *mocks.MockEventSender) {
		for _, event := range getEvents(cfg.ChannelSize) {
			repo.EXPECT().Lock(cfg.ConsumeSize).Return([]model.RentEvent{event}, nil).MinTimes(1).MaxTimes(1)
			sender.EXPECT().Send(&event).Return(fmt.Errorf("testError")).MinTimes(1).MaxTimes(1)
			repo.EXPECT().Unlock([]uint64{event.ID}).Return(nil).MinTimes(1).MaxTimes(1)
		}
	})
}

func given(t *testing.T) Config {
	cfg := Config{
		ChannelSize:    512,
		ConsumerCount:  2,
		ConsumeSize:    10,
		ConsumeTimeout: 50 * time.Millisecond,
		ProducerCount:  2,
		WorkerCount:    2,
	}

	ctrl := gomock.NewController(t)

	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	cfg.Repo = repo
	cfg.Sender = sender

	return cfg
}

func when(cfg Config) Retranslator {
	rtr := NewRetranslator(cfg)
	rtr.Start()
	return rtr
}

func then(cfg Config, exec func(repo *mocks.MockEventRepo, sender *mocks.MockEventSender)) {
	exec(cfg.Repo.(*mocks.MockEventRepo), cfg.Sender.(*mocks.MockEventSender))
}

func getEvents(channelSize uint64) []model.RentEvent {
	events := make([]model.RentEvent, 0)

	for i := uint64(0); i < channelSize; i++ {
		objType := model.House
		if i%2 == 0 {
			objType = model.Car
		}

		period := time.Duration(i+1) * time.Hour
		bigUint := new(big.Int).SetUint64(i * 100)
		price := decimal.NewFromBigInt(bigUint, 0)

		event := model.RentEvent{
			ID:     i,
			Type:   model.EventType(utils.GetRandomInt(1, 3)),
			Status: model.EventStatus(utils.GetRandomInt(1, 2)),
			Entity: &model.Rent{
				ID:         i,
				RenterId:   i * 10,
				ObjectType: objType,
				ObjectInfo: fmt.Sprintf("Best offer: %s (id: %v) for %v just only $%v",
					objType, i, period, price),
				Period: period,
				Price:  price,
			},
		}
		events = append(
			events,
			event,
		)
	}
	return events
}
