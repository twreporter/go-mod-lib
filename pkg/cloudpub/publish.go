package cloudpub

import (
	"context"
	"sync"
	"fmt"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	f "github.com/twreporter/logformatter"
	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
)

type(
	Config struct {
		ProjectID string
		Topic     string
	}

	Message struct {
		ID          uint
		OrderNumber string
		Type        string
	}

	publisher struct {
		Topic *pubsub.Topic
	}
)

var (
	entry *publisher
)

func NewPublisher(ctx context.Context, conf *Config) (*publisher, error) {
	log.Infof("new publisher %+v", conf)
	c, err := pubsub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t := c.Topic(conf.Topic)

	entry = &publisher{Topic: t}
	return entry, nil
}

func (p *publisher) publish(ctx context.Context, msg []byte) error {
	m := &pubsub.Message{Data: msg}

	res := p.Topic.Publish(ctx, m)
	_, err := res.Get(ctx)
	return err
}

func PublishNotifications(ctx context.Context, ms []*Message) {
	var wg sync.WaitGroup

	if len(ms) == 0 {
		return
	}

	wg.Add(len(ms))
	for _, m := range ms {
		data, _ := json.Marshal(m)
		go func(id uint, orderNumber string, d []byte) {
			var err error

			defer func() {
				if err != nil {
					log.WithField("err", err).Errorf("%s", f.FormatStack(err))
				}
			}()
			defer wg.Done()

			err = entry.Publish(ctx, d)
			if err != nil {
				err = errors.Wrap(err, fmt.Sprintf("Fail to publish ID: %d, order: %s", id, orderNumber))
			}
		}(m.ID, m.OrderNumber, data)
	}
}
