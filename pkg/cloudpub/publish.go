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

	ErrorStack struct {
		Message
		Err     error
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

func PublishNotifications(ctx context.Context, ms []*Message) ([]ErrorStack){
	var wg sync.WaitGroup
	var errors []ErrorStack

	if len(ms) == 0 {
		return errors
	}

	ch := make(chan ErrorStack, len(ms))
	wg.Add(len(ms))
	for _, m := range ms {
		data, err := json.Marshal(m)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Fail to marshal ID: %d, order: %s", m.ID, m.OrderNumber))
			errors = append(errors, ErrorStack{Err: err, Message: m})
			wg.Done()
			continue
		}
		go func(m Message, d []byte) {
			var err error

			defer func() {
				if err != nil {
					log.WithField("err", err).Errorf("%s", f.FormatStack(err))
					ch <- ErrorStack{Err: err, Message: m}
				}
			}()
			defer wg.Done()

			err = entry.publish(ctx, d)
			if err != nil {
				err = errors.Wrap(err, fmt.Sprintf("Fail to publish ID: %d, order: %s", m.ID, m.OrderNumber))
			}
		}(m, data)
	}
	wg.Wait()

	// close channel here to avoid deadlock
	close(ch)

	// consuming error channel
	wg.Add(1)
	go func(ch chan ErrorStack, errors *[]ErrorStack) {
		for err := range ch {
			*errors = append(*errors, err)
		}
	}(ch, &errors)

	wg.Wait()
	return errors
}
