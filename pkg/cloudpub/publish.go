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
	if p.Topic == nil {
		return errors.New("invalid publisher")
	}
	m := &pubsub.Message{Data: msg}

	res := p.Topic.Publish(ctx, m)
	_, err := res.Get(ctx)
	return err
}

func PublishNotifications(ctx context.Context, ms []*Message) ([]ErrorStack){
	var wg sync.WaitGroup
	var es []ErrorStack

	if len(ms) == 0 {
		return es
	}

	ch := make(chan ErrorStack, len(ms))
	wg.Add(len(ms))
	for _, m := range ms {
		if (entry == nil) {
			es = append(es, ErrorStack{
				Err: errors.New("Publisher is nil"),
				Message: Message{
					ID: m.ID,
					OrderNumber: m.OrderNumber,
					Type: m.Type,
				},
			})
			wg.Done()
			continue
		}
		data, err := json.Marshal(m)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Fail to marshal ID: %d, order: %s", m.ID, m.OrderNumber))
			es = append(es, ErrorStack{
				Err: err,
				Message: Message{
					ID: m.ID,
					OrderNumber: m.OrderNumber,
					Type: m.Type,
				},
			})
			wg.Done()
			continue
		}
		go func(id uint, order string, donationType string, d []byte) {
			defer func() {
				if err != nil {
					log.WithField("err", err).Errorf("%s", f.FormatStack(err))
					ch <- ErrorStack{
						Err: err,
						Message: Message{
							ID: id,
							OrderNumber: order,
							Type: donationType,
						},
					}
				}
			}()
			defer wg.Done()

			err := entry.publish(ctx, d)
			if err != nil {
				err = errors.Wrap(err, fmt.Sprintf("Fail to publish ID: %d, order: %s", id, order))
			}
		}(m.ID, m.OrderNumber, m.Type, data)
	}
	wg.Wait()

	// close channel here to avoid deadlock
	close(ch)

	// consuming error channel
	wg.Add(1)
	go func(ch chan ErrorStack, es *[]ErrorStack) {
		for err := range ch {
			*es = append(*es, err)
		}
		wg.Done()
	}(ch, &es)

	wg.Wait()
	return es
}
