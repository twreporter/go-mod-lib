package cloudpub

import (
	"context"
	"io"

	log "github.com/sirupsen/logrus"
	f "github.com/twreporter/logformatter"
	"cloud.google.com/go/pubsub"
)

type(
	Config struct {
		ProjectID string
		Topic     string
	}

	Message struct {
		ID   int
		Type string
	}

	publisher struct {
		Topic *pubsub.Topic
	}
)

var (
	entry *publisher
)

func newPublisher(ctx context.Context, conf *Config) (*publisher, error) {
	log.Infof("new publisher %+v", conf)
	c, err := pubsub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t := c.Topic(conf.Topic)

	entry = &publisher{
		Topic: t
	}
	return entry, nil
}

func (p *publisher) Publish(ctx context.Context, msg []byte) error {
	m := &pubsub.Message{Data: msg}

	res := p.Topic.Publish(ctx, m)
	_, err := res.Get(ctx)
	return err
}

func publishNotifications(ctx context.Context, ms []*Message) {
	var wg sync.WaitGroup

	if len(ms) == 0 {
		return
	}

	wg.add(len(ms))
	for _, m := range ms {
		data := json.Marshal(m)
		go func(id int, d []byte) {
			var err error

			defer func() {
				if err != nil {
					log.WithField("err", err).Errorf("%s", f.FormatStack(err))
				}
			}()
			defer wg.Done()

			err = entry.Publish(ctx, d)
			if err != nil {
				err = errors.Wrap(err, fmt.Sprintf("Fail to publish ID: %d", id))
			}
		}(m.ID, data)
	}
}
