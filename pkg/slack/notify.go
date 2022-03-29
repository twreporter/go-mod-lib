package slack

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	slackgo "github.com/slack-go/slack"
	log "github.com/sirupsen/logrus"
	f "github.com/twreporter/logformatter"
	"github.com/twreporter/go-mod-lib/pkg/cloudpub"
)

type Config struct {
	Webhook string
}

type Client struct {
	webhook string
}

var (
	entry *Client
)

func NewClient(conf *Config) (*Client, error) {
	entry := &Client{
		webhook: conf.Webhook,
	}
	return entry, nil
}

func (c *Client) Notify(ctx context.Context, message string) error {
	webhookMsg := &slackgo.WebhookMessage{
		Text: message,
	}

	err := slackgo.PostWebhookContext(ctx, c.webhook, webhookMsg)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func generateMessage(m cloudpub.Message) string {
	msg := fmt.Sprintf("Publish to cloud pub/sub err:\n")
	msg += fmt.Sprintf("Message:\n  %+v", m)
	return msg
}

func NeticrmNotify(ctx context.Context, es []*cloudpub.ErrorStack) {
	var wg sync.WaitGroup

	if len(es) == 0 {
		return
	}

	wg.Add(len(es))
	for _, e := range es {
		go func() {
			defer wg.Done()

			slackMsg := generateMessage(e.Message)
			err := entry.Notify(ctx, slackMsg)
			if err != nil {
				log.WithField("Notify slack err", err).Errorf("%s", f.FormatStack(err))
			}
		}()
	}

	wg.Wait()
}
