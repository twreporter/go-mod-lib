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

func generateMessage(id uint, orderNumber string, donationType string) string {
	msg := fmt.Sprintf("Publish to cloud pub/sub err:\n")
	msg += fmt.Sprintf("Message:\n  type: %s, id: %d, orderNumber: %s", donationType, id, orderNumber)
	return msg
}

func NeticrmNotify(ctx context.Context, es []cloudpub.ErrorStack) {
	var wg sync.WaitGroup

	if len(es) == 0 {
		return
	}

	wg.Add(len(es))
	for _, e := range es {
		go func(id uint, orderNumber string, donationType string) {
			defer wg.Done()

			slackMsg := generateMessage(id, orderNumber, donationType)
			err := entry.Notify(ctx, slackMsg)
			if err != nil {
				log.WithField("Notify slack err", err).Errorf("%s", f.FormatStack(err))
			}
		}(e.Message.ID, e.Message.OrderNumber, e.Message.Type)
	}

	wg.Wait()
}
