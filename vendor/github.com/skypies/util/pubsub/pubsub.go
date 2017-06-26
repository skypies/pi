package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	
	"github.com/skypies/adsb"
)

// {{{ NewClient

func NewClient(ctx context.Context, projectName string) *pubsub.Client {
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		panic(fmt.Sprintf("pubsub.NewClient failed: %v", err))
	}

	return client
}

// }}}

// {{{ DeleteSub, CreateSub, PurgeSub

func DeleteSub (ctx context.Context, client *pubsub.Client, subscription string) error {
	return client.Subscription(subscription).Delete(ctx)
}
func CreateSub (ctx context.Context, client *pubsub.Client, subscription, topicId string) error {
	topic := client.Topic(topicId)
	cfg := pubsub.SubscriptionConfig{Topic:topic, AckDeadline: 10*time.Second}
	_,err := client.CreateSubscription(ctx, subscription, cfg)//topic, 10*time.Second, nil)
	return err
}

func RecreateSub(ctx context.Context, client *pubsub.Client, subscription, topic string) error {
	if err := DeleteSub(ctx, client, subscription); err != nil {
		return err
	}
	return CreateSub(ctx, client, subscription, topic)
}

// }}}
// {{{ Setup

func Setup (ctx context.Context, client *pubsub.Client, inTopic, inSub string) error {
	if exists,err := client.Topic(inTopic).Exists(ctx); err != nil {
		return err
	} else if !exists {
		if _,err := client.CreateTopic(ctx,inTopic); err != nil { return err }
	}

	if exists,err := client.Subscription(inSub).Exists(ctx); err != nil {
		return err
	} else if !exists {
		if err := CreateSub (ctx, client, inSub, inTopic); err != nil {
			return err
		}
	}
	return nil
}

// }}}

// {{{ PackPubsubMessage

func PackPubsubMessage(msgs []*adsb.CompositeMsg, receiverName string) (*pubsub.Message, error) {
	for i,_ := range msgs {
		msgs[i].ReceiverName = receiverName // Claim this message, for upstream fame & glory
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msgs); err != nil {
		return nil, err
	}

	return &pubsub.Message{
		Data: buf.Bytes(),
	}, nil
}

// }}}
// {{{ UnpackPubsubMessage

func UnpackPubsubMessage(msg *pubsub.Message) ([]*adsb.CompositeMsg, error) {
	contents := []*adsb.CompositeMsg{}

	buf := bytes.NewBuffer(msg.Data)
	if err := gob.NewDecoder(buf).Decode(&contents); err != nil {
		return nil, fmt.Errorf("UnpackPubsubMsg: %v\n", err)
	}
	return contents, nil

}

// }}}

// https://godoc.org/cloud.google.com/go/pubsub
// {{{ PublishMsgs

// This function runs in its own goroutine, so as not to hold up reading new messages
func PublishMsgs(ctx context.Context, client *pubsub.Client, topic,receiverName string, msgs []*adsb.CompositeMsg) error {
	m,err := PackPubsubMessage(msgs, receiverName)
	if err != nil { return err }

	_,err = client.Topic(topic).Publish(ctx, m).Get(ctx)
	
/*
	if err != nil {
		//log.Errorf(c, "pubsub.Publish failed: %v", err)
	} else {
		//log.Infof(c, "Published a message with a message id: %s\n", msgIDs[0])
	}
*/
	
	return err
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
