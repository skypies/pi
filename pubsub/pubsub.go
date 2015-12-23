// Package pubsub abstracts away publishing and receiving of messages across our two topics
package pubsub

// https://cloud.google.com/pubsub/docs

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"google.golang.org/appengine"

	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	
	"github.com/skypies/adsb"
)

// {{{ GetAppEngineContext, GetDevContext

func wrapContext(projectName string, in context.Context) context.Context {
	client, err := google.DefaultClient(
    in,
    pubsub.ScopeCloudPlatform,
    pubsub.ScopePubSub,
	)
	if err != nil {
		panic(fmt.Sprintf("g.DefaultClient failed: %v", err))
	}

	return cloud.NewContext(projectName, client)
}

func GetAppEngineContext(projectName string) context.Context {
	return wrapContext(projectName, appengine.BackgroundContext())
}

// cp serfr0-fdb-blahblah.json ~/.config/gcloud/application_default_credentials.json
func GetDevContext(projectName string) context.Context {
	return wrapContext(projectName, context.TODO())
}

// }}}
// {{{ PurgeSub

func PurgeSub(c context.Context, subscription, topic string) error {
	if err := pubsub.DeleteSub(c, subscription); err != nil {
		return err
	}
	return pubsub.CreateSub(c, subscription, topic, 10*time.Second, "")
}

// }}}
// {{{ Setup

func Setup (c context.Context, inTopic, inSub, outTopic string) error {
	if exists,err := pubsub.TopicExists(c,inTopic); err != nil {
		return err
	} else if !exists {
		if err := pubsub.CreateTopic(c,inTopic); err != nil { return err }
	}

	if exists,err := pubsub.TopicExists(c,outTopic); err != nil {
		return err
	} else if !exists {
		if err := pubsub.CreateTopic(c,outTopic); err != nil { return err }
	}

	if exists,err := pubsub.SubExists(c,inSub); err != nil {
		return err
	} else if !exists {
		if err := pubsub.CreateSub(c, inSub, inTopic, 10*time.Second, ""); err != nil {
			return err
		}
	}
	return nil
}

// }}}

// {{{ PublishMsgs

// This function runs in its own goroutine, so as not to hold up reading new messages
// https://godoc.org/google.golang.org/cloud/pubsub#Publish
func PublishMsgs(c context.Context, topic,receiverName string, msgs *[]*adsb.CompositeMsg) error {
	for i,_ := range *msgs {
		//log.Infof(c, "- [%02d]%s\n", i, msg)
		(*msgs)[i].ReceiverName = receiverName // Claim this message, for upstream fame & glory
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(*msgs); err != nil {
		return err
	}
	_, err := pubsub.Publish(c, topic, &pubsub.Message{
		Data: buf.Bytes(),
	})

	if err != nil {
		//log.Errorf(c, "pubsub.Publish failed: %v", err)
	} else {
		//log.Infof(c, "Published a message with a message id: %s\n", msgIDs[0])
	}
		
	return err
}

// }}}
// {{{ Pull

// https://godoc.org/google.golang.org/cloud/pubsub#example-Publish
func Pull(c context.Context, subscription string, numBundles int) (*[]*adsb.CompositeMsg, error) {
	msgs := []*adsb.CompositeMsg{}

	bundles,err := pubsub.Pull(c,subscription,numBundles)
	if err != nil {
		return nil, err
	}

	for _,bundle := range bundles {
		bundleContents := []*adsb.CompositeMsg{}
		buf := bytes.NewBuffer(bundle.Data)
		if err := gob.NewDecoder(buf).Decode(&bundleContents); err != nil {
			return nil, err
		}
		msgs = append(msgs, bundleContents...)

		if err := pubsub.Ack(c, subscription, bundle.AckID); err != nil {
			return nil,err
		}
	}
	
	return &msgs,nil
}

// }}}


// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
