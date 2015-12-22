// Package pubsub abstracts away publishing and receiving of messages across our two topics
package pubsub

// https://cloud.google.com/pubsub/docs

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"time"

	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/skypies/adsb"
)

// I'm not convinced this pacakge should have its own logger
var Log *log.Logger
func init() {
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
}

func GetContext(jsonAuthFilename, projectName string) context.Context {
	jsonKey, err := ioutil.ReadFile(jsonAuthFilename)
	if err != nil { Log.Fatal(err) }

	conf, err := google.JWTConfigFromJSON(
    jsonKey,
    pubsub.ScopeCloudPlatform,
    pubsub.ScopePubSub,
	)
	if err != nil { Log.Fatalf("JWTConfigFromJson failed: %v", err) }

	return cloud.NewContext(projectName, conf.Client(oauth2.NoContext))
}

// This function runs in its own goroutine, so as not to hold up reading new messages
// https://godoc.org/google.golang.org/cloud/pubsub#Publish
func PublishMsgs(c context.Context, topic,receiverName string, msgs *[]*adsb.CompositeMsg) error {
	for i,msg := range *msgs {
		Log.Printf("- [%02d]%s\n", i, msg)
		(*msgs)[i].ReceiverName = receiverName // Claim this message, for upstream fame & glory
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(*msgs); err != nil {
		return err
	}
	msgIDs, err := pubsub.Publish(c, topic, &pubsub.Message{
		Data: buf.Bytes(),
	})

	if err != nil {
		Log.Fatalf("pubsub.Publish failed: %v", err)
	} else {
		Log.Printf("Published a message with a message id: %s\n", msgIDs[0])
	}
		
	return err
}

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

func PurgeSub(c context.Context, subscription, topic string) {
	//if exists,err := pubsub.TopicExists(c, topic); ... {}

	if err := pubsub.DeleteSub(c, subscription); err != nil {
		Log.Fatalf("DeleteSub failed: %v", err)
	}
	if err := pubsub.CreateSub(c, subscription, topic, 10*time.Second, ""); err != nil {
		Log.Fatalf("CreateSub failed: %v", err)
	}
}
