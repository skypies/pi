// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.
// The ideal deployment model for consolidator is as a user-managed AppEngine VM.
package main

// go run consolidator.go

// https://godoc.org/google.golang.org/cloud/pubsub#Publish

import (
	"flag"
	"log"
	"os"
	"time"
	
	"github.com/skypies/pi/airspace"
	"github.com/skypies/pi/pubsub"
)

var (
	Log                   *log.Logger

	fJsonAuthFile          string
	fProjectName           string
	fPubsubSubscription    string
	fPubsubOutputTopic     string
)

func init() {
	flag.StringVar(&fJsonAuthFile, "auth", "serfr0-fdb-48c34ecd36c9.json",
		"The JSON auth file for a Google service worker account")
	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubSubscription, "sub", "consolidator",
		"Name of the pubsub subscription on the adsb-inbound topic")
	flag.StringVar(&fPubsubOutputTopic, "output", "adsb-consolidated",
		"Name of the pubsub topic to post deduped output on (short name, not projects/blah...)")

	flag.Parse()	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
}

func main() {
	airspace := airspace.Airspace{}
	c := pubsub.GetContext(fJsonAuthFile, fProjectName)

	// While developing - purge things
	pubsub.PurgeSub(c, fPubsubSubscription, "adsb-inbound")
	
	for {
		msgs,err := pubsub.Pull(c, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			continue
		} else if msgs == nil || len(*msgs) == 0 {
			//Log.Printf("-- nothing found, sleeping")
			time.Sleep(time.Second * 1)
			continue
		}
		//Log.Printf("Pull: found %d ADSB", len(*msgs))

		newMsgs := airspace.MaybeUpdate(msgs)
		//Log.Printf(" %d were new (%d already seen)", len(newMsgs), len(*msgs)-len(newMsgs))

		for i,msg := range newMsgs {
			Log.Printf(" new: [%2d] %s\n", i, msg)
		}

		// Update the memcache thing
		
		if fPubsubOutputTopic != "" {
			// Publish newMsgs
		}
	}

	Log.Print("Final clean shutdown")
}
