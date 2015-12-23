// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.
package main

// aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse
// go run consolidator.go -ae=false

import (
	"flag"
	"fmt"
	"log"
	"os"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/skypies/pi/airspace"
	"github.com/skypies/pi/pubsub"
)

var (
	Log                   *log.Logger
	Ctx                    context.Context

	fProjectName           string
	fPubsubInputTopic      string
	fPubsubSubscription    string
	fPubsubOutputTopic     string
	fOnAppEngine           bool
	
	// Junky globals for basic status check
	startTime time.Time
	sizeAirspace int
	nMessages int
	nNewMessages int
	lastMessage time.Time
	stations map[string]int
)

func init() {
	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubInputTopic, "input", "adsb-inbound",
		"Name of the pubsub topic we read from (i.e. add our subscription to)")
	flag.StringVar(&fPubsubSubscription, "sub", "consolidator",
		"Name of the pubsub subscription on the adsb-inbound topic")
	flag.StringVar(&fPubsubOutputTopic, "output", "adsb-consolidated",
		"Name of the pubsub topic to post deduped output on (short name, not projects/blah...)")
	flag.BoolVar(&fOnAppEngine, "ae", true, "on appengine (use http://metadata/ etc)")

	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
	
	http.HandleFunc("/", handler)
	http.HandleFunc("/now", nowHandler)
	http.HandleFunc("/_ah/start", startHandler)
}

func main() {
	airspace := airspace.Airspace{}
	fmt.Printf("(main)")
	startTime = time.Now()
	stations = map[string]int{}
	
	if fOnAppEngine {
		Ctx = pubsub.GetAppEngineContext(fProjectName)
		Log.Printf("(ae context created)")
		pubsub.Setup(Ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
	} else {
		Ctx = pubsub.GetLocalContext(fProjectName)
		Log.Printf("(dev context created)")
		fPubsubSubscription += "DEV"
		pubsub.Setup(Ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
		pubsub.PurgeSub(Ctx, fPubsubSubscription, "adsb-inbound")
	}

	// Kick off the webservery bit
	go func(){
		if err := http.ListenAndServe(":8080", nil); err != nil {
			Log.Fatalf("http server fail: %v", err)
		}
	}()

	for {
		msgs,err := pubsub.Pull(Ctx, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			time.Sleep(time.Second * 5)
			continue

		} else if msgs == nil || len(*msgs) == 0 {
			time.Sleep(time.Second * 1)
			continue
		}

		newMsgs := airspace.MaybeUpdate(msgs)

		if fOnAppEngine {
			airspace.ToMemcache()  // Update the memcache thing
		}
		if fPubsubOutputTopic != "" {
			// Publish newMsgs
		}

		//Log.Printf(" %d were new (%d already seen)", len(newMsgs), len(*msgs)-len(newMsgs))
		//for i,msg := range newMsgs {
		//	Log.Printf(" new: [%2d] %s\n", i, msg)
		//}

		// Update crappy globals
		nMessages += len(*msgs)
		nNewMessages += len(newMsgs)
		lastMessage = time.Now()
		if b,err := airspace.ToBytes(); err == nil { sizeAirspace = len(b) }
		for _,m := range *msgs { stations[m.ReceiverName]++ }
	}

	Log.Printf("Final clean shutdown")
}

func handler(w http.ResponseWriter, r *http.Request) {
	stationStr := ""
	for k,v := range stations {
		stationStr += fmt.Sprintf("  * %-20.20s: %7d\n", k, v)
	}
	w.Write([]byte(fmt.Sprintf("OK\n* %d messages (%d dupe, %d new)\n"+
		"* up %s (since %s)\n"+
		"* Last received %s (%s ago)\n* Airspace %d bytes\n* Messages per station:-\n%s\n",
		nMessages, nMessages-nNewMessages, nNewMessages,
		time.Since(startTime), startTime,
		lastMessage, time.Since(lastMessage), sizeAirspace, stationStr)))
}


func nowHandler(w http.ResponseWriter, r *http.Request) {
	a := airspace.Airspace{}

	if err := a.FromMemcache(); err != nil {
		w.Write([]byte(fmt.Sprintf("not OK: fetch fail: %v\n", err)))
		return
	}

	w.Write([]byte(fmt.Sprintf("OK\n * Airspace\n%s\n", a)))
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))	
}
