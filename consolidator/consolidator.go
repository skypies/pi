// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.

// It should be deployed as a Managed VM AppEngine App, as per the .yaml file:
//   $ aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse

// You may need to deploy some datastore indices, too:
//   $ aedeploy gcloud preview app deploy ./index.yaml --promote --bucket gs://gcloud-arse

// You can it also run it locally, without interfering with the prod subscriptions:
//   $ go run consolidator.go -ae=false

// TODO: proper shutdown (incl. serialization of airspace deduping buffers into datastore)

package main

import (
	"io/ioutil"
	"flag"
	"fmt"
	"log"
	"os"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/pi/pubsub"

	fdb "github.com/skypies/flightdb2"
	"github.com/skypies/flightdb2/fgae"
)

// {{{ globals

var (
	// Command line flags
	fProjectName           string
	fPubsubInputTopic      string
	fPubsubSubscription    string
	fPubsubOutputTopic     string
	fOnAppEngine           bool
	fTrackPostURL          string
	
	Log                   *log.Logger

	// Junky globals for basic status check
	startTime time.Time
	sizeAirspace int
	stringAirspace string
	nMessages int
	nNewMessages int
	lastMessage time.Time
	receivers map[string]int
	receiversLastSeen map[string]time.Time
)

// }}}
// {{{ init

func init() {
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)

	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubInputTopic, "input", "adsb-inbound",
		"Name of the pubsub topic we read from (i.e. add our subscription to)")
	flag.StringVar(&fPubsubSubscription, "sub", "consolidator",
		"Name of the pubsub subscription on the adsb-inbound topic")
	flag.StringVar(&fPubsubOutputTopic, "output", "adsb-consolidated",
		"Name of the pubsub topic to post deduped output on (short name, not projects/blah...)")
	flag.StringVar(&fTrackPostURL, "trackpost", "http://localhost:8080/fdb/add-frag", "test only")
	flag.BoolVar(&fOnAppEngine, "ae", true, "on appengine (use http://metadata/ etc)")
	flag.Parse()
}

// }}}
// {{{ setup

// Can't call this until we've got some kind of context
func setup(ctx context.Context) {
	// Init hacky stats globals
	startTime = time.Now()
	receivers = map[string]int{}
	receiversLastSeen = map[string]time.Time{}
	
	if fOnAppEngine {
		pubsub.Setup(ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
	} else {
		fPubsubSubscription += "DEV"
		pubsub.Setup(ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
		pubsub.PurgeSub(ctx, fPubsubSubscription, "adsb-inbound")
	}

	http.HandleFunc("/", statsHandler)
	http.HandleFunc("/_ah/start", startHandler)
	http.HandleFunc("/_ah/stop", stopHandler)
	Log.Printf("(setup)\n")
}

// }}}
// {{{ flushTrackToPOST

func flushTrackToPOST(msgs []*adsb.CompositeMsg) {
	if len(msgs) == 0 {
		fmt.Printf("No messages to post!\n")
		return
	}
	
	msgsBase64,_ := adsb.Base64EncodeMessages(msgs)
	debugUrl := fTrackPostURL + "?callsign=" + msgs[0].Callsign
	resp,err := http.PostForm(debugUrl, url.Values{ "msgs": {msgsBase64} })
	if err != nil {
		fmt.Printf("flushPost/POST: %v\n", err)
		return
	}

	body,_ := ioutil.ReadAll(resp.Body)		
	fmt.Printf(" ------------> %s",body)
	//defer resp.Body.Close()
}

// }}}
// {{{ flushTrackToDatastore

func flushTrackToDatastore(msgs []*adsb.CompositeMsg) {
	if len(msgs) == 0 {
		return
	}
	frag := fdb.MessagesToADSBTrackFragment(msgs)
	db := fgae.FlightDB{C:appengine.BackgroundContext()}

	if err := db.AddADSBTrackFragment(frag); err != nil {
		fmt.Printf("flushPost/ToDatastore: %v\n", err)
	}
}

// }}}

// {{{ storeMessagesByAircraftMainloop

type MsgChanItem struct {
	msgs []*adsb.CompositeMsg
}

func storeMessagesByAircraftMainloop(msgChan chan MsgChanItem) {
	tb := trackbuffer.NewTrackBuffer()
	
	flushFunc := func(msgs []*adsb.CompositeMsg) {		
		for i,m := range msgs {
			fmt.Printf(" [%2d] %s\n", i, m)
		}
		fmt.Printf(" --\n")
	}

	if fTrackPostURL != "" {
		flushFunc = flushTrackToPOST
	}

	if fOnAppEngine {
		flushFunc = flushTrackToDatastore
	}
	
	for item := range msgChan {
		for _,m := range item.msgs {
			tb.AddMessage(m)
		}
		
		tb.Flush(flushFunc)

		//if fPubsubOutputTopic != "" {}
	}
}

// }}}
// {{{ getMessagesMainloop

func getMessagesMainloop(suppliedCtx context.Context) {
	Log.Printf("(getMessages starting)")

	// Be careful about contexts. For memcache, we need 'an App Engine context', which is what
	// we're passed in. For pubsub, we need to derive a 'Cloud context' (this derived context is
	// no longer considered 'an App Engine context').
	memcacheCtx := suppliedCtx
	pubsubCtx := pubsub.WrapContext(fProjectName, suppliedCtx)
	setup(pubsubCtx)
	
	airspace := airspace.Airspace{}
	msgChan := make(chan MsgChanItem, 3)
	go storeMessagesByAircraftMainloop(msgChan)

	for {
		msgs,err := pubsub.Pull(pubsubCtx, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			time.Sleep(time.Second * 10)
			continue

		} else if len(msgs) == 0 {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		newMsgs := airspace.MaybeUpdate(msgs)

		// Update crappy globals
		nMessages += len(msgs)
		nNewMessages += len(newMsgs)
		lastMessage = time.Now()
		if b,err := airspace.ToBytes(); err == nil { sizeAirspace = len(b) }
		stringAirspace = airspace.String()
		for _,m := range msgs { receivers[m.ReceiverName]++ }
		receiversLastSeen[msgs[0].ReceiverName] = time.Now()

		// Update the memcache, and send them off down the channel
		if len(newMsgs) > 0 {
			if fOnAppEngine {
				if err := airspace.ToMemcache(memcacheCtx); err != nil {
					Log.Printf("main/ToMemcache: err: %v", err)
				}
			} else {
				Log.Printf("- %2d were new (%2d already seen) - %s",
					len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
			}

			// Pass them to the other goroutine for dissemination, and get back to busines.
			msgChan <- MsgChanItem{msgs: newMsgs}
		}
	}

	Log.Printf("Final clean shutdown")
}

// }}}

// {{{ main

func main() {
	Log.Printf("(main)\n")

	// If we're on appengine, use their special background context; else
	// we can't talk to appengine services such as Memcache. If we're
	// not on appengine, https://metadata/ won't exist and so we can't
	// sue that context; but any old context will do.
	ctx := context.TODO()
	if fOnAppEngine {
		ctx = appengine.BackgroundContext()
	}
	
	if fOnAppEngine {
		go func(){
			getMessagesMainloop(ctx)
		}()
		appengine.Main()
		
	} else {
/*
		go func(){
			if err := http.ListenAndServe(":8081", nil); err != nil {Log.Fatalf("http server: %v", err)}
		}()
*/
		getMessagesMainloop(ctx)
	}
}

// }}}

// {{{ startHandler

func startHandler(w http.ResponseWriter, r *http.Request) {

	ctx1 := appengine.BackgroundContext()
	ctx2 := appengine.NewContext(r)

	fmt.Printf("(startHandler)\n")
	fmt.Printf("ctx1=%s\nctx2=%s\n", ctx1, ctx2)

	w.Write([]byte("OK"))	
}

// }}}
// {{{ stopHandler

func stopHandler(w http.ResponseWriter, r *http.Request) {
	ctx1 := appengine.BackgroundContext()
	// ctx2 := appengine.NewContext(r)

	fmt.Printf("(stopHandler)\n")
	fmt.Printf("ctx1=%s\n", ctx1)

	// Shut down the pubsub goroutine (which should save airspace into datastore.)
	
	w.Write([]byte("OK"))	
}

// }}}
// {{{ statsHandler

func statsHandler(w http.ResponseWriter, r *http.Request) {
	receiversStr := ""
	for k,v := range receivers {
		receiversStr += fmt.Sprintf("    %-20.20s: %7d (last update: %.1f seconds ago)\n",
			k, v, time.Since(receiversLastSeen[k]).Seconds())
	}

	w.Write([]byte(fmt.Sprintf("OK\n* %d messages (%d dupe, %d new)\n"+
		"* Up since %s (%s)\n"+
		"* Last update %.1f seconds ago\n\n"+
		"* Receivers:-\n%s\n"+
		"* Airspace (%d bytes):-\n%s\n",
		nMessages, nMessages-nNewMessages, nNewMessages,
		startTime, time.Since(startTime),
		time.Since(lastMessage).Seconds(),
		receiversStr,
		sizeAirspace, stringAirspace)))
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
