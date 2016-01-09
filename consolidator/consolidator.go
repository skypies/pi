// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.

// It should be deployed as a Managed VM AppEngine App, as per the .yaml file:
//   $ aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse

// You may need to deploy some datastore indices, too:
//   $ aedeploy gcloud preview app deploy ./index.yaml --promote --bucket gs://gcloud-arse

// You can it also run it locally, without interfering with the prod subscriptions:
//   $ go run consolidator.go -ae=false

package main

// {{{ import()

import (
	"io/ioutil"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	fdb "github.com/skypies/flightdb2"
	"github.com/skypies/flightdb2/fgae"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/util/pubsub"
)

// }}}
// {{{ var()

var (
	// Command line flags
	fProjectName           string
	fPubsubInputTopic      string
	fPubsubSubscription    string
	fPubsubOutputTopic     string
	fOnAppEngine           bool
	fTrackPostURL          string
	
	Log                   *log.Logger
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
	flag.StringVar(&fTrackPostURL, "trackpost", "", "e.g. http://localhost:8080/fdb/add-frag")
	flag.BoolVar(&fOnAppEngine, "ae", true, "on appengine (use http://metadata/ etc)")
	flag.Parse()

	http.HandleFunc("/", statsHandler)
	http.HandleFunc("/_ah/start", startHandler)
	http.HandleFunc("/_ah/stop", stopHandler)

	addSIGINTHandler()
}

// }}}
// {{{ setup

// Can't call this until we've got some kind of context
func setup(ctx context.Context) {	
	if fOnAppEngine {
		pubsub.Setup(ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
	} else {
		fPubsubSubscription += "DEV"
		pubsub.Setup(ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
		pubsub.DeleteSub(ctx, fPubsubSubscription)
		pubsub.CreateSub(ctx, fPubsubSubscription, "adsb-inbound")
	}

	Log.Printf("(setup)\n")
}

// }}}

// {{{ flushTrackToPOST

func flushTrackToPOST(msgs []*adsb.CompositeMsg) {
	if len(msgs) == 0 {
		Log.Printf("No messages to post!\n")
		return
	}
	
	msgsBase64,_ := adsb.Base64EncodeMessages(msgs)
	debugUrl := fTrackPostURL + "?callsign=" + msgs[0].Callsign
	resp,err := http.PostForm(debugUrl, url.Values{ "msgs": {msgsBase64} })
	if err != nil {
		Log.Printf("flushPost/POST: %v\n", err)
		return
	}

	body,_ := ioutil.ReadAll(resp.Body)		
	Log.Printf(" ------------> %s",body)
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
		Log.Printf("flushPost/ToDatastore: %v\n", err)
	}
}

// }}}

// {{{ startHandler

func startHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("(startHandler)\n")
	w.Write([]byte("OK"))	
}

// }}}
// {{{ stopHandler

func stopHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("(stopHandler)\n")
	close(done) // Shut down the pubsub goroutine (which should save airspace into datastore.)
	w.Write([]byte("OK"))	
}

// When running a local instance ...
func addSIGINTHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(sig <-chan os.Signal){
		<-sig
		Log.Printf("(SIGINT received)\n")
		close(done)
	}(c)
}

// }}}
// {{{ statsHandler

func statsHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_output"}
	str := <-vitalsResponseChan
	w.Write([]byte(fmt.Sprintf("OK\n%s", str)))
}

// }}}

// {{{ trackVitals

// These two channels are accessible from all goroutines
var vitalsRequestChan = make(chan VitalsRequest, 40)
var vitalsResponseChan = make(chan string, 5)  // Only used for stats output

type VitalsRequest struct {
	Name      string  // _output, _reset, or _update
	Str,Str2  string
	I,J,K     int64
	T         time.Time
}

type ReceiverSummary struct {
	NumMessagesSent int64
	NumBundlesSent  int64
	LastBundleTime  time.Time
}

func trackVitals() {
	startupTime := time.Now().Round(time.Second)
	strings := map[string]string{}
	counters := map[string]int64{}
	receivers := map[string]ReceiverSummary{}
	
	for {
		if weAreDone() { return }

		select {
		case req := <-vitalsRequestChan:

			if req.Name == "_reset" {
				counters = map[string]int64{}
				receivers = map[string]ReceiverSummary{}

			} else if req.Name == "_update" {
				// This represents "we received a bundle", and we update according to the LastMsg
				s := receivers[req.Str]
				s.NumBundlesSent += 1
				s.NumMessagesSent += req.I
				s.LastBundleTime = req.T
				receivers[req.Str] = s
				counters["nBundles"] += 1
				counters["nAll"] += req.I
				counters["nNew"] += req.J
				counters["nDupes"] += (req.I - req.J)
				strings["airspaceSize"] = fmt.Sprintf("%d", req.K)
				strings["airspaceText"] = req.Str2

			} else if req.Name == "_output" {
				rcvrs := ""
				for k,v := range receivers {
					rcvrs += fmt.Sprintf(
						"    %-20.20s: %7d msgs (%7d bundles) (last update: %.1f seconds ago)\n",
						k, v.NumMessagesSent, v.NumBundlesSent, time.Since(v.LastBundleTime).Seconds())
				}
				str := fmt.Sprintf(
						"* %d messages received (%d dupes; %d messages in total; %d bundles)\n"+
						"* Uptime: %s (started %s)\n"+
						"* Preload: %s\n"+
						"\n"+
						"* Receivers:-\n%s\n"+
						"* Airspace (%s bytes, incl. deduping):-\n%s\n",
					counters["nNew"], counters["nDupes"], counters["nAll"], counters["nBundles"],
					time.Second * time.Duration(int(time.Since(startupTime).Seconds())), startupTime,
					strings["loadedOnStartup"],
					rcvrs, strings["airspaceSize"], strings["airspaceText"])

				vitalsResponseChan <- str

			} else if req.Name != "" {
				if req.Str != "" { strings[req.Name] = req.Str }
			}
		}
	}
}

// }}}
// {{{ pullNewFromPubsub

func pullNewFromPubsub(suppliedCtx context.Context, msgsOut chan<- []*adsb.CompositeMsg) {
	// Be careful about contexts. For memcache, we need 'an App Engine context', which is what
	// we're passed in. For pubsub, we need to derive a 'Cloud context' (this derived context is
	// no longer considered 'an App Engine context').
	memcacheCtx := suppliedCtx
	pubsubCtx := pubsub.WrapContext(fProjectName, suppliedCtx)
	setup(pubsubCtx)

	// Setup our primary piece of state: the airspace. Load it from last time if we can.
	as := airspace.Airspace{}
	if fOnAppEngine {
		loadedOnStartup := ""
		if err := as.EverythingFromMemcache(memcacheCtx); err != nil {
			Log.Printf("airspace.EverythingFromMemcache: %v", err)
			loadedOnStartup = fmt.Sprintf("error loading: %v", err)
			as = airspace.Airspace{}
		} else {
			loadedOnStartup = fmt.Sprintf("loaded curr=%d (prev=%d), aircraft=%d",
				len(as.Signatures.CurrMsgs),len(as.Signatures.PrevMsgs),
				len(as.Aircraft))
		}
		vitalsRequestChan<- VitalsRequest{Name: "loadedOnStartup", Str:loadedOnStartup}
	}
	
	for {
		if weAreDone() { break }
		msgs,err := pubsub.Pull(pubsubCtx, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			time.Sleep(time.Second * 10)
			continue

		} else if len(msgs) == 0 {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		newMsgs := as.MaybeUpdate(msgs)

		// Update our vital stats, with the bundle
		airspaceBytes,_ := as.ToBytes()
		vitalsRequestChan<- VitalsRequest{
			Name: "_update",
			Str:msgs[0].ReceiverName,
			I:int64(len(msgs)),
			J:int64(len(newMsgs)),
			Str2: as.String(),
			K: int64(len(airspaceBytes)),
			T: msgs[len(msgs)-1].GeneratedTimestampUTC,
		}
		
		if len(newMsgs) > 0 {
			// Pass them to the other goroutine for dissemination, and get back to business.
			msgsOut <- newMsgs

			if fOnAppEngine {
				if err := as.JustAircraftToMemcache(memcacheCtx); err != nil {
					Log.Printf("main/JustAircraftToMemcache: err: %v", err)
				}
			} else {
				Log.Printf("- %2d were new (%2d already seen) - %s",
					len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
			}
		}
	}

	if fOnAppEngine {
		// We're shutting down, so save all the deduping signatures
		if err := as.EverythingToMemcache(memcacheCtx); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; memcache: %v", err)
		}

	} else {
		// We're not on AppEngine; this is a wasteful subscription
		if err := pubsub.DeleteSub(pubsubCtx, fPubsubSubscription); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; del '%s': %v", fPubsubSubscription, err)
		}			
	}
	
	Log.Printf(" -- pullNewFromPubsub clean exit\n")
}

// }}}
// {{{ bufferTracks

func bufferTracks(c context.Context, msgsIn <-chan []*adsb.CompositeMsg) {	
	flushFunc := flushTrackToDatastore
	if !fOnAppEngine {
		if fTrackPostURL != "" {
			flushFunc = flushTrackToPOST
		} else {
			flushFunc = func(msgs []*adsb.CompositeMsg) {		
				// for i,m := range msgs { fmt.Printf(" [%2d] %s\n", i, m) }
				fmt.Printf(" -- (%d pushed for %s)\n", len(msgs), string(msgs[0].Icao24))
			}
		}
	}

	// Our primary piece of state !
	tb := trackbuffer.NewTrackBuffer()
	//tb.MaxAge = time.Second*300

	for {
		select {
		case <-time.After(time.Second):
		case msgs := <-msgsIn:
			//Log.Printf("buffertracks: received %d msgs\n", len(msgs))
			for _,m := range msgs {
				tb.AddMessage(m)
			}
			tb.Flush(flushFunc)
			//if fPubsubOutputTopic != "" {}
		}
			
		if weAreDone() { break }
	}
	Log.Printf(" -- bufferTracks clean exit\n")
}

// }}}

// {{{ main

var done = make(chan struct{}) // Gets closed when everything is done
func weAreDone() bool {
	select{
	case <-done:
		return true
	default:
		return false
	}
}

func main() {
	Log.Printf("(main)\n")

	// If we're on appengine, use their special background context; else
	// we can't talk to appengine services such as Memcache. If we're
	// not on appengine, https://metadata/ won't exist and so we can't
	// use the appengine context; but any old context will do.
	ctx := context.TODO()
	if fOnAppEngine {
		ctx = appengine.BackgroundContext()
	}

	// Do all the work (get new messages from pubsub; buffer up tracks) in a two-step pipeline
	msgChan := make(chan []*adsb.CompositeMsg, 3)
	go pullNewFromPubsub(ctx, msgChan)
	go bufferTracks(ctx, msgChan)

	// Manage vital statistics in a thread safe way
	go trackVitals()
	
	// Now do basically nothing in the main thread
	if fOnAppEngine {
		appengine.Main()		
	} else {
		go func(){ Log.Fatalf("locallisten: %v\n", http.ListenAndServe(":8081", nil)) }()

		// Block until done channel lights up
		<-done
		time.Sleep(time.Second * 2)
		Log.Printf("(main clean exit)\n")
	}
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
