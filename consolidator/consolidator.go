// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.

// It should be deployed as a Managed VM AppEngine App, as per the .yaml file:
//   $ aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse

// You may need to deploy some datastore indices, too:
//   $ aedeploy gcloud preview app deploy ./index.yaml --promote --bucket gs://gcloud-arse

// You can it also run it locally ...
//   $ go run consolidator.go -ae=false -input=testing    [attaches to a testing topic]
//   $ go run consolidator.go -ae=false                   [attaches to prod topic, but with new subscription]


package main

// {{{ import()

import (
	"io/ioutil"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"net/http"
	"net/url"
	"sort"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/appengine"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	fdb "github.com/skypies/flightdb2"
	"github.com/skypies/flightdb2/fgae"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/util/metrics"
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

	http.HandleFunc("/", statusHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/reset", resetHandler)
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
		fPubsubSubscription += "-DEV"
		pubsub.Setup(ctx, fPubsubInputTopic, fPubsubSubscription, fPubsubOutputTopic)
		pubsub.DeleteSub(ctx, fPubsubSubscription)
		pubsub.CreateSub(ctx, fPubsubSubscription, fPubsubInputTopic)
	}

	Log.Printf("(setup)\n")
}

// }}}
// {{{ getContexts

// This is fiddly.

// If we're on appengine, use their special background context; we
// need it to talk to appengine services such as Memcache. If we're
// not on appengine, https://metadata/ won't exist and so we can't
// use the appengine context; but any old context will do.

func getBaseContext() context.Context {
	ctx := context.TODO()
	if fOnAppEngine {
		ctx = appengine.BackgroundContext()
	}
	return ctx
}

// But, pubsub needs a 'cloud context' with ACLs, but that is no
// longer considered the 'AppEngine context' that memcache needs. So
// we need to return two contexts.
func getContexts() (context.Context, context.Context)  {
	memcacheCtx := getBaseContext()
	pubsubCtx := pubsub.WrapContext(fProjectName, getBaseContext())
	return memcacheCtx,pubsubCtx
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

	// For now, don't persist any MLAT data
	// :O   if msgs[0].DataSystem() != "ADSB" { return }

	frag := fdb.MessagesToTrackFragment(msgs)
	db := fgae.FlightDB{C:appengine.BackgroundContext()}

	if err := db.AddTrackFragment(frag); err != nil {
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
// {{{ statusHandler

func statusHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_output"}
	str := <-vitalsResponseChan
	w.Write([]byte(fmt.Sprintf("OK\n%s", str)))
}

// }}}
// {{{ resetHandler

func resetHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_reset"}
	w.Write([]byte(fmt.Sprintf("OK\n")))
}

// }}}

// {{{ trackVitals

// These two channels are accessible from all goroutines
var vitalsRequestChan = make(chan VitalsRequest, 40)
var vitalsResponseChan = make(chan string, 5)  // Only used for stats output

type VitalsRequest struct {
	Name             string  // _output, _reset, or _update
	Str,Str2         string
	I,J,K,L,M,N,O,P  int64
	T                time.Time
}

type ReceiverSummary struct {
	NumMessagesSent int64
	NumBundlesSent  int64
	LastBundleTime  time.Time
}

func trackVitals() {
	startupTime := time.Now().Round(time.Second)
	strings := map[string]string{}
	workers := map[string]int64{}
	counters := map[string]int64{}
	receivers := map[string]ReceiverSummary{}
	m := metrics.NewMetrics()
	
	for {
		if weAreDone() { return }

		select {
		case req := <-vitalsRequestChan:

			if req.Name == "_reset" {
				counters = map[string]int64{}
				receivers = map[string]ReceiverSummary{}
				startupTime = time.Now().Round(time.Second)

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

				// Update metrics
				m.RecordValue("BundleSize", req.I)
				m.RecordValue("NumSleeps", req.L)
				m.RecordValue("PullMillis", req.M)
				m.RecordValue("ToQueueMillis", req.N)
				m.RecordValue("MemcacheMillis", req.O)
				m.RecordValue("PreludeMillis", req.P)

			} else if req.Name == "_flush" {
				m.RecordValue("DBWriteMillis", req.I)
				workers[fmt.Sprintf("%03d",req.J)]++
				counters["nFrags"]++
				
			} else if req.Name == "_output" {
				rcvrs := ""
				keys := []string{}
				for k,_ := range receivers { keys = append(keys,k) }
				sort.Strings(keys)
				for _,k := range keys {
					v := receivers[k]
					rcvrs += fmt.Sprintf(
						"    %-15.15s: %7d msgs, %7d bundles, last %.1f s\n",
						k, v.NumMessagesSent, v.NumBundlesSent, time.Since(v.LastBundleTime).Seconds())
				}

				workersStr := ""
				keys = []string{}
				for k,_ := range workers { keys = append(keys,k) }
				sort.Strings(keys)
				for _,k := range keys {
					workersStr += fmt.Sprintf("    %s  %9d\n", k, workers[k])
				}

				str := fmt.Sprintf(
						"* %d messages (%d dupes; %d total; %d bundles received, %d frags written)\n"+
						"* Uptime: %s (started %s)\n"+
						"* Preload: %s\n"+
						"\n"+
						"* Receivers:-\n%s\n"+
						"* Workers:-\n%s\n"+
						"* Metrics:-\n%s\n"+
						"* Airspace (%s bytes, incl. deduping):-\n%s\n",
					counters["nNew"], counters["nDupes"], counters["nAll"], counters["nBundles"], counters["nFrags"],
					time.Second * time.Duration(int(time.Since(startupTime).Seconds())), startupTime,
					strings["loadedOnStartup"],
					rcvrs,
					workersStr,
					m.String(),
					strings["airspaceSize"],
					strings["airspaceText"])

				vitalsResponseChan <- str

			} else if req.Name != "" {
				if req.Str != "" { strings[req.Name] = req.Str }
			}
		}
	}
}

// }}}
// {{{ pullNewFromPubsub

func pullNewFromPubsub(msgsOut chan<- []*adsb.CompositeMsg) {
	memcacheCtx,pubsubCtx := getContexts()
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
			loadedOnStartup = fmt.Sprintf("loaded sigs=(%d,%d), aircraft=%d, age=%s",
				len(as.Signatures.CurrMsgs),len(as.Signatures.PrevMsgs),
				len(as.Aircraft), as.Youngest())
		}
		vitalsRequestChan<- VitalsRequest{Name: "loadedOnStartup", Str:loadedOnStartup}
	}

	Log.Printf("(now pulling from %s)", fPubsubSubscription)

	tPrevEnd := time.Now()

	for {
		if weAreDone() { break }
		tStart := time.Now()
		nSleeps := 0
		msgs,err := pubsub.Pull(pubsubCtx, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			time.Sleep(time.Second * 10)

			// Pubsub failures have a habit of screwing up the context, such that it says
			//  "Call error 8: There is no active request context for this API call"
			// which (possibly) makes subsequent API calls all die - and be slow - and wedge up
			// the process. So, generate some fresh ones.
			memcacheCtx,pubsubCtx = getContexts()
			continue

		} else if len(msgs) == 0 {
			time.Sleep(time.Millisecond * 200)
			nSleeps++
			continue
		}

		// Short cut data we can't handle right now
		if len(msgs) > 0 && msgs[0].ReceiverName == "CulverCity" {
			continue
		}

		tPullDone := time.Now()
		tMsgsSent := tPullDone
		newMsgs := as.MaybeUpdate(msgs)
		
		if len(newMsgs) > 0 {
			// Pass them to the other goroutine for dissemination, and get back to business.
			msgsOut <- newMsgs
			tMsgsSent = time.Now()

			if fOnAppEngine {
				if err := as.JustAircraftToMemcache(memcacheCtx); err != nil {
					Log.Printf("main/JustAircraftToMemcache: err: %v", err)
				}
			} else {
				Log.Printf("- %2d were new (%2d already seen) - %s",
					len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
			}
		}
		tMemcacheDone := time.Now()

		// Update our vital stats, with info about this bundle
		airspaceBytes,_ := as.ToBytes()
		vitalsRequestChan<- VitalsRequest{
			Name: "_update",
			Str:msgs[0].ReceiverName,
			I:int64(len(msgs)),
			J:int64(len(newMsgs)),

			// Some primitive wait state data (latencies in millis)
			L:int64(nSleeps),
			M:(tPullDone.Sub(tStart).Nanoseconds() / 1000000),
			N:(tMsgsSent.Sub(tPullDone).Nanoseconds() / 1000000),
			O:(tMemcacheDone.Sub(tMsgsSent).Nanoseconds() / 1000000),
			P:(tStart.Sub(tPrevEnd).Nanoseconds() / 1000000),  // prelude/zombie time

			Str2: as.String(),
			K: int64(len(airspaceBytes)),
			T: msgs[len(msgs)-1].GeneratedTimestampUTC,
		}
		tPrevEnd = time.Now()
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

func bufferTracks(msgsIn <-chan []*adsb.CompositeMsg, msgsOut chan<- []*adsb.CompositeMsg) {	
	// Our primary piece of state ! It groups msgs into tracks for distinct aircraft, and
	// then flushes out individual tracks as/when they have data older than MaxAge
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
			tb.Flush(msgsOut)
			//if fPubsubOutputTopic != "" {} // If we ever wanted to publish a consolidated stream, start here
		}
			
		if weAreDone() { break }
	}
	Log.Printf(" -- bufferTracks clean exit\n")
}

// }}}
// {{{ workerDispatch

func workerDispatch(msgsIn <-chan []*adsb.CompositeMsg, workersOut []chan []*adsb.CompositeMsg) {
	for {
		select {
		case <-time.After(time.Second):
		case msgs := <-msgsIn:
			// pick a worker, based on the icao24; it's important that we don't process frags for the same icaoid
			// in parallel, or we'll suffer a write-write conflict and overwrite some data.
			h := fnv.New32()
			h.Write([]byte(msgs[0].Icao24))
			i := h.Sum32()
			workerId := i % uint32(len(workersOut))

			// Log.Printf("{%s} -> %20.20d (%% %d) -> %6d", string(msgs[0].Icao24), i, len(workersOut), workerId)

			// Send the message frag to the worker.
			workersOut[workerId] <- msgs
		}

		if weAreDone() { break }
	}
	Log.Printf(" -- workerDispatch clean exit\n")
}

// }}}
// {{{ flushTracks

// The worker bee function
func flushTracks(myId int, msgsIn <-chan []*adsb.CompositeMsg) {
	Log.Printf("(flushTracks/%03d starting)\n", myId)

	for {
		select {
		case <-time.After(time.Second):
		case msgs := <-msgsIn:
			tStart := time.Now()
			if fOnAppEngine {
				flushTrackToDatastore(msgs)
			} else if fTrackPostURL != "" {
				flushTrackToPOST(msgs)
			// } else {
			//	Log.Printf(" -- (%d pushed for %s)\n", len(msgs), string(msgs[0].Icao24))
			}

			vitalsRequestChan<- VitalsRequest{
				Name: "_flush",
				I:(time.Since(tStart).Nanoseconds() / 1000000),
				J:int64(myId),
			}
		}
			
		if weAreDone() { break }
	}

	Log.Printf(" -- flushTracks/%03d clean exit\n", myId)
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

	msgChan1 := make(chan []*adsb.CompositeMsg, 3)
	msgChan2 := make(chan []*adsb.CompositeMsg, 3)
	workerChans := []chan []*adsb.CompositeMsg{}

	nWorkers := 8 // avoid being blocked on DB writes
	for i:=0; i<nWorkers; i++ {
		workerChan := make(chan []*adsb.CompositeMsg, 3)
		workerChans = append(workerChans, workerChan)
		go flushTracks(i, workerChan)          // worker bee, write per-flight fragments to disc
	}

	go pullNewFromPubsub(msgChan1)           // sends mixed bundles down chan1
	go bufferTracks(msgChan1, msgChan2)      // ... sorts messages into per-flight frags, sends them down chan2 ...
	go workerDispatch(msgChan2, workerChans) // ... takes a per-flight frag, picks a workerChan to handle it

	// Manage vital statistics in a thread safe way
	go trackVitals()
	
	// Now do basically nothing in the main thread
	if fOnAppEngine {
		appengine.Main()		
	} else {
		go func(){ Log.Fatalf("locallisten: %v\n", http.ListenAndServe(":8081", nil)) }()

		// Block until done channel lights up
		<-done
		time.Sleep(time.Second * 20)  // Give the pubsub loop a chance to unblock and exit
		Log.Printf("(main clean exit)\n")
	}
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
