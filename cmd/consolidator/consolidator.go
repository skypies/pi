// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.

// It should be deployed as an AppEngine Flexible App, as per the app.yaml file:
//   $ gcloud app deploy
//   https://cloud.google.com/appengine/docs/flexible/go/testing-and-deploying-your-app

// If data is written to datastore, you may need to deploy some datastore indices, too:
//   $ gcloud app deploy ./index.yaml --promote

// When running in the cloud, these oneliners might be handy:
//   $ gcloud app logs read -s consolidator
//   $ curl -s fdb.serfr1.org/con/stack | pp -force-color -parse=false -aggressive

// You can it also run it locally with the appengine=false flag:
//   $ go run consolidator.go -ae=false -input=testing    [attaches to a testing topic]
//   $ go run consolidator.go -ae=false                   [prod topic, but with new subscription]

package main

// {{{ import()

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	
	"golang.org/x/net/context"

	// Deprecated: https://cloud.google.com/appengine/docs/flexible/go/upgrading
	//"google.golang.org/appengine"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	fdb "github.com/skypies/flightdb"
	"github.com/skypies/flightdb/fgae"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/util/dsprovider"
	//"github.com/skypies/util/gaeutil"
	"github.com/skypies/util/histogram"
	"github.com/skypies/util/metrics"
	mypubsub "github.com/skypies/util/pubsub" // This is adding less value over time; kill ?
)

// }}}
// {{{ var()

var (
	// Command line flags
	fProjectName           string
	fPubsubInputTopic      string
	fPubsubSubscription    string
	fOnAppEngine           bool

	tGlobalStart           time.Time
	stackTraceBytes      []byte

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
	flag.BoolVar(&fOnAppEngine, "ae", true, "on appengine (use http://metadata/ etc)")
	flag.Parse()

	http.HandleFunc("/", statusHandler)
	http.HandleFunc("/con/status", statusHandler)
	http.HandleFunc("/con/stack", stackTraceHandler)
	http.HandleFunc("/con/reset", resetHandler)

	// https://github.com/GoogleCloudPlatform/golang-samples
	http.HandleFunc("/_ah/start", startHandler)
	http.HandleFunc("/_ah/stop", stopHandler)
	http.HandleFunc("/_ah/health", healthCheckHandler)

	tGlobalStart = time.Now()

	addSIGINTHandler()
}

// }}}
// {{{ setupPubsub

func setupPubsub() {		
	ctx := getContext()
	pc := mypubsub.NewClient(ctx, fProjectName)

	if fOnAppEngine {
		mypubsub.Setup(ctx, pc, fPubsubInputTopic, fPubsubSubscription)
	} else {
		fPubsubSubscription += "-DEV"
		mypubsub.Setup(ctx, pc, fPubsubInputTopic, fPubsubSubscription)
		mypubsub.DeleteSub(ctx, pc, fPubsubSubscription)
		mypubsub.CreateSub(ctx, pc, fPubsubSubscription, fPubsubInputTopic)
	}
}

// }}}

// {{{ {start,stop,healthCheck,status,reset,stackTrace}Handler

func startHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("(startHandler)\n")
	w.Write([]byte("OK"))	
}

func getStackTraceBytes() []byte {
	bytes := make([]byte, 256000)
	n := runtime.Stack(bytes, true)
	return bytes[:n]
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	if weAreDone() {
		fmt.Printf("(stopHandler, already stopping)\n")
	} else {
		fmt.Printf("(stopHandler, after %s)\n", time.Since(tGlobalStart))
		close(done) // Shut down the pubsub goroutine (which should save airspace into datastore.)
		fmt.Printf("\nFinal post-close stack trace:-\n\n%s\n", getStackTraceBytes())
	}
	
	w.Write([]byte("OK"))	
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
        fmt.Fprint(w, "ok")
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_output"}
	vr := <-vitalsResponseChan
	w.Write([]byte(fmt.Sprintf("OK\n%s", vr.Str)))
}

func stackTraceHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write(getStackTraceBytes())
}

func resetHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_reset"}
	w.Write([]byte(fmt.Sprintf("OK\n")))
}

// }}}

// {{{ weAreDone

var done = make(chan struct{}) // Gets closed when everything is done
func weAreDone() bool {
	select{
	case <-done:
		return true
	default:
		return false
	}
}

// When running a local instance ...
func addSIGINTHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(sig <-chan os.Signal){
		<-sig
		Log.Printf("(SIGINT received)\n")
		//Log.Printf("Final stack trace:-\n\n%s\n", getStackTraceBytes())
		close(done)
	}(c)
}

// }}}
// {{{ getContext

// If we're on appengine, use their special background context; we
// need it to talk to appengine services such as Memcache. If we're
// not on appengine, https://metadata/ won't exist and so we can't use
// the appengine context; but in that case any old context will do.

func getContext() context.Context {
	return context.Background()
/*
	if fOnAppEngine {
		return appengine.BackgroundContext()
	}
	return context.TODO()*/
}

// }}}
// {{{ flushTrackToDatastore

func flushTrackToDatastore(myId int, msgs []*adsb.CompositeMsg) {
	if ! fOnAppEngine {
		//Log.Printf(" -- (%d pushed for %s)\n", len(msgs), string(msgs[0].Icao24))
		vitalsRequestChan<- VitalsRequest{Name: "_dbwrite", J:int64(myId)}
		return
	}

	tStart := time.Now()

	db := fgae.NewDB(getContext())
	db.Backend = dsprovider.CloudDSProvider{fProjectName, nil}

	frag := fdb.MessagesToTrackFragment(msgs)

	/*
	if err := db.AddTrackFragment(frag); err != nil {
		Log.Printf("flushPost/ToDatastore: err: %v\n--\n", err)
	}
*/
	vitalsRequestChan<- VitalsRequest{
		Name: "_dbwrite",
		I:(time.Since(tStart).Nanoseconds() / 1000000),
		J:int64(myId),
	}
}

// }}}

// {{{ trackVitals

// These two channels are accessible from all goroutines
var vitalsRequestChan = make(chan VitalsRequest, 40)
var vitalsResponseChan = make(chan VitalsResponse, 5)  // Only used for stats output

var nCallbackStarts = 0
var nCallbackEnds = 0

type VitalsRequest struct {
	Name             string  // _blah
	Str,Str2         string
	I,J,K,L,M,N,O,P  int64
	T                time.Time
}

type VitalsResponse struct {
	Str string
	I int64
	T time.Time
}

type ReceiverSummary struct {
	NumMessagesSent int64
	NumBundlesSent  int64
	LastBundleTime  time.Time
}

func memStats() string {
	ms := runtime.MemStats{}

	runtime.ReadMemStats(&ms)
	return fmt.Sprintf("go:% 5d(% 4d cb); heap:% 13d, % 13d; stack:% 13d",
		runtime.NumGoroutine(), nReceiveCallbacks,
		ms.HeapObjects, ms.HeapAlloc, ms.StackInuse)
}

func trackVitals() {
	startupTime := time.Now().Round(time.Second)
	lastBundleTime := time.Now()
	strings := map[string]string{}
	workers := map[string]int64{}
	counters := map[string]int64{}
	receivers := map[string]ReceiverSummary{}
	m := metrics.NewMetrics()

	// {{{ vitals2str()

	vitals2str := func() string {
		rcvrs := ""
		keys := []string{}
		for k,_ := range receivers { keys = append(keys,k) }
		sort.Strings(keys)
		for _,k := range keys {
			v := receivers[k]
			rcvrs += fmt.Sprintf(
				"    %-15.15s: %9d msgs, %8d bundles, last %.1f s\n",
				k, v.NumMessagesSent, v.NumBundlesSent, time.Since(v.LastBundleTime).Seconds())
		}

		workerHist := histogram.Histogram{NumBuckets:40, ValMin:0, ValMax:400}
		// Measure if each worker did more (or less) of its fair share (where fair is 100)
		expectedFraction := 1 / float64(len(workers))
		tot := 0
		for _,count := range workers {
			tot += int(count)
		}
		for _,count := range workers {
			actualFraction := float64(count) / float64(tot)
			shareOfLoad := actualFraction / expectedFraction
			workerHist.Add(histogram.ScalarVal(shareOfLoad * 100))
		}

		str := fmt.Sprintf(
			"* %d messages (%d dupes; %d total; %d bundles; %d writes)\n"+
				"* Uptime: %s (started %s; last bundle:%5.3fs)\n"+
				"* Trackbuffer: %d elems, Airspace: (%d,%d) elems\n"+
				"\n"+
				"* Receivers:-\n%s\n"+
				"* Workers: %s\n\n"+
				"* Metrics:-\n%s\n",
			(counters["nAll"] - counters["nDupes"]), counters["nDupes"], counters["nAll"],
			counters["nBundles"], counters["nFrags"],
			time.Second * time.Duration(int(time.Since(startupTime).Seconds())), startupTime,
			time.Since(lastBundleTime).Seconds(),
			counters["TrackbufferSize"], counters["AirspaceSigCount"], counters["AirspaceAircraftCount"],
			rcvrs,
			workerHist,
			m.String())

		return str
	}

	// }}}

	tLastDump := time.Now()
	tLastCounts := time.Now()
	
	for {
		if weAreDone() { break }

		if time.Since(tLastDump) > time.Minute {
			Log.Printf("vital dump:-\n%s", vitals2str())
			tLastDump = time.Now()
		}
		if time.Since(tLastCounts) > 5 * time.Second {
			Log.Printf("* memstats  %s\n", memStats())
			Log.Printf("* Trackbuffer: %d msgs, Airspace: %d sigs, %d aircraft\n",
				counters["TrackbufferSize"], counters["AirspaceSigCount"],
				counters["AirspaceAircraftCount"])
			tLastCounts = time.Now()
		}

		select {
		case <-time.After(time.Second):
			// break

		case req := <-vitalsRequestChan:
			if req.Name == "_reset" {
				counters = map[string]int64{}
				receivers = map[string]ReceiverSummary{}
				workers = map[string]int64{}
				m = metrics.NewMetrics()

			} else if req.Name == "_bundle" {
				lastBundleTime = time.Now()
				s := receivers[req.Str]
				s.NumBundlesSent += 1
				s.NumMessagesSent += req.I
				s.LastBundleTime = req.T
				receivers[req.Str] = s
				counters["nBundles"] += 1
				counters["nAll"] += req.I
				//counters["nNew"] += req.J
				m.RecordValue("BundleSize", req.I)

			} else if req.Name == "_dbwrite" {
				m.RecordValue("DBWriteMillis", req.I)
				workers[fmt.Sprintf("%03d",req.J)]++
				counters["nFrags"]++

			} else if req.Name == "_memcache" {
				m.RecordValue("MemcacheMillis", req.I)

			} else if req.Name == "_filterMessages" {
				counters["nDupes"] += req.I
				counters["AirspaceSigCount"] = req.J
				counters["AirspaceAircraftCount"] = req.K
				
			} else if req.Name == "_bufferTracks" {
				counters["TrackbufferSize"] = req.I

			} else if req.Name == "_lastBundleTime" {
				vitalsResponseChan<- VitalsResponse{T: lastBundleTime}

			} else if req.Name == "_output" {
				vitalsResponseChan<- VitalsResponse{Str: vitals2str()}

			} else if req.Name != "" {
				if req.Str != "" { strings[req.Name] = req.Str }
			}
		}
	}

	Log.Printf(" -- trackVitals clean exit\n")
}

// }}}
// {{{ pullNewFromPubsub

var nReceiveCallbacks = 0

func pullNewFromPubsub(msgsOut chan<- []*adsb.CompositeMsg) {
	ctx, ctxCancelFunc := context.WithCancel(getContext())

	as := airspace.Airspace{}
	as.RollWhenThisMany = 5000                      // Dedupe set consists of 1-2x this number

	setupPubsub()
	Log.Printf("(pullNewFromPubsub starting)\n")	

	pc := mypubsub.NewClient(ctx, fProjectName)
	sub := pc.Subscription(fPubsubSubscription)

	sub.ReceiveSettings.MaxOutstandingMessages = 10 // put a limit on how many we juggle

/*
	if fOnAppEngine {
		if err := as.EverythingFromMemcache(ctx); err != nil {
			Log.Printf("airspace.EverythingFromMemcache: %v", err)
			as = airspace.Airspace{}
		}
	}
*/

	var mu = &sync.Mutex{}
	
	// sub.Receive invokes concurrent instances of this callback; we funnel their
	// (unpacked) responses back into the msgsOut channel, for the processing pipeline to eat
	callback := func(ctx context.Context, m *pubsub.Message) {
		mu.Lock()
		nReceiveCallbacks++
		mu.Unlock()

		m.Ack()
		msgs,err := mypubsub.UnpackPubsubMessage(m)
		if err != nil {
			Log.Printf("[%d] Unpack: err: %v", err)
			return
		}

		msgsOut <- msgs

		// Update our vital stats, with info about this bundle
		vitalsRequestChan<- VitalsRequest{
			Name: "_bundle",
			Str: msgs[0].ReceiverName,
			I: int64(len(msgs)),
			T: msgs[len(msgs)-1].GeneratedTimestampUTC,
		}
	}
	
	// sub.Receive doesn't terminate, so spin off into a goroutine
	go func() {
		Log.Printf("(sub.Receive starting)\n")
		if err := sub.Receive(ctx, callback); err != nil {
			Log.Printf("sub.Receive() err:%v", err)
			return
		}
	}()

	// Block until done channel lights up
	<-done

	ctxCancelFunc() // terminates call to sub.Receive()

	if fOnAppEngine {
/*
		// We're shutting down, so save all the deduping signatures
		if err := as.EverythingToMemcache(ctx); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; memcache: %v", err)
		}
*/
	} else {
		// We're not on AppEngine; clean up our wasteful subscription
		pc := mypubsub.NewClient(ctx, fProjectName)
		if err := mypubsub.DeleteSub(ctx, pc, fPubsubSubscription); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; del '%s': %v", fPubsubSubscription, err)
		}
		Log.Printf("  - pullNewFromPubsub, deleted %q\n", fPubsubSubscription)
	}
	
	Log.Printf(" -- pullNewFromPubsub clean exit\n")
}

// }}}
// {{{ filterNewMessages

// pubsub.Receiver's goroutines will send to the msgIns channel; we dedupe and send on
// This goroutine owns the airspace object (which is not concurrent safe)
func filterNewMessages(msgsIn <-chan []*adsb.CompositeMsg, msgsOut chan<- []*adsb.CompositeMsg) {
	as := airspace.NewAirspace()
	as.Signatures.RollAfter = 10 * time.Second // very aggressive, while we have probs
	
/*	ctx := getContext()
	if fOnAppEngine {
		if err := as.EverythingFromMemcache(ctx); err != nil {
			Log.Printf("airspace.EverythingFromMemcache: %v", err)
			as = airspace.Airspace{}
		}
	} */

	for {
		if weAreDone() { break } // Clean exit

		select {
		case <-time.After(time.Second):
			// break, in case weAreDone

		case msgs := <-msgsIn:
			newMsgs := as.MaybeUpdate(msgs)
			if len(newMsgs) > 0 {
				// Pass them to the other goroutine for dissemination, and get back to business.
				msgsOut <- newMsgs

				if fOnAppEngine {
					// Memcache no longer available on appengine flex (!)
					// pushAirspaceToMemcache(ctx, as)
				} else {
					//Log.Printf("- %2d were new (%2d already seen) - %s",
					//	len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
				}
			}

			// Update our vital stats, with info about the airspace & deduping
			nSigs,nAircraft := as.Sizes()
			vitalsRequestChan<- VitalsRequest{
				Name: "_filterMessages",
				I:int64(len(msgs) - len(newMsgs)),
				J:nSigs,
				K:nAircraft,
			}
		}
	}
	
	Log.Printf(" -- filterNewMessages clean exit\n")
}

// }}}
// {{{ bufferTracks

func bufferTracks(msgsIn <-chan []*adsb.CompositeMsg, msgsOut chan<- []*adsb.CompositeMsg) {	
	// Our primary piece of state ! It groups msgs into tracks for distinct aircraft, and
	// then flushes out individual tracks as/when they have data older than MaxAge
	tb := trackbuffer.NewTrackBuffer()

	for {
		if weAreDone() { break }

		select {
		case <-time.After(time.Second):
			// break
		case msgs := <-msgsIn:
			for _,m := range msgs {
				tb.AddMessage(m)
			}
			tb.Flush(msgsOut)

			vitalsRequestChan<- VitalsRequest{Name: "_bufferTracks", I:tb.Size()}
		}
	}

	Log.Printf(" -- bufferTracks clean exit\n")
}

// }}}
// {{{ workerDispatch

func workerDispatch(msgsIn <-chan []*adsb.CompositeMsg, workersOut []chan []*adsb.CompositeMsg) {
	for {
		if weAreDone() { break }

		select {
		case <-time.After(time.Second):
			// break
		case msgs := <-msgsIn:
			// pick a worker, based on the icao24; it's important that we
			// don't process frags for the same icaoid in parallel, or we'll
			// suffer a write-write conflict and overwrite some data.
			h := fnv.New32a()
			h.Write([]byte(msgs[0].Icao24))
			i := h.Sum32()
			workerId := i % uint32(len(workersOut))
			workersOut[workerId] <- msgs // Send the message frag to the worker.
		}
	}
	Log.Printf(" -- workerDispatch clean exit\n")
}

// }}}
// {{{ flushTracks

// The worker bee function
func flushTracks(myId int, msgsIn <-chan []*adsb.CompositeMsg) {
	//Log.Printf("(flushTracks/%03d starting)\n", myId)

	for {
		if weAreDone() { break }

		select {
		case <-time.After(time.Second):
			// break
		case msgs := <-msgsIn:
			flushTrackToDatastore(myId, msgs)
		}			
	}

	//Log.Printf(" ---- flushTracks/%03d clean exit\n", myId)
}

// }}}

// {{{ main

func main() {
	Log.Printf("(main starting)\n")
	
	msgChan1 := make(chan []*adsb.CompositeMsg, 3)
	msgChan2 := make(chan []*adsb.CompositeMsg, 3)
	msgChan3 := make(chan []*adsb.CompositeMsg, 3)
	workerChans := []chan []*adsb.CompositeMsg{}

	nWorkers := 256 // avoid getting backed up on DB writes
	if !fOnAppEngine { nWorkers = 16 }
	for i:=0; i<nWorkers; i++ {
		workerChan := make(chan []*adsb.CompositeMsg, 3)
		workerChans = append(workerChans, workerChan)
		go flushTracks(i, workerChan)          // worker bee, write per-flight fragments to disc
	}

	go pullNewFromPubsub(msgChan1)           // sends mixed bundles down chan1
	go filterNewMessages(msgChan1, msgChan2) // ... dedupes them, into chan2 ...
	go bufferTracks(msgChan2, msgChan3)      // ... sorts msgs into per-flight frags, into chan3 ...
	go workerDispatch(msgChan3, workerChans) // ... takes a per-flight frag, shards over workerChans

	// Manage vital statistics in a thread safe way
	go trackVitals()
	
	go func(){ Log.Fatal(http.ListenAndServe(":8080", nil)) }()

	// Block until done channel lights up
	<-done
	time.Sleep(time.Second * 4)  // Give the pubsub loop a chance to unblock and exit
	Log.Printf("(-- main clean exit)\n")
}

// }}}

/* Old */
/*
// {{{ pullNewFromPubsub

func pullNewFromPubsub(msgsOut chan<- []*adsb.CompositeMsg) {
	ctx := getContext()
	as := airspace.Airspace{}

	setupPubsub()
	Log.Printf("(pubsub setup done)\n")

	dWedgeThresh := time.Minute // We get ~2.5/s, so nothing for 60s is bad
	
	if fOnAppEngine {
		if err := as.EverythingFromMemcache(ctx); err != nil {
			Log.Printf("airspace.EverythingFromMemcache: %v", err)
			as = airspace.Airspace{}
		}
	}
	
	nSpawns := 0
outerLoop:
	for {
		if weAreDone() { break outerLoop }

		Log.Printf("(pubsub: starting from top of outerloop)")

		youWereDiscarded := make(chan struct{}) // Closed when we give up on the worker

		go pullPubsubUntilWedge(ctx, &as, nSpawns, youWereDiscarded, msgsOut)

		// Now wait until it all goes wrong; each 5s, check we're still getting bundles.
	innerLoop:
		for {
			if weAreDone() { break outerLoop }
			time.Sleep(5 * time.Second)
			
			vitalsRequestChan<- VitalsRequest{Name:"_lastBundleTime"}
			vr := <-vitalsResponseChan

			if time.Since(vr.T) > dWedgeThresh {
				Log.Printf("Watchdog: last bundle was %s ago, respawning (%d)", time.Since(vr.T), nSpawns)
				vitalsRequestChan<- VitalsRequest{Name: "_pubsubwedge"}
				nSpawns++
				break innerLoop
			}
		}
		
		// Unplanned exit; prob a wedge; tell it to die (if it wakes up), then start up another.
		close(youWereDiscarded)
		time.Sleep(2 * time.Second)
	}

	if fOnAppEngine {
		// We're shutting down, so save all the deduping signatures
		if err := as.EverythingToMemcache(ctx); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; memcache: %v", err)
		}

	} else {
		// We're not on AppEngine; clean up our wasteful subscription
		pc := mypubsub.NewClient(ctx, fProjectName)
		if err := mypubsub.DeleteSub(ctx, pc, fPubsubSubscription); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; del '%s': %v", fPubsubSubscription, err)
		}			
	}
	
	Log.Printf(" -- pullNewFromPubsub clean exit\n")
}

// }}}
// {{{ pullPubsubUntilWedge

// Keep reading the pubsub iterator indefinitely, until it errors. May well wedge. If the iAmDone
// channel closes, will terminate.
func pullPubsubUntilWedge(ctx context.Context, as *airspace.Airspace, id int, iWasDiscarded <-chan struct{}, msgsOut chan<- []*adsb.CompositeMsg) {

	pc := mypubsub.NewClient(ctx, fProjectName)	
	it,err := pc.Subscription(fPubsubSubscription).Pull(ctx)
	if err != nil {
		Log.Printf("pc.Sub(%s).Pull err:%v", fPubsubSubscription, err)
		return
	}
  defer it.Stop()

	Log.Printf("(pubsub innerloop %d: pulling from topic:sub %s:%s)",
		id, fPubsubInputTopic, fPubsubSubscription)

	tPrevEnd := time.Now()
	
	for {
		if weAreDone() { break } // Clean exit
		
		// Check if the watchdog timed out on us and started another; if so we should go away.
		select{
		case <-iWasDiscarded:
			Log.Printf("pullPubsubUntilWedge %d woke up, was discarded, aborting", id)
			return
		default: // carry on, we've not been discarded
		}
		
		tStart := time.Now()

		m,err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			Log.Printf("[%d] it.Next err: %v", id, err)
			return
		}

		m.Done(true) // Acknowledge the message.

		msgs,err := mypubsub.UnpackPubsubMessage(m)
		if err != nil {
			Log.Printf("[%d] Unpack: err: %v", id, fPubsubSubscription, err)
			time.Sleep(time.Second * 10)
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
				pushAirspaceToMemcache(ctx, as)
			} else {
				//Log.Printf("- %2d were new (%2d already seen) - %s",
				//	len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
			}
		}

		// Update our vital stats, with info about this bundle
		//airspaceBytes,_ := as.ToBytes()
		vitalsRequestChan<- VitalsRequest{
			Name: "_bundle",
			Str:msgs[0].ReceiverName,
			I:int64(len(msgs)),
			J:int64(len(newMsgs)),
			K:int64(id),
			
			// Some primitive wait state data (latencies in millis)
			L:(tPullDone.Sub(tStart).Nanoseconds() / 1000000),
			M:(tMsgsSent.Sub(tPullDone).Nanoseconds() / 1000000),
			N:(tStart.Sub(tPrevEnd).Nanoseconds() / 1000000),  // prelude/zombie time
			
			T: msgs[len(msgs)-1].GeneratedTimestampUTC,
		}
		tPrevEnd = time.Now()
	}

	Log.Printf(" -- pullPubsubUntilWedge %d clean exit", id)
	return
}

// }}}
// {{{ pushAirspaceToMemcache

var tLastMemcache = time.Now()
var memcacheMutex = sync.Mutex{}

func pushAirspaceToMemcache(ctx context.Context, as *airspace.Airspace) {
	memcacheMutex.Lock()
	defer memcacheMutex.Unlock()

	if time.Since(tLastMemcache) < 500 * time.Millisecond { return }

	// Take from airspace/memcache:JustAircraftToMemcache, and goroutinzed
	justAircraft := airspace.Airspace{Aircraft: as.Aircraft}
	if b,err := justAircraft.ToBytes(); err == nil {
		go func(){
			tStart := time.Now()

			if err := gaeutil.SaveSingletonToMemcache(ctx, "airspace", b); err != nil {
				Log.Printf("main/JustAircraftToMemcache: err: %v", err)
			} else {
				vitalsRequestChan<- VitalsRequest{
					Name: "_memcache",
					I:(time.Since(tStart).Nanoseconds() / 1000000),
				}
			}
		}()
	}
	
	tLastMemcache = time.Now()
}

// }}}
*/

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
