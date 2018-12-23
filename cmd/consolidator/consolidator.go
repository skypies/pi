// The consolidator program subscribes to a topic on Google Cloud
// Pubsub, and reads bundles of composite ADSB messages from it. These
// are deduped, and unique ones are published to a different topic.
// Updates are written to a flight database. A snapshot is written to
// memcache twice a second, for other apps to access.

// Handy oneliners:
//   $ curl -s fdb.serfr1.org/con/stack | pp -force-color -parse=false -aggressive

// To test, using a clone of the prod pubsub inputs and not touching datastore:
//   $ export GOOGLE_APPLICATION_CREDENTIALS=~/something.json
//   $ go run consolidator.go                [prod topic, but with now subscription]
//   $ go run consolidator.go -input=testing [attaches to a testing topic]

// To run in full prod mode, upload to a micro VM (that has full cloud API access), and then:
//   $ go run consolidator.go -dryrun=false

// If there is a backlog to clear, consider a 4x vCPU machine size. This should clear
//  at the rate of ~10K bundles/minute. The micro size will thrash if it tries this;
//  it is only good for steady state, which is a few hundred bundles/minute.

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

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	fdb "github.com/skypies/flightdb"
	"github.com/skypies/flightdb/fgae"
	"github.com/skypies/flightdb/ref"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/util/ae"
	dsprovider "github.com/skypies/util/gcp/ds"
	"github.com/skypies/util/histogram"
	"github.com/skypies/util/metrics"
	mypubsub "github.com/skypies/util/gcp/pubsub" // This is adding less value over time; kill ?
)

// }}}
// {{{ var()

var (
	// Command line flags
	fProjectName           string
	fPubsubInputTopic      string
	fPubsubSubscription    string
	fAirspaceWebhook       string
	fVerbosity             int
	fDatabaseWorkers       int

	fDryrunMode            bool

	tGlobalStart           time.Time
	stackTraceBytes      []byte

	Log                   *log.Logger

	// These globals are updated from time to time
	airframeRefdata       *ref.AirframeCache
	scheduleRefdata       *ref.ScheduleCache
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
	flag.StringVar(&fAirspaceWebhook, "hook-airspace", "fdb.serfr1.org/fdb/memcachesingleton",
		"URL to publish airspace updates to")

	flag.BoolVar(&fDryrunMode, "dryrun", true, "else uses prod pubsub & datastore")

	flag.IntVar(&fVerbosity, "v", 0, "verbosity level")
	flag.IntVar(&fDatabaseWorkers, "n", 256, "number of database workers")

	flag.Parse()

	if fDryrunMode { fDatabaseWorkers = 16 } // Do we need this ?

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

	if !fDryrunMode {
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

// Since Appengine Flexible Environment banned direct AppEngine APIs, we're using cloud
// APIs everywhere, so no longer need special contexts for particular services.

func getContext() context.Context {
	return context.Background()
}

// }}}
// {{{ flushTrackToDatastore

func flushTrackToDatastore(myId int, p dsprovider.DatastoreProvider, msgs []*adsb.CompositeMsg) {
	if fDryrunMode {
		//Log.Printf(" -- (%d pushed for %s)\n", len(msgs), string(msgs[0].Icao24))
		vitalsRequestChan<- VitalsRequest{Name: "_dbwrite", J:int64(myId)}
		return
	}

	tStart := time.Now()
	perf := map[string]time.Time{}

	db := fgae.New(getContext(), p)

	frag := fdb.MessagesToTrackFragment(msgs)
	if err := db.AddTrackFragment(frag, airframeRefdata, scheduleRefdata, perf); err != nil {
		Log.Printf("flushPost/ToDatastore: err: %v\n--\n", err)
	}

	durMillis := func(s,e string) int64 {
		return int64(perf[e].Sub(perf[s]).Nanoseconds() / 1000000)
	}

	wasNewEntity := 0
	if _,exists := perf["03_notplausible"]; exists {
		wasNewEntity = 1
	}

	vitalsRequestChan<- VitalsRequest{
		Name: "_dbwrite",
		I:(time.Since(tStart).Nanoseconds() / 1000000),
		J:int64(myId),
		// perf timings, in millis. Stages are: 01_start, 02_mostrecent,
		// 03_plausible, 03_notplausible, 04_trackbuild, 05_waypoints, 06_persist
		L:durMillis("01_start", "02_mostrecent"),
		M:durMillis("02_mostrecent", "04_trackbuild"),
		N:durMillis("04_trackbuild", "05_waypoints"),
		O:durMillis("05_waypoints", "06_persist"),
		// whether we created a new DS entity
		P:int64(wasNewEntity),
	}
}

// }}}
// {{{ maybePostAirspace

var tLastMemcache = time.Now()
var memcacheMutex = sync.Mutex{}

var junkMutex = sync.Mutex{}

func maybePostAirspace(ctx context.Context, as *airspace.Airspace) {
	if fAirspaceWebhook == "" { return }
	
	memcacheMutex.Lock()
	defer memcacheMutex.Unlock()

	if time.Since(tLastMemcache) < 500 * time.Millisecond { return }

	justAircraft := airspace.Airspace{Aircraft: as.Aircraft}
	if b,err := justAircraft.ToBytes(); err == nil {
		go func(){
			tStart := time.Now()
			junkMutex.Lock()
			nMemcacheStarts++
			junkMutex.Unlock()
			
			// There is no cloud API for memcache, so we have to update the entry indirectly, via
			// a handler running in an appengine standard app.
			if err := ae.SaveSingletonToMemcacheURL("airspace", b, fAirspaceWebhook); err != nil {
				Log.Printf("main/maybePostAirspace(%s): err: %v", fAirspaceWebhook, err)
			} else {
				vitalsRequestChan<- VitalsRequest{
					Name: "_memcache",
					I:(time.Since(tStart).Nanoseconds() / 1000000),
				}
			}
			junkMutex.Lock()
			nMemcacheEnds++
			junkMutex.Unlock()
		}()

	}
	
	tLastMemcache = time.Now()
}

// }}}

// {{{ cacheRefdata

func cacheRefdata(p dsprovider.DatastoreProvider) {
	ctx := getContext()
	db := fgae.New(ctx, p)
	sp := db.SingletonProvider

	pollInterval := time.Second * 30
	lastPoll := time.Now().Add(-10 * pollInterval)
	
	for {
		if weAreDone() { break }

		if time.Since(lastPoll) > pollInterval {
			if newAirframes,err := ref.LoadAirframeCache(ctx,sp); err != nil {
				Log.Printf("LoadAirframeCache err: %v\n", err)
			} else {
				airframeRefdata = newAirframes
			}
			if newSchedules,err := ref.LoadScheduleCache(ctx,sp); err != nil {
				Log.Printf("LoadScheduleCache err: %v\n", err)
			} else {
				scheduleRefdata = newSchedules
			}

			Log.Printf("-- cacheRefdata polling (every %s), loaded %d airframes, %d schedules",
				pollInterval, len(airframeRefdata.Map), len(scheduleRefdata.Map))

			lastPoll = time.Now()
		}

		time.Sleep(time.Second)
	}

	Log.Printf(" -- cacheRefdata clean exit\n")
}

// }}}
// {{{ trackVitals

// These two channels are accessible from all goroutines
var vitalsRequestChan = make(chan VitalsRequest, 40)
var vitalsResponseChan = make(chan VitalsResponse, 5)  // Only used for stats output

var nMemcacheStarts int
var nMemcacheEnds int

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
	return fmt.Sprintf("go:% 5d(% 4d cb; %d/%d mc); heap:% 13d, % 13d; stack:% 13d",
		runtime.NumGoroutine(), nReceiveCallbacks, nMemcacheStarts, nMemcacheEnds,
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

		if time.Since(tLastDump) > time.Minute * 5 {
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

				m.RecordValue("Z02_MostRecentMillis", req.L)
				m.RecordValue("Z05_WaypointsMillis", req.N)
				if req.P > 0 {
					m.RecordValue("Z04_New_TrackbuildMillis", req.M)
					m.RecordValue("Z06_New_PersistMillis", req.O)
				} else {
					m.RecordValue("Z04_Extend_TrackbuildMillis", req.M)
					m.RecordValue("Z06_Extend_PersistMillis", req.O)
				}

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

	setupPubsub()
	Log.Printf("(pullNewFromPubsub starting)\n")	

	pc := mypubsub.NewClient(ctx, fProjectName)
	sub := pc.Subscription(fPubsubSubscription)

	sub.ReceiveSettings.MaxOutstandingMessages = 10 // put a limit on how many we juggle

	var mu = &sync.Mutex{}
	
	// sub.Receive invokes concurrent instances of this callback; we funnel their
	// (unpacked) responses back into the msgsOut channel, for the processing pipeline to eat
	callback := func(ctx context.Context, m *pubsub.Message) {

		// I'm not entirely sure we need to count how many callbacks are running ...
		mu.Lock()
		nReceiveCallbacks++
		mu.Unlock()

		defer func() {
			mu.Lock()
			nReceiveCallbacks--
			mu.Unlock()
		}()
		
		m.Ack()
		msgs,err := mypubsub.UnpackPubsubMessage(m)
		if err != nil {
			Log.Printf("[%d] Unpack: err: %v", err)
			return
		}

		// We're blacklisting CulverCity, to see if that's the problem
		if msgs[0].ReceiverName == "CulverCity" {
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

	if fDryrunMode {
		// We're not on AppEngine; clean up our wasteful subscription
		ctx = getContext() // use a new context, the one above has been canceled
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
	//as.Signatures.RollAfter = 10 * time.Second // very aggressive, while we have probs
	as.RollWhenThisMany = 10000                // Dedupe set consists of 1-2x this number
	
	ctx := getContext()

	// TODO - make `as.EverythingFromMemcache(ctx)` work using webhook thing

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

				maybePostAirspace(ctx, &as)

				if fVerbosity > 0 {
					Log.Printf("- %2d were new (%2d already seen) - %s",
						len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
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

	// TODO - make `as.EverythingToMemcache(ctx)` work
	
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
func flushTracks(myId int, p dsprovider.DatastoreProvider, msgsIn <-chan []*adsb.CompositeMsg) {
	//Log.Printf("(flushTracks/%03d starting)\n", myId)

	for {
		if weAreDone() { break }

		select {
		case <-time.After(time.Second):
			// break
		case msgs := <-msgsIn:
			flushTrackToDatastore(myId, p, msgs)
		}			
	}

	//Log.Printf(" ---- flushTracks/%03d clean exit\n", myId)
}

// }}}

// {{{ main

func main() {
	Log.Printf("(main starting)\n")

	// The cloud provider's client leaks goroutines, so just use one client forever
	db,err := dsprovider.NewCloudDSProvider(getContext(), fProjectName)
	if err != nil { Log.Fatal(err) }

	msgChan1 := make(chan []*adsb.CompositeMsg, 3)
	msgChan2 := make(chan []*adsb.CompositeMsg, 3)
	msgChan3 := make(chan []*adsb.CompositeMsg, 3)
	workerChans := []chan []*adsb.CompositeMsg{}

	nWorkers := fDatabaseWorkers // avoid getting backed up on DB writes
	Log.Printf("(spawning %d DB workers)\n", nWorkers)
	for i:=0; i<nWorkers; i++ {
		workerChan := make(chan []*adsb.CompositeMsg, 3)
		workerChans = append(workerChans, workerChan)
		go flushTracks(i, db, workerChan)      // worker bee, write per-flight fragments to disc
	}

	go pullNewFromPubsub(msgChan1)           // sends mixed bundles down chan1
	go filterNewMessages(msgChan1, msgChan2) // ... dedupes them, into chan2 ...
	go bufferTracks(msgChan2, msgChan3)      // ... sorts msgs into per-flight frags, into chan3 ...
	go workerDispatch(msgChan3, workerChans) // ... takes a per-flight frag, shards over workerChans

	go trackVitals()    // Manage vital statistics via global channels
	go cacheRefdata(db) // Cache some refdata

	go func(){ Log.Fatal(http.ListenAndServe(":8080", nil)) }()

	// Block until done channel lights up
	<-done
	time.Sleep(time.Second * 4)  // Give the pubsub loop a chance to unblock and exit
	Log.Printf("(-- main clean exit)\n")
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
