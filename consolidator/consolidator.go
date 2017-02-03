// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access.

// It should be deployed as a Managed VM AppEngine App, as per the .yaml file:
//   $ aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse

// You may need to deploy some datastore indices, too:
//   $ aedeploy gcloud preview app deploy ./index.yaml --promote --bucket gs://gcloud-arse

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
	"sort"
	"time"

	gpubsub "cloud.google.com/go/pubsub"

	"golang.org/x/net/context"

	"google.golang.org/api/iterator"
	"google.golang.org/appengine"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/trackbuffer"
	fdb "github.com/skypies/flightdb2"
	"github.com/skypies/flightdb2/fgae"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/util/gaeutil"
	"github.com/skypies/util/histogram"
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
	fOnAppEngine           bool
	
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
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/reset", resetHandler)
	http.HandleFunc("/_ah/start", startHandler)
	http.HandleFunc("/_ah/stop", stopHandler)

	addSIGINTHandler()
}

// }}}

// {{{ {start,stop,status,reset}Handler

func startHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("(startHandler)\n")
	w.Write([]byte("OK"))	
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("(stopHandler)\n")
	close(done) // Shut down the pubsub goroutine (which should save airspace into datastore.)
	w.Write([]byte("OK"))	
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	vitalsRequestChan<- VitalsRequest{Name:"_output"}
	str := <-vitalsResponseChan
	w.Write([]byte(fmt.Sprintf("OK\n%s", str)))
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
	if fOnAppEngine {
		return appengine.BackgroundContext()
	}
	return context.TODO()
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

	db := fgae.FlightDB{C:getContext()}
	frag := fdb.MessagesToTrackFragment(msgs)

	if err := db.AddTrackFragment(frag); err != nil {
		Log.Printf("flushPost/ToDatastore: %v\n", err)
	}

	vitalsRequestChan<- VitalsRequest{
		Name: "_dbwrite",
		I:(time.Since(tStart).Nanoseconds() / 1000000),
		J:int64(myId),
	}
}

// }}}
// {{{ pushAirspaceToMemcache

var tLastMemcache = time.Now()

func pushAirspaceToMemcache(ctx context.Context, as *airspace.Airspace) {
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
// {{{ iteratePubsubForever

// Keep reading the pubsub iterator indefinitely. Will return in two cases:
//  1. the done channel wakes up (returns nil) (or, somehow, it.Stop was called)
//  2. an error is encountered (returns err)
func iteratePubsubForever(ctx context.Context, pc *gpubsub.Client, as *airspace.Airspace, msgsOut chan<- []*adsb.CompositeMsg) error {

	it,err := pc.Subscription(fPubsubSubscription).Pull(ctx)
	if err != nil { return fmt.Errorf("pc.Sub(%s).Pull err:%v", fPubsubSubscription, err) }
	
	tPrevEnd := time.Now()

	tLastFreshIterator := time.Now()
	dIteratorLifetime := time.Hour
	
	for {
		if weAreDone() { break } // Clean exit

		if time.Since(tLastFreshIterator) > dIteratorLifetime {
			it.Stop()
			return fmt.Errorf("expiring iterator on purpose")
		}
		
		tStart := time.Now()

		m,err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			it.Stop()
			return fmt.Errorf("it.Next err:%v", err)
		}

		m.Done(true) // Acknowledge the message.

		msgs,err := pubsub.UnpackPubsubMessage(m)
		if err != nil {
			Log.Printf("Unpack: err: %v", fPubsubSubscription, err)
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
				Log.Printf("- %2d were new (%2d already seen) - %s",
					len(newMsgs), len(msgs)-len(newMsgs), msgs[0].ReceiverName)
			}
		}

		// Update our vital stats, with info about this bundle
		//airspaceBytes,_ := as.ToBytes()
		vitalsRequestChan<- VitalsRequest{
			Name: "_bundle",
			Str:msgs[0].ReceiverName,
			I:int64(len(msgs)),
			J:int64(len(newMsgs)),

			// Some primitive wait state data (latencies in millis)
			L:(tPullDone.Sub(tStart).Nanoseconds() / 1000000),
			M:(tMsgsSent.Sub(tPullDone).Nanoseconds() / 1000000),
			N:(tStart.Sub(tPrevEnd).Nanoseconds() / 1000000),  // prelude/zombie time
			
			T: msgs[len(msgs)-1].GeneratedTimestampUTC,
		}
		tPrevEnd = time.Now()
	}

	it.Stop()
	return nil
}

// }}}

// {{{ trackVitals

// These two channels are accessible from all goroutines
var vitalsRequestChan = make(chan VitalsRequest, 40)
var vitalsResponseChan = make(chan string, 5)  // Only used for stats output

type VitalsRequest struct {
	Name             string  // _blah
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
		if weAreDone() { break }

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
				s := receivers[req.Str]
				s.NumBundlesSent += 1
				s.NumMessagesSent += req.I
				s.LastBundleTime = req.T
				receivers[req.Str] = s
				counters["nBundles"] += 1
				counters["nAll"] += req.I
				counters["nNew"] += req.J
				counters["nDupes"] += (req.I - req.J)
				m.RecordValue("BundleSize", req.I)
				m.RecordValue("PullMillis", req.L)
				m.RecordValue("ToQueueMillis", req.M)
				m.RecordValue("PreludeMillis", req.N)

			} else if req.Name == "_dbwrite" {
				m.RecordValue("DBWriteMillis", req.I)
				workers[fmt.Sprintf("%03d",req.J)]++
				counters["nFrags"]++

			} else if req.Name == "_memcache" {
				m.RecordValue("MemcacheMillis", req.I)
				
			} else if req.Name == "_output" {
				// {{{ fmt

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
						"* %d messages (%d dupes; %d total; %d bundles received, %d frags written)\n"+
						"* Uptime: %s (started %s)\n"+
						"\n"+
						"* Receivers:-\n%s\n"+
						"* Worker workloads: %s\n\n"+
						"* Metrics:-\n%s\n",
					counters["nNew"], counters["nDupes"], counters["nAll"], counters["nBundles"], counters["nFrags"],
					time.Second * time.Duration(int(time.Since(startupTime).Seconds())), startupTime,
					rcvrs,
					workerHist,
					m.String())

				// }}}

				vitalsResponseChan <- str

			} else if req.Name != "" {
				if req.Str != "" { strings[req.Name] = req.Str }
			}
		}
	}

	Log.Printf(" -- trackVitals clean exit\n")
}

// }}}
// {{{ pullNewFromPubsub

func pullNewFromPubsub(msgsOut chan<- []*adsb.CompositeMsg) {
	ctx := getContext()

	pc := pubsub.NewClient(ctx, fProjectName)
	if fOnAppEngine {
		pubsub.Setup(ctx, pc, fPubsubInputTopic, fPubsubSubscription)
	} else {
		fPubsubSubscription += "-DEV"
		pubsub.Setup(ctx, pc, fPubsubInputTopic, fPubsubSubscription)
		pubsub.DeleteSub(ctx, pc, fPubsubSubscription)
		pubsub.CreateSub(ctx, pc, fPubsubSubscription, fPubsubInputTopic)
	}
	Log.Printf("(pubsub setup)\n")
	
	// Setup our primary piece of state: the airspace. Load it from last time if we can.
	as := airspace.Airspace{}
	if fOnAppEngine {
		if err := as.EverythingFromMemcache(ctx); err != nil {
			Log.Printf("airspace.EverythingFromMemcache: %v", err)
			as = airspace.Airspace{}
		}
	}

	Log.Printf("(now pulling from topic:sub %s:%s)", fPubsubInputTopic, fPubsubSubscription)

	for {
		if weAreDone() { break }

		Log.Printf("(starting a new outerloop)")

		err := iteratePubsubForever(ctx, pc, &as, msgsOut)
		if err == nil { break } // Planned exit via weAreDone

		// Unplanned exit; close down iterator, wait a few secs, and start another
		Log.Printf("(iteratePubsubForever failed: %s)", err)
		time.Sleep(2 * time.Second)
	}

	if fOnAppEngine {
		// We're shutting down, so save all the deduping signatures
		if err := as.EverythingToMemcache(ctx); err != nil {
			Log.Printf(" -- pullNewFromPubsub clean exit; memcache: %v", err)
		}

	} else {
		// We're not on AppEngine; clean up our wasteful subscription
		if err := pubsub.DeleteSub(ctx, pc, fPubsubSubscription); err != nil {
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

	Log.Printf(" ---- flushTracks/%03d clean exit\n", myId)
}

// }}}

// {{{ main

func main() {
	Log.Printf("(main)\n")

	msgChan1 := make(chan []*adsb.CompositeMsg, 3)
	msgChan2 := make(chan []*adsb.CompositeMsg, 3)
	workerChans := []chan []*adsb.CompositeMsg{}

	nWorkers := 256 // avoid getting backed up on DB writes
	if !fOnAppEngine { nWorkers = 16 }
	for i:=0; i<nWorkers; i++ {
		workerChan := make(chan []*adsb.CompositeMsg, 3)
		workerChans = append(workerChans, workerChan)
		go flushTracks(i, workerChan)          // worker bee, write per-flight fragments to disc
	}

	go pullNewFromPubsub(msgChan1)           // sends mixed bundles down chan1
	go bufferTracks(msgChan1, msgChan2)      // ... sorts msgs into per-flight frags, into chan2 ...
	go workerDispatch(msgChan2, workerChans) // ... takes a per-flight frag, shards over workerChans

	// Manage vital statistics in a thread safe way
	go trackVitals()
	
	// Now do basically nothing in the main goroutine
	if fOnAppEngine {
		appengine.Main()		
	} else {
		go func(){ Log.Fatalf("locallisten: %v\n", http.ListenAndServe(":8081", nil)) }()

		// Block until done channel lights up
		<-done
		time.Sleep(time.Second * 4)  // Give the pubsub loop a chance to unblock and exit
		Log.Printf("(main clean exit)\n")
	}
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
