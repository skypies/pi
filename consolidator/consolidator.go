// The consolidator program subscribes to a topic on Google Cloud Pubsub, and reads bundles of
// composite ADSB messages from it. These are deduped, and unique ones are published to a
// different topic. A snapshot is written to memcache, for other apps to access. The program
// should be deployed as a Managed VM AppEngine App, as per the .yaml file; but can be run
// locally via '-ae=false'.
package main

// $ aedeploy gcloud preview app deploy ./consolidator.yaml --promote --bucket gs://gcloud-arse
// $ go run consolidator.go -ae=false

import (
	"flag"
	"fmt"
	"log"
	"os"
	"net/http"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/skypies/adsb"
	"github.com/skypies/pi/airspace"
	"github.com/skypies/pi/pubsub"
)
// {{{ globals

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
	receivers map[string]int
	receiversLastSeen map[string]time.Time
)

// }}}

// {{{ init

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
	
	// Kick off the webservery bit
	http.HandleFunc("/", statsHandler)
	http.HandleFunc("/_ah/start", startHandler)
	go func(){
		if err := http.ListenAndServe(":8080", nil); err != nil {
			Log.Fatalf("http server fail: %v", err)
		}
	}()

	// Setup the pubsub stuff, and the context
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
}

// }}}

// {{{ TrackTable

// Accumulates chunks of messages, grouped by aircraft, to send on to the flight DB

type TrackTable struct {
	MaxAge      time.Duration // Flush any track with data older than this
	Tracks      map[adsb.IcaoId]*Track
}

func NewTrackTable() *TrackTable {
	table := TrackTable{
		MaxAge: time.Second*30,
		Tracks: make(map[adsb.IcaoId]*Track),
	}
	return &table
}

type Track struct {
	Messages  []*adsb.CompositeMsg
}

func (t *Track)Age() time.Duration {
	if len(t.Messages)==0 { return time.Duration(time.Hour * 24) }
	return time.Since(t.Messages[0].GeneratedTimestampUTC)
}

func (table *TrackTable)AddTrack(icao adsb.IcaoId) {
	track := Track{
		Messages: []*adsb.CompositeMsg{},
	}
	table.Tracks[icao] = &track
}

func (table *TrackTable)RemoveTracks(icaos []adsb.IcaoId) []*Track{
	removed := []*Track{}
	for _,icao := range icaos {
		removed = append(removed, table.Tracks[icao])
		delete(table.Tracks, icao)
	}
	return removed
}

func (table *TrackTable)AddMessage(m *adsb.CompositeMsg) {
	if _,exists := table.Tracks[m.Icao24]; exists == false {
		table.AddTrack(m.Icao24)
	}
	track := table.Tracks[m.Icao24]
	track.Messages = append(track.Messages, m)
}

func (table *TrackTable)Flush(flushFunc func (*[]*adsb.CompositeMsg)) {
	toRemove := []adsb.IcaoId{}
	
	for id,_ := range table.Tracks {
		if table.Tracks[id].Age() > table.MaxAge {
			toRemove = append(toRemove, id)
		}
	}

	for _,t := range table.RemoveTracks(toRemove) {
		sort.Sort(adsb.CompositeMsgPtrByTimeAsc(t.Messages))
		flushFunc(&t.Messages)
	}
}

// }}}
// {{{ handleNewMessages

type MsgChanItem struct {
	msgs *[]*adsb.CompositeMsg
}

func handleNewMessages(msgChan chan MsgChanItem) {
	table := NewTrackTable()
	
	flushFunc := func(msgs *[]*adsb.CompositeMsg) {
/*
		for i,m := range *msgs {
			fmt.Printf(" [%2d] %s\n", i, m)
		}
		fmt.Printf(" --\n")
*/
	}
	
	for item := range msgChan {
		for _,m := range *item.msgs {
			table.AddMessage(m)
		}

		table.Flush(flushFunc)

		//if fPubsubOutputTopic != "" {}
	}
}

// }}}

// {{{ main

func main() {
	fmt.Printf("(main)\n")
	startTime = time.Now()
	receivers = map[string]int{}
	receiversLastSeen = map[string]time.Time{}

	airspace := airspace.Airspace{}
	msgChan := make(chan MsgChanItem, 3)
	go handleNewMessages(msgChan)

	for {
		msgs,err := pubsub.Pull(Ctx, fPubsubSubscription, 10)
		if err != nil {
			Log.Printf("Pull/sub=%s: err: %s", fPubsubSubscription, err)
			time.Sleep(time.Second * 10)
			continue

		} else if msgs == nil || len(*msgs) == 0 {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		newMsgs := airspace.MaybeUpdate(msgs)

		if fOnAppEngine {
			airspace.ToMemcache(Ctx)  // Update the memcache thing
		} else {
			Log.Printf("- %2d were new (%2d already seen) - %s",
				len(newMsgs), len(*msgs)-len(newMsgs), (*msgs)[0].ReceiverName)
		}

		// Pass them to the other goroutine for dissemination, and get back to busines.
		msgChan <- MsgChanItem{msgs: &newMsgs}

		// Update crappy globals
		nMessages += len(*msgs)
		nNewMessages += len(newMsgs)
		lastMessage = time.Now()
		if b,err := airspace.ToBytes(); err == nil { sizeAirspace = len(b) }
		for _,m := range *msgs { receivers[m.ReceiverName]++ }
		receiversLastSeen[(*msgs)[0].ReceiverName] = time.Now()
	}

	Log.Printf("Final clean shutdown")
}

// }}}

// {{{ startHandler

func startHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))	
}

// }}}
// {{{ statsHandler

func statsHandler(w http.ResponseWriter, r *http.Request) {
	stationStr := ""
	for k,v := range receivers {
		stationStr += fmt.Sprintf("  * %-20.20s: %7d (last update: %.1f seconds ago)\n",
			k, v, time.Since(receiversLastSeen[k]).Seconds())
	}
	w.Write([]byte(fmt.Sprintf("OK\n* %d messages (%d dupe, %d new)\n"+
		"* Up since %s (%s)\n"+
		"* Last update %.1f seconds ago\n"+
		"* Airspace %d bytes\n* Receivers:-\n%s\n",
		nMessages, nMessages-nNewMessages, nNewMessages,
		startTime, time.Since(startTime),
		time.Since(lastMessage).Seconds,
		sizeAirspace, stationStr)))
}

// }}}

// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
