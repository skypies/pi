// The skypi application attaches to a socket that writes out SBS formatted ADS-B messages.
// It filters out the interesting ones, and aggregates together fields from different messages, to
// build useful packets. These are then published up to a topic in Google Cloud PubSub.
package main

// cp serfr0-fdb-blahblah.json ~/.config/gcloud/application_default_credentials.json
// go get github.com/skypies/pi/skypi
// go build github.com/skypies/pi/skypi
// $GOPATH/bin/skypi -receiver="MyStationName"
// ... maybe also: -h=southpi:30003 -maxage=4s -timeloc="America/Los_angeles" -v=2 -topic=""

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
	
	"github.com/skypies/adsb"
	"github.com/skypies/adsb/msgbuffer"
	"github.com/skypies/util/pubsub"
)

var Log *log.Logger

var fHostPort              string
var fProjectName           string
var fPubsubTopic           string
var fReceiverName          string
var fDump1090TimeLocation  string
var fBufferMaxAge          time.Duration
var fBufferMinPublish      time.Duration
var fVerbose               int

func init() {
	flag.StringVar(&fReceiverName, "receiver", "TestStation", "Name for this data source")
	flag.StringVar(&fHostPort, "host", "localhost:30003", "host:port of dump1090-box:30003")	
	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubTopic, "topic", "adsb-inbound",
		"Name of the pubsub topic to post to (set to empty for dry-run mode)")
	flag.StringVar(&fDump1090TimeLocation, "timeloc", "UTC",
		"Which timezone dump1090 thinks it is in (e.g. America/Los_Angeles)")
	flag.DurationVar(&fBufferMaxAge, "maxage", 10*time.Second,
		"If we're holding a message this old, ship 'em all out to pubsub")
	flag.DurationVar(&fBufferMinPublish, "minwait", 5*time.Second,
		"maxage notwithstanding, *always* wait at least this long between shipping bundles to pubsub")
	flag.IntVar(&fVerbose, "v", 0, "how verbose to get")	
	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)	
	Log.Printf("(max message age is %s, min interval is %s)\n", fBufferMaxAge, fBufferMinPublish)
	if fPubsubTopic == "" {
		Log.Printf("(no topic defined, in dry-run mode)\n")
	}

	addSIGINTHandler()
}

var done = make(chan struct{}) // Gets closed when everything is done
func weAreDone() bool {
	select{
	case <-done:
		return true
	default:
		return false
	}
}

func addSIGINTHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(sig <-chan os.Signal){
		<-sig
		Log.Printf("(SIGINT received)\n")
		close(done)
	}(c)
}

func getIoReader() io.Reader {
	for {
		if weAreDone() { break }
		if conn,err := net.Dial("tcp", fHostPort); err != nil {
			Log.Printf("connect '%s': err %s; trying again soon ...", fHostPort, err)
			time.Sleep(time.Second * 5)
		} else {
			Log.Printf("connecting to '%s'", fHostPort)
			return conn // a net.Conn implements io.Reader
		}
	}
	return nil
}

func publishMsgBundles(ch <-chan []*adsb.CompositeMsg) {
	c := pubsub.GetLocalContext(fProjectName)
	wg := &sync.WaitGroup{}

	for msgs := range ch {
		wg.Add(1)

		go func(msgs []*adsb.CompositeMsg) {
			if fVerbose > 0 {
				age := time.Since(msgs[0].GeneratedTimestampUTC)
				Log.Printf("-- flushing %d msgs, oldest %s\n", len(msgs), age)
				if fVerbose > 1 {
					for i,m := range msgs {
						Log.Printf(" [%2d] %s\n", i, m)
					}
				}
			}
			if fPubsubTopic != "" {
				if err := pubsub.PublishMsgs(c, fPubsubTopic, fReceiverName, msgs); err != nil {
					Log.Printf("-- err: %v\n", err)
				}
			}
			wg.Done()
		}(msgs)
		
		if weAreDone() { break }
	}

	wg.Wait()
	Log.Printf(" -- publishMsgBundles, clean shutdown\n")
}

func main() {
	adsb.TimeLocation = fDump1090TimeLocation  // We should really autodetect this, somehow

	// The message buffer will flush out bundles of messages to this channel
	ch := make(chan []*adsb.CompositeMsg, 3)
	go publishMsgBundles(ch)

	mb := msgbuffer.NewMsgBuffer()
	mb.FlushChannel = ch
	mb.MaxMessageAge = fBufferMaxAge
	mb.MinPublishInterval = fBufferMinPublish

outerLoop:
	for {
		scanner := bufio.NewScanner(getIoReader())
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				Log.Printf("scanner err (will retry): %v\n", err)
				time.Sleep(time.Second * 10)
				continue outerLoop
			}

			msg := adsb.Msg{}
			text := scanner.Text()
			if err := msg.FromSBS1(text); err != nil {
				Log.Printf("SBS parse fail '%v', input:%q", err, text)
				continue
			}
			mb.Add(&msg)

			offset := time.Since(msg.GeneratedTimestampUTC)
			if offset > time.Minute * 30 || offset < time.Minute * -30 {
				Log.Fatalf("do you need to set -timeloc ?\nNow = %s\nmsg = %s\n", time.Now(),
					msg.GeneratedTimestampUTC)
			}
			if weAreDone() { break outerLoop}
		}
		Log.Print("Scanner died, starting another in 5s ...")
		time.Sleep(time.Second * 5)
	}

	mb.FinalFlush()
	time.Sleep(5 * time.Second)
	Log.Print("Final clean shutdown")
}
