// The receiver application attaches to a socket that writes out SBS formatted messages.
// It filters out the interesting ones, and aggregates together fields from different messages, to
// build useful packets. These are then published up to a topic in Google Cloud PubSub.
package main

// cp  serfr0-fdb-blahblah.json ~/.config/gcloud/application_default_credentials.json
// go get github.com/skypies/pi/receiver
// go build github.com/skypies/pi/receiver
// $GOPATH/bin/receiver -h northpi:30003 -receiver="MyStationName"
// ... maybe also:  -maxage=4 -timeloc="America/Los_angeles"

// go run receiver.go -f ~/skypi/sbs1.out

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
	
	"github.com/skypies/adsb"
	"github.com/skypies/adsb/msgbuffer"
	"github.com/skypies/util/pubsub"
)

var Log *log.Logger

var fFilename              string

var fHostPort              string
var fProjectName           string
var fPubsubTopic           string

var fReceiverName          string
var fDump1090TimeLocation  string
var fBufferMaxAgeSeconds   int64
var fVerbose               int

func init() {
	flag.StringVar(&fFilename, "f", "", "sbs formatted CSV file thing to read")

	flag.StringVar(&fReceiverName, "receiver", "TestStation", "Name for this data source")
	flag.StringVar(&fHostPort, "h", "", "host:port of dump1090-box:30003")	
	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubTopic, "topic", "adsb-inbound",
		"Name of the pubsub topic to post to (short name, not projects/blah...)")
	flag.StringVar(&fDump1090TimeLocation, "timeloc", "UTC",
		"Which timezone dump1090 thinks it is in (e.g. America/Los_Angeles)")

	flag.Int64Var(&fBufferMaxAgeSeconds, "maxage", 10,
		"How many seconds we wait before shipping a batch of messages out to pubsub")
	flag.IntVar(&fVerbose, "v", 0, "how verbose to get")
	
	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
	
	Log.Printf("(max message age is %d seconds)\n", fBufferMaxAgeSeconds)
	if fPubsubTopic == "" {
		Log.Printf("(no topic defined, in dry-run mode)\n")
	}
}

func getIoReader() io.Reader {
	if fFilename != "" {
		if osFile, err := os.Open(fFilename); err != nil {
			panic(err)
		} else {
			Log.Printf("reading file '%s'", fFilename)
			return osFile
		}
	} else if fHostPort != "" {
		if conn,err := net.Dial("tcp", fHostPort); err != nil {
			panic(err)
		} else {
			Log.Printf("connecting to '%s'", fHostPort)
			return conn // a net.Conn implements io.Reader
		}
	} else {
		panic("No inputs defined")
	}
}

func main() {
	wg := &sync.WaitGroup{}

	mb := msgbuffer.NewMsgBuffer()

	mb.MaxMessageAgeSeconds = fBufferMaxAgeSeconds // Not applicable when reading from files
	adsb.TimeLocation = fDump1090TimeLocation  // We should really autodetect this, somehow
	
	c := pubsub.GetLocalContext(fProjectName)
	mb.FlushFunc = func(msgs []*adsb.CompositeMsg) {
		wg.Add(1)
		go func(msgs []*adsb.CompositeMsg) {
			if fVerbose > 0 {
				Log.Printf("-- flushing %d msgs\n", len(msgs))
				if fVerbose > 1 {
					for i,m := range msgs {
						Log.Printf(" [%2d] %s\n", i, m)
					}
				}
			}
			if fPubsubTopic != "" {
				pubsub.PublishMsgs(c, fPubsubTopic, fReceiverName, msgs)
			}
			wg.Done()
		}(msgs)
	}

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
		}
		Log.Print("Scanner died, starting another in 5s ...")
		time.Sleep(time.Second * 5)
	}

	mb.FinalFlush()
	wg.Wait()
	Log.Print("Final clean shutdown")
}
