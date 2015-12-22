// The receiver application attaches to a socket that writes out SBS formatted messages.
// It filters out the interesting ones, and aggregates together fields from different messages, to
// build useful packets. These are then published up to a topic in Google Cloud PubSub.
package main

// go get github.com/skypies/pi/receiver
// go build github.com/skypies/pi/receiver
// receiver -h northpi:30003

// go run receiver.go -f ~/skypi/sbs1.out

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"sync"
	
	"github.com/skypies/adsb"
	"github.com/skypies/pi/pubsub"
	"github.com/skypies/pi/msgbuffer"
)

var Log *log.Logger

var fFilename              string

var fReceiverName          string
var fHostPort              string
var fJsonAuthFile          string
var fProjectName           string
var fPubsubTopic           string
var fDump1090TimeLocation  string

func init() {
	flag.StringVar(&fFilename, "f", "", "sbs formatted CSV file thing to read")

	flag.StringVar(&fReceiverName, "receiver", "TestStation", "Name for this data source")
	flag.StringVar(&fHostPort, "h", "", "host:port of dump1090-box:30003")	
	flag.StringVar(&fJsonAuthFile, "auth", "serfr0-fdb-48c34ecd36c9.json", "The JSON auth file for a Google service worker account")
	flag.StringVar(&fProjectName, "project", "serfr0-fdb", "Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubTopic, "topic", "adsb-inbound", "Name of the pubsub topic to post to (short name, not projects/blah...)")
	flag.StringVar(&fDump1090TimeLocation, "timeloc", "UTC", "Which timezone dump1090 thinks it is in")
	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
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
	scanner := bufio.NewScanner(getIoReader())
	mb := msgbuffer.NewMsgBuffer()
	mb.MaxMessageAgeSeconds = 10//1000000 // Kinda screwy when reading old data from files

	adsb.TimeLocation = fDump1090TimeLocation  // This is grievous
	
	c := pubsub.GetContext(fJsonAuthFile, fProjectName)
	mb.FlushFunc = func(msgs []*adsb.CompositeMsg) {
		wg.Add(1)
		go func(msgs *[]*adsb.CompositeMsg) {
			pubsub.PublishMsgs(c, fPubsubTopic, fReceiverName, msgs)
			wg.Done()
		}(&msgs)
	}
	
	for scanner.Scan() {
		msg := adsb.Msg{}
		text := scanner.Text()
		if err := msg.FromSBS1(text); err != nil {
			Log.Print(err)
			continue
		}
		//msg.ReceiverName = fReceiverName // Claim this message, for upstream fame & glory
		mb.Add(&msg)

		if err := scanner.Err(); err != nil {
			Log.Fatal(err)
		}
	}

	mb.FinalFlush()
	wg.Wait()
	Log.Print("Final clean shutdown")
}
