package main

// go run receiver.go -f ~/skypi/sbs1.out
// go run receiver.go -h northpi:30003

// https://godoc.org/google.golang.org/cloud/pubsub#Publish

import (
	"bufio"
	"bytes"
	"encoding/gob"
	//"fmt"	
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	//"time"

	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/skypies/adsb"
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

func getPubsubContext() context.Context {
	jsonKey, err := ioutil.ReadFile(fJsonAuthFile)
	if err != nil { Log.Fatal(err) }

	conf, err := google.JWTConfigFromJSON(
    jsonKey,
    pubsub.ScopeCloudPlatform,
    pubsub.ScopePubSub,
	)
	if err != nil { Log.Fatalf("JWTConfigFromJson failed: %v", err) }

	return cloud.NewContext(fProjectName, conf.Client(oauth2.NoContext))
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

// This function runs in its own goroutine, so as not to hold up reading new messages
func publishMsgs(c context.Context, msgs *[]*adsb.CompositeMsg) error {
	for i,msg := range *msgs {
		Log.Printf("- [%02d]%s\n", i, msg)
		(*msgs)[i].ReceiverName = fReceiverName // Claim this message, for upstream fame & glory
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(*msgs); err != nil {
		return err
	}
	msgIDs, err := pubsub.Publish(c, fPubsubTopic, &pubsub.Message{
    Data: buf.Bytes(),
	})

	if err != nil {
		Log.Fatalf("pubsub.Publish failed: %v", err)
	} else {
		Log.Printf("Published a message with a message id: %s\n", msgIDs[0])
	}
	
	return err
}

func main() {
	scanner := bufio.NewScanner(getIoReader())
	mb := msgbuffer.NewMsgBuffer()
	mb.MaxMessageAgeSeconds = 0

	adsb.TimeLocation = fDump1090TimeLocation  // This is kinda grievous
	
	c := getPubsubContext()
	mb.FlushFunc = func(msgs []*adsb.CompositeMsg) {
		go publishMsgs(c, &msgs)
	}
	
	for scanner.Scan() {
		msg := adsb.Msg{}
		text := scanner.Text()
		if err := msg.FromSBS1(text); err != nil {
			Log.Fatal(err)
			continue
		}
		mb.Add(&msg)

		if err := scanner.Err(); err != nil {
			Log.Fatal(err)
		}
	}
}
