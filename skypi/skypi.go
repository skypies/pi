// The skypi application attaches to sockets that write out SBS formatted ADS-B messages.
// It filters out the interesting ones, and aggregates together fields from different messages, to
// build useful packets. These are then published up to a topic in Google Cloud PubSub in bundles.
package main

// cp serfr0-fdb-blahblah.json ~/.config/gcloud/application_default_credentials.json
// go get github.com/skypies/pi/skypi
// GOOS=linux GOARCH=arm go build github.com/skypies/pi/skypi

// $GOPATH/bin/skypi -receiver="MyStationName"
// ... maybe also: -h=southpi:30003 -maxage=4s -timeloc="America/Los_angeles" -v=2 -topic=""

import (
	"bufio"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/skypies/adsb"
	"github.com/skypies/adsb/msgbuffer"
	"github.com/skypies/util/pubsub"
)

var Log *log.Logger

var fHostPorts             string
var fProjectName           string
var fPubsubTopic           string
var fReceiverName          string
var fDump1090TimeLocation  string
var fBufferMaxAge          time.Duration
var fBufferMinPublish      time.Duration
var fVerbose               int

func init() {
	flag.StringVar(&fReceiverName, "receiver", "TestStation", "Name for this receiver gizmo")
	flag.StringVar(&fHostPorts, "hosts", "localhost:30003", "host:port[,host2:port2]")	
	flag.StringVar(&fProjectName, "project", "serfr0-fdb",
		"Name of the Google cloud project hosting the pubsub")
	flag.StringVar(&fPubsubTopic, "topic", "adsb-inbound",
		"Name of the pubsub topic to post to (set to empty for dry-run mode)")
	flag.StringVar(&fDump1090TimeLocation, "timeloc", "UTC",
		"Which timezone dump1090 thinks it is in (e.g. America/Los_Angeles)")
	flag.DurationVar(&fBufferMaxAge, "maxage", 2*time.Second,
		"If we're holding a message this old, ship 'em all out to pubsub")
	flag.DurationVar(&fBufferMinPublish, "minwait", 1500*time.Millisecond,
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

// acceptMsg is a goroutine which owns the message buffer data structure.
// All input sources of messages submit to this routine, which adds the to the message buffer.
func acceptMsg(msgChan <-chan *adsb.Msg, publishChan chan<- []*adsb.CompositeMsg) {
	mb := msgbuffer.NewMsgBuffer()
	mb.FlushChannel = publishChan
	mb.MaxMessageAge = fBufferMaxAge
	mb.MinPublishInterval = fBufferMinPublish

	for msg := range msgChan {
		mb.Add(msg)
		if weAreDone() { break }
	}

	mb.FinalFlush()

	close(publishChan)
	
	Log.Printf(" ---- acceptMsg, clean shutdown\n")
}

func publishMsgBundles(thisGoroutineWG *sync.WaitGroup, ch <-chan []*adsb.CompositeMsg) {
	thisGoroutineWG.Add(1)

	ctx := context.TODO()
	client := pubsub.NewClient(ctx,fProjectName)
	// c := pubsub.GetLocalContext(fProjectName)
	wg := &sync.WaitGroup{}
	
	for msgs := range ch {
		if len(msgs) == 0 { continue }

		wg.Add(1)

		go func(msgs []*adsb.CompositeMsg) {
			if fVerbose > 0 {
				age := time.Since(msgs[0].GeneratedTimestampUTC)
				Log.Printf("-- flushing %d msgs, oldest %s\n", len(msgs), age)
				if fVerbose > 1 { for i,m := range msgs { Log.Printf(" [%2d] %s\n", i, m) } }
			}
			if fPubsubTopic != "" {
				if err := pubsub.PublishMsgs(ctx, client, fPubsubTopic, fReceiverName, msgs); err != nil {
					Log.Printf("-- err: %v\n", err)
				}
			}
			wg.Done()
		}(msgs)
		
		if weAreDone() { break }
	}

	wg.Wait()
	thisGoroutineWG.Done() // Tell the master-controller that we're all finished
	Log.Printf(" ---- publishMsgBundles, clean shutdown\n")
}

// readMsgFromSocket will pull basestation (and extended basestation)
// formatted messages from the socket, and send them down the channel.
// It will retry the connection on failure.
func readMsgFromSocket(wg *sync.WaitGroup, hostport string, msgChan chan<-*adsb.Msg) {
	nTimeMismatches := 0
	lastBackoff := time.Second

	wg.Add(1)

outerLoop:
	for {
		if weAreDone() { break } // outer

		conn,err := net.Dial("tcp", hostport)
		if err != nil {
			Log.Printf("connect '%s': err %s; trying again in %s ...", hostport, err, lastBackoff*2)
			time.Sleep(lastBackoff)
			if lastBackoff < time.Minute*5 { lastBackoff *= 2 }
			continue
		}
		
		lastBackoff = time.Second
		Log.Printf("connected to '%s'", hostport)

		// a net.Conn implements io.Reader
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() { // This can block indefinitely ...
			if weAreDone() { break outerLoop }

			if err := scanner.Err(); err != nil {
				Log.Printf("killing connection, scanner err: %v\n", err)
				conn.Close()
				break // inner
			}

			msg := adsb.Msg{}
			text := scanner.Text()
			if err := msg.FromSBS1(text); err != nil {
				Log.Printf("killing connection, SBS  input:%q, parse fail: %v", text, err)
				break // inner
			}

			// If there is significant clock skew, we should bail. But, it seems
			// that sometimes we pick up stale data from dump1090; so wait to see
			// if it passes.
			offset := time.Since(msg.GeneratedTimestampUTC)
			if offset > time.Minute * 30 || offset < time.Minute * -30 {
				nTimeMismatches++
				if nTimeMismatches < 100 {
					continue // do not process this message
				} else {
					Log.Fatalf("100 bad msgs; set -timeloc ?\nNow = %s\nmsg = %s\n", time.Now(),
						msg.GeneratedTimestampUTC)
				}
			}

			// If the message is flagged as one we should mask, honor that
			if msg.IsMasked() {
				continue
			}
			
			msgChan <- &msg
		}
	}

	wg.Done()
	Log.Printf(" ---- readMsgFromSocket, clean shutdown\n")
}

func main() {
	adsb.TimeLocation = fDump1090TimeLocation  // We should really autodetect this, somehow

	// For clean shutdown, we need to know when various goroutines finish cleanly
	readersWaitgroup := &sync.WaitGroup{}
	publisherWG := &sync.WaitGroup{}

	// Setup the channel for new messages, and launch goroutines to write to it
	msgChan := make(chan *adsb.Msg, 20)
	for _,hostport := range strings.Split(fHostPorts, ",") {
		go readMsgFromSocket(readersWaitgroup, hostport, msgChan)
	}

	// Setup the channel for publishing outbound bundles of messages, and launch its goroutines
	publishChan := make(chan []*adsb.CompositeMsg, 3)
	go acceptMsg(msgChan, publishChan)
	go publishMsgBundles(publisherWG, publishChan)

	// Now wait until all the readers have closed down.
	<-done
	readersWaitgroup.Wait()
	close(msgChan) // Closing this triggers the publish chain to shutdown
	publisherWG.Wait()

	Log.Print(" ---- main, clean shutdown completed")
}
