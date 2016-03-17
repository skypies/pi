// go run ./mockdump1090.go replay.go -p 30003 -delay=10s         (will listen on localhost:30003)
// go run ./mockdump1090.go replay.go -p 39003 -delay=10s -mlat   (generate MLAT messages)

// go run ./skypi.go -topic="" -v=1

package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"
	
	"github.com/skypies/geo"
	"github.com/skypies/adsb"
)

var delay time.Duration
var port int
var mlat bool
var replayFiles []string
var icaoWhitelist = map[string]int{}
func init() {
	flag.DurationVar(&delay, "delay", 0, "simulate messages delayed by this long (e.g. 1s, 2m30s)")
	flag.IntVar(&port, "p", 30003, "which port to listen on")
	flag.BoolVar(&mlat, "mlat", false, "send MLAT packets")

	replay := ""
	flag.StringVar(&replay, "replay", "", "comma-sep list of files to replay")

	wlist := ""
	flag.StringVar(&wlist, "ids", "", "Icao IDs to replay (comma-sep), blank for all")
	
	flag.Parse()

	replayFiles = strings.Split(replay, ",")
	for _,id := range strings.Split(wlist, ",") {
		icaoWhitelist[id] = 1
	}
}

func main() {
	if len(replayFiles) > 0 {
		replayData(replayFiles, icaoWhitelist)
	} else {
		generateData()
	}
}

func generateData() {
	fmt.Printf("(launching mock dump0190 on localhost:%d; mlat=%v)\n", port, mlat)

	ln, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d",port))

outerLoop:
	for {
		conn, _ := ln.Accept()
		fmt.Printf("(connection started)\n")

		m := adsb.Msg{
			Icao24: adsb.IcaoId("A81BD0"),
			Callsign: "ABW123",
			Type: "MSG",
			Altitude: 12345,
			GroundSpeed: 300,
			Track: 315,
			VerticalRate: 64,
			Position: geo.Latlong{36.0, -122.0},
			GeneratedTimestampUTC: time.Now().UTC(),
			LoggedTimestampUTC: time.Now().UTC(),
		}

		// We need to prime the pump, and trick the msgbuffer
		m.SubType = 3  // Get an entry in the sender table for our Icao, by proving we have pos data
		conn.Write([]byte(fmt.Sprintf("%s\n", m.ToSBS1())))
		m.SubType = 1  // Populate the sender table entry with a callsign (MSG,1 only)
		conn.Write([]byte(fmt.Sprintf("%s\n", m.ToSBS1())))
		m.SubType = 4  // Populate the sender table entry with velocity data (MSG,4 only)
		conn.Write([]byte(fmt.Sprintf("%s\n", m.ToSBS1())))
		m.SubType = 3 // All future messages are linear position updates (MSG,3 only)
		
		for {
			now := time.Now().UTC().Add(-1 * delay)
			m.Position.Lat += 0.01
			m.GeneratedTimestampUTC = now
			m.LoggedTimestampUTC = now

			if mlat { m.Type = "MLAT" }
			
			if _,err := conn.Write([]byte(fmt.Sprintf("%s\n", m.ToSBS1()))); err != nil {
				fmt.Printf("(connection ended)\n")
				continue outerLoop
			}

			time.Sleep(time.Millisecond * 1000)
		}
	}
}



