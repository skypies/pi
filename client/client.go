package main

// go run client.go -f ~/skypi/sbs1.out
// go run client.go -h skypi:30003


import (
	"bufio"
	//"fmt"	
	"flag"
	"io"
	"log"
	"net"
	"os"
	"time"
	
	"github.com/skypies/adsb"
	"github.com/skypies/pi/tracktable"
)

var Log *log.Logger

var fHostPort string
var fFilename string
var fDumpPos  bool

func init() {
	flag.StringVar(&fHostPort, "h", "", "host:port of dump1090:30003")
	flag.StringVar(&fFilename, "f", "", "sbs formatted CSV file thing to read")
	flag.BoolVar  (&fDumpPos,  "pos", false, "just dump out positions")
	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
}

func getIoReader() io.Reader {
	if fFilename != "" {
		if osFile, err := os.Open(fFilename); err != nil {
			panic(err)
		} else {
			Log.Printf("reading file '%s' (dumpPos=%v)", fFilename, fDumpPos)
			return osFile
		}
	} else if fHostPort != "" {
		if conn,err := net.Dial("tcp", fHostPort); err != nil {
			panic(err)
		} else {
			Log.Printf("connecting to '%s' (dumpPos=%v)", fHostPort, fDumpPos)
			return conn // a net.Conn implements io.Reader
		}

	} else {
		panic("No inputs defined")
	}
}

var kSweepAfter = 100

func main() {
	table := tracktable.New()
	table.WaitTime = 300 // If a given transponder goes quiet for 3000, ship the track
	table.StationName = "ScottsValley"
	table.PostUrl = "http://stop.jetnoise.net/fdb/addtrack"
	// ... etc etc
	
	// Main goroutine: read input, add it to the TrackTable
	i := 1
outerLoop:
	for {
		scanner := bufio.NewScanner(getIoReader())
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				Log.Printf("scanner err (will retry): %v\n", err)
				time.Sleep(time.Second * 10)
				continue outerLoop
			}

			m := adsb.Msg{}
			text := scanner.Text()
			if err := m.FromSBS1(text); err != nil {
				Log.Printf("SBS parse fail '%v', input:%q", err, text)
				continue
			}

			//Log.Printf("   --- %s\n", text)
			table.AddMessage(&m)
			if (i % kSweepAfter) == 0 { table.Sweep() }
			i++
		}

		Log.Print("Scanner died, starting another in 5s ...")
		time.Sleep(time.Second * 5)
	}
//	table.Sweep()
//	time.Sleep(1 * time.Second)
//	Log.Printf("Processed %d records\nTable:-\n%s", i, table)
}
