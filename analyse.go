package main

// go run analyse.go [-pos] -f ~/skypi/sbs1.out


import (
	"bufio"
	"fmt"	
	"flag"
	"log"
	"os"

	"github.com/skypies/adsb"
	
	"abw/histogram"  // Remove ?
)

var Log *log.Logger

var fFilename string
var fDumpPos  bool

func init() {
	flag.StringVar(&fFilename, "f", "", "sbs formatted CSV file thing to read")
	flag.BoolVar  (&fDumpPos,  "pos", false, "just dump out positions")
	flag.Parse()
	
	Log = log.New(os.Stdout,"", log.Ldate|log.Ltime)//|log.Lshortfile)
}

func main() {
	Log.Printf("reading file '%s' (dumpPos=%v)", fFilename, fDumpPos)

	h := histogram.Histogram{	
		ValMin:      0,
		ValMax:     80,
		NumBuckets: 80,
	}
	_=h
	if osFile, err := os.Open(fFilename); err != nil {
		Log.Fatal(err)
	} else {
		scanner := bufio.NewScanner(osFile) // os.File implements io.Reader
		for scanner.Scan() {
			m := adsb.Msg{}
			text := scanner.Text()
			if err := m.FromSBS1(text); err != nil {
				Log.Fatal(err)
				break
			}

			if fDumpPos {
				if m.HasPosition() {
					fmt.Printf("\"%.5f,%.5f\"\n", m.Position.Lat, m.Position.Long)
				}
			} else {
				Log.Print(m)
			}
			
		}
		if err := scanner.Err(); err != nil {
			Log.Fatal(err)
		}
	}
}
