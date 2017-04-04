package main

// go run ./mockdump1090.go replay.go -replay=30003.out,31009.out  (replay, rewriting timestamps)

// go run ./skypi.go -topic="" -v=1 -hosts=localhost:30003,localhost:31009

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"time"
	
	"github.com/skypies/adsb"
)

// {{{ file2msgs

type ByLoggedTime []adsb.Msg
func (a ByLoggedTime) Len() int           { return len(a) }
func (a ByLoggedTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLoggedTime) Less(i, j int) bool {
	return a[i].GeneratedTimestampUTC.Before(a[j].GeneratedTimestampUTC)
}

// Load all the messages, and sort into a single time-ordered stream.
func file2msgs(filename string) []adsb.Msg {
	file, err := os.Open(filename)
	if err != nil { log.Fatal(err) }
	defer file.Close()

	msgs := []adsb.Msg{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
    if err := scanner.Err(); err != nil { log.Fatal(err) }

		msg := adsb.Msg{}
		if err := msg.FromSBS1(scanner.Text()); err != nil {
			log.Fatal("Bad parse '%s'\n%v\n", scanner.Text(), err)
		}

		// We drop the useless "111" fields, so this doesn't work. Maybe add them in, for ToSBS1 ?
		//if msg.ToSBS1() != scanner.Text() {
		//	log.Fatalf("parse/print fail\nread: %s\ngen : %s\n", scanner.Text(), msg.ToSBS1())
		//}
		
		msgs = append(msgs, msg)
	}
	return msgs
}

// }}}
// {{{ handlePort

// Basically, turn an Accept event into something we can put into an select{} statement.
func handlePort(port int, socketChan chan<- *net.TCPConn) {
	ln, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d",port))
	for {
		conn,_ := ln.Accept()
		fmt.Printf("(connection made to :%d)\n", port)
		socketChan<- conn.(*net.TCPConn)
	}
}

// }}}
			
func replayData(files []string, whitelist map[string]int) {
	fmt.Printf("(loading %v)\n", files)

	msgs := []adsb.Msg{}
	for _,filename := range files {
		newMsgs := file2msgs(filename)
		msgs = append(msgs, newMsgs...)
	}
	sort.Sort(ByLoggedTime(msgs))
	fmt.Printf("(loaded %d lines from %d files)\n", len(msgs), len(files))

	adsbSocketChan := make(chan *net.TCPConn)
	mlatSocketChan := make(chan *net.TCPConn)
	
	go handlePort(30003, adsbSocketChan)
	go handlePort(31009, mlatSocketChan)

	adsbSockets := []*net.TCPConn{}
	mlatSockets := []*net.TCPConn{}
	
	i := 0
	delta := 1 * time.Second
	
	for {
		select {
		case socket := <-adsbSocketChan: adsbSockets = append(adsbSockets, socket)
		case socket := <-mlatSocketChan: mlatSockets = append(mlatSockets, socket)

		case <-time.After(delta):
			msg := msgs[i]

			msg.GeneratedTimestampUTC = time.Now().UTC()
			msg.LoggedTimestampUTC = time.Now().UTC()
			
			_,exists := whitelist[string(msg.Icao24)]
			if len(whitelist)==0 || exists {
				sockets := adsbSockets
				if msg.Type == "MLAT" { sockets = mlatSockets }
				for i,socket := range sockets {
					if _,err := socket.Write([]byte(fmt.Sprintf("%s\n", msg.ToSBS1()))); err != nil {
						fmt.Printf("write err: %v\n", err)
						sockets = append(sockets[:i], sockets[i+1:]...)
					}
				}
			
				fmt.Printf(">>> %s\n", msg.ToSBS1())
				//if msg.HasPosition() { fmt.Printf("MSG %s\n", msg.ToSBS1()) }
			}

			if i+2 >= len(msgs) {
				fmt.Printf("(ran out of data !)\n")
				return
			}
			i++
			delta = msgs[i+1].GeneratedTimestampUTC.Sub(msgs[i].GeneratedTimestampUTC)
		}
	}
}


// {{{ -------------------------={ E N D }=----------------------------------

// Local variables:
// folded-file: t
// end:

// }}}
