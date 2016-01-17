package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	
	"golang.org/x/net/context"
	"google.golang.org/appengine/socket"

	"github.com/skypies/flightdb2/fa"
)

func firehoseBySocket(c context.Context) string {
	host, port := "firehose.flightaware.com", 1501

	str := fmt.Sprintf("\n**** Connecting to %s:%d\n", host, port)
	
	tcpConn,err := socket.Dial(c, "tcp", fmt.Sprintf("%s:%d", host, port))

	tlsConfig := tls.Config{ServerName:host}
	tlsConn := tls.Client(tcpConn, &tlsConfig)

	if err != nil {
		str += fmt.Sprintf("* conn err: %v\n", err)
		return str
	}
	defer tlsConn.Close()

	cmd := fmt.Sprintf("live username %s password %s\n", TestAPIUsername, TestAPIKey)
	if _,err := tlsConn.Write([]byte(cmd)); err != nil {
		str += fmt.Sprintf("* write err: %v\n", err)
		return str
	}

	scanner := bufio.NewScanner(tlsConn)
	for scanner.Scan() {
		msgText := scanner.Text()
		if err := scanner.Err(); err != nil {
			str += fmt.Sprintf("scanner err: %v\n", err)
			break
		}

		m := fa.FirehoseMessage{}
		if err := json.Unmarshal([]byte(msgText), &m); err != nil {
			str += fmt.Sprintf("<!!<\n%s\n", msgText)
			continue
		}
		str += fmt.Sprintf("<<<< %s\n%v\n", msgText, m)	
	}

	return str
}


/* use native golang network libs

func firehoseSilliness() string {
	firehoseUrl := "firehose.flightaware.com:1501"

	str := "\n**** Connecting to "+firehoseUrl+"\n"
	
	conn,err := tls.Dial("tcp", firehoseUrl, nil)
	if err != nil {
		str += fmt.Sprintf("* conn err: %v\n", err)
		return str
	}
	defer conn.Close()

	cmd := fmt.Sprintf("live username %s password %s\n", TestAPIUsername, TestAPIKey)
	if _,err := conn.Write([]byte(cmd)); err != nil {
		str += fmt.Sprintf("* write err: %v\n", err)
		return str
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		str := scanner.Text()
		if err := scanner.Err(); err != nil {
			str += fmt.Sprintf("scanner err: %v\n", err)
			break
		}

		m := fa.FirehoseMessage{}
		if err := json.Unmarshal([]byte(str), &m); err != nil {
			str += fmt.Sprintf("<!!<\n%s\n", str)
			continue
		}
		str += fmt.Sprintf("<<<< %s\n%v\n", str, m)	
	}

	return str
}
*/
