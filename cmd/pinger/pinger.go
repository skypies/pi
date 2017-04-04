package main

import(
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"
	fastping "github.com/tatsushid/go-fastping"
)

var(
	timeout = time.Second * 10
	//ips = []string{"8.8.8.8"}

/*

traceroute to pubsub.googleapis.com (216.58.193.106), 30 hops max, 60 byte packets
 1  router.asus.com (192.168.111.1)  9.587 ms  9.399 ms  9.305 ms
 2  192.167.1.233 (192.167.1.233)  9.233 ms  9.161 ms  9.098 ms
 3  10.2.2.1 (10.2.2.1)  18.871 ms  23.156 ms  23.092 ms
 4  10.2.1.1 (10.2.1.1)  27.379 ms  27.318 ms *
 5  64-62-134-194.static.hilltopinternet.com (64.62.134.194)  88.423 ms  88.352 ms  88.279 ms
 6  10ge7-2.core1.sjc2.he.net (72.52.92.118)  88.204 ms 100ge1-1.core1.sjc2.he.net (184.105.213.94)  69.613 ms  69.476 ms
 7  72.14.219.161 (72.14.219.161)  22.931 ms  27.167 ms  22.722 ms
 8  216.239.49.168 (216.239.49.168)  27.015 ms  26.953 ms 216.239.49.170 (216.239.49.170)  23.696 ms
 9  209.85.246.20 (209.85.246.20)  23.554 ms  23.482 ms 209.85.249.63 (209.85.249.63)  23.418 ms
10  216.239.49.198 (216.239.49.198)  41.141 ms 72.14.232.63 (72.14.232.63)  41.080 ms  40.975 ms
11  209.85.254.19 (209.85.254.19)  49.680 ms 72.14.238.39 (72.14.238.39)  45.202 ms 209.85.254.19 (209.85.254.19)  53.848 ms
12  66.249.94.200 (66.249.94.200)  49.274 ms 74.125.37.210 (74.125.37.210)  49.179 ms 216.239.54.112 (216.239.54.112)  49.071 ms
13  64.233.174.51 (64.233.174.51)  35.900 ms  35.845 ms  34.978 ms
14  sea15s08-in-f10.1e100.net (216.58.193.106)  43.616 ms  39.054 ms  43.383 ms

*/
	
	ips = []string{"192.168.111.1","10.2.1.1","64.62.134.194","8.8.8.8", "216.58.193.106"}
)

func main() {
	sort.Strings(ips)

	p := fastping.NewPinger()
	p.Network("udp")
	p.MaxRTT = timeout

	for _,ip := range ips {
		ra, err := net.ResolveIPAddr("ip4:icmp", ip)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		p.AddIPAddr(ra)
	}

	results := map[string]string{}
	
	p.OnRecv = func(addr *net.IPAddr, rtt time.Duration) {
		results[addr.String()] = fmt.Sprintf("%.0f", rtt.Seconds() * 1000) // integer millis
	}

	if err := p.Run(); err != nil {
    fmt.Printf("p.Run failed with: %v", err)
	}

	strs := []string{time.Now().UTC().Format(time.RFC3339)}

	for _,ip := range ips {
		strs = append(strs, ip)
		if v,exists := results[ip]; exists {
			strs = append(strs, fmt.Sprintf("%-7.7s",v))
		} else {
			strs = append(strs, "timeout")
		}
	}
	
	fmt.Printf(strings.Join(strs,", ")+"\n")
}
