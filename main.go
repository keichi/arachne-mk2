package main

import (
	"errors"
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/davecheney/profile"
	metrics "github.com/rcrowley/go-metrics"
	"log"
	"math/rand"
	"net"
	"os"
	"regexp"
	"runtime"
	"time"
)

var boostrapNodes = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
}

func buildFindNodeRequest() []byte {
	buf := []byte("d1:ad2:i" +
		"d20:                    6:target20:                    e" +
		"1:q9:find_node1:t4:    1:y1:qe")
	rand.Read(buf[12 : 12+20])
	rand.Read(buf[43 : 43+20])
	rand.Read(buf[83 : 83+4])

	return buf
}

func parseNodeAddresses(nodes []byte) []*net.UDPAddr {
	var addrs []*net.UDPAddr

	for i := 20; i <= len(nodes)-6; i += 26 {
		addr := new(net.UDPAddr)
		addr.IP = net.IP(nodes[i : i+4])
		addr.Port = (int(nodes[i+4]) << 8) + int(nodes[i+5])

		addrs = append(addrs, addr)
	}

	return addrs
}

var regexpNodes = regexp.MustCompile(`5:nodes208:`)

func parseFindNodeResponse(b []byte) ([]byte, error) {
	matches := regexpNodes.FindSubmatchIndex(b)

	if len(matches) != 2 {
		return nil, errors.New("parseFindNodeResponse")
	}

	return b[matches[1] : matches[1]+208], nil
}

func recvLoop(q *queue.RingBuffer, conn *net.UDPConn, quit chan bool) {
	m1 := metrics.NewMeter()
	metrics.Register("rxPacketPerSec", m1)
	m2 := metrics.NewMeter()
	metrics.Register("nodesPacketPerSec", m2)

	visited := map[string]bool{}

	fmt.Println("Starting receive loop...")

loop:
	for {
		select {
		case <-quit:
			fmt.Println("Quitting receive loop...")
			break loop
		default:
		}

		var b2 [1024]byte
		n, _, err := conn.ReadFromUDP(b2[:])
		if n == 0 || err != nil {
			panic(err)
		}
		if n > 0 {
			m1.Mark(1)
			nodes, err := parseFindNodeResponse(b2[:])
			if err != nil {
				continue
			}
			addrs := parseNodeAddresses(nodes)
			m2.Mark(int64(len(addrs)))

			for _, v := range addrs {
				key := v.IP.String()

				if _, has := visited[key]; !has {
					q.Offer(v)
					visited[key] = true
				}
			}
		}
	}
}

func sendLoop(q *queue.RingBuffer, conn *net.UDPConn, quit chan bool) {
	m := metrics.NewMeter()
	metrics.Register("txPacketPerSec", m)

	g := metrics.NewGauge()
	metrics.Register("rngBufLen", g)

	fmt.Println("Starting send loop...")

loop:
	for {
		select {
		case <-quit:
			fmt.Println("Quitting send loop...")
			break loop
		default:
		}

		g.Update(int64(q.Len()))

		v, _ := q.Get()
		addr := v.(*net.UDPAddr)
		b := buildFindNodeRequest()

		conn.WriteToUDP(b, addr)

		m.Mark(1)
	}
}

func main() {
	defer profile.Start(profile.CPUProfile).Stop()
	rand.Seed(time.Now().UnixNano())

	runtime.GOMAXPROCS(runtime.NumCPU())

	q := queue.NewRingBuffer(1 << 17)

	for _, node := range boostrapNodes {
		if addr, err := net.ResolveUDPAddr("udp4", node); err == nil {
			q.Put(addr)
		}
	}

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	fmt.Println("Listening at:", conn.LocalAddr())
	defer conn.Close()
	if err != nil {
		panic(err)
	}
	conn.SetReadBuffer(100 * 1024 * 1024)

	qc1 := make(chan bool)
	go recvLoop(q, conn, qc1)

	qc2 := make(chan bool)
	go sendLoop(q, conn, qc2)

	go metrics.Log(metrics.DefaultRegistry, 10*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	time.Sleep(1 * time.Minute)

	qc1 <- true
	qc2 <- true
}
