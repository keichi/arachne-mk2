package main

import (
	"errors"
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/davecheney/profile"
	metrics "github.com/rcrowley/go-metrics"
	// "log"
	"math/rand"
	"net"
	// "os"
	"regexp"
	"runtime"
	"strconv"
	"time"
)

var boostrapNodes = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
}

func buildFindNodeRequest() []byte {
	buf := []byte("d1:a" +
		"d2:id20:                    6:target20:                    e" +
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
		addr.IP = net.IPv4(nodes[i], nodes[i+1], nodes[i+2], nodes[i+3])
		addr.Port = (int(nodes[i+4]) << 8) + int(nodes[i+5])

		addrs = append(addrs, addr)
	}

	return addrs
}

var regexpNodes = regexp.MustCompile(`5:nodes([0-9]+):`)

func parseFindNodeResponse(b []byte) ([]byte, error) {
	matches := regexpNodes.FindSubmatch(b)
	if len(matches) != 2 {
		return nil, errors.New("parseFindNodeResponse")
	}

	nodesLen, _ := strconv.Atoi(string(matches[1]))
	if nodesLen == 0 {
		return nil, errors.New("parseFindNodeResponse")
	}

	indices := regexpNodes.FindSubmatchIndex(b)
	if len(indices) < 2 {
		return nil, errors.New("parseFindNodeResponse")
	}

	return b[indices[1] : indices[1]+nodesLen], nil
}

func recvLoop(q *queue.RingBuffer, conn *net.UDPConn, quit chan bool) {
	m1 := metrics.NewMeter()
	metrics.Register("skipNodesPerSec", m1)
	m2 := metrics.NewMeter()
	metrics.Register("nodesPerSec", m2)

	var buf [64 * 1024]byte
	visited := make(map[string]byte)

	fmt.Println("Starting receive loop...")

loop:
	for {
		select {
		case <-quit:
			break loop
		default:
		}

		if n, _, err := conn.ReadFromUDP(buf[:]); n == 0 || err != nil {
			continue
		} else {
			nodes, err := parseFindNodeResponse(buf[:])
			if err != nil {
				continue
			}
			addrs := parseNodeAddresses(nodes)

			for _, v := range addrs {
				key := string([]byte(v.IP))

				if visited[key] < 10 {
					m2.Mark(1)
					q.Offer(v)
					visited[key] += 1
				} else {
					m1.Mark(1)
				}
			}
		}
	}

	fmt.Println("Quitting receive loop...")
	fmt.Printf("Number of nodes visited: %d\n", len(visited))
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
			break loop
		default:
		}

		g.Update(int64(q.Len()))

		v, err := q.Get()
		if err != nil {
			break loop
		}
		addr := v.(*net.UDPAddr)
		b := buildFindNodeRequest()

		conn.WriteToUDP(b, addr)

		m.Mark(1)
	}

	fmt.Println("Quitting send loop...")
}

func seedQueue(q *queue.RingBuffer) {
	for _, node := range boostrapNodes {
		if addr, err := net.ResolveUDPAddr("udp4", node); err == nil {
			q.Offer(addr)
		}
	}
}

func main() {
	defer profile.Start(profile.CPUProfile).Stop()
	rand.Seed(time.Now().UnixNano())

	runtime.GOMAXPROCS(runtime.NumCPU())

	q := queue.NewRingBuffer(1 << 20)

	seedQueue(q)

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

	//	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	time.Sleep(20 * time.Minute)

	q.Dispose()
	qc1 <- true
	qc2 <- true
}
