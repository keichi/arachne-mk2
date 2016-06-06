package main

import (
	"bytes"
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/davecheney/profile"
	bencode "github.com/jackpal/bencode-go"
	metrics "github.com/rcrowley/go-metrics"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"time"
)

var boostrapNodes = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
}

type krpcRequest struct {
	TransactionID string            `bencode:"t"`
	Type          string            `bencode:"y"`
	Query         string            `bencode:"q"`
	Arguments     map[string]string `bencode:"a"`
}

type krpcFindNodeResponse struct {
	TransactionID string `bencode:"t"`
	Type          string `bencode:"y"`
	Response      struct {
		ID    string `bencode:"id"`
		Nodes string `bencode:"nodes"`
	} `bencode:"r"`
}

func getRandomString(c int) string {
	b := make([]byte, c)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	return string(b)
}

func buildFindNodeRequest() ([]byte, error) {
	r := krpcRequest{
		TransactionID: getRandomString(4),
		Type:          "q",
		Query:         "find_node",
		Arguments: map[string]string{
			"id":     getRandomString(20),
			"target": getRandomString(20),
		},
	}

	buf := new(bytes.Buffer)
	err := bencode.Marshal(buf, r)

	return buf.Bytes(), err
}

func parseNodeAddresses(resp *krpcFindNodeResponse) []*net.UDPAddr {
	nodes := resp.Response.Nodes
	var addrs []*net.UDPAddr

	for i := 20; i <= len(nodes)-6; i += 26 {
		addr := new(net.UDPAddr)
		addr.IP = net.IP(nodes[i : i+4])
		addr.Port = (int(nodes[i+4]) << 8) + int(nodes[i+5])

		addrs = append(addrs, addr)
	}

	return addrs
}

func parseFindNodeResponse(b []byte) (*krpcFindNodeResponse, error) {
	resp := new(krpcFindNodeResponse)
	reader := bytes.NewReader(b)

	err := bencode.Unmarshal(reader, resp)

	return resp, err
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
			resp, err := parseFindNodeResponse(b2[:])
			if err != nil {
				continue
			}
			addrs := parseNodeAddresses(resp)
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
		b, _ := buildFindNodeRequest()

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
