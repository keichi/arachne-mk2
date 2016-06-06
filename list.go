package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/syndtr/goleveldb/leveldb"
)

func toUDPAddr(b []byte) *net.UDPAddr {
	addr := new(net.UDPAddr)
	r := bytes.NewReader(b)

	var ip [4]byte
	if err := binary.Read(r, binary.BigEndian, &ip); err != nil {
		return nil
	}
	addr.IP = ip[:]

	var port uint16
	if err := binary.Read(r, binary.BigEndian, &port); err != nil {
		return nil
	}
	addr.Port = int(port)

	return addr
}

func main() {
	db, err := leveldb.OpenFile("leveldb", nil)
	defer db.Close()
	if err != nil {
		panic(err)
	}

	count := 0
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		// key := iter.Key()
		// addr := toUDPAddr(key)
		// fmt.Println(addr)
		count += 1
	}
	iter.Release()

	fmt.Println(count)
}
