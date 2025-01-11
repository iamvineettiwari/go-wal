package main

import (
	"fmt"
	"log"

	"github.com/iamvineettiwari/go-wal/wal"
)

func main() {
	w := wal.NewWal("data", 2)

	for i := 0; i < 100; i++ {
		w.Write([]byte(fmt.Sprintf("Hello - %d", i)))
	}

	w.Sync()

	data, err := w.ReadFromSegment(1)

	if err != nil {
		log.Println(err)
	}

	for _, item := range data {
		log.Println(string(item))
	}
}
