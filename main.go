package main

import (
	"bma-arbeit/generator"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil)) // Starts a debug server
	}()
}

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	generator.GeneratePrimes()
}
