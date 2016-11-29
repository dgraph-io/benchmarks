// This script is used to load data into Dgraph from an RDF file by performing
// mutations using the HTTP interface.
//
// You can run the script like
// go build . && ./mutations --rdf $GOPATH/src/github.com/dgraph-io/benchmarks/data/names.gz --concurrent 100
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/x"
)

var (
	file       = flag.String("rdf", "", "Location of rdf file to load")
	dgraph     = flag.String("dgraph", "http://127.0.0.1:8080/query", "Dgraph server address")
	concurrent = flag.Int("concurrent", 10, "Number of concurrent requests to make to Dgraph")
)

func body(rdf string) string {
	return `mutation {
  set {
	` + rdf + `
      }
  }`
}

type response struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

var hc http.Client
var r response

func makeRequest(ch chan string, c *uint64, wg *sync.WaitGroup) {
	var counter uint64
	for m := range ch {
		counter = atomic.AddUint64(c, 1)
		fmt.Printf("Request: %v\n", counter)
		req, err := http.NewRequest("POST", *dgraph, strings.NewReader(m))
		x.Check(err)
		res, err := hc.Do(req)
		x.Check(err)

		body, err := ioutil.ReadAll(res.Body)
		x.Check(err)
		x.Check(json.Unmarshal(body, &r))
		if r.Code != "ErrorOk" {
			log.Fatalf("Error while performing mutation: %v, err: %v", m, r.Message)
		}
	}
	wg.Done()
}

func main() {
	flag.Parse()
	f, err := os.Open(*file)
	x.Check(err)
	defer f.Close()
	gr, err := gzip.NewReader(f)
	x.Check(err)

	hc = http.Client{Timeout: 5 * time.Minute}
	ch := make(chan string, 1000)
	var count uint64 = 0
	var wg sync.WaitGroup
	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go makeRequest(ch, &count, &wg)
	}

	scanner := bufio.NewScanner(gr)
	for scanner.Scan() {
		m := body(scanner.Text())
		ch <- m
	}
	x.Check(scanner.Err())
	close(ch)
	wg.Wait()
	fmt.Println("Final count of mutations run: ", count)
}
