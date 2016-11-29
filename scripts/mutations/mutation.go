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
	concurrent = flag.Int("concurrent", 1000, "Number of concurrent requests to make to Dgraph")
	numRdf     = flag.Int("mutations", 100, "Number of RDF N-Quads to send as part of a mutation.")
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

func makeRequest(mutation chan string, c *uint64, wg *sync.WaitGroup) {
	var counter uint64
	for m := range mutation {
		counter = atomic.AddUint64(c, 1)
		if counter%10000 == 0 {
			fmt.Printf("Request: %v\n", counter)
		}
		req, err := http.NewRequest("POST", *dgraph, strings.NewReader(body(m)))
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

func buildMutation(rdf chan string, m chan string, rdfCount *uint64, wg *sync.WaitGroup) {
	set := ""
	count := 0
	for r := range rdf {
		atomic.AddUint64(rdfCount, 1)
		count++
		set += r + "\n"
		if count == *numRdf {
			m <- set
			set = ""
			count = 0
		}

	}
	if set != "" {
		m <- set
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

	rdf := make(chan string, 1000)
	mutation := make(chan string, 100)

	var rdfCount uint64 = 0
	var wgm sync.WaitGroup
	for i := 0; i < 10; i++ {
		wgm.Add(1)
		go buildMutation(rdf, mutation, &rdfCount, &wgm)
	}

	var count uint64 = 0
	var wg sync.WaitGroup
	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go makeRequest(mutation, &count, &wg)
	}

	scanner := bufio.NewScanner(gr)
	for scanner.Scan() {
		// Lets send the rdf's to the channel.
		rdf <- scanner.Text()
	}
	x.Check(scanner.Err())
	close(rdf)

	wgm.Wait()
	close(mutation)
	wg.Wait()
	fmt.Println("Number of RDF's parsed: ", rdfCount)
	fmt.Println("Number of mutations run: ", count)
}
