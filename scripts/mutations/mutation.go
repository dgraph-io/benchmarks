// This script is used to load data into Dgraph from an RDF file by performing
// mutations using the HTTP interface.
//
// You can run the script like
// go build . && ./mutations --rdf $GOPATH/src/github.com/dgraph-io/benchmarks/data/names.gz --concurrent 100
package main

import (
	"bufio"
	"bytes"
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
	file       = flag.String("r", "", "Location of rdf file to load")
	dgraph     = flag.String("d", "http://127.0.0.1:8080/query", "Dgraph server address")
	concurrent = flag.Int("c", 1000, "Number of concurrent requests to make to Dgraph")
	numRdf     = flag.Int("m", 100, "Number of RDF N-Quads to send as part of a mutation.")
)

func body(rdf string) string {
	return fmt.Sprintf("mutation { set { %s } }", rdf)
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
		if counter%100 == 0 {
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

func main() {
	flag.Parse()
	f, err := os.Open(*file)
	x.Check(err)
	defer f.Close()
	gr, err := gzip.NewReader(f)
	x.Check(err)

	hc = http.Client{Timeout: time.Minute}
	mutation := make(chan string, 3*(*concurrent))

	var count uint64 = 0
	var wg sync.WaitGroup
	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go makeRequest(mutation, &count, &wg)
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(gr)
	num := 0
	var rdfCount uint64 = 0
	for scanner.Scan() {
		buf.WriteString(scanner.Text())
		buf.WriteRune('\n')

		if num >= *numRdf {
			mutation <- buf.String()
			buf.Reset()
			num = 0
		}
		rdfCount++
		num++
	}
	x.Check(scanner.Err())
	if buf.Len() > 0 {
		mutation <- buf.String()
	}
	close(mutation)

	wg.Wait()
	fmt.Println("Number of RDF's parsed: ", rdfCount)
	fmt.Println("Number of mutations run: ", count)
}
