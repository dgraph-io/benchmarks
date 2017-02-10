/*
 * Copyright 2017 Dgraph Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This program is used to read RDF files and load them into Neo4j.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/query/graph"
	"github.com/dgraph-io/dgraph/rdf"
	"github.com/dgraph-io/dgraph/types"
	"github.com/dgraph-io/dgraph/x"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

var (
	src        = flag.String("r", "", "Gzipped RDF data file.")
	concurrent = flag.Int("c", 20, "No of concurrent requests to perform.")
	size       = flag.Int("m", 200, "No of mutations to send per request.")
)

func getReader(fname string) (*os.File, *bufio.Reader) {
	f, err := os.Open(fname)
	if err != nil {
		log.Fatal("Unable to open file", err)
	}

	r, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal("Unable to open file", err)
	}

	return f, bufio.NewReader(r)
}

func getStrVal(rnq graph.NQuad) string {
	switch rnq.ObjectType {
	case 0:
		return rnq.ObjectValue.GetStrVal()
	case 5:
		src := types.Val{types.DateID, rnq.ObjectValue.GetDateVal()}
		dst, err := types.Convert(src, types.StringID)
		if err != nil {
			log.Fatal(err)
		}
		return dst.Value.(string)
	case 6:
		src := types.Val{types.DateTimeID, rnq.ObjectValue.GetDatetimeVal()}
		dst, err := types.Convert(src, types.StringID)
		if err != nil {
			log.Fatal(err)
		}
		return dst.Value.(string)
	}
	log.Fatal("Types should be one of the above.")
	return ""
}

func makeRequests(d bolt.DriverPool, wg *sync.WaitGroup, rc chan request) {
TRY:
	conn, err := d.OpenPool()
	if err != nil {
		time.Sleep(5 * time.Millisecond)
		goto TRY
	}

	for r := range rc {
		pipeline, err := conn.PreparePipeline(r.req...)
		if err != nil {
			log.Fatal(err)
			continue
		}
		_, err = pipeline.ExecPipeline(r.params...)
		if err != nil {
			log.Fatal(err)
		}

		atomic.AddUint64(&s.rdfs, uint64(len(r.req)))
		atomic.AddUint64(&s.mutations, 1)

		err = pipeline.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	conn.Close()
	wg.Done()
}

type request struct {
	req    []string
	params []map[string]interface{}
}

type state struct {
	rdfs      uint64
	mutations uint64
	start     time.Time
}

var s state

func printCounters(ticker *time.Ticker) {
	for range ticker.C {
		rdfs := atomic.LoadUint64(&s.rdfs)
		mutations := atomic.LoadUint64(&s.mutations)
		elapsed := time.Since(s.start)
		rate := float64(rdfs) / elapsed.Seconds()
		fmt.Printf("[Request: %6d] Total RDFs done: %8d RDFs per second: %7.0f\r", mutations, rdfs, rate)
	}
}

func main() {
	flag.Parse()
	driver, err := bolt.NewDriverPool("bolt://localhost:7687", *concurrent)
	if err != nil {
		log.Fatal(err)
	}

	s.start = time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	go printCounters(ticker)
	var wg sync.WaitGroup
	reqCh := make(chan request, 2*(*size))

	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go makeRequests(driver, &wg, reqCh)
	}

	fmt.Printf("\nProcessing %s\n", *src)
	f, bufReader := getReader(*src)

	var strBuf bytes.Buffer
	var r request
	count := 0
	for {
		err = x.ReadLine(bufReader, &strBuf)
		if err != nil {
			break
		}
		rnq, err := rdf.Parse(strBuf.String())
		pred := rnq.Predicate
		x.Checkf(err, "Unable to parse line: [%v]", strBuf.String())
		if len(rnq.ObjectId) > 0 {
			r.req = append(r.req, fmt.Sprintf("MERGE (n { xid: {xid1} }) MERGE (n2 { xid: {xid2} }) MERGE (n) - [r:`%s`] - (n2)", pred))
			r.params = append(r.params, map[string]interface{}{"xid1": rnq.Subject, "xid2": rnq.ObjectId})
		} else {
			// Merge will check if a node with this xid as property exists, else create it. In either case it will
			// add another property with the predicate.
			r.req = append(r.req, fmt.Sprintf("MERGE (n { xid: {xid} }) ON CREATE SET n.`%s` = {val} ON MATCH SET n.`%s` = {val}", pred, pred))
			r.params = append(r.params, map[string]interface{}{"xid": rnq.Subject, "val": getStrVal(rnq)})
		}
		count++
		if int(count)%(*size) == 0 {
			rc := request{
				req:    make([]string, len(r.req)),
				params: make([]map[string]interface{}, len(r.params)),
			}
			copy(rc.params, r.params)
			copy(rc.req, r.req)
			reqCh <- rc
			r.req = r.req[:0]
			r.params = r.params[:0]
		}

	}
	if err != nil && err != io.EOF {
		err := x.Errorf("Error while reading file: %v", err)
		log.Fatalf("%+v", err)
	}

	if len(r.req) > 0 {
		reqCh <- r
	}
	x.Check(f.Close())
	close(reqCh)
	wg.Wait()
}
