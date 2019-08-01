package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/dgryski/go-farm"

	"github.com/dgraph-io/dgraph/chunker"
	"github.com/dgraph-io/dgraph/x"

	"github.com/dgraph-io/dgo/protos/api"
)

var (
	input      = flag.String("input", "", "File with list of ids for the entity")
	data       = flag.String("data", "", "Path to data file to read")
	properties = flag.String("properties", "", "Comma separated list of properties")
	edges      = flag.String("edges", "", "Comma separated list of edges")
	output     = flag.String("output", "output.json", "Output file to write the data to")
)

func typeValFrom(val *api.Value) string {
	switch val.Val.(type) {
	case *api.Value_StrVal:
		return val.GetStrVal()
	case *api.Value_DatetimeVal:
		date := val.GetDatetimeVal()
		var t time.Time
		err := t.UnmarshalBinary(date)
		x.Check(err)
		val, err := t.MarshalText()
		x.Check(err)
		return string(val)
	case *api.Value_DefaultVal:
		return val.GetDefaultVal()
	}

	return ""
}

// 1. Read the ids from input file into a map.
// 2. Read RDF's from the given data file and skip those which don't match the input ids, properties
// and edges.
// 3. Store rest of them into a in-memory JSON map and write it out to a file.

func main() {
	flag.Parse()

	jsonData := make(map[uint32]map[string]interface{})
	f, err := os.Open(*input)
	x.Check(err)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		xid := scanner.Text()
		xid = strings.TrimSpace(xid)
		if xid != "" {
			id := farm.Fingerprint32([]byte(xid))
			jsonData[id] = map[string]interface{}{"id": id}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	props := strings.Split(*properties, ",")
	fmt.Println(props)
	p := make(map[string]bool)
	for _, pr := range props {
		p[pr] = true
	}

	edges := strings.Split(*edges, ",")
	fmt.Println(edges)
	e := make(map[string]bool)
	for _, ed := range edges {
		e[ed] = true
	}

	count := 0
	r, cleanup := chunker.FileReader(*data)
	defer cleanup()
	chunker := chunker.NewChunker(chunker.RdfFormat)
	x.Check(chunker.Begin(r))
	for {
		chunkBuf, err := chunker.Chunk(r)
		if chunkBuf != nil && chunkBuf.Len() > 0 {
			nqs, err := chunker.Parse(chunkBuf)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("err", err)
			}

			for _, nq := range nqs {
				uid := farm.Fingerprint32([]byte(nq.Subject))
				doc, ok := jsonData[uid]
				if !ok {
					continue
				}

				if nq.Lang != "" && nq.Lang != "en" {
					continue
				}
				if _, ok := p[nq.Predicate]; ok {
					ov := typeValFrom(nq.ObjectValue)
					doc[nq.Predicate] = ov
				} else if _, ok := e[nq.Predicate]; ok {
					oid := farm.Fingerprint32([]byte(nq.ObjectId))
					list, ok := doc[nq.Predicate].([]interface{})
					if !ok {
						list = []interface{}{}
					}
					list = append(list, oid)
					doc[nq.Predicate] = list
				}
				jsonData[uid] = doc
				count++
			}

		}
		if err == io.EOF {
			break
		} else if err != nil {
			x.Check(err)
		}
	}
	x.Check(chunker.End(r))
	fmt.Printf("Total nquads: %d\n", count)

	out := make([]interface{}, 0, len(jsonData))
	for _, d := range jsonData {
		out = append(out, d)
	}
	b, err := json.MarshalIndent(out, "", "\t")
	x.Check(err)
	err = ioutil.WriteFile(*output, b, 0644)
	x.Check(err)
}
