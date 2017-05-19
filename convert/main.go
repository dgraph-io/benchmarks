package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/dgraph-io/dgraph/protos"
	"github.com/dgraph-io/dgraph/rdf"
	"github.com/dgraph-io/dgraph/x"
)

var (
	input  = flag.String("input", "", "Input .gz file")
	output = flag.String("output", "out.rdf.gz", "Output .gz file")
)

func isRequired(nq protos.NQuad) bool {
	switch nq.Predicate {
	case "actor.film", "performance.film", "starring", "performance.actor":
		return true
	default:
		return false
	}
	return false
}

func main() {
	flag.Parse()
	o, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	x.Check(err)
	w := gzip.NewWriter(o)
	m := &sync.Mutex{}

	readFile := func(ch chan protos.NQuad) {
		f, err := os.Open(*input)
		x.Check(err)
		r, err := gzip.NewReader(f)
		x.Check(err)
		defer r.Close()
		defer f.Close()

		bufreader := bufio.NewReader(r)
		nline := 0
		log.Println("Starting conversion")
		for {
			line, err := bufreader.ReadString('\n')
			if err != nil && err == io.EOF {
				break
			}
			nline++
			if nline%1000000 == 0 {
				log.Printf("Finished %v lines\n", nline)
			}
			nq, err := rdf.Parse(line)
			x.Check(err)
			if nq.Predicate == "name" {
				m.Lock()
				w.Write([]byte(line))
				m.Unlock()
			}
			if !isRequired(nq) {
				continue
			}
			ch <- nq
		}
		close(ch)
	}

	ch := make(chan protos.NQuad, 100000)
	go readFile(ch)

	actormp := make(map[string]string)
	starringmp := make(map[string]string)
	r1 := make(map[string]string)
	r2 := make(map[string]string)
	for nq := range ch {
		if nq.Predicate == "actor.film" {
			actormp[nq.ObjectId] = nq.Subject
		} else if nq.Predicate == "starring" {
			starringmp[nq.ObjectId] = nq.Subject
		} else if nq.Predicate == "performance.film" {
			if d, ok := actormp[nq.Subject]; ok {
				writeOut(m, w, d, "acted_in", nq.ObjectId)
				continue
			}
			r1[nq.Subject] = nq.ObjectId
		} else if nq.Predicate == "performance.actor" {
			if d, ok := starringmp[nq.Subject]; ok {
				writeOut(m, w, d, "actors", nq.ObjectId)
				continue
			}
			r2[nq.Subject] = nq.ObjectId
		}
		nq.String()
	}

	for k, v := range r1 {
		if d, ok := actormp[k]; ok {
			writeOut(m, w, d, "acted_in", v)
		}
	}
	for k, v := range r2 {
		if d, ok := starringmp[k]; ok {
			writeOut(m, w, d, "actors", v)
		}
	}
	x.Check(w.Flush())
	x.Check(w.Close())
	x.Check(o.Close())
}

func writeVal(m *sync.Mutex, w *gzip.Writer, s, p, o string) {
	str := fmt.Sprintf("<%v> <%v> \"%v\" .\n", s, p, o)
	m.Lock()
	defer m.Unlock()
	w.Write([]byte(str))
}

func writeOut(m *sync.Mutex, w *gzip.Writer, s, p, o string) {
	str := fmt.Sprintf("<%v> <%v> <%v> .\n", s, p, o)
	m.Lock()
	defer m.Unlock()
	w.Write([]byte(str))
}
