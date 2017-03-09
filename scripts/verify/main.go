package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"

	"github.com/Sirupsen/logrus"
	"github.com/dgraph-io/dgraph/query/graph"
	"github.com/dgraph-io/dgraph/rdf"
	"github.com/dgraph-io/dgraph/x"
)

var (
	src  = flag.String("src", "", "Gzipped RDF data file.")
	dst  = flag.String("dst", "", "Gzipped RDF data file.")
	out  = flag.String("out", "diff.rdf", "Output differences to this file.")
	head = flag.Int("head", -1, "Number of lines to read from head. -1 means read all.")
	glog = logrus.WithField("pkg", "verify")
)

type R struct {
	s uint64
	p string
	o uint64
	v string

	os string
	oo string
}

func (r R) String() string {
	return fmt.Sprintf("%x %s %x %s [%s,%s]", r.s, r.p, r.o, r.v, r.os, r.oo)
}

type ByR []R

func (b ByR) Len() int {
	return len(b)
}

func equal(a, b R) bool {
	if a.s != b.s ||
		a.p != b.p ||
		a.o != b.o ||
		a.v != b.v {
		return false
	}
	return true
}

func less(bi, bj R) bool {
	if bi.s != bj.s {
		return bi.s < bj.s
	}
	if bi.p != bj.p {
		return bi.p < bj.p
	}
	if bi.o != bj.o {
		return bi.o < bj.o
	}
	return bi.v < bj.v
}

func (b ByR) Less(i int, j int) bool {
	bi := b[i]
	bj := b[j]
	return less(bi, bj)
}

func (b ByR) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

func getReader(fname string) (*os.File, *bufio.Reader) {
	f, err := os.Open(fname)
	if err != nil {
		glog.WithError(err).Fatal("Unable to open file")
	}

	r, err := gzip.NewReader(f)
	if err != nil {
		glog.WithError(err).Fatal("Unable to open file")
	}

	return f, bufio.NewReader(r)
}

func convert(n graph.NQuad) R {
	r := R{}
	var err error
	r.os = n.Subject
	r.s = rdf.GetUid(n.Subject)
	x.Checkf(err, "Subject: %v", n.Subject)
	r.p = n.Predicate

	if len(n.ObjectId) > 0 {
		r.o = rdf.GetUid(n.ObjectId)
		x.Checkf(err, "Object: %v", n.ObjectId)
		r.oo = n.ObjectId
	}
	r.v = n.ObjectValue.GetStrVal()
	return r
}

func compare(srcl, dstl []R) {
	var buf bytes.Buffer
	var ps, ds, loop, matches int
	for ps < len(srcl) && ds < len(dstl) {
		loop++
		if loop%100 == 0 {
			//fmt.Printf(".")
		}
		s := srcl[ps]
		d := dstl[ds]
		if equal(s, d) {
			//fmt.Printf("Found match: %v\n", s)
			matches++
			ps++
			ds++
		} else if less(s, d) {
			// s < d, advance s
			buf.WriteString(s.String())
			buf.WriteRune('\n')
			ps++
		} else {
			ds++
		}
	}
	fmt.Printf("Found matches: %v\n", matches)
	x.Check(ioutil.WriteFile(*out, buf.Bytes(), 0644))
}

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)
	var srcl, dstl []R
	f, bufReader := getReader(*src)
	var err error

	srcCount := 0
	var strBuf bytes.Buffer
	for {
		err = x.ReadLine(bufReader, &strBuf)
		if err != nil {
			break
		}
		srcCount++
		rnq, err := rdf.Parse(strBuf.String())
		x.Checkf(err, "Unable to parse line: [%v]", strBuf.String())
		srcl = append(srcl, convert(rnq))
	}
	if err != nil && err != io.EOF {
		err := x.Errorf("Error while reading file: %v", err)
		log.Fatalf("%+v", err)
	}
	x.Check(f.Close())
	fmt.Println("Source done")

	f, bufReader = getReader(*dst)
	dstCount := 0
	for {
		err = x.ReadLine(bufReader, &strBuf)
		if err != nil {
			break
		}
		dstCount++
		rnq, err := rdf.Parse(strBuf.String())
		x.Checkf(err, "Unable to parse line: [%v]", strBuf.String())
		dstl = append(dstl, convert(rnq))
	}
	if err != nil && err != io.EOF {
		err := x.Errorf("Error while reading file: %v", err)
		log.Fatalf("%+v", err)
	}
	x.Check(f.Close())

	fmt.Printf("Src: [%d] Dst: [%d]\n", srcCount, dstCount)
	sort.Sort(ByR(srcl))
	sort.Sort(ByR(dstl))
	fmt.Println("Comparing now")
	//for i := 0; i < 100; i++ {
	//fmt.Printf("[S,D] %v %v\n", srcl[i], dstl[i])
	//}
	compare(srcl, dstl)
}
