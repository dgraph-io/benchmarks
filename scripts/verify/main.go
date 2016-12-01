package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/Sirupsen/logrus"
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

func getScanner(fname string) (*os.File, *bufio.Scanner) {
	f, err := os.Open(fname)
	if err != nil {
		glog.WithError(err).Fatal("Unable to open file")
	}

	r, err := gzip.NewReader(f)
	if err != nil {
		glog.WithError(err).Fatal("Unable to open file")
	}

	return f, bufio.NewScanner(r)
}

func convert(n rdf.NQuad) R {
	r := R{}
	var err error
	r.os = n.Subject
	r.s, err = rdf.GetUid(n.Subject)
	x.Checkf(err, "Subject: %v", n.Subject)
	r.p = n.Predicate

	if len(n.ObjectId) > 0 {
		r.o, err = rdf.GetUid(n.ObjectId)
		x.Checkf(err, "Object: %v", n.ObjectId)
		r.oo = n.ObjectId
	}
	r.v = string(n.ObjectValue)
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
	f, scanner := getScanner(*src)

	srcCount := 0
	for scanner.Scan() {
		srcCount++
		rnq, err := rdf.Parse(scanner.Text())
		x.Checkf(err, "Unable to parse line: [%v]", scanner.Text())
		srcl = append(srcl, convert(rnq))
	}
	x.Check(scanner.Err())
	x.Check(f.Close())
	fmt.Println("Source done")

	f, scanner = getScanner(*dst)
	dstCount := 0
	for scanner.Scan() {
		dstCount++
		rnq, err := rdf.Parse(scanner.Text())
		x.Checkf(err, "Unable to parse line: [%v]", scanner.Text())
		dstl = append(dstl, convert(rnq))
	}
	x.Check(scanner.Err())
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
