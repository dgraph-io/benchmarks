package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/Sirupsen/logrus"
	"github.com/dgraph-io/dgraph/rdf"
	"github.com/dgraph-io/dgraph/x"
)

var (
	src  = flag.String("src", "", "Gzipped RDF data file.")
	dst  = flag.String("dst", "", "Gzipped RDF data file.")
	head = flag.Int("head", -1, "Number of lines to read from head. -1 means read all.")
	glog = logrus.WithField("pkg", "verify")
)

type R struct {
	s uint64
	p string
	o uint64
	v string
}

type ByR []R

func (b ByR) Len() int {
	return len(b)
}

func less(bi, bj R) bool {
	if bi.s < bj.s {
		return true
	}
	if bi.p < bj.p {
		return true
	}
	if bi.o < bj.o {
		return true
	}
	return bi.v <= bj.v
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
	r.s, err = rdf.GetUid(n.Subject)
	x.Checkf(err, "Subject: %v", n.Subject)
	r.p = n.Predicate

	if len(n.ObjectId) > 0 {
		r.o, err = rdf.GetUid(n.ObjectId)
		x.Checkf(err, "Object: %v", n.ObjectId)
	}
	r.v = string(n.ObjectValue)
	return r
}

func compare(srcl, dstl []R) {
	for _, s := range srcl {
		i := sort.Search(len(dstl), func(i int) bool {
			return less(s, dstl[i])
		})
		if i < len(dstl) {
			if s == dstl[i] {
				fmt.Printf("MATCH: %v %v\n", s, dstl[i])
			} else {
				fmt.Printf("DIFFERENT: %v %v\n", s, dstl[i])
				return
			}
		} else {
			fmt.Printf("NO MATCH: %x %v %x %q\n", s.s, s.p, s.o, s.v)
			return
		}
	}
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
	compare(srcl, dstl)
}
