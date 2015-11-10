/*
 * Copyright 2015 Manish R Jain <manishrjain@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/fvbock/trie"
)

var entities = flag.String("entities", "",
	"Gzipped file containing list of entities")
var rdf = flag.String("rdf", "",
	"Gzipped RDF data file.")
var output = flag.String("output", "",
	"Gzipped output file.")
var numroutines = flag.Int("numr", 10,
	"Number of goroutines to use for data processing.")
var glog = logrus.WithField("pkg", "namefinder")

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

type SyncWriter struct {
	sync.RWMutex
	wr *gzip.Writer
}

func (sw *SyncWriter) Write(buf []byte) {
	sw.Lock()
	defer sw.Unlock()
	_, err := sw.wr.Write(buf)
	if err != nil {
		glog.WithError(err).Fatal("Unable to write to writer.")
	}
}

func findAndWrite(idx int, tr trie.Trie, ch chan string,
	sw *SyncWriter, wg *sync.WaitGroup) {

	mlog := glog.WithField("routine", idx)
	buf := new(bytes.Buffer)
	buf.Grow(50 << 20) // 50 MB

	count := 0
	for l := range ch {
		count += 1
		if count%10000 == 3 {
			mlog.WithField("count", count).Debug("Processed rows.")
		}

		left := strings.Index(l, "<")
		if left < 0 {
			continue
		}
		right := strings.Index(l[left:], ">")
		e := l[left : right+1]

		if tr.Has(e) {
			buf.WriteString(l)
			buf.WriteRune('\n')
			if buf.Len() > 40<<20 { // 40 MB
				mlog.WithField("bytes", buf.Len()).Debug("Wrote bytes.")
				sw.Write(buf.Bytes())
				buf.Reset()
				if buf.Len() != 0 {
					mlog.Fatal("Length should be zero.")
				}
			}
		}
	}
	if buf.Len() > 0 {
		sw.Write(buf.Bytes())
	}
	wg.Done()
}

func grep(tr *trie.Trie) {
	f, scanner := getScanner(*rdf)
	defer f.Close()

	out, err := os.Create(*output)
	if err != nil {
		glog.WithError(err).Fatal("Unable to open output file.")
	}
	wr, err := gzip.NewWriterLevel(out, gzip.BestCompression)
	if err != nil {
		glog.WithError(err).Fatal("Unable to create gzip writer.")
	}
	sw := new(SyncWriter)
	sw.wr = wr

	ch := make(chan string, 100<<20) // 100 Million
	wg := new(sync.WaitGroup)
	for i := 0; i < *numroutines; i++ {
		wg.Add(1)
		go findAndWrite(i, *tr, ch, sw, wg)
	}
	for scanner.Scan() {
		ch <- scanner.Text()
	}
	close(ch)
	wg.Wait()

	if err := wr.Close(); err != nil {
		glog.WithError(err).Fatal("Unable to close output writer.")
	}
	if err := out.Close(); err != nil {
		glog.WithError(err).Fatal("Unable to close output file.")
	}
}

func main() {
	flag.Parse()
	logrus.SetLevel(logrus.DebugLevel)
	glog.WithField("numcpus", runtime.NumCPU()).Info("Number of cpus found.")

	procs := runtime.GOMAXPROCS(runtime.NumCPU())
	glog.WithField("prev_procs", procs).Info("Previous setting.")

	f, scanner := getScanner(*entities)
	defer f.Close()

	tr := trie.NewTrie()
	count := 0
	for scanner.Scan() {
		tr.Add(scanner.Text())
		count += 1
	}
	glog.WithField("count", count).Debug("Entities read.")
	grep(tr)

	/*
		var mstats runtime.MemStats
		runtime.ReadMemStats(&mstats)
		fmt.Printf("Memory Stats: %+v\n\n", mstats)
	*/
}
