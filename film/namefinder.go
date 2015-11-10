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
	"compress/gzip"
	"flag"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/fvbock/trie"
)

var entities = flag.String("entities", "",
	"Gzipped file containing list of entities")
var rdf = flag.String("rdf", "",
	"Gzipped RDF data file.")
var output = flag.String("output", "",
	"Gzipped output file.")
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

	count := 0
	for scanner.Scan() {
		l := scanner.Text()
		left := strings.Index(l, "<")
		if left < 0 {
			continue
		}
		right := strings.Index(l[left:], ">")
		e := l[left : right+1]
		if tr.Has(e) {
			wr.Write([]byte(l + "\n"))
			count += 1
			if count%1000 == 3 {
				glog.WithField("count", count).Debug("Processed rows.")
			}
		}
	}
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
