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
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
)

var glog = logrus.WithField("package", "neoloader")
var rdfGzips = flag.String("rdfgzips", "",
	"Comma separated gzip files containing RDF data")
var mod = flag.Uint64("mod", 1, "Only pick entities, where uid % mod == 0.")
var threads = flag.Int("threads", 1, "Use these many threads.")

func main() {
	flag.Parse()

	numCpus := runtime.NumCPU()
	prevProcs := runtime.GOMAXPROCS(*threads)
	glog.WithField("num_cpu", numCpus).
		WithField("threads", *threads).
		WithField("prev_maxprocs", prevProcs).
		Info("Set max procs to threads")

	if len(*rdfGzips) == 0 {
		glog.Fatal("No RDF GZIP files specified")
	}

	db, err := neoism.Connect("http://localhost:7474/db/data")
	if err != nil {
		log.Fatal(err)
	}

	files := strings.Split(*rdfGzips, ",")
	for _, path := range files {
		if len(path) == 0 {
			continue
		}
		glog.WithField("path", path).Info("Handling...")
		f, err := os.Open(path)
		if err != nil {
			glog.WithError(err).Fatal("Unable to open rdf file.")
		}

		r, err := gzip.NewReader(f)
		if err != nil {
			glog.WithError(err).Fatal("Unable to create gzip reader.")
		}

		count, err := HandleRdfReader(db, r, *mod)
		if err != nil {
			glog.WithError(err).Fatal("While handling rdf reader.")
		}
		glog.WithField("count", count).Info("RDFs parsed")

		r.Close()
		f.Close()
	}

	p := neoism.Props{"_xid_": 10}
	n, created, err := db.GetOrCreateNode("Entity", "_xid_", p)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created", created)
	p, err = n.Properties()
	fmt.Println(n.Id(), p, err)
}
