package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/dgraph-io/dgraph/x"
	farm "github.com/dgryski/go-farm"
	"github.com/twpayne/go-geom/encoding/geojson"
)

var (
	jsonFile = flag.String("geo", "", "Json file to upload")
	outFile  = flag.String("rdf", "", "File to be written to")
)

func findFeatureArray(dec *json.Decoder) error {
	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if s, ok := t.(string); ok && s == "features" && dec.More() {
			// we found the features element
			d, err := dec.Token()
			if err != nil {
				return err
			}
			if delim, ok := d.(json.Delim); ok {
				if delim.String() == "[" {
					// we have our start of the array
					break
				} else {
					// A different kind of delimiter
					return fmt.Errorf("Expected features to be an array.")
				}
			}
		}
	}

	if !dec.More() {
		return fmt.Errorf("Cannot find any features.")
	}
	return nil
}

func main() {
	flag.Parse()
	f, err := os.Open(*jsonFile)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", *jsonFile, err)
	}
	defer f.Close()

	g, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", *outFile, err)
	}
	defer g.Close()

	var r io.Reader
	r = f
	if strings.HasSuffix(*jsonFile, ".gz") {
		r, err = gzip.NewReader(f)
		if err != nil {
			log.Fatalf("Error reading gzip file %s: %v", *jsonFile, err)
		}
	}

	dec := json.NewDecoder(r)
	err = findFeatureArray(dec)
	x.Check(err)
	w := bufio.NewWriterSize(g, 1000000)
	gw, err := gzip.NewWriterLevel(w, gzip.BestCompression)
	x.Check(err)
	buf := new(bytes.Buffer)
	buf.Grow(50000)

	for dec.More() {
		var f geojson.Feature
		err := dec.Decode(&f)
		x.Check(err)
		geo, err := geojson.Marshal(f.Geometry)
		geoS := strings.Replace(string(geo), "\"", "'", 6)
		x.Check(err)
		uid := farm.Fingerprint32([]byte(f.ID))
		x.Check2(buf.WriteString(fmt.Sprintf("<_uid_:%#x> <loc> \"%s\"^^<geo:geojson> .\n", uid, geoS)))
		for k, v := range f.Properties {
			va, ok := v.(string)
			if !ok {
				continue
			}
			va = strings.Replace(va, "\"", "'", -1)
			pred := strings.Replace(strings.Trim(k, "@#"), ":", ".", -1)
			x.Check2(buf.WriteString(fmt.Sprintf("<_uid_:%#x> <%s> \"%s\" .\n", uid, pred, va)))
		}
		if buf.Len() > 40000 {
			gw.Write(buf.Bytes())
			buf.Reset()
		}
	}
	if buf.Len() > 0 {
		gw.Write(buf.Bytes())
		buf.Reset()
	}
	x.Check(gw.Flush())
	x.Check(gw.Close())
	x.Check(w.Flush())
}
