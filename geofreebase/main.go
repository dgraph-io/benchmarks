package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dgraph-io/dgraph/x"
	"github.com/twpayne/go-geom/encoding/geojson"
)

var geoData = flag.String("geojson", "", "Path to geoJson file having country data.")
var rdf = flag.String("countryrdf", "", "Path to rdf file having country data.")

func readLine(r *bufio.Reader, buf *bytes.Buffer) error {
	isPrefix := true
	var err error
	buf.Reset()
	for isPrefix && err == nil {
		var line []byte
		// The returned line is an internal buffer in bufio and is only
		// valid until the next call to ReadLine. It needs to be copied
		// over to our own buffer.
		line, isPrefix, err = r.ReadLine()
		if err == nil {
			buf.Write(line)
		}
	}
	return err
}

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
	f, err := os.Open(*geoData)
	x.Check(err)

	gr, err := gzip.NewReader(f)
	x.Check(err)

	//var strBuf bytes.Buffer
	//bufReader := bufio.NewReader(gr)
	dec := json.NewDecoder(gr)

	countryToGeo := make(map[string]string)
	findFeatureArray(dec)
	for dec.More() {
		var f geojson.Feature
		err := dec.Decode(&f)
		fmt.Println(f.Properties["NAME_LONG"])
		gg, err := geojson.Marshal(f.Geometry)
		ggg := strings.Replace(string(gg), "\"", "'", -1)
		country, ok := f.Properties["NAME_LONG"].(string)
		if ok {
			countryToGeo[country] = ggg
		}
		//fmt.Printf("\"%s\"", ggg)
		if err != nil {
			fmt.Println(err)
		}
	}
	gr.Close()
	f.Close()

	f, err = os.Open(*rdf)
	x.Check(err)

	gr, err = gzip.NewReader(f)
	x.Check(err)

	scanner := bufio.NewScanner(gr)

	out, err := os.Create("countryGeoData")
	x.Check(err)
	defer out.Close()
	count1, count2 := 0, 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "@en") {
			items := strings.Split(line, "\t")
			country := strings.Trim(strings.Split(items[2], "@")[0], "\"")
			fmt.Println(country)
			if geoD, ok := countryToGeo[country]; ok {
				count1++
				out.WriteString(fmt.Sprintf("%s <loc> \"%s\"^^<geo:geojson> .\n", items[0], geoD))
			} else {
				count2++
			}
		}
	}
	fmt.Println(count1, count2)
}
