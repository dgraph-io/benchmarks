package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/dgraph/x"
)

var (
	output = flag.String("output", "out.rdf.gz", "Output rdf.gz file")

	genre = flag.String("genre", "ml-100k/u.genre", "")
	users = flag.String("rating", "ml-100k/u.user", "")
	data  = flag.String("user", "ml-100k/u.data", "")
	movie = flag.String("movie", "ml-100k/u.item", "")
)

func main() {
	o, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	x.Check(err)
	w := gzip.NewWriter(o)

	gf, err := os.Open(*genre)
	x.Check(err)
	uf, err := os.Open(*users)
	x.Check(err)
	df, err := os.Open(*data)
	x.Check(err)
	mf, err := os.Open(*movie)
	x.Check(err)

	var str string

	br := bufio.NewReader(gf)
	log.Println("Reading genre file")
	for {
		line, err := br.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		line = strings.Trim(line, "\n")
		csv := strings.Split(line, "|")
		if len(csv) != 2 {
			continue
		}
		g, err := strconv.ParseInt(csv[1], 10, 32)
		x.Check(err)
		gi := int(g)
		str = fmt.Sprintf("<%v> <name> \"%v\" .\n", 1000000+gi, csv[0])
		w.Write([]byte(str))
	}

	br = bufio.NewReader(uf)
	log.Println("Reading user file")
	for {
		line, err := br.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		line = strings.Trim(line, "\n")
		csv := strings.Split(line, "|")
		if len(csv) != 5 {
			continue
		}
		str = fmt.Sprintf("<%v> <age> \"%v\"^^<xs:int> .\n", csv[0], csv[1])
		w.Write([]byte(str))
		str = fmt.Sprintf("<%v> <gender> \"%v\" .\n", csv[0], csv[2])
		w.Write([]byte(str))
		str = fmt.Sprintf("<%v> <occupation> \"%v\" .\n", csv[0], csv[3])
		w.Write([]byte(str))
		str = fmt.Sprintf("<%v> <zipcode> \"%v\" .\n", csv[0], csv[4])
		w.Write([]byte(str))
	}

	count := 9999989
	br = bufio.NewReader(df)
	log.Println("Reading rating file")
	for {
		line, err := br.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		line = strings.Trim(line, "\n")
		csv := strings.Split(line, "\t")
		if len(csv) != 4 {
			continue
		}
		str = fmt.Sprintf("<%v> <action.rate> <%v> .\n", csv[0], count)
		w.Write([]byte(str))
		str = fmt.Sprintf("<%v> <movie> <%v> .\n", count, csv[1])
		w.Write([]byte(str))
		str = fmt.Sprintf("<%v> <rating> \"%v\"^^<xs:float> .\n", count, csv[2])
		w.Write([]byte(str))
		// TODO: can add timestamp
		count++
	}

	br = bufio.NewReader(mf)
	log.Println("Reading movies file")
	for {
		line, err := br.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		line = strings.Trim(line, "\n")
		csv := strings.Split(line, "|")
		if len(csv) != 24 {
			continue
		}
		str = fmt.Sprintf("<%v> <name> \"%v\" .\n", csv[0], csv[1])
		w.Write([]byte(str))
		for i := 5; i < 24; i++ {
			if csv[i] == "0" {
				continue
			}
			str = fmt.Sprintf("<%v> <genre> <%v> .\n", csv[0], 1000000+i-5)
			w.Write([]byte(str))
		}
	}

	log.Println("Finised.")
	x.Check(w.Flush())
	x.Check(w.Close())
	x.Check(o.Close())
}
