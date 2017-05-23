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
	"sync"

	"github.com/dgraph-io/dgraph/x"
)

var (
	output = flag.String("output", "out.rdf.gz", "Output rdf.gz file")

	users = flag.String("rating", "ml-20m/ratings.csv", "")
	movie = flag.String("movie", "ml-20m/movies.csv", "")

	UC = 1000000
	MC = 9000000
)

func main() {
	o, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	x.Check(err)
	w := gzip.NewWriter(o)

	uf, err := os.Open(*users)
	x.Check(err)
	mf, err := os.Open(*movie)
	x.Check(err)

	var wg sync.WaitGroup
	wg.Add(1)
	ch := make(chan string, 10000)
	go func() {
		for it := range ch {
			w.Write([]byte(it))
		}
		wg.Done()
	}()

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		var str string
		br1 := bufio.NewReader(uf)
		log.Println("Reading rating file")
		for {
			line, err := br1.ReadString('\n')
			if err != nil && err == io.EOF {
				break
			}
			line = strings.Trim(line, "\n")
			csv := strings.Split(line, ",")
			if len(csv) != 4 {
				continue
			}
			g, err := strconv.ParseInt(csv[1], 10, 32)
			x.Check(err)
			gi := int(g)
			u, err := strconv.ParseInt(csv[0], 10, 32)
			x.Check(err)
			ui := int(u)
			str = fmt.Sprintf("<%v> <rated> <%v>  (rating=%v) .\n", UC+ui, MC+gi, csv[2])
			ch <- str
			// TODO: can add timestamp in facets.
		}
		wg1.Done()
	}()

	var str string
	genreCount := 1
	genreMap := make(map[string]int)
	br2 := bufio.NewReader(mf)
	log.Println("Reading movies file")
	for {
		line, err := br2.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		line = strings.Trim(line, "\n")
		csv := strings.Split(line, ",")
		if len(csv) != 3 {
			continue
		}
		g, err := strconv.ParseInt(csv[0], 10, 32)
		x.Check(err)
		gi := int(g)
		name := strings.Replace(csv[1], "\"\r", "", -1)
		str = fmt.Sprintf("<%v> <name> \"%v\" .\n", MC+gi, name)
		ch <- str
		genres := strings.Split(csv[2], "|")
		for _, g := range genres {
			g = strings.Replace(g, "\"\r", "", -1)
			d, ok := genreMap[g]
			if !ok {
				fmt.Println(g)
				d = genreCount
				genreMap[g] = genreCount
				genreCount++
			}
			str = fmt.Sprintf("<%v> <genre> <%v> .\n", MC+gi, d)
			ch <- str
		}
	}

	for k, v := range genreMap {
		fmt.Println(k)
		str = fmt.Sprintf("<%v> <genre> \"%v\" .\n", v, k)
		ch <- str
	}

	wg1.Wait()
	close(ch)
	wg.Wait()
	log.Println("Finised.")
	x.Check(w.Flush())
	x.Check(w.Close())
	x.Check(o.Close())
}
