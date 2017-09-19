package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/x"
)

var (
	queries = flag.String("queries", "", "Folder which contains the query files.")
	baseURL = flag.String("d", "http://localhost:8080/query", "Dgraph server address")
	src     = flag.String("src", "", "Folder to compare the responses against.")
	dest    = flag.String("dest", "", "Folder to which responses are written.")
	c       = flag.Int("c", 1, "No of concurrent workers to run.")
)

type Query struct {
	text string
	no   string
}

var qch chan Query

type errRes struct {
	Code    string `json:"code"`
	Message string `json:"Message"`
}

type Res struct {
	Errors []errRes    `json:"errors"`
	Data   interface{} `json:"data"`
	//	Extensions query.Extensions `json:"extensions"`
}

func getResponse(q string) Res {
	client := &http.Client{}
	body := bytes.NewBufferString(q)
	req, err := http.NewRequest("POST", *baseURL, body)
	x.Check(err)

	resp, err := client.Do(req)
	x.Check(err)
	defer resp.Body.Close()

	responseData, err := ioutil.ReadAll(resp.Body)
	x.Check(err)
	//	fmt.Println(string(responseData))

	var res Res
	err = json.Unmarshal(responseData, &res)
	x.Check(err)
	x.AssertTruef(len(res.Errors) == 0, "Got errors for: %+v, error: %+v", q, res.Errors)
	return res
}

func responseFile(folder, no string) string {
	return fmt.Sprintf("%s/response-%s", folder, no)
}

func runQuery(qch chan Query, wg *sync.WaitGroup) {
	defer wg.Done()

	for q := range qch {
		res := getResponse(q.text)
		if len(*src) == 0 {
			fmt.Printf("Got reply for query: %s\n", q.no)
		}
		d, err := json.Marshal(res.Data)
		x.Check(err)

		var indentedRes bytes.Buffer
		err = json.Indent(&indentedRes, d, "", "  ")
		x.Check(err)
		err = ioutil.WriteFile(responseFile(*dest, q.no), indentedRes.Bytes(), 0755)
		x.Check(err)
		if len(*src) > 0 {
			r, err := ioutil.ReadFile(responseFile(*src, q.no))
			x.Check(err)
			if !bytes.Equal(r, indentedRes.Bytes()) {
				fmt.Printf("Response for query: %s doesn't match.\n", q.no)
			} else {
				fmt.Printf("Response matched for query: %s\n", q.no)
			}
		}
		atomic.AddUint32(&count, 1)
		indentedRes.Reset()
	}
}

func walk(path string, info os.FileInfo, err error) error {
	x.Check(err)
	if info.IsDir() {
		return nil
	}
	b, err := ioutil.ReadFile(path)
	x.Check(err)
	name := strings.Split(info.Name(), "-")
	x.AssertTrue(len(name) >= 2)
	qch <- Query{
		text: string(b),
		no:   name[len(name)-1],
	}
	return nil
}

var count uint32

func printCount() {
	t := time.NewTicker(time.Second)
	for range t.C {
		fmt.Printf("Num queries done: %d\n", atomic.LoadUint32(&count))
	}
}

func main() {
	flag.Parse()
	qch = make(chan Query, 100)
	x.AssertTrue(*dest != "")
	err := os.MkdirAll(*dest, 0755)
	x.Check(err)

	var wg sync.WaitGroup
	numWorkers := *c
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go runQuery(qch, &wg)
	}

	//	go printCount()
	err = filepath.Walk(*queries, walk)
	x.Check(err)
	close(qch)
	wg.Wait()
}
