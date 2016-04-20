//	Sample use :
//
//	./pinger --ip http://dgraph.io/query --numuser 3
//
//

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dgraph-io/dgraph/x"
)

var (
	numUser    = flag.Int("numuser", 1, "number of users hitting simultaneously")
	numSec     = flag.Float64("numsec", 10, "number of request per user")
	serverAddr = flag.String("ip", ":8081", "IP addr of server")
	countC     chan int
	jsonP      chan float64
	serverP    chan float64
	parsingP   chan float64
	totalP     chan float64
	glog       = x.Log("Pinger")
)

func runUser(wg *sync.WaitGroup) {
	var proT, parT, jsonT, totT time.Duration
	var count int
	var query = `{
		  me(_xid_: m.0f4vbz) {
			    type.object.name.en
			    film.actor.film {
				      film.performance.film {
					        type.object.name.en
				      }
			    }
		  }
		}`

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 100,
	}}
	var dat map[string]interface{}
	var latency map[string]interface{}

	tix := time.Now()
	for time.Now().Sub(tix).Seconds() < *numSec {
		r, _ := http.NewRequest("POST", *serverAddr, bytes.NewBufferString(query))
		resp, err := client.Do(r)

		count++

		if err != nil {
			glog.WithField("Err", resp.Status).Fatalf("Error in query")
		} else {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatalf("Couldn't parse response body. %+v", err)
			}
			resp.Body.Close()
			err = json.Unmarshal(body, &dat)
			if err != nil {
				glog.Fatalf("Error in reply")
			}

			temp := dat["server_latency"]
			latency = temp.(map[string]interface{})

			pro, _ := time.ParseDuration(latency["processing"].(string))
			proT += pro
			js, _ := time.ParseDuration(latency["json"].(string))
			jsonT += js
			par, _ := time.ParseDuration(latency["parsing"].(string))
			parT += par
			tot, _ := time.ParseDuration(latency["total"].(string))
			totT += tot
		}
	}
	countC <- count
	serverP <- proT.Seconds()
	jsonP <- jsonT.Seconds()
	parsingP <- parT.Seconds()
	totalP <- totT.Seconds()

	fmt.Println("Done")
	wg.Done()
}

func main() {
	flag.Parse()
	var serTi, jsonTi, parTi, totTi float64
	var totCount int
	var wg sync.WaitGroup
	countC = make(chan int, *numUser)
	serverP = make(chan float64, *numUser)
	totalP = make(chan float64, *numUser)
	parsingP = make(chan float64, *numUser)
	jsonP = make(chan float64, *numUser)

	wg.Add(*numUser)
	for i := 0; i < *numUser; i++ {
		fmt.Println("user", i)
		go runUser(&wg)
	}
	wg.Wait()
	close(countC)
	close(serverP)
	close(parsingP)
	close(jsonP)
	close(totalP)

	for it := range countC {
		totCount += it
	}
	for it := range serverP {
		serTi += it
	}
	for it := range parsingP {
		parTi += it
	}
	for it := range jsonP {
		jsonTi += it
	}
	for it := range totalP {
		totTi += it
	}

	fmt.Println("Throughput : ", totCount)
	fmt.Println("Total time : ", totTi, totTi/float64(totCount))
	fmt.Println("Json time : ", jsonTi, jsonTi/float64(totCount))
	fmt.Println("Processing  time : ", serTi, serTi/float64(totCount))
	fmt.Println("Parsing time : ", parTi, parTi/float64(totCount))

}
