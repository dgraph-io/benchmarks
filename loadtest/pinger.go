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
	numReq     = flag.Int("numreq", 10, "number of request per user")
	serverAddr = flag.String("ip", ":8081", "IP addr of server")
	avg        chan float64
	jsonP      chan float64
	serverP    chan float64
	parsingP   chan float64
	totalP     chan float64
	glog       = x.Log("Pinger")
)

func runUser(wg *sync.WaitGroup) {
	var ti, proT, parT, jsonT, totT time.Duration
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
	for i := 0; i < *numReq; i++ {
		r, _ := http.NewRequest("POST", *serverAddr, bytes.NewBufferString(query))

		t0 := time.Now()
		//fmt.Println(i)
		resp, err := client.Do(r)
		t1 := time.Now()
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
			//fmt.Println(dat["server_latency"])
			ti += t1.Sub(t0)
			// json:1.144568ms parsing:4.031346ms processing:3.726298ms total:8.904975ms
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
		//		fmt.Println("user", i)
	}
	avg <- ti.Seconds()
	serverP <- proT.Seconds()
	jsonP <- jsonT.Seconds()
	parsingP <- parT.Seconds()
	totalP <- totT.Seconds()

	fmt.Println("Done")
	wg.Done()
}

func main() {
	flag.Parse()
	var totTime, serTi, jsonTi, parTi, totTi float64
	var wg sync.WaitGroup
	avg = make(chan float64, *numUser)
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
	close(avg)
	close(serverP)
	close(parsingP)
	close(jsonP)
	close(totalP)

	for it := range avg {
		totTime += it
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

	fmt.Println("Average time : ", totTime, totTime/float64(*numUser*(*numReq)))
	fmt.Println("Total time : ", totTi, totTi/float64(*numUser*(*numReq)))
	fmt.Println("Json time : ", jsonTi, jsonTi/float64(*numUser*(*numReq)))
	fmt.Println("Processing  time : ", serTi, serTi/float64(*numUser*(*numReq)))
	fmt.Println("Parsing time : ", parTi, parTi/float64(*numUser*(*numReq)))

}
