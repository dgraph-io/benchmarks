//	Sample use :
//
//	./pinger --ip http://dgraph.io/query --numuser 3
//
//

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/dgraph/x"
)

var (
	numUser           = flag.Int("numuser", 1, "number of users hitting simultaneously")
	numSec            = flag.Float64("numsec", 10, "number of request per user")
	serverAddr        = flag.String("ip", ":8081", "IP addr of server")
	countC            chan int
	jsonP             chan float64
	serverP           chan float64
	parsingP          chan float64
	totalP            chan float64
	glog              = x.Log("Pinger")
	actors, directors []string
)

var qa1 = `{
		  me(_xid_:`
var qa2 = `) {
			    type.object.name.en
			    film.actor.film {
				      film.performance.film {
					        type.object.name.en
				      }
			    }
		  }
		}`
var qd1 = `{
			  me(_xid_:`
var qd2 = `) {
					    type.object.name.en
					    film.director.film  {
							      film.film.genre {
							        type.object.name.en
							      }
					    }
			  }
		}`

func runUser(wg *sync.WaitGroup) {
	var proT, parT, jsonT, totT time.Duration
	var count int

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 100,
	}}
	var dat map[string]interface{}
	var latency map[string]interface{}

	tix := time.Now()

	for time.Now().Sub(tix).Seconds() < *numSec {
		var choose = rand.Intn(2)
		var query string
		if choose == 1 {
			var ridx = rand.Intn(len(actors))
			query = qa1 + actors[ridx] + qa2
		} else {
			var ridx = rand.Intn(len(directors))
			query = qd1 + directors[ridx] + qd2
		}

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

	actorfile, err := os.Open("listofactors")
	directorfile, err1 := os.Open("listofdirectors")
	if err != nil || err1 != nil {
		return
	}
	defer actorfile.Close()
	defer directorfile.Close()

	scanner := bufio.NewScanner(actorfile)
	for scanner.Scan() {
		actors = append(actors, scanner.Text())
	}
	scanner = bufio.NewScanner(directorfile)
	for scanner.Scan() {
		directors = append(directors, scanner.Text())
	}

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

	fmt.Println("Throughput (num request per second) : ", float64(totCount)/(*numSec))
	fmt.Println("Total number of queries : ", totCount)
	fmt.Println("Total time (Seconds) : ", totTi, totTi/float64(totCount))
	fmt.Println("Json time (Seconds): ", jsonTi, jsonTi/float64(totCount))
	fmt.Println("Processing  time (Seconds): ", serTi, serTi/float64(totCount))
	fmt.Println("Parsing time (Seconds): ", parTi, parTi/float64(totCount))

}
