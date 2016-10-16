package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	numUser           = flag.Int("numuser", 1, "number of users hitting simultaneously")
	numSec            = flag.Float64("numsec", 10, "number of request per user")
	serverAddr        = flag.String("ip", ":10001", "IP addr of server")
	countC            chan int
	jsonP             chan float64
	serverP           chan float64
	parsingP          chan float64
	totalP            chan float64
	latC              chan float64
	actors, directors []string
	serverList        []string
)

var qa1 = `{
		  debug(_xid_:`
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
			  debug(_xid_:`
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

		r, _ := http.NewRequest("POST", serverList[rand.Intn(len(serverList))], bytes.NewBufferString(query))
		resp, err := client.Do(r)

		count++

		if err != nil {
			log.Fatal(err)
		} else {
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatalf("Couldn't parse response body. %+v", err)
			}
			resp.Body.Close()
			err = json.Unmarshal(body, &dat)
			if err != nil {
				log.Fatal(err)
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

			latC <- tot.Seconds()
		}
	}
	countC <- count
	totalP <- totT.Seconds()

	wg.Done()
}

func main() {
	flag.Parse()
	var meanLat, sdLat, serTi, jsonTi, parTi, totTi float64
	var totCount int
	var wg sync.WaitGroup
	var allLat []float64
	serverList = strings.Split(*serverAddr, ",")
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

	countC = make(chan int, 5*(*numUser))
	serverP = make(chan float64, 5*(*numUser))
	totalP = make(chan float64, 5*(*numUser))
	parsingP = make(chan float64, 5*(*numUser))
	jsonP = make(chan float64, 5*(*numUser))
	latC = make(chan float64, 100000)

	go func() {
		for t := range latC {
			allLat = append(allLat, t)
		}
	}()
	wg.Add(*numUser)
	fmt.Println("First run")
	for i := 0; i < *numUser; i++ {
		go runUser(&wg)
	}

	wg.Wait()
	time.Sleep(1 * time.Second)
	wg.Add(*numUser)
	fmt.Println("Second run")
	for i := 0; i < *numUser; i++ {
		go runUser(&wg)
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
	wg.Add(*numUser)
	fmt.Println("Third run")
	for i := 0; i < *numUser; i++ {
		go runUser(&wg)
	}
	wg.Wait()

	close(countC)
	close(serverP)
	close(parsingP)
	close(jsonP)
	close(totalP)
	close(latC)
	fmt.Println("DONE!")
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

	meanLat = serTi / float64(totCount)
	for _, it := range allLat {
		sdLat += math.Pow((it - meanLat), 2)
	}
	sort.Float64s(allLat)
	sdLat = math.Sqrt(sdLat / float64(len(allLat)-1))

	fmt.Println("------------------------------------------------------------------------")
	fmt.Println("\n NumUser :", *numUser)
	fmt.Println("Throughput (num request per second) : ", float64(totCount)/(3*(*numSec)))
	fmt.Println("Total number of queries : ", totCount)
	fmt.Println("Avg time (ms) : ", 1000*totTi/float64(totCount))
	fmt.Println("95 percentile latency : ", 1000*allLat[int(len(allLat)/2)], 1000*allLat[int(95*len(allLat)/100)])
	fmt.Println("Min, Max : ", 1000*allLat[0], 1000*allLat[len(allLat)-1])
	fmt.Println("------------------------------------------------------------------------")
}
