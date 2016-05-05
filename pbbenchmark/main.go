// The code here is used to benchmark JSON latency against protocol buffer
// latency

package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/dgraph/query/graph"
	"github.com/dgraph-io/dgraph/x"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	numsecs = flag.Float64("numsecs", 10,
		"number of seconds for which to run the benchmark")
	glog       = x.Log("PbBenchmark")
	serverAddr = flag.String("ip", "http://127.0.0.1:8080/query",
		"Addr of server for http request")
)

func checkErr(err error, msg string) {
	if err != nil {
		glog.WithField("err", err).Fatal(msg)
	}
}

func numChildren(node *graph.Node) int {
	c := 0
	for _, child := range node.Children {
		c += numChildren(child)
	}
	return c + 1
}

func getRatio(q string, hc *http.Client, c graph.DGraphClient) (numEntities int,
	ratio float64, jsonLatency int64) {
	var dat map[string]interface{}
	var latency map[string]interface{}

	r, err := http.NewRequest("POST", *serverAddr, bytes.NewBufferString(q))
	checkErr(err, "Error while forming request")

	resp, err := hc.Do(r)
	checkErr(err, "Error in query")
	if err != nil {
		glog.WithField("Err", err).Fatalf("Error in query")
	} else {

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Couldn't parse response body. %+v", err)
		}
		resp.Body.Close()
		if err = json.Unmarshal(body, &dat); err != nil {
			glog.Fatalf("Error in reply")
		}
		numEntities = strings.Count(string(body), "_uid_")
	}
	temp := dat["server_latency"]
	latency = temp.(map[string]interface{})

	jsonTime, _ := time.ParseDuration(latency["json"].(string))

	res, err := c.Query(context.Background(), &graph.Request{Query: q})
	if err != nil {
		log.Fatal("Error in getting response from server")
	}

	if numEntities != numChildren(res.N) {
		log.Fatal("Number of entities in json and protocol buffer differ")
	}
	pbTime, _ := time.ParseDuration(res.L.Pb)
	ratio = float64(pbTime.Nanoseconds()) / float64(jsonTime.Nanoseconds())
	// Latency in milliseconds
	jsonLatency = jsonTime.Nanoseconds()/10 ^ 6
	return
}

type eRatio struct {
	// Count of results that have the same number of entities.
	count int
	// Sum of ratio of protocolBuffer/JSON parsing corresponding to nunber
	// of entities.
	ratio float64
	// Sum of json latencies
	jsonL int64
}

func Query(entity []string, q1 string, q2 string) map[int]eRatio {
	rmap := make(map[int]eRatio)

	conn, err := grpc.Dial(":8081", grpc.WithInsecure())
	if err != nil {
		x.Err(glog, err).Fatal("DialTCPConnection")
	}
	defer conn.Close()
	// Client for getting protocol buffer response
	c := graph.NewDGraphClient(conn)

	// Http client for getting JSON response.
	hc := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 100,
	}}

	tInitial := time.Now()
	counter := 0
	for time.Now().Sub(tInitial).Seconds() < *numsecs && counter < len(entity) {
		fmt.Println("counter", counter)
		d := entity[counter]
		q := q1 + d + q2
		ne, r, jl := getRatio(q, hc, c)
		er := rmap[ne]
		er.count += 1
		er.ratio = er.ratio + r
		er.jsonL = er.jsonL + jl
		rmap[ne] = er
		counter++
	}

	return rmap
}

func testQuery(entity []string, name string, q1 string, q2 string) {
	rm := Query(entity, q1, q2)
	file, err := os.Create(name + ".csv")
	checkErr(err, "Error while opening file")
	defer file.Close()

	w := csv.NewWriter(file)

	for k, v := range rm {
		numEntities := strconv.Itoa(k)
		avgRatio := strconv.FormatFloat(v.ratio/float64(v.count), 'E', -1, 64)
		jsonL := strconv.FormatInt(v.jsonL/int64(v.count), 10)
		if err := w.Write([]string{numEntities, avgRatio, jsonL}); err != nil {
			glog.WithField("err", err).Fatal("error writing record to csv")
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()

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

	var directors, actors []string

	af, err := os.Open("../throughputtest/listofactors")
	checkErr(err, "Error while opening file")
	defer af.Close()

	scanner := bufio.NewScanner(af)
	for scanner.Scan() {
		actors = append(actors, scanner.Text())
	}

	testQuery(actors, "actors", qa1, qa2)

	df, err := os.Open("../throughputtest/listofdirectors")
	checkErr(err, "Error while opening file")
	defer df.Close()

	scanner = bufio.NewScanner(df)
	for scanner.Scan() {
		directors = append(directors, scanner.Text())
	}

	testQuery(directors, "directors", qd1, qd2)

}
