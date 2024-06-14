package main

// Generates random events and imports to the database
//
// The intent is to simulate day-to-day usage (read before update)
// The code is a very good bad example

import "C"
import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"pg-dg-test/pkg/client"
	"pg-dg-test/pkg/generator"
	"pg-dg-test/pkg/util"

	_ "github.com/lib/pq"
)

const (
	totalEvents    = 100000
	eventsPerFrame = 10
	workers        = 8
	ipCount        = 200000
	domainCount    = 100000

	startTs int64 = 1514764800

	// filenames to export values
	postgresIPFile   = "pg_ip.txt"
	postgresAddrFile = "pg_addr.txt"
	dgraphIPFile     = "dg_ip.txt"
	dgraphAddrFile   = "dg_addr.txt"

	dbUser     = "postgres"
	dbPassword = ""
	dbName     = "pgsql-test"
)

//----------------------------------------------------------------------------------------------------------------------
// workers
//----------------------------------------------------------------------------------------------------------------------

// Events worker
func timeTickerWorker(ticker chan int64, c client.Client, ips, domains []string, done, kill chan bool) {
	for true {
		select {
		case i := <-ticker:
			fmt.Println("Event frame #" + strconv.FormatInt(i-startTs, 10) + " ...")
			client.Ticker(c, i, eventsPerFrame, ips, domains)
			done <- true

		case <-kill:
			done <- true
			return
		}
	}
}

// IP Address Worker
func ipWorker(ips chan string, c client.Client, done, kill chan bool) {
	for true {
		select {
		case ip := <-ips:
			c.AddIp(ip)
			done <- true

		case <-kill:
			done <- true
			return
		}
	}
}

// Domain Worker
func domainWorker(domainlist chan string, c client.Client, done, kill chan bool) {
	for true {
		select {
		case fqdn := <-domainlist:
			c.AddDomain(fqdn)
			done <- true

		case <-kill:
			done <- true
			return
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Generators
//----------------------------------------------------------------------------------------------------------------------

// Generate random ip addresses and insert them into the database
func genIPAddr(c client.Client) []string {

	fmt.Println("Generating ip addresses...")
	ips := generator.GenerateIpv4(ipCount)

	fmt.Println("Inserting ip addresses...")

	const pageSize = 1000
	const pageCount = ipCount / pageSize
	const pageRem = ipCount % pageSize

	ipchan := make(chan string)
	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case ip := <-ips:
					c.AddIp(ip)
					done <- true

				case <-done:
					return
				}
			}
		}()
	}

	var ofs uint32
	for i := 0; i < int(pageCount); i++ {
		for j := 0; j < int(pageSize); j++ {
			go func(ip string) {
				ipchan <- ip
			}(ips[ofs])
			ofs++
		}
	}

	for j := 0; j < int(pageRem); j++ {
		go func(ip string) {
			ipchan <- ip
		}(ips[ofs])
		ofs++
	}

	// shutdown workers
	close(done)
	wg.Wait()

	return ips
}

// Generate domain names and insert them into the database
func genDomain(c client.Client) []string {

	fmt.Println("Generating domain names...")
	domains := generator.GenerateFQDN(4, 16, domainCount)

	fmt.Println("Inserting domain names...")

	domainchan := make(chan string)
	done := make(chan bool)
	kill := make(chan bool)

	// Create workers
	for i := 0; i < workers; i++ {
		go domainWorker(domainchan, c, done, kill)
	}

	pageSize := 1000
	pageCount := domainCount / pageSize
	pageRem := domainCount % pageSize

	ofs := 0
	w := 0
	for i := 0; i < pageCount; i++ {
		w = 0
		for j := 0; j < pageSize; j++ {
			go func(fqdn string) {
				domainchan <- fqdn
			}(domains[ofs])
			ofs++
			w++
		}

		// wait for workers
		for w > 0 {
			<-done
			w--
		}
	}

	w = 0
	for j := 0; j < pageRem; j++ {
		go func(fqdn string) {
			domainchan <- fqdn
		}(domains[ofs])
		ofs++
		w++
	}

	// wait for workers
	for w > 0 {
		<-done
		w--
	}

	// kill workers
	w = 0
	for i := 0; i < workers; i++ {
		kill <- true
		w++
	}

	for w > 0 {
		<-done
		w--
	}

	close(done)
	close(domainchan)
	close(kill)

	return domains
}

// Generate Events and link them to random ip and domains
func genEvents(totalEvents int64, c client.Client, ips, domains []string) {

	fmt.Println("Generating events...")

	tickerchan := make(chan int64)
	done := make(chan bool)
	kill := make(chan bool)

	// Create workers
	for i := 0; i < workers; i++ {
		go timeTickerWorker(tickerchan, c, ips, domains, done, kill)
	}

	var pageSize int64 = 1000
	pageCount := totalEvents / pageSize
	pageRem := totalEvents % pageSize

	var ofs = startTs
	w := 0
	for i := 0; i < int(pageCount); i++ {
		w := 0
		for j := 0; j < int(pageSize); j++ {
			go func(i int64) {
				tickerchan <- i
			}(ofs)
			ofs++
			w++
		}

		for w > 0 {
			<-done
			w--
		}
	}

	// remainder
	for j := 0; j < int(pageRem); j++ {
		go func(i int64) {
			tickerchan <- i
		}(ofs)
		ofs++
		w++
	}
	for w > 0 {
		<-done
		w--
	}

	w = 0
	for i := 0; i < workers; i++ {
		kill <- true
		w++
	}

	for w > 0 {
		<-done
		w--
	}

	close(tickerchan)
	close(done)
	close(kill)
}

// runPgSQL runs PostgreSQL cycle
func runPgSQL(export bool) {
	fmt.Println("==== Running PostgreSQL Client ====")
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", dbUser, dbPassword, dbName)
	c := client.NewPgClient(connStr)
	defer c.Close()

	ips := genIPAddr(c)
	domains := genDomain(c)

	if export {
		fmt.Println("Exporting to file...")
		err := generator.Save(ips, postgresIPFile)
		util.CheckError(err)

		err = generator.Save(domains, postgresAddrFile)
		util.CheckError(err)
	}

	genEvents(totalEvents, c, ips, domains)
}

// Dgraph cycle
func runDgraph(export bool) {
	fmt.Println("==== Running DGraph Client ====")
	conn_str := "127.0.0.1:9080"
	c := client.NewDgClient(conn_str)

	ips := genIPAddr(c)
	domains := genDomain(c)

	if export {
		fmt.Println("Exporting to file...")
		err := generator.Save(ips, dgraphIPFile)
		util.CheckError(err)

		err = generator.Save(domains, dgraphAddrFile)
		util.CheckError(err)
	}

	genEvents(totalEvents, c, ips, domains)
}

//----------------------------------------------------------------------------------------------------------------------
// Main
//----------------------------------------------------------------------------------------------------------------------
var (
	pg     = flag.Bool("pg", false, "Execute Postgresql insertion")
	dg     = flag.Bool("dg", false, "Execute Dgraph insertion")
	export = flag.Bool("e", false, "Export ip & domains to file")
)

func main() {
	rand.Seed(time.Now().UnixNano())

	flag.Parse()

	if *dg {
		runDgraph(*export)
	}

	if *pg {
		runPgSQL(*export)
	}

	fmt.Println("Done!")
}
