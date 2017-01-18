package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/query/graph"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func getDgraphConn(ch chan *grpc.ClientConn) *grpc.ClientConn {
	select {
	case c := <-ch:
		return c
	default:
		log.Fatal("Ran out of connections in the channel")
	}
	return nil
}

func putDgraphConn(ch chan *grpc.ClientConn, c *grpc.ClientConn) {
	select {
	case ch <- c:
	default:
		log.Fatal("Not enough capacity, can't put it in")
	}
}

func getVal() *graph.Value {
	return &graph.Value{&graph.Value_StrVal{strconv.Itoa(rand.Int())}}
}

func BenchmarkDgraph(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{
			"Simple", `{
					me(id: m.06pj8) {
						type.object.name.en
						film.director.film  {
							film.film.genre {
								type.object.name.en
							}
							type.object.name.en
							film.film.initial_release_date
						}
					}
				}`,
		},
		{
			"GetStarted1", `{
				director(allof("type.object.name.en", "steven spielberg")) {
					type.object.name.en
					film.director.film (order: film.film.initial_release_date) {
						type.object.name.en
						film.film.initial_release_date
					}
				}
			}`,
		},
		{
			"GetStarted2", `{
				director(allof("type.object.name.en", "steven spielberg")) {
						type.object.name.en
						film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1984-08")) {
							type.object.name.en
							film.film.initial_release_date
						}
					}
				}`,
		}, {
			"GetStarted3", `{
					director(allof("type.object.name.en", "steven spielberg")) {
						type.object.name.en
						film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1990") && leq("film.film.initial_release_date", "2000")) {
							type.object.name.en
							film.film.initial_release_date
						}
					}
				}`,
		},
	}

	poolSize := 8
	connCh := make(chan *grpc.ClientConn, poolSize)
	for i := 0; i < poolSize; i++ {
		conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
		if err != nil {
			b.Fatal(err)
		}
		putDgraphConn(connCh, conn)
	}
	var err error

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query", q.name), func(b *testing.B) {
			conn := getDgraphConn(connCh)
			c := graph.NewDgraphClient(conn)
			req := client.Req{}
			req.SetQuery(q.query)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err = c.Run(context.Background(), req.Request())
				if err != nil {
					b.Fatalf("Error in getting response from server, %s", err)
				}
			}
			b.StopTimer()
			putDgraphConn(connCh, conn)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query-parallel", q.name), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				conn := getDgraphConn(connCh)
				c := graph.NewDgraphClient(conn)
				req := client.Req{}
				req.SetQuery(q.query)
				for pb.Next() {
					_, err = c.Run(context.Background(), req.Request())
					if err != nil {
						b.Fatal("Error in query", err)
					}
				}
				putDgraphConn(connCh, conn)
			})
		})
	}

	// NQuad mutation which is sent as part of the request.
	nq := graph.NQuad{
		Subject:   "m.0h_b6x1",
		Predicate: "type.object.name.en",
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation", q.name), func(b *testing.B) {

			conn := getDgraphConn(connCh)
			c := graph.NewDgraphClient(conn)
			req := client.Req{}
			req.SetQuery(q.query)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				nq.ObjectValue = getVal()
				req.AddMutation(nq, client.SET)
				_, err := c.Run(context.Background(), req.Request())
				if err != nil {
					b.Fatalf("Error in getting response from server, %s", err)
				}
			}
			b.StopTimer()
			putDgraphConn(connCh, conn)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation-parallel", q.name), func(b *testing.B) {

			b.RunParallel(func(pb *testing.PB) {
				conn := getDgraphConn(connCh)
				c := graph.NewDgraphClient(conn)
				req := client.Req{}
				req.SetQuery(q.query)
				for pb.Next() {
					nq.ObjectValue = getVal()
					req.AddMutation(nq, client.SET)
					_, err = c.Run(context.Background(), req.Request())
					if err != nil {
						b.Fatal("Error in query", err)
					}
				}
				putDgraphConn(connCh, conn)
			})
		})
	}
}

func getNeoConn(ch chan bolt.Conn) bolt.Conn {
	select {
	case c := <-ch:
		return c
	default:
		log.Fatal("Ran out of connections in the channel")
	}
	return nil
}

func putNeoConn(ch chan bolt.Conn, c bolt.Conn) {
	select {
	case ch <- c:
	default:
		log.Fatal("Not enough capacity, can't put it in")
	}
}

func BenchmarkNeo(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{"Simple", `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`},
		{"GetStarted1", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
		{"GetStarted2", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984-08" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
		{"GetStarted3", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984" AND f.release_date <= "2000" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
	}

	driver := bolt.NewDriver()
	poolSize := 8
	connCh := make(chan bolt.Conn, poolSize)
	for i := 0; i < poolSize; i++ {
		conn, err := driver.OpenNeo("bolt://localhost:7687")
		if err != nil {
			b.Fatal(err)
		}
		putNeoConn(connCh, conn)
	}
	mutation := `MATCH (n:Film { filmId: {id} }) SET n.name = {name}`
	params := map[string]interface{}{"id": "m.0h_b6x1", "name": "Terminal"}
	var err error

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query", q.name), func(b *testing.B) {
			conn := getNeoConn(connCh)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, _, err = conn.QueryNeoAll(q.query, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			putNeoConn(connCh, conn)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query-parallel", q.name), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				conn := getNeoConn(connCh)
				for pb.Next() {
					_, _, _, err = conn.QueryNeoAll(q.query, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
				putNeoConn(connCh, conn)
			})
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation", q.name), func(b *testing.B) {
			conn := getNeoConn(connCh)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, _, err = conn.QueryNeoAll(q.query, nil)
				if err != nil {
					b.Fatal(err)
				}
				params["name"] = strconv.Itoa(rand.Int())
				_, err = conn.ExecNeo(mutation, params)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			putNeoConn(connCh, conn)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation-parallel", q.name), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				conn := getNeoConn(connCh)
				for pb.Next() {
					_, _, _, err = conn.QueryNeoAll(q.query, nil)
					if err != nil {
						b.Fatal(err)
					}
					params["name"] = strconv.Itoa(rand.Int())
					_, err = conn.ExecNeo(mutation, params)
					if err != nil {
						b.Fatal(err)
					}
				}
				putNeoConn(connCh, conn)
			})
		})
	}
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
