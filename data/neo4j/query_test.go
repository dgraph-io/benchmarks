package main

import (
	"context"
	"fmt"
	// "log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

func DgraphClient(serviceAddr string) (*dgo.Dgraph, error) {
	conn, err := grpc.Dial(serviceAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return dgo.NewDgraphClient(api.NewDgraphClient(conn)), nil
}

func getVal() *api.Value {
	return &api.Value{Val: &api.Value_StrVal{strconv.Itoa(rand.Int())}}
}

func BenchmarkDgraph(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{
			"Simple", `{
					me(func: uid(0xff)) {
						name@en
						director.film  {
							genre {
								name@en	
							}
							name@en
							initial_release_date
						}
					}
				}`,
		},
		{
			"GetStarted1", `{
				director(func: allofterms(name@en, "steven spielberg")) {
					name@en
					director.film (orderasc: initial_release_date) {
						name@en
						initial_release_date
					}
				}
			}`,
		},
		{
			"GetStarted2", `{
				director(func: allofterms(name@en, "steven spielberg")) {
						name@en
						director.film (orderasc: initial_release_date) @filter(ge(initial_release_date, "1984-08")) {
							name@en
							initial_release_date
						}
					}
				}`,
		}, {
			"GetStarted3", `{
					director(func: allofterms(name@en, "steven spielberg")) {
						name@en
						director.film (orderasc: initial_release_date) @filter(ge(initial_release_date, "1990") AND le(initial_release_date, "2000")) {
							name@en
							initial_release_date
						}
					}
				}`,
		},
	}

	client, err := DgraphClient("localhost:9180")
	if err != nil {
		b.Fatalf("Error while getting client: %v", err)
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query", q.name), func(b *testing.B) {
			txn := client.NewTxn()
			defer txn.Discard(context.Background())
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := txn.Query(context.Background(), q.query)
				if err != nil {
					b.Fatalf("Error in getting response from server, %s", err)
				}
			}

			b.StopTimer()
			time.Sleep(time.Second)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-Query-parallel", q.name), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				txn := client.NewTxn()
				defer txn.Discard(context.Background())

				for pb.Next() {
					_, err := txn.Query(context.Background(), q.query)
					if err != nil {
						b.Fatalf("Error in getting response from server, %s", err)
					}
				}
			})
			time.Sleep(time.Second)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation", q.name), func(b *testing.B) {
			// NQuad mutation which is sent as part of the request.
			nq := api.NQuad{
				Subject:   "_:node",
				Predicate: "name",
				Lang:      "en",
			}

			txn := client.NewTxn()
			defer txn.Commit(context.Background())
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := txn.Query(context.Background(), q.query)
				if err != nil {
					b.Fatalf("Error in getting response from server, %s", err)
				}

				nq.ObjectValue = getVal()
				mu := api.Mutation{
					Set: []*api.NQuad{&nq},
				}
				_, err = txn.Mutate(context.Background(), &mu)
				if err != nil {
					b.Fatalf("Error in getting response from server, %s", err)
				}
			}
			b.StopTimer()
			time.Sleep(time.Second)
		})
	}

	for _, q := range queries {
		b.Run(fmt.Sprintf("%v-QueryAndMutation-parallel", q.name), func(b *testing.B) {

			b.RunParallel(func(pb *testing.PB) {
				// NQuad mutation which is sent as part of the request.
				nq := api.NQuad{
					Subject:   "_:node",
					Predicate: "name",
					Lang:      "en",
				}

				txn := client.NewTxn()
				defer txn.Commit(context.Background())

				for pb.Next() {
					_, err := txn.Query(context.Background(), q.query)
					if err != nil {
						b.Fatalf("Error in getting response from server, %s", err)
					}

					nq.ObjectValue = getVal()
					mu := api.Mutation{
						Set: []*api.NQuad{&nq},
					}
					_, err = txn.Mutate(context.Background(), &mu)
					if err != nil {
						b.Fatalf("Error in getting response from server, %s", err)
					}
				}
			})
			time.Sleep(time.Second)
		})
	}
}

// func getNeoConn(ch chan bolt.Conn) bolt.Conn {
// 	select {
// 	case c := <-ch:
// 		return c
// 	default:
// 		log.Fatal("Ran out of connections in the channel")
// 	}
// 	return nil
// }

// func putNeoConn(ch chan bolt.Conn, c bolt.Conn) {
// 	select {
// 	case ch <- c:
// 	default:
// 		log.Fatal("Not enough capacity, can't put it in")
// 	}
// }

// func BenchmarkNeo(b *testing.B) {
// 	queries := []struct {
// 		name  string
// 		query string
// 	}{
// 		{"Simple", `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`},
// 		{"GetStarted1", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
// 		{"GetStarted2", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984-08" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
// 		{"GetStarted3", `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984" AND f.release_date <= "2000" WITH d,f ORDER BY f.release_date ASC RETURN d, f`},
// 	}

// 	driver := bolt.NewDriver()
// 	poolSize := 8
// 	connCh := make(chan bolt.Conn, poolSize)
// 	for i := 0; i < poolSize; i++ {
// 		conn, err := driver.OpenNeo("bolt://localhost:7687")
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		putNeoConn(connCh, conn)
// 	}
// 	mutation := `MATCH (n:Film { filmId: {id} }) SET n.name = {name}`
// 	var err error

// 	for _, q := range queries {
// 		b.Run(fmt.Sprintf("%v-Query", q.name), func(b *testing.B) {
// 			conn := getNeoConn(connCh)
// 			b.ResetTimer()

// 			for i := 0; i < b.N; i++ {
// 				_, _, _, err = conn.QueryNeoAll(q.query, nil)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}
// 			b.StopTimer()
// 			putNeoConn(connCh, conn)
// 			time.Sleep(time.Second)
// 		})
// 	}

// 	for _, q := range queries {
// 		b.Run(fmt.Sprintf("%v-Query-parallel", q.name), func(b *testing.B) {
// 			b.RunParallel(func(pb *testing.PB) {
// 				conn := getNeoConn(connCh)
// 				for pb.Next() {
// 					_, _, _, err = conn.QueryNeoAll(q.query, nil)
// 					if err != nil {
// 						b.Fatal(err)
// 					}
// 				}
// 				putNeoConn(connCh, conn)
// 			})
// 			time.Sleep(time.Second)
// 		})
// 	}

// 	for _, q := range queries {
// 		b.Run(fmt.Sprintf("%v-QueryAndMutation", q.name), func(b *testing.B) {
// 			conn := getNeoConn(connCh)
// 			params := map[string]interface{}{"id": "m.0h_b6x1", "name": "Terminal"}
// 			b.ResetTimer()

// 			for i := 0; i < b.N; i++ {
// 				_, _, _, err = conn.QueryNeoAll(q.query, nil)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 				params["name"] = strconv.Itoa(rand.Int())
// 				_, err = conn.ExecNeo(mutation, params)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}
// 			b.StopTimer()
// 			putNeoConn(connCh, conn)
// 			time.Sleep(time.Second)
// 		})
// 	}

// 	for _, q := range queries {
// 		b.Run(fmt.Sprintf("%v-QueryAndMutation-parallel", q.name), func(b *testing.B) {
// 			b.RunParallel(func(pb *testing.PB) {
// 				conn := getNeoConn(connCh)
// 				params := map[string]interface{}{"id": "m.0h_b6x1", "name": "Terminal"}
// 				for pb.Next() {
// 					_, _, _, err = conn.QueryNeoAll(q.query, nil)
// 					if err != nil {
// 						b.Fatal(err)
// 					}
// 					params["name"] = strconv.Itoa(rand.Int())
// 					_, err = conn.ExecNeo(mutation, params)
// 					if err != nil {
// 						b.Fatal(err)
// 					}
// 				}
// 				putNeoConn(connCh, conn)
// 			})
// 			time.Sleep(time.Second)
// 		})
// 	}
// }

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
