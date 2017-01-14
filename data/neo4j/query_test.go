package main

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/query/graph"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func BenchmarkDgraphSimpleQuery(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `
				{
					me(_xid_: m.06pj8) {
						type.object.name.en
						film.director.film  {
						film.film.genre {
							type.object.name.en
						}
						type.object.name.en
						film.film.initial_release_date
						}
					}
				}
		`

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := c.Run(context.Background(), &graph.Request{Query: query})
		if err != nil {
			b.Fatalf("Error in getting response from server, %s", err)
		}
		b.StopTimer()

		if err != nil {
			b.Fatal("Error in query", err)
		}
	}
}

func BenchmarkNeo4jSimpleQuery(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.PrepareNeo(query)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = stmt.QueryNeo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt.Close()
	}
}

func BenchmarkDgraphSimpleQueryParallel(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `
				{
					me(_xid_: m.06pj8) {
						type.object.name.en
						film.director.film  {
						film.film.genre {
							type.object.name.en
						}
						type.object.name.en
						film.film.initial_release_date
						}
					}
				}
		`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := c.Run(context.Background(), &graph.Request{Query: query})
			if err != nil {
				b.Fatal("Error in query", err)
			}
		}
	})
}

func BenchmarkNeo4jSimpleQueryParallel(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`
	driver := bolt.NewDriver()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := driver.OpenNeo("bolt://localhost:7687")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()

			stmt, err := conn.PrepareNeo(query)
			if err != nil {
				b.Fatal(err)
			}
			_, err = stmt.QueryNeo(nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.Close()
		}
	})
}

func BenchmarkDgraphSimpleQueryAndMutation(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	req := client.Req{}
	req.SetQuery(`	{
					me(_xid_: m.06pj8) {
						type.object.name.en
						film.director.film  {
						film.film.genre {
							type.object.name.en
						}
						type.object.name.en
						film.film.initial_release_date
						}
					}
				}
    `)
	req.AddMutation(graph.NQuad{
		Subject:     "m.0322yj",
		Predicate:   "type.object.name.en",
		ObjectValue: &graph.Value{&graph.Value_StrVal{"Terminal"}},
	}, client.SET)

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		b.StartTimer()
		_, err := c.Run(context.Background(), req.Request())
		b.StopTimer()

		if err != nil {
			b.Fatal("Error in query", err)
		}
	}
}

func BenchmarkNeo4jSimpleQueryAndMutation(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`
	mutation := `MATCH (n:Director { directorId: {id} }) SET n.name = {name}`
	params := map[string]interface{}{"id": "m.0322yj", "name": "Terminal"}
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.PrepareNeo(query)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = stmt.QueryNeo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt.Close()
		stmt2, err := conn.PrepareNeo(mutation)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		_, err = stmt2.ExecNeo(params)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt2.Close()
	}
}

func BenchmarkDgraphSimpleQueryAndMutationParallel(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()
	c := graph.NewDgraphClient(conn)
	req := client.Req{}
	req.SetQuery(`	{
					me(_xid_: m.06pj8) {
						type.object.name.en
						film.director.film  {
						film.film.genre {
							type.object.name.en
						}
						type.object.name.en
						film.film.initial_release_date
						}
					}
				}
    `)
	req.AddMutation(graph.NQuad{
		Subject:     "m.0322yj",
		Predicate:   "type.object.name.en",
		ObjectValue: &graph.Value{&graph.Value_StrVal{"Terminal"}},
	}, client.SET)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			_, err := c.Run(context.Background(), req.Request())
			if err != nil {
				b.Fatal("Error in query", err)
			}
		}
	})
}

func BenchmarkNeo4jSimpleQueryAndMutationParallel(b *testing.B) {
	driver := bolt.NewDriver()
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) - [r2:GENRE] -> (g:Genre) WHERE d.directorId="m.06pj8" RETURN d, f, g`
	mutation := `MATCH (n:Director { directorId: {id} }) SET n.name = {name}`
	params := map[string]interface{}{"id": "m.0322yj", "name": "Terminal"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := driver.OpenNeo("bolt://localhost:7687")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()

			stmt, err := conn.PrepareNeo(query)

			if err != nil {
				b.Fatal(err)
			}
			_, err = stmt.QueryNeo(nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.Close()
			stmt2, err := conn.PrepareNeo(mutation)
			if err != nil {
				b.Fatal(err)
			}

			_, err = stmt2.ExecNeo(params)
			if err != nil {
				b.Fatal(err)
			}
			stmt2.Close()
		}
	})
}

func BenchmarkDgraphGetStarted1(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	req := client.Req{}
	req.SetQuery(`{
	     director(allof("type.object.name.en", "steven spielberg")) {
		    type.object.name.en
		    film.director.film (orderdesc: film.film.initial_release_date) {
		        type.object.name.en
				film.film.initial_release_date
	      }
  }
}
`)

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		resp, err := c.Run(context.Background(), req.Request())
		fmt.Println(resp)
		b.StopTimer()

		if err != nil {
			b.Fatal("Error in query", err)
		}
	}
}

func BenchmarkNeo4jGetStarted1(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" WITH d,f ORDER BY f.release_date DESC RETURN d, f`
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.PrepareNeo(query)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = stmt.QueryNeo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt.Close()
	}
}

func BenchmarkDgraphGetStarted1Parallel(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `{
	     director(allof("type.object.name.en", "steven spielberg")) {
		    type.object.name.en
		    film.director.film (orderdesc: film.film.initial_release_date) {
		        type.object.name.en
				film.film.initial_release_date
	      }
  }
}
		`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := c.Run(context.Background(), &graph.Request{Query: query})
			if err != nil {
				b.Fatal("Error in query", err)
			}
			fmt.Println(resp)
		}
	})
}

func BenchmarkNeo4jGetStarted1Parallel(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" WITH d,f ORDER BY f.release_date DESC RETURN d, f`
	driver := bolt.NewDriver()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := driver.OpenNeo("bolt://localhost:7687")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()

			stmt, err := conn.PrepareNeo(query)
			if err != nil {
				b.Fatal(err)
			}
			_, err = stmt.QueryNeo(nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.Close()
		}
	})
}

func BenchmarkDgraphGetStarted2(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `{
	    director(allof("type.object.name.en", "steven spielberg")) {
			type.object.name.en
		    film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1984-08")) {
				type.object.name.en
				film.film.initial_release_date
	    }
  }
}
`
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		b.StartTimer()
		resp, err := c.Run(context.Background(), &graph.Request{Query: query})
		fmt.Println(resp)
		b.StopTimer()

		if err != nil {
			b.Fatal("Error in query", err)
		}
	}
}

func BenchmarkNeo4jGetStarted2(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984-08" WITH d,f ORDER BY f.release_date ASC RETURN d, f`
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.PrepareNeo(query)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = stmt.QueryNeo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt.Close()
	}
}

func BenchmarkDgraphGetStarted2Parallel(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `{
	    director(allof("type.object.name.en", "steven spielberg")) {
			type.object.name.en
		    film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1984-08")) {
				type.object.name.en
				film.film.initial_release_date
	    }
  }
}`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := c.Run(context.Background(), &graph.Request{Query: query})
			if err != nil {
				b.Fatal("Error in query", err)
			}
		}
	})
}

func BenchmarkNeo4jGetStarted2Parallel(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984-08" WITH d,f ORDER BY f.release_date ASC RETURN d, f`
	driver := bolt.NewDriver()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := driver.OpenNeo("bolt://localhost:7687")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()

			stmt, err := conn.PrepareNeo(query)
			if err != nil {
				b.Fatal(err)
			}
			_, err = stmt.QueryNeo(nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.Close()
		}
	})
}

func BenchmarkDgraphGetStarted3(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `{
  director(allof("type.object.name.en", "steven spielberg")) {
    type.object.name.en
    film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1990") && leq("film.film.initial_release_date", "2000")) {
      type.object.name.en
      film.film.initial_release_date
    }
  }
}
`
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := c.Run(context.Background(), &graph.Request{Query: query})
		b.StopTimer()

		if err != nil {
			b.Fatal("Error in query", err)
		}
	}
}

func BenchmarkNeo4jGetStarted3(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984" AND f.release_date <= "2000" WITH d,f ORDER BY f.release_date ASC RETURN d, f`
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := conn.PrepareNeo(query)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		_, err = stmt.QueryNeo(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt.Close()
	}
}

func BenchmarkDgraphGetStarted3Parallel(b *testing.B) {
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		b.Fatal("DialTCPConnection")
	}
	defer conn.Close()

	c := graph.NewDgraphClient(conn)
	query := `{
  director(allof("type.object.name.en", "steven spielberg")) {
    type.object.name.en
    film.director.film (order: film.film.initial_release_date) @filter(geq("film.film.initial_release_date", "1990") && leq("film.film.initial_release_date", "2000")) {
      type.object.name.en
      film.film.initial_release_date
    }
  }
}`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := c.Run(context.Background(), &graph.Request{Query: query})
			if err != nil {
				b.Fatal("Error in query", err)
			}
		}
	})
}

func BenchmarkNeo4jGetStarted3Parallel(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name CONTAINS "Steven Spielberg" AND f.release_date >= "1984" AND f.release_date <= "2000" WITH d,f ORDER BY f.release_date ASC RETURN d, f`
	driver := bolt.NewDriver()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := driver.OpenNeo("bolt://localhost:7687")
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()

			stmt, err := conn.PrepareNeo(query)
			if err != nil {
				b.Fatal(err)
			}
			_, err = stmt.QueryNeo(nil)
			if err != nil {
				b.Fatal(err)
			}
			stmt.Close()
		}
	})
}
