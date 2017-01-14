package main

import (
	"bytes"
	"net/http"
	"testing"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func BenchmarkDgraphSimpleQuery(b *testing.B) {
	hc := &http.Client{}
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
		r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

		b.StartTimer()
		_, err = hc.Do(r)
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
	hc := &http.Client{}
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
			r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

			_, err = hc.Do(r)

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
	hc := &http.Client{}
	query := `	{
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

	mutation := `
		mutation {
			set {
				<m.0322yj> <type.object.name.en> "Terminal" .
			}
		}`

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))
		r2, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(mutation))

		b.StartTimer()
		_, err = hc.Do(r)
		_, err = hc.Do(r2)
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
	hc := &http.Client{}
	query := `	{
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
	mutation := `
		mutation {
			set {
				<m.0322yj> <type.object.name.en> "Terminal" .
			}
		}`

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))
			r2, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(mutation))

			_, err = hc.Do(r)
			_, err = hc.Do(r2)

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
	hc := &http.Client{}
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
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

		b.StartTimer()
		_, err = hc.Do(r)
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
	hc := &http.Client{}
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
			r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

			_, err = hc.Do(r)

			if err != nil {
				b.Fatal("Error in query", err)
			}
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
	hc := &http.Client{}
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
		r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

		b.StartTimer()
		_, err = hc.Do(r)
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
	hc := &http.Client{}
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
			r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

			_, err = hc.Do(r)

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
	hc := &http.Client{}
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
		r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

		b.StartTimer()
		_, err = hc.Do(r)
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
	hc := &http.Client{}
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
			r, err := http.NewRequest("POST", "http://127.0.0.1:8080/query", bytes.NewBufferString(query))

			_, err = hc.Do(r)

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
