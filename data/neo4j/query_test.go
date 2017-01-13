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

func BenchmarkDgraphFilterAndSortQuery(b *testing.B) {
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

func BenchmarkNeo4jFilterAndSortQuery(b *testing.B) {
	query := `MATCH (d: Director) - [r:FILMS] -> (f:Film) WHERE d.name="Steven Spielberg" AND f.release_date >= "1984-08" WITH d,f ORDER BY f.release_date ASC RETURN d, f`
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

func BenchmarkDgraphSimpleQueryAndMutation(b *testing.B) {
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
		// Executing a statement just returns summary information
		_, err = stmt2.ExecNeo(params)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		stmt2.Close()
	}
}
