package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	farm "github.com/dgryski/go-farm"

	"github.com/dgraph-io/dgraph/x"
)

const (
	filename = "/home/jchiu/dgraphtest/goldendata"
)

var (
	invGraph map[string]map[uint64][]uint64
	goodUIDs []uint64
)

func bracketed(s string) bool {
	return strings.HasPrefix(s, "<") && strings.HasSuffix(s, ">")
}

func removeFirstLast(s string) string {
	return s[1 : len(s)-1]
}

// uniqueUIDs return the unique UIDs in the values of map m. It also returns
// a map from unique UIDs to their counts, i.e., the number of times they
// appear in the values of map m.
func uniqueUIDs(m map[uint64][]uint64) ([]uint64, []int) {
	uniq := make(map[uint64]int)
	for _, v := range m {
		for _, u := range v {
			uniq[u]++
		}
	}
	out := make([]uint64, 0, len(uniq))
	outCount := make([]int, 0, len(uniq))
	for k, v := range uniq {
		out = append(out, k)
		outCount = append(outCount, v)
	}
	return out, outCount
}

func expand(uids []uint64) map[string]map[uint64][]uint64 {
	x.AssertTrue(invGraph != nil)
	out := make(map[string]map[uint64][]uint64)
	for pred, m := range invGraph {
		outM := make(map[uint64][]uint64)
		for _, u := range uids { // srcUID.
			z := m[u]
			if z == nil {
				continue
			}
			outM[u] = z
		}
		if len(outM) > 0 {
			out[pred] = outM
		}
	}
	return out
}

func main() {
	x.Init()

	fin, err := os.Open(filename)
	x.Check(err)
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	var numLines, numValues, numNames, numReleaseDates int
	invGraph = make(map[string]map[uint64][]uint64)

	for scanner.Scan() {
		numLines++
		tokens := strings.Split(scanner.Text(), "\t")
		x.AssertTruef(len(tokens) == 4, scanner.Text())

		src := tokens[0]
		x.AssertTrue(bracketed(src))
		src = removeFirstLast(src)
		srcUID := farm.Fingerprint64([]byte(src))

		pred := tokens[1]
		x.AssertTrue(bracketed(pred))
		pred = removeFirstLast(pred)

		value := tokens[2]

		if bracketed(value) {
			// Normal edge.
			value = removeFirstLast(value)
			destUID := farm.Fingerprint64([]byte(value))
			m, found := invGraph[pred]
			if !found {
				m = make(map[uint64][]uint64)
				invGraph[pred] = m
			}
			// We are building an inverse map!
			m[destUID] = append(m[destUID], srcUID)
		} else {
			// A value.
			numValues++
			value = removeFirstLast(value)
			if pred == "type.object.name" {
				numNames++

				// Do some custom processing here.
				value = strings.ToLower(value)
				vTokens := strings.Split(value, " ")
				var found bool
				for _, t := range vTokens {
					if t == "the" {
						found = true
						break
					}
				}
				if found {
					goodUIDs = append(goodUIDs, srcUID)
				}

			} else if pred == "film.film.initial_release_date" {
				numReleaseDates++
			}
		}
	}

	fmt.Printf("Num lines read: %d\n", numLines)
	fmt.Printf("Num predicates: %d\n", len(invGraph))
	fmt.Printf("Num values read: %d\n", numValues)
	fmt.Printf("Num names read: %d\n", numNames)
	fmt.Printf("Num release dates read: %d\n", numReleaseDates)
	fmt.Printf("Num good UIDs: %d\n", len(goodUIDs))

	x.AssertTrue(numLines > 0)
	x.AssertTrue(len(invGraph) > 0)
	x.AssertTrue(numValues > 0)
	x.AssertTrue(numNames > 0)
	x.AssertTrue(numReleaseDates > 0)
	x.AssertTrue(len(goodUIDs) > 0)

	doGood()
}

func doGood() {
	// goodUIDs are UIDs with type.object.name containing "good".
	results := expand(goodUIDs)
	directorEdges := results["film.director.film"]
	x.AssertTrue(len(directorEdges) > 100)

	// Directors are UIDs which go to goodUIDs via "film.director.film".
	directorUIDs, _ := uniqueUIDs(directorEdges)

	results = expand(directorUIDs)
	filmEdges := results["film.film.directed_by"]
	x.AssertTrue(len(filmEdges) > 100)

	// Films are UIDs which go to directorUIDs via "film.film.directed_by".
	filmUIDs, _ := uniqueUIDs(filmEdges)

	results = expand(filmUIDs)
	directorEdges = results["film.director.film"]
	x.AssertTrue(len(directorEdges) > 100)

	// Directors are UIDs which go to filmUIDs via "film.director.film".
	directorUIDs, counts := uniqueUIDs(directorEdges)

	var maxCount int
	var maxDirector uint64
	for i, c := range counts {
		if c > maxCount {
			maxCount = c
			maxDirector = directorUIDs[i]
		}
	}
	fmt.Printf("maxDirector %d with count %d\n", maxDirector, maxCount)
}
