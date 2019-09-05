package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

// typeName should include the escaped quotes,  e.g "\"Person\""
func printTypePreds(path string, typeName string) error {
	// List of nodes with the given typeName.
	typeNodes := make([]string, 0)
	// Map of nodes to a set of their predicates.
	nodePredMap := make(map[string]map[string]struct{})

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		subject := parts[0]
		predicate := parts[1]
		object := parts[2]

		if predicate == "<dgraph.type>" && object == typeName {
			typeNodes = append(typeNodes, subject)
		}

		if _, ok := nodePredMap[subject]; !ok {
			nodePredMap[subject] = make(map[string]struct{})
		}

		nodePredMap[subject][predicate] = struct{}{}
	}

	preds := make(map[string]struct{})
	for _, node := range typeNodes {
		nodePreds := nodePredMap[node]
		for pred := range nodePreds {
			preds[pred] = struct{}{}
		}
	}

	predsList := make([]string, 0)
	for pred := range preds {
		predsList = append(predsList, pred)
	}

	sort.Strings(predsList)
	for _, pred := range predsList {
		fmt.Println(pred)
	}

	return nil
}

func main() {
	if err := printTypePreds("21million.rdf.gz", "\"Movie\""); err != nil {
		log.Fatal(err)
	}

}
