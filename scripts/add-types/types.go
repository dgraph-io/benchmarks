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

func printTypes(path string) error {
	typeMap := make(map[string]struct{})

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
		predicate := parts[1]
		object := parts[2]
		if predicate == "<dgraph.type>" {
			typeMap[object] = struct{}{}
		}
	}

	var types []string
	for typeName := range typeMap {
		types = append(types, typeName)
	}
	sort.Strings(types)
	for _, typeName := range types {
		fmt.Println(typeName)
	}

	return nil
}

func main() {
	if err := printTypes("21million.rdf.gz.new"); err != nil {
		log.Fatal(err)
	}

}
