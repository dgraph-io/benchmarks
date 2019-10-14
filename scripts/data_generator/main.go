package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
	"strings"
	"bufio"
	"os"
)

const charset = "abcdefghijklmnopqrtuvwxyz" + "0123456789" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var seededRand* rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomString(length int) string {
	b := make([] byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	types := flag.String("types", "string", "comma-separated list of types to be generated")
	totalSize := flag.Int("total-size", 1 * 1024 * 1024 * 1024, "total size of data that should be generated")
	stringLength := flag.Int("string-length", 1024, "length of the string to generate; ignore if string type is not specified")
	repeatSubjects := flag.Int("repeat-subjects", 1, "Number of times to repeat the subject")
	outputFile := flag.String("output", "out.rdf", "Output file to write the RDF document")

	flag.Parse()

	typesArray := strings.Split(*types, ",")

	for _, typeg := range typesArray {
		switch typeg {
		case "string":
			break
		default:
			fmt.Printf("Type %s not supported. Ignored\n", typeg)
		}
	}

	fmt.Println("types:", *types)
	fmt.Println("total size:", *totalSize)
	fmt.Println("string length:", *stringLength)
	fmt.Println("repeat subjects:", *repeatSubjects)
	fmt.Println("output:", *outputFile)

	f, _ := os.Create(*outputFile)
	w := bufio.NewWriter(f)
	
	for generatedLength, subjectNumber, uid := 1, 1, 1; generatedLength < *totalSize; generatedLength += *stringLength * *repeatSubjects {
		for i := 0; i < *repeatSubjects; i++ {
			fmt.Fprintf(w, "<0x%x> <Predicate-%d> \"%s\".\n", uid, subjectNumber, randomString(*stringLength))
			uid++
		}
		subjectNumber++
	}
}

