package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var (
	file        = flag.String("file", "21million.rdf.gz", "Input RDF data file name.")
	output      = flag.String("out", "21million-new.rdf.gz", "Output RDF data file name.")
	createPlain = flag.Bool("plain", true, `Keep language strings and create "plain_" predicates.`)
)

func removeLang(path, outputPath string) error {
	readFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer readFile.Close()

	gzReader, err := gzip.NewReader(readFile)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)

	writeFile, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer writeFile.Close()

	gzWriter := gzip.NewWriter(writeFile)
	defer gzWriter.Close()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "	")
		subject := parts[0]
		predicate := parts[1]
		object := parts[2]

		if oneOfLanguage(predicate) {
			if *createPlain {
				gzWriter.Write([]byte(line))
				gzWriter.Write([]byte("\n"))
			}
			if strings.HasSuffix(object, "@en") {
				var newLine string
				pred := predicate
				if *createPlain {
					fmt.Printf("Creating plain pred for: %v\n", line)
					pred = plain(predicate)
				}
				newLine = fmt.Sprintf("%s\t%s\t%s\t.\n", subject, pred, object[0:len(object)-3])
				gzWriter.Write([]byte(newLine))
			} else {
				// Ignore non-@en predicates
			}
		} else {
			gzWriter.Write([]byte(line))
			gzWriter.Write([]byte("\n"))
		}
	}

	return nil
}

func oneOfLanguage(pred string) bool {
	switch pred {
	case "<name>":
		return true
	case "<performance.character_note>":
		return true
	case "<tagline>":
		return true
	case "<cut.note>":
		return true
	default:
		return false
	}
}

// plain prefixes the predicate name with "plain_".
// e.g., <name> becomes <plain_name>
func plain(pred string) string {
	return pred[0:1] + "plain_" + pred[1:]
}

func main() {
	flag.Parse()
	if err := removeLang(*file, *output); err != nil {
		log.Fatal(err)
	}

}
