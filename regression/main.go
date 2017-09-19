// This tool is used to extract runnable queries from out docs.

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/dgraph-io/dgraph/x"
)

var file = flag.String("file", "", "File to extract queries from")

func main() {
	flag.Parse()
	f, err := os.Open(*file)
	x.Check(err)

	os.Mkdir("queries", 0755)

	start := `{{< runnable >}}`
	end := `{{< /runnable >}}`
	scanner := bufio.NewScanner(f)

	counter := 0
	var b bytes.Buffer
	var insideRunnable bool
	for scanner.Scan() {
		l := scanner.Text()
		trimmed := strings.TrimSpace(l)
		if trimmed == start {
			insideRunnable = true
			continue
		} else if trimmed == end {
			insideRunnable = false
			// Write to file.
			counter++
			err = ioutil.WriteFile(fmt.Sprintf("queries/query-%d.txt", counter), b.Bytes(), 0755)
			x.Check(err)
			b.Reset()
		}

		if !insideRunnable {
			continue
		}

		_, err = b.WriteString(l)
		x.Check(err)
		_, err = b.WriteRune('\n')
		x.Check(err)
	}
	x.Check(scanner.Err())
}
