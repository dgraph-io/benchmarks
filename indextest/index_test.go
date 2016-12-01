// indextest runs some index related queries and check their outputs. Before
// running, you need to get the golden dataset and load it either using the
// loader or by mutation calls to main dgraph client. In the tests below,
// we will send queries to localhost and check their responses.
package indextest

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	//	"log"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	dgraph = flag.String("d", "http://127.0.0.1:8236/query", "Dgraph server address")
)

func testHelper(t *testing.T, prefix string) {
	var r map[string]interface{}

	input, err := ioutil.ReadFile(prefix + ".in")
	require.NoError(t, err)
	expectedOutput, err := ioutil.ReadFile(prefix + ".out")
	require.NoError(t, err)

	// Checking expected output.
	require.NoError(t, json.Unmarshal(expectedOutput, &r))
	_, found := r["me"]
	require.True(t, found)
	expectedJS, err := json.Marshal(r["me"])
	require.NoError(t, err)

	// Post the query.
	var client http.Client
	req, err := http.NewRequest("POST", *dgraph, bytes.NewReader(input))

	require.NoError(t, err)
	res, err := client.Do(req)
	require.NoError(t, err)

	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	// Check the response.
	require.NoError(t, json.Unmarshal(body, &r))
	_, found = r["me"]
	require.True(t, found)

	js, err := json.Marshal(r["me"])
	require.NoError(t, err)

	require.JSONEq(t, string(expectedJS), string(js))
}

func TestFilterStrings(t *testing.T) {
	for _, s := range []string{"basic", "allof_the", "allof_the_a",
		"allof_the_count", "anyof_the_a", "allof_the_first"} {
		t.Run(s, func(t *testing.T) {
			testHelper(t, "data/"+s)
		})
	}
}

func TestSortReleaseDates(t *testing.T) {
	for _, s := range []string{"releasedate", "releasedate_sort",
		"releasedate_sort_count", "releasedate_sort_first_offset"} {
		t.Run(s, func(t *testing.T) {
			testHelper(t, "data/"+s)
		})
	}
}

func TestGenerator(t *testing.T) {
	for _, s := range []string{"gen_anyof_good_bad"} {
		t.Run(s, func(t *testing.T) {
			testHelper(t, "data/"+s)
		})
	}
}
