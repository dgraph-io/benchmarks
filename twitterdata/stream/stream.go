package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

const (
	cEveryNMessages = 100

	cConfAccessToken       = "TWITTER_ACCESS_TOKEN"
	cConfAccessTokenSecret = "TWITTER_ACCESS_TOKEN_SECRET"
	cConfConsumerKey       = "TWITTER_CONSUMER_KEY"
	cConfConsumerSecret    = "TWITTER_CONSUMER_SECRET"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("invalid command!")
		fmt.Println("Usage: stream.go <keywords_file> <num_workers> <output_dir>")
		return
	}

	keywordsFile := os.Args[1]
	numWorkersStr := os.Args[2]
	outDir := os.Args[3]

	numWorkers, err := strconv.Atoi(numWorkersStr)
	if err != nil {
		panic(err)
	}

	// setup twitter client
	client := getTwitterClient()

	// TODO: setup a file watcher
	keywords, err := readKeywords(keywordsFile)
	if err != nil {
		panic(err)
	}

	params := &twitter.StreamFilterParams{
		Track:         keywords,
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	if err != nil {
		panic(err)
	}

	// start worker routines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go doWork(i, &wg, stream.Messages, outDir)
	}

	wg.Wait()
}

func doWork(id int, wg *sync.WaitGroup, work <-chan interface{}, outDir string) {
	defer wg.Done()

	fd, err := os.Create(fmt.Sprintf("%s/twitter_feed_%d.json", outDir, id))
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)
	var totalMessages, erroredMessages int
	for message := range work {
		totalMessages++

		data, err := json.Marshal(message)
		if err != nil {
			fmt.Println("error in marshalling feed item ::", err)
			erroredMessages++
			continue
		}

		data = append(data, '\n')
		if _, err := writer.Write(data); err != nil {
			fmt.Println("error writing to file ::", err)
			erroredMessages++
			continue
		}

		if totalMessages%cEveryNMessages == 0 {
			fmt.Printf("Routine: %d, Total: %d, error: %d\n", id,
				totalMessages, erroredMessages)
		}
	}
}

// readKeywords reads keywords from a file and passes to the Twitter Filter API
func readKeywords(file string) ([]string, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	keywords := make([]string, 0)
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		keyword := scanner.Text()
		keywords = append(keywords, keyword)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return keywords, nil
}

// getTwitterClient sets up a twitter client
func getTwitterClient() *twitter.Client {
	accessToken := os.Getenv(cConfAccessToken)
	accessTokenSecret := os.Getenv(cConfAccessTokenSecret)
	token := oauth1.NewToken(accessToken, accessTokenSecret)

	consumerKey := os.Getenv(cConfConsumerKey)
	consumerSecret := os.Getenv(cConfConsumerSecret)
	config := oauth1.NewConfig(consumerKey, consumerSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	return twitter.NewClient(httpClient)
}
