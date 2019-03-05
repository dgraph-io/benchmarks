package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dghubble/go-twitter/twitter"
)

const (
	cEveryNMessages = 100

	cTimeFormat       = "Mon Jan 02 15:04:05 -0700 2006"
	cDgraphTimeFormat = "2006-01-02T15:04:05.999999999+10:00"
)

var (
	errNotATweet = errors.New("message in the stream is not a tweet")
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("invalid command!")
		fmt.Println("Usage: pp.go <num_workers> <input_dir> <output_dir>")
		return
	}

	numWorkersStr := os.Args[1]
	inputDir := os.Args[2]
	outDir := os.Args[3]

	numWorkers, err := strconv.Atoi(numWorkersStr)
	if err != nil {
		panic(err)
	}

	// start workers
	var wg sync.WaitGroup
	work := make(chan string, 10000)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go doWork(i, &wg, work, outDir)
	}

	err = filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("error occurred while walking directory ::", err)
			return err
		}

		if !strings.HasSuffix(path, ".json") {
			return nil
		}

		return processFile(path, work)
	})
	if err != nil {
		panic(err)
	}

	close(work)
	wg.Wait()
}

func processFile(path string, work chan<- string) error {
	fmt.Println("processing", path)

	fd, err := os.Open(path)
	if err != nil {
		fmt.Println("error in processing file ::", err)
		return err
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		work <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error in reading file ::", err)
		return err
	}

	return nil
}

func doWork(id int, wg *sync.WaitGroup, work <-chan string, outDir string) {
	defer wg.Done()

	fd, err := os.Create(fmt.Sprintf("%s/twitter_feed_%d.rdf", outDir, id))
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)
	var totalMessages, erroredMessages, notTweetMessages int
	for message := range work {
		totalMessages++

		if err := processMessage(writer, message); err != nil {
			if err == errNotATweet {
				notTweetMessages++
			} else {
				fmt.Println("error in processing message ::", err)
				erroredMessages++
			}

			continue
		}

		if totalMessages%cEveryNMessages == 0 {
			fmt.Printf("Routine: %d, Total: %d, notTweet:%d, error: %d\n", id,
				totalMessages, notTweetMessages, erroredMessages)
		}
	}

	writer.Flush()
	fmt.Printf("Routine: %d, Total: %d, notTweet:%d, error: %d\n", id,
		totalMessages, notTweetMessages, erroredMessages)
}

func processMessage(writer io.Writer, message string) error {
	tweet, err := parseMessage(message)
	if err != nil {
		return err
	}

	// produce RDF data from tweet
	if err := tweetToRDF(writer, tweet); err != nil {
		return err
	}

	return nil
}

func parseMessage(message string) (*twitter.Tweet, error) {
	bytes := []byte(message)

	// unmarshal JSON encoded token into a map for
	var data map[string]interface{}
	err := json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, err
	}

	if _, ok := data["retweet_count"]; ok {
		tweet := new(twitter.Tweet)
		json.Unmarshal(bytes, tweet)
		return tweet, nil
	}

	return nil, errNotATweet
}

func tweetToRDF(writer io.Writer, tweet *twitter.Tweet) error {
	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <author> _:%v .\n",
		tweet.IDStr, tweet.User.IDStr))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <tweet> _:%v .\n",
		tweet.User.IDStr, tweet.IDStr))); err != nil {
		return err
	}

	// Handle Tweet Attributes
	t, err := time.Parse(cTimeFormat, tweet.CreatedAt)
	if err != nil {
		return err
	}
	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <created_at> \"%v\" .\n",
		tweet.IDStr, t.Format(cDgraphTimeFormat)))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <id_str> \"%v\" .\n",
		tweet.IDStr, tweet.IDStr))); err != nil {
		return err
	}

	var tweetText string
	if tweet.Truncated {
		tweetText = tweet.ExtendedTweet.FullText
	} else {
		tweetText = tweet.FullText
	}
	if tweetText != "" {
		if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <message> %v .\n",
			tweet.IDStr, strconv.Quote(tweetText)))); err != nil {
			return err
		}
	}

	var urlEntities []twitter.URLEntity
	if tweet.Truncated {
		urlEntities = tweet.ExtendedTweet.Entities.Urls
	} else {
		urlEntities = tweet.Entities.Urls
	}
	for _, url := range urlEntities {
		if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <url> \"%v\" .\n",
			tweet.IDStr, url.ExpandedURL))); err != nil {
			return err
		}
	}

	var hashTags []twitter.HashtagEntity
	if tweet.Truncated {
		hashTags = tweet.ExtendedTweet.Entities.Hashtags
	} else {
		hashTags = tweet.Entities.Hashtags
	}
	for _, tag := range hashTags {
		if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <hashtags> \"%v\" .\n",
			tweet.IDStr, tag.Text))); err != nil {
			return err
		}
	}

	// User Attributes
	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <user_id> \"%v\" .\n",
		tweet.User.IDStr, tweet.User.IDStr))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <screen_name> \"%v\" .\n",
		tweet.User.IDStr, tweet.User.ScreenName))); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(fmt.Sprintf("_:%v <user_name> %v .\n",
		tweet.User.IDStr, strconv.Quote(tweet.User.Name)))); err != nil {
		return err
	}

	return nil
}
