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
	cEveryNMessages = 1000

	cTimeFormat       = "Mon Jan 02 15:04:05 -0700 2006"
	cDgraphTimeFormat = "2006-01-02T15:04:05.999999999+10:00"
)

var (
	errNotATweet = errors.New("message in the stream is not a tweet")
)

type twitterUser struct {
	UID              string `json:"uid"`
	UserID           string `json:"user_id"`
	UserName         string `json:"user_name,omitempty"`
	ScreenName       string `json:"screen_name,omitempty"`
	Description      string `json:"description,omitempty"`
	FriendsCount     int    `json:"friends_count,omitempty"`
	Verified         bool   `json:"verified"`
	ProfileBannerURL string `json:"profile_banner_url,omitempty"`
	ProfileImageURL  string `json:"profile_image_url,omitempty"`
	Tweet            []struct {
		UID string `json:"uid"`
	} `json:"tweet"`
}

type twitterTweet struct {
	UID       string        `json:"uid"`
	IDStr     string        `json:"id_str"`
	CreatedAt string        `json:"created_at"`
	Message   string        `json:"message,omitempty"`
	URLs      []string      `json:"urls,omitempty"`
	HashTags  []string      `json:"hashtags,omitempty"`
	Author    twitterUser   `json:"author"`
	Mention   []twitterUser `json:"mention,omitempty"`
	Retweet   bool          `json:"retweet"`
}

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

	fd, err := os.Create(fmt.Sprintf("%s/twitter_feed_pp_%d.json", outDir, id))
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)
	if _, err := writer.WriteString("[\n"); err != nil {
		panic(err)
	}

	var totalMessages, erroredMessages, notTweetMessages int
	var noErr bool
	for message := range work {
		if noErr {
			if _, err := writer.WriteString(",\n"); err != nil {
				panic(err)
			}
		}
		totalMessages++

		if err := processMessage(writer, message); err != nil {
			noErr = false
			if err == errNotATweet {
				notTweetMessages++
			} else {
				fmt.Println("error in processing message ::", err)
				erroredMessages++
			}

			continue
		}

		noErr = true
		if totalMessages%cEveryNMessages == 0 {
			fmt.Printf("Routine: %d, Total: %d, notTweet:%d, error: %d\n", id,
				totalMessages, notTweetMessages, erroredMessages)
		}
	}

	if _, err := writer.WriteString("\n]"); err != nil {
		panic(err)
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
	createdAt, err := time.Parse(cTimeFormat, tweet.CreatedAt)
	if err != nil {
		return err
	}

	var tweetText string
	if tweet.Truncated {
		tweetText = tweet.ExtendedTweet.FullText
	} else {
		tweetText = tweet.FullText
	}

	var urlEntities []twitter.URLEntity
	if tweet.Truncated {
		urlEntities = tweet.ExtendedTweet.Entities.Urls
	} else {
		urlEntities = tweet.Entities.Urls
	}
	expandedURLs := make([]string, len(urlEntities))
	for _, url := range urlEntities {
		expandedURLs = append(expandedURLs, url.ExpandedURL)
	}

	var hashTags []twitter.HashtagEntity
	if tweet.Truncated {
		hashTags = tweet.ExtendedTweet.Entities.Hashtags
	} else {
		hashTags = tweet.Entities.Hashtags
	}
	hashTagTexts := make([]string, len(hashTags))
	for _, tag := range hashTags {
		hashTagTexts = append(hashTagTexts, tag.Text)
	}

	var userMentions []twitterUser
	for _, userMention := range tweet.Entities.UserMentions {
		userMentions = append(userMentions, twitterUser{
			UID:        fmt.Sprintf("_:%v", userMention.IDStr),
			UserID:     userMention.IDStr,
			UserName:   userMention.Name,
			ScreenName: userMention.ScreenName,
		})
	}

	dt := twitterTweet{
		UID:       fmt.Sprintf("_:%v", tweet.IDStr),
		IDStr:     tweet.IDStr,
		CreatedAt: createdAt.Format(cDgraphTimeFormat),
		Message:   unquote(strconv.Quote(tweetText)),
		URLs:      expandedURLs,
		HashTags:  hashTagTexts,
		Author: twitterUser{
			UID:              fmt.Sprintf("_:%v", tweet.User.IDStr),
			UserID:           tweet.User.IDStr,
			UserName:         unquote(strconv.Quote(tweet.User.Name)),
			ScreenName:       tweet.User.ScreenName,
			Description:      unquote(strconv.Quote(tweet.User.Description)),
			FriendsCount:     tweet.User.FriendsCount,
			Verified:         tweet.User.Verified,
			ProfileBannerURL: tweet.User.ProfileBannerURL,
			ProfileImageURL:  tweet.User.ProfileImageURL,
			Tweet: []struct {
				UID string `json:"uid"`
			}{
				{
					UID: fmt.Sprintf("_:%v", tweet.IDStr),
				},
			},
		},
		Mention: userMentions,
		Retweet: tweet.Retweeted,
	}

	data, err := json.Marshal(dt)
	if err != nil {
		return err
	}

	if _, err := writer.Write(data); err != nil {
		return err
	}

	return nil
}

func unquote(s string) string {
	return s[1 : len(s)-1]
}
