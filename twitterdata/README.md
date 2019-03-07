## Twitter data in Dgraph
This benchmark provides code to download twitter data using
streaming APIs and output is written in json files. Then, the
data is post-processed and converted into live loader acceptable
json files.

## Downloading twitter data
### Setup
export TWITTER_ACCESS_TOKEN_SECRET=<access_token_secret>
export TWITTER_ACCESS_TOKEN=<access_token>
export TWITTER_CONSUMER_KEY=<consumer_key>
export TWITTER_CONSUMER_SECRET=<consumer_secret>

### keywords.txt
You can update this file in order to add or remove keywords Each line has one keyword.

### Output
Generates as many files as number of workers, and each line has one tweet.

### Run
```bash
mkdir json
go run stream/stream.go stream/keywords.txt 10 json
```

## Post Processing
```bash
mkdir pp
go run postprocess/pp.go 10 json pp
```

## Run Live Loader
### Setup Dgraph
```bash
docker run --rm -it -p 5080:5080 -p 6080:6080 -p 8080:8080 -p 9080:9080 -p 8000:8000 --name dgraph dgraph/dgraph dgraph zero
docker exec -it dgraph dgraph alpha --lru_mb 2048 --zero localhost:5080
docker exec -it dgraph dgraph-ratel
```
(Note, all data would be lost when container stops)

### Setup Schema
Use dgraph ratel to setup following schema -
```
tweet: uid .
created_at: dateTime @index(day) .
id_str: string @index(exact) .
message: string .
author: uid .
urls: [string] .
hashtags: [string] @index(exact) .
mention: uid .
retweet: bool .

user_name: string @index(hash) .
user_id: string @index(exact) .
screen_name: string @index(term) .
description: string .
friends_count: int @index(int) .
verified: bool @index(bool) .
profile_image_url: string .
profile_banner_url: string .
```

### Run
```bash
dgraph live -f pp -x xidmap --zero localhost:5080 -c 1
```
