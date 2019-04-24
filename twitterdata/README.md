## Twitter data in Dgraph
This benchmark provides code to download twitter data using streaming APIs and output is
written in json files. Then, the data is post-processed and converted into live loader
acceptable json files.

## Downloading twitter data

### Output
Generates as many files as number of workers, and each line has one tweet.

### Run
```bash
mkdir json
go run postprocess/pp.go -i json -o pp
```

## Post Processing
This step cleans up the data downloaded from twitter. This step also assigns an external id
to each tweet and user (author or user mentioned in a tweet).
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
user_id: string @index(exact) @upsert .
user_name: string @index(hash) .
screen_name: string @index(term) .

id_str: string @index(exact) @upsert .
created_at: dateTime @index(hour) .
hashtags: [string] @index(exact) .

author: uid @count @reverse .
mention: uid @reverse .
```

### Run
```bash
dgraph live -f pp -x xidmap --zero localhost:5080 -c 1
```
