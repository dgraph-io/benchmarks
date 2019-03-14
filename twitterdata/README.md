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
created_at: dateTime .
id_str: string @index(exact) .
message: string .
author: uid .
urls: [string] .
hashtags: [string] @index(exact) .
mention: uid @reverse .
retweet: bool .

user_name: string @index(hash) .
user_id: string @index(exact) .
screen_name: string @index(exact) .
description: string .
friends_count: int .
verified: bool .
profile_image_url: string .
profile_banner_url: string .
```

### Run
```bash
dgraph live -f pp -x xidmap --zero localhost:5080 -c 1
```

### Spark analysis
Run Spark
```bash
docker run --rm -it -v $(pwd)/bulk/input:/input -v $(pwd)/bulk/spark:/output mangalaman93/spark /opt/spark/bin/spark-shell --master local[4] --driver-memory 6g
```

#### Dedup Data
```scala
import scala.util.Try

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

val df = spark.read.json("/input")
val fdf = df.filter("_corrupt_record is null").select(df.columns.filter(c => c != "_corrupt_record").map(col): _*)
val authorSchema = StructType(
  List(
    StructField("description", StringType, true),
    StructField("friends_count", LongType, true),
    StructField("profile_banner_url", StringType, true),
    StructField("profile_image_url", StringType, true),
    StructField("screen_name", StringType, true),
    StructField("tweet", ArrayType(StructType(List(StructField("uid", StringType, true))), true), true),
    StructField("uid", StringType, true),
    StructField("user_id", StringType, true),
    StructField("user_name", StringType, true),
    StructField("verified", BooleanType, true)
  )
)
val authorDF = fdf.map{ row =>
  row.getAs[Row]("author")
}(RowEncoder(authorSchema))

val mentionSchema = StructType(
  List(
    StructField("screen_name", StringType, true),
    StructField("tweet", ArrayType(StructType(List(StructField("uid", StringType, true))), true), true),
    StructField("uid", StringType, true),
    StructField("user_id", StringType, true),
    StructField("user_name", StringType, true),
    StructField("verified", BooleanType, true)
  )
)
val mentionDF = fdf.flatMap{ row =>
  val mention = row.getAs[Seq[Row]]("mention")
  if (mention == null) List() else mention
}(RowEncoder(mentionSchema)).withColumn("description", lit(null).cast(StringType)).withColumn("friends_count", lit(0).cast(LongType)).withColumn("profile_banner_url", lit(null).cast(StringType)).withColumn("profile_image_url", lit(null).cast(StringType))

val usersDF = mentionDF.select(authorDF.columns.map(col): _*).union(authorDF)
val dedupDF = usersDF.groupByKey(row => row.getAs[String]("user_id")).mapGroups { (_, iter) =>
  iter.foldLeft[Row](null) { (acc, row) =>
    if (acc == null) row else {
      Row.fromSeq(
        acc.toSeq.zip(row.toSeq).map { e =>
          if (e._1 == null) e._2 else e._1
        })
    }
  }
}(RowEncoder(authorSchema))

val tweetSchema = StructType(
  List(
    StructField("author", StructType(List(StructField("uid", StringType, true))), true),
    StructField("created_at", StringType, true),
    StructField("hashtags", ArrayType(StringType, true), true),
    StructField("id_str", StringType, true),
    StructField("mention", ArrayType(StructType(List(StructField("uid", StringType, true))), true), true),
    StructField("message", StringType, true),
    StructField("retweet", BooleanType, true),
    StructField("uid", StringType, true),
    StructField("urls", ArrayType(StringType, true), true)
  )
)

val authorIndex = fdf.columns.indexOf("author")
val mentionIndex = fdf.columns.indexOf("mention")
val tweetDF = fdf.groupByKey(row => row.getAs[String]("id_str")).mapGroups { (_, iter) =>
  val row = iter.toList(0)
  Row.fromSeq(
    row.toSeq.zipWithIndex.map { e =>
      if (e._1 == null) null
      else if (e._2 == authorIndex) Row.fromSeq(List(e._1.asInstanceOf[Row].toSeq(6)))
      else if (e._2 == mentionIndex) e._1.asInstanceOf[Seq[Row]].map(ir => Row.fromSeq(Seq(ir.toSeq(2))))
      else e._1
    })
}(RowEncoder(tweetSchema))

// perform check quickly
// fdf.groupByKey(row => row.getAs[String]("id_str")).count.toDF("value", "count").filter("count > 1").count

usersDF.write.json("/output/users")
tweetDF.write.mode(SaveMode.Overwrite).json("/output/tweets")

```

### Queries
1. Find # of tweets for one more given hashtag
```
{
  hashTweet(func: eq(hashtags, "IWD2019")) @filter(eq(hashtags, "love")) {
    uid
    message
    retweet
    id_str
    author
    created_at
  }
}
```

2. Find tweets from a user with given username (traversal depth 1)
```
{
  me(func: eq(screen_name, "mangalaman93")) {
    uid
    screen_name
    user_name
    user_id
    tweet {
      uid
      id_str
      message
      retweet
      created_at
      mention
    }
  }
}
```

3. Find number of times a user mentions other users in his/her tweets, in the order of number of mentions (traversal depth 1 with group by and sorting)


4. Let say the set of users mentioned by a given user in his/her tweets is called MentionUserSet. Find the number of times the same user is mentioned in tweets by the users in MentionUserSet. (traversal depth 4)

5. Find the total number of unique users that have used a given hashtag.
