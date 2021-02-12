package main

import (
	"bytes"
	"context"
	"flag"
	"io/ioutil"

	"github.com/davecgh/go-spew/spew"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	AccessKey = flag.String("access-key", "", "AWS Access Key")
	SecretKey = flag.String("secret-key", "", "AWS Secret Key")
)

func init() {
	flag.Parse()
}

func main() {
	data, err := ioutil.ReadFile("/home/karl/random") // 1gb file
	if err != nil {
		panic(err)
	}

	dataReader := bytes.NewReader(data)

	c, err := minio.New("s3.amazonaws.com", &minio.Options{
		Creds:  credentials.NewStaticV4(*AccessKey, *SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		panic(err)
	}

	// starts uploading immediately
	spew.Dump(
		c.PutObject(
			context.Background(),
			"dgraph-backup-testing",
			"throughput-test",
			dataReader,
			int64(len(data)),
			minio.PutObjectOptions{}),
	)
}
