package main

import (
	"flag"

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
	c, err := minio.New("", &minio.Options{
		Creds:  credentials.NewStaticV4(*AccessKey, *SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		panic(err)
	}

	spew.Dump(c)
}
