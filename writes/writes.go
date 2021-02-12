package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type ReaderSmall struct {
	i int
}

func (r *ReaderSmall) Read(p []byte) (n int, err error) {
	size := 117000000 // 117 MB (standard backup batch size)
	if size > len(p) {
		size = len(p)
	}
	fmt.Printf("minio asks for %s, providing %s\n",
		humanize.Bytes(uint64(len(p))),
		humanize.Bytes(uint64(size)))
	r.i++
	if r.i == 12 {
		fmt.Println("EOF")
		err = io.EOF
		return
	}
	n, err = rand.Read(p[:size])
	return
}

type ReaderFull struct {
	i int
}

func (r *ReaderFull) Read(p []byte) (n int, err error) {
	fmt.Printf("minio asks for %s, providing %s\n",
		humanize.Bytes(uint64(len(p))),
		humanize.Bytes(uint64(len(p))))
	r.i++
	if r.i == 2 {
		fmt.Println("EOF")
		err = io.EOF
		return
	}
	n, err = rand.Read(p)
	return
}

var (
	AccessKey = flag.String("access-key", "", "AWS Access Key")
	SecretKey = flag.String("secret-key", "", "AWS Secret Key")
)

func init() {
	flag.Parse()
}

func upload(c *minio.Client, r io.Reader) (minio.UploadInfo, error) {
	return c.PutObject(
		context.Background(),
		"dgraph-backup-testing",
		"throughput-test",
		r,
		-1,
		minio.PutObjectOptions{})
}

func main() {
	c, err := minio.New("s3.amazonaws.com", &minio.Options{
		Creds:  credentials.NewStaticV4(*AccessKey, *SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("full reader:")
	// starts uploading immediately, automatically does s3 multipart
	spew.Dump(upload(c, &ReaderFull{}))

	fmt.Println("\nbatched reader:")
	// only starts uploading once the full p []byte is filled by Reader
	//
	// if p []byte is never fully filled, it only starts uploading once EOF is reached
	spew.Dump(upload(c, &ReaderSmall{}))
}
