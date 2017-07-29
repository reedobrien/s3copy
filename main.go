package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	s3copy "gitlab.com/reedobrien/s3copy/lib"
)

// object details the location of a specific object.
type object struct {
	bucket string
	key    string
}

// Bucket returns the string pointer value for the bucket.
func (o object) Bucket() *string {
	return aws.String(o.bucket)
}

// Key returns the string pointer value for the object key.
func (o object) Key() *string {
	return aws.String(o.key)
}

// CopySourceString returns the string pointer value for passing into various
// copy functions.
func (o object) CopySourceString() *string {
	return aws.String(fmt.Sprintf("%s/%s", o.bucket, o.key))
}

// String implements the Stringer interface.
func (o object) String() string {
	return fmt.Sprintf("s3://%s/%s", o.bucket, o.key)
}

func main() {
	var err error

	source := flag.String("source", "", "The source bucket and key. e.g. bucket/key/one")
	dest := flag.String("dest", "", "The destination bucket and key.")
	move := flag.Bool("move", false, "Delete the file after copy")

	region := flag.String("region", os.Getenv("AWS_DEFAULT_REGION"), "Destination bucket region.")
	srcRegion := flag.String("srcRegion", "", "Source bucket region, if different")

	flag.Parse()

	// Not enough checking here.
	srcElems := strings.SplitN(*source, "/", 2)
	src := object{bucket: srcElems[0], key: srcElems[1]}
	destElems := strings.SplitN(*dest, "/", 2)
	dst := object{bucket: destElems[0], key: destElems[1]}

	in := s3copy.CopierInput{Source: src, Dest: dst, Delete: *move, Region: region, SrcRegion: srcRegion}
	sess := session.Must(session.NewSession(
		&aws.Config{Region: in.Region}))

	copier := s3copy.NewCopier(sess)
	// copier := s3copy.NewCopier(sess, func(c *s3copy.Copier) {
	// 	c.Timeout = 1 * time.Second
	// })
	err = copier.Copy(in)

	if err != nil {
		os.Exit(1)
	}
}
