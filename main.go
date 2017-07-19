package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const defaultPartSize = 1024 * 1024 * 100

const defaultConcurrency = 64

// object details the location of a specific object.
type object struct {
	// region string
	bucket string
	key    string
}

func (o object) Bucket() *string {
	return aws.String(o.bucket)
}

func (o object) Key() *string {
	return aws.String(o.key)
}

func (o object) CopySourceString() *string {
	return aws.String(fmt.Sprintf("/%s/%s", o.bucket, o.key))
}

// String implements the Stringer interface.
func (o object) String() string {
	return fmt.Sprintf("s3://%s/%s", o.bucket, o.key)
}

type Input struct {
	Size   int64
	Source object
	Dest   object
	Delete bool
}

type Copier struct {
	// The chunk size for parts.
	PartSize int64

	// How many parts to copy at once.
	Concurrency int

	// The s3 client ot use when copying.
	S3 s3iface.S3API

	// Source is the source if set, it is a second region. Needed to delete.
	Source s3iface.S3API

	// Request options to be passed to the individual calls.
	RequestOptions []request.Option
}

func (c Copier) Copy(i Input) error {
	var err error
	info, err := c.objectInfo(i.Source)
	if err != nil {
		return err
	}
	if i.Delete {
		defer func() {
			if err != nil {
				return
			}
			c.deleteObject(i.Source)
		}()
	}
	// If smaller than part size, just copy.
	if *info.ContentLength < c.PartSize {
		coi := &s3.CopyObjectInput{
			Bucket:     i.Dest.Bucket(),
			Key:        i.Dest.Key(),
			CopySource: i.Source.CopySourceString(),
		}
		_, err = c.S3.CopyObject(coi)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				fmt.Fprint(
					os.Stderr, "Failed to get source info for %s: %s",
					input.Source, aerr.Error())
			} else {
				fmt.Fprintf(
					os.Stderr, "Failed to get source info for %s: %s",
					input.Source, err)
			}
			return err
		}
		return nil
	}

	// Otherwise do a multipart copy.
	cmui := &s3.CreateMultipartUploadInput{
		Bucket: i.Dest.Bucket(),
		Key:    i.Dest.Key(),
	}
	resp, err := c.S3.CreateMultipartUpload(cmui)
	if err != nil {
		return err
	}

	uid := *resp.UploadId

	return nil
}

func (c Copier) objectInfo(o object) (*s3.HeadObjectOutput, error) {
	info, err := c.Source.HeadObject(&s3.HeadObjectInput{
		Bucket: o.Source.Bucket(),
		Key:    o.Source.Key(),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Fprint(
				os.Stderr, "Failed to get source info for %s: %s",
				input.Source, aerr.Error())
		} else {
			fmt.Fprintf(
				os.Stderr, "Failed to get source info for %s: %s",
				input.Source, err)
		}
		return nil, err
	}
	return info, nil
}

// deleteObject deletes and object. We can use it after copy, say for a move.
func (c *Copier) deleteObject(o object) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	}
	_, err := c.Source.DeleteObject(params)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to delete %s: %s", object, err)
	}
}

func main() {
	var err error

	if aerr, ok := err.(awserr.Error); ok {
		fmt.Fprint(
			os.Stderr, "Failed to get source info for %s: %s",
			input.Source, aerr.Error())
	} else {
		fmt.Fprintf(
			os.Stderr, "Failed to get source info for %s: %s",
			input.Source, err)
	}
	os.Exit(1)

}
