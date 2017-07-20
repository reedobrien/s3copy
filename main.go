package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const DefaultCopyPartSize = 1024 * 1024 * 500

const DefaultCopyConcurrency = 64
const DefaultCopyTimeout = 60 * time.Minute

// object details the location of a specific object.
type object struct {
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
	return aws.String(fmt.Sprintf("%s/%s", o.bucket, o.key))
}

// String implements the Stringer interface.
func (o object) String() string {
	return fmt.Sprintf("s3://%s/%s", o.bucket, o.key)
}

type Input struct {
	Source    object
	Dest      object
	Delete    bool
	SrcRegion *string
	Region    *string
}

type Copier struct {
	// The chunk size for parts.
	PartSize int64

	// How many parts to copy at once.
	Concurrency int

	// The s3 client ot use when copying.
	S3 s3iface.S3API

	// SrcS3 is the source if set, it is a second region. Needed to delete.
	SrcS3 s3iface.S3API

	// RequestOptions to be passed to the individual calls.
	RequestOptions []request.Option
}

// WithCopierRequestOptions appends to the Copier's API requst options.
func WithCopierRequestOptions(opts ...request.Option) func(*Copier) {
	return func(c *Copier) {
		c.RequestOptions = append(c.RequestOptions, opts...)
	}
}

// NewCopier creates a new Copier instance to copy opbjects concurrently from
// one s3 location to another.
func NewCopier(cfgp client.ConfigProvider, options ...func(*Copier)) *Copier {
	c := &Copier{
		PartSize:    DefaultCopyPartSize,
		S3:          s3.New(cfgp),
		Concurrency: DefaultCopyConcurrency,
	}
	for _, option := range options {
		option(c)
	}

	return c
}

func NewCopierWithClient(svc s3iface.S3API, options ...func(*Copier)) *Copier {
	c := &Copier{
		S3:          svc,
		PartSize:    DefaultCopyPartSize,
		Concurrency: DefaultCopyConcurrency,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

type maxRetrier interface {
	MaxRetries() int
}

func (c Copier) Copy(i Input) error {
	if *i.SrcRegion != "" {
		srcSess := session.Must(session.NewSession(
			&aws.Config{Region: i.SrcRegion}))
		c.SrcS3 = s3.New(srcSess)
	} else {
		c.SrcS3 = c.S3
	}

	return c.CopyWithContext(context.Background(), i)
}

func (c Copier) CopyWithContext(ctx aws.Context, input Input, options ...func(*Copier)) error {
	ctx, cancel := context.WithCancel(ctx)
	impl := copier{in: input, cfg: c, ctx: ctx, cancel: cancel, wg: &sync.WaitGroup{}}

	for _, option := range options {
		option(&impl.cfg)
	}
	impl.cfg.RequestOptions = append(impl.cfg.RequestOptions, request.WithAppendUserAgent("S3Manager"))

	if s, ok := c.S3.(maxRetrier); ok {
		impl.maxRetries = s.MaxRetries()
	}

	if impl.cfg.Concurrency == 0 {
		impl.cfg.Concurrency = DefaultCopyConcurrency
	}
	if impl.cfg.PartSize == 0 {
		impl.cfg.PartSize = DefaultCopyPartSize
	}

	return impl.copy()
}

type copier struct {
	ctx    aws.Context
	cancel context.CancelFunc
	cfg    Copier

	in      Input
	parts   []*s3.CompletedPart
	results chan copyPartResult

	wg *sync.WaitGroup
	m  sync.Mutex

	err error

	maxRetries int
}

func (c copier) copy() error {
	info, err := c.objectInfo(c.in.Source)
	if err != nil {
		return err
	}

	// If there's a request to delete the source copy, do it on exit if there
	// was no error copying.
	if c.in.Delete {
		defer func() {
			if c.err != nil {
				return
			}
			c.deleteObject(c.in.Source)
		}()
	}

	fmt.Printf("Got info %#v\n", *info)
	// If smaller than part size, just copy.
	if *info.ContentLength < c.cfg.PartSize {
		coi := &s3.CopyObjectInput{
			Bucket:     c.in.Dest.Bucket(),
			Key:        c.in.Dest.Key(),
			CopySource: c.in.Source.CopySourceString(),
		}
		_, err = c.cfg.S3.CopyObject(coi)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				fmt.Fprintf(
					os.Stderr, "Failed to get source info for %s: %s\n",
					c.in.Source, aerr.Error())
			} else {
				fmt.Fprintf(
					os.Stderr, "Failed to get source info for %s: %s\n",
					c.in.Source, err)
			}
			return err
		}
		return nil
	}

	// Otherwise do a multipart copy.
	uid, err := c.startMulipart(c.in.Dest)
	if err != nil {
		return err
	}
	fmt.Printf("Started MultipartUpload %s\n", *uid)

	partCount := int(math.Ceil(float64(*info.ContentLength) / float64(c.cfg.PartSize)))
	c.parts = make([]*s3.CompletedPart, partCount)
	c.results = make(chan copyPartResult, c.cfg.Concurrency)
	var partNum int64
	size := int64(*info.ContentLength)
	for size >= 0 {
		for i := 0; i < c.cfg.Concurrency; i++ {
			offset := c.cfg.PartSize * partNum
			endByte := offset + c.cfg.PartSize - 1
			if endByte >= *info.ContentLength {
				endByte = *info.ContentLength - 1
			}
			mci := multipartCopyInput{
				Part:            partNum + 1,
				Bucket:          c.in.Dest.Bucket(),
				Key:             c.in.Dest.Key(),
				CopySource:      c.in.Source.CopySourceString(),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, endByte)),
				UploadId:        uid,
			}
			// fmt.Printf("Starting copy for part %d\nrange: %s\n", partNum, *mci.CopySourceRange)
			// fmt.Println("incrementing wg", partNum+1)
			c.wg.Add(1)
			go c.copyPart(mci)
			partNum++
			size -= c.cfg.PartSize
			if size <= 0 {
				break
			}

		}
	}
	go c.collect()
	c.wait()

	return c.complete(uid)
}

func (c copier) collect() {
	fmt.Println("collecting")
	for r := range c.results {
		// fmt.Println("finished part ", r.Part)
		c.parts[r.Part-1] = &s3.CompletedPart{
			ETag:       r.CopyPartResult.ETag,
			PartNumber: aws.Int64(r.Part)}
	}
}

func (c copier) wait() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	done := make(chan struct{})
	go func() {
		fmt.Println("waiting")
		c.wg.Wait()
		close(c.results)
		done <- struct{}{}
	}()

	// TODO(ro) 2017-07-20 make an abort method and call
	// it here when we exit early.
	select {
	case <-done:
		return
	case sig := <-sigs:
		c.cancel()
		fmt.Fprintf(os.Stderr, "Caught signal %s\n", sig)
		os.Exit(0)
	case <-time.After(time.Duration(DefaultCopyTimeout)):
		c.cancel()
		fmt.Fprintf(os.Stderr, "Copy timed out in %s seconds\n", DefaultCopyTimeout)
		os.Exit(1)
	}
}

func (c copier) getErr() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.err
}

func (c copier) setErr(e error) {
	c.m.Lock()
	defer c.m.Unlock()

	c.err = e
}

func (c copier) copyPart(in multipartCopyInput) {
	var err error
	upci := &s3.UploadPartCopyInput{
		Bucket:          in.Bucket,
		Key:             in.Key,
		CopySource:      in.CopySource,
		CopySourceRange: in.CopySourceRange,
		PartNumber:      aws.Int64(in.Part),
		UploadId:        in.UploadId,
	}
	for retry := 0; retry <= c.maxRetries; retry++ {
		resp, err := c.cfg.S3.UploadPartCopy(upci)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n Part: %d\n Input %#v\n", err, in.Part, *upci)
			continue
		}
		c.results <- copyPartResult{
			Part:           in.Part,
			CopyPartResult: resp.CopyPartResult}
		// fmt.Printf("copied part %d\n", in.Part)
		break
	}
	if err != nil {
		c.setErr(err)
	}
	// fmt.Println("decrementing wg", in.Part)
	c.wg.Done()
	return
}

func (c copier) complete(uid *string) error {
	fmt.Println("finishing")
	cmui := &s3.CompleteMultipartUploadInput{
		Bucket:   c.in.Dest.Bucket(),
		Key:      c.in.Dest.Key(),
		UploadId: uid,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: c.parts,
		},
	}
	_, err := c.cfg.S3.CompleteMultipartUpload(cmui)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to complete copy for %s: %s\n", *c.in.Source.CopySourceString(), err)
		return err
	}
	return nil

}

type copyPartResult struct {
	Part int64
	*s3.CopyPartResult
}

type multipartCopyInput struct {
	Part int64

	Bucket          *string
	CopySource      *string
	CopySourceRange *string
	Key             *string
	UploadId        *string
}

func (c copier) startMulipart(o object) (*string, error) {
	cmui := &s3.CreateMultipartUploadInput{
		Bucket: c.in.Dest.Bucket(),
		Key:    c.in.Dest.Key(),
	}
	resp, err := c.cfg.S3.CreateMultipartUpload(cmui)
	if err != nil {
		return nil, err
	}
	return resp.UploadId, nil
}

func (c copier) objectInfo(o object) (*s3.HeadObjectOutput, error) {
	info, err := c.cfg.SrcS3.HeadObject(&s3.HeadObjectInput{
		Bucket: c.in.Source.Bucket(),
		Key:    c.in.Source.Key(),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Fprintf(
				os.Stderr, "Failed to get source info for %s: %s\n",
				c.in.Source, aerr.Error())
		} else {
			fmt.Fprintf(
				os.Stderr, "Failed to get source info for %s: %s\n",
				c.in.Source, err)
		}
		return nil, err
	}
	return info, nil
}

// deleteObject deletes and object. We can use it after copy, say for a move.
func (c *copier) deleteObject(o object) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.key),
	}
	_, err := c.cfg.SrcS3.DeleteObject(params)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to delete %s: %s", o, err)
	}
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

	in := Input{Source: src, Dest: dst, Delete: *move, Region: region, SrcRegion: srcRegion}
	sess := session.Must(session.NewSession(
		&aws.Config{Region: in.Region}))

	copier := NewCopier(sess)
	err = copier.Copy(in)

	if err != nil {
		fmt.Fprintf(
			os.Stderr, "Failed to get source info for %s: %s\n",
			in.Source, err)
		os.Exit(1)
	}
}
