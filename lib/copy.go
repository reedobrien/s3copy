package s3copy

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
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

// DefaultCopyPartSize declares the default size of chunks to get copied. It is currently set dumbly to 500MB. So that the maximum object size (5TB) will work without exceeding the maximum part count (10,000).
const DefaultCopyPartSize = 1024 * 1024 * 100

// DefaultCopyConcurrency sets the number of parts to request copying at once.
const DefaultCopyConcurrency = 64

// DefaultCopyTimeout is the max time we expect the copy operation to take. For a lambda < 5 minutes is best, but for a large copy it could take hours.
// const DefaultCopyTimeout = 260 * time.Second
const DefaultCopyTimeout = 18 * time.Hour

type object interface {
	Bucket() *string
	Key() *string
	CopySourceString() *string
	String() string
	Size() int
}

// CopierInput holds the input paramters for Copier.Copy.
type CopierInput struct {
	Source    object
	Dest      object
	Delete    bool
	SrcRegion *string
	Region    *string
}

// Copier holds the configuration details for copying from an s3 object to another s3 location.
type Copier struct {
	// The chunk size for parts.
	PartSize int64

	// How long to run before we quit waiting.
	Timeout time.Duration

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
		Timeout:     DefaultCopyTimeout,
		S3:          s3.New(cfgp),
		Concurrency: DefaultCopyConcurrency,
	}
	for _, option := range options {
		option(c)
	}

	return c
}

// NewCopierWithClient returns a Copier using the provided s3API client.
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

// maxRetrier provices an interface to MaRetries. This was copied from aws sdk.
// TODO(ro) 2017-07-22 remove if part of the s3manager package.
type maxRetrier interface {
	MaxRetries() int
}

// Copy copies the source object to the tagret object.
func (c Copier) Copy(i CopierInput) error {
	if i.SrcRegion != nil && *i.SrcRegion != "" {
		srcSess := session.Must(session.NewSession(
			&aws.Config{Region: i.SrcRegion}))
		c.SrcS3 = s3.New(srcSess)
	} else {
		c.SrcS3 = c.S3
	}

	return c.CopyWithContext(context.Background(), i)
}

// CopyWithContext performs Copy with the provided context.Context.
func (c Copier) CopyWithContext(ctx aws.Context, input CopierInput, options ...func(*Copier)) error {
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

	in      CopierInput
	parts   []*s3.CompletedPart
	results chan copyPartResult
	work    chan multipartCopyInput

	wg *sync.WaitGroup
	m  *sync.Mutex

	err error

	maxRetries int
}

func (c copier) getContentLength() (int64, error) {
	var size int64
	size = int64(c.in.Source.Size())
	// If less than 1 we want to double check, because unset == 0. We can make
	// it a pointer and check for nil later.
	if size <= 0 {
		info, err := c.objectInfo(c.in.Source)
		if err != nil {
			return -1, err
		}
		size = *info.ContentLength
	}

	return size, nil
}
func (c copier) copy() error {
	contentLength, err := c.getContentLength()
	if err != nil {
		return err
	}
	// log.Printf("ContentLength (%d) for %q", contentLength, c.in.Source.String())

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

	// If smaller than part size, just copy.
	if contentLength < c.cfg.PartSize {
		log.Printf("Single part copy: %q", c.in.Source.String())
		return c.copyObject()

	}

	// Otherwise do a multipart copy.
	uid, err := c.startMulipart(c.in.Dest)
	if err != nil {
		return err
	}
	log.Printf("Started MultipartUpload %q for %q", *uid, c.in.Source.String())

	partCount := int(math.Ceil(float64(contentLength) / float64(c.cfg.PartSize)))
	c.parts = make([]*s3.CompletedPart, partCount)
	c.results = make(chan copyPartResult, c.cfg.Concurrency)
	c.work = make(chan multipartCopyInput, c.cfg.Concurrency)
	var partNum int64
	size := contentLength
	go func() {
		for size >= 0 {
			offset := c.cfg.PartSize * partNum
			endByte := offset + c.cfg.PartSize - 1
			if endByte >= contentLength {
				endByte = contentLength - 1
			}
			mci := multipartCopyInput{
				Part:            partNum + 1,
				Bucket:          c.in.Dest.Bucket(),
				Key:             c.in.Dest.Key(),
				CopySource:      c.in.Source.CopySourceString(),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, endByte)),
				UploadID:        uid,
			}
			c.wg.Add(1)
			c.work <- mci
			partNum++
			size -= c.cfg.PartSize
			if size <= 0 {
				break
			}

		}
		// log.Printf("MultipartUpload copy parts assembled %d parts", len(c.parts))
		close(c.work)
	}()

	// log.Println("Copying parts")
	for i := 0; i < c.cfg.Concurrency; i++ {
		go c.copyParts()
	}
	go c.collect()
	c.wait()

	return c.complete(uid)
}

func (c copier) copyObject() error {
	coi := &s3.CopyObjectInput{
		Bucket:     c.in.Dest.Bucket(),
		Key:        c.in.Dest.Key(),
		CopySource: c.in.Source.CopySourceString(),
	}
	_, err := c.cfg.S3.CopyObject(coi)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			log.Printf(
				"Failed to get source info for %s: %s\n",
				c.in.Source, aerr.Error())
		} else {
			log.Printf(
				"Failed to get source info for %s: %s\n",
				c.in.Source, err)
		}
		return err
	}
	return nil

}
func (c copier) collect() {
	fmt.Println("collecting")
	var received int
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		select {
		case r := <-c.results:
			// log.Printf("collected %d: %s", r.Part, *c.in.Source.Key())
			c.parts[r.Part-1] = &s3.CompletedPart{
				ETag:       r.CopyPartResult.ETag,
				PartNumber: aws.Int64(r.Part)}
			received++
		case <-time.After(time.Second):
			if received == len(c.parts) {
				close(c.results)
				return
			}
		}
	}
}

func (c copier) wait() {
	fmt.Println("waiting")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		done <- struct{}{}
	}()

	// TODO(ro) 2017-07-20 make an abort method and call
	// it here when we exit early.
	select {
	case <-done:
		return
	case sig := <-sigs:
		c.cancel()
		log.Printf("Caught signal %s\n", sig)
		os.Exit(0)
	case <-time.After(c.cfg.Timeout):
		c.cancel()
		log.Printf("Copy timed out in %s seconds\n", c.cfg.Timeout)
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

func (c copier) copyParts() {
	var err error
	var resp *s3.UploadPartCopyOutput
	for in := range c.work {
		upci := &s3.UploadPartCopyInput{
			Bucket:          in.Bucket,
			Key:             in.Key,
			CopySource:      in.CopySource,
			CopySourceRange: in.CopySourceRange,
			PartNumber:      aws.Int64(in.Part),
			UploadId:        in.UploadID,
		}
		log.Printf("Copy part: %d %#v", *upci.PartNumber, *upci.Key)
		for retry := 0; retry <= c.maxRetries; retry++ {
			resp, err = c.cfg.S3.UploadPartCopy(upci)
			if err != nil {
				log.Printf("Error: %s\n Part: %d\n Input %#v\n", err, in.Part, *upci)
				continue
			}
			// log.Printf("sending copyPartResult for part %d: %s", in.Part, *in.Key)
			c.results <- copyPartResult{
				Part:           in.Part,
				CopyPartResult: resp.CopyPartResult}
			// log.Printf("copyParts done part %d for %s", in.Part, *in.Key)
			c.wg.Done()
			break
		}
		// c.wg.Done()
		if err != nil {
			log.Printf("setting error received retrying part upload %d for : %q", in.Part, *in.Key)
			c.setErr(err)
		}
	}
	return
}

func (c copier) complete(uid *string) error {
	log.Printf("completing...%q without error? %s", *uid, c.err)

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
		log.Printf("Failed to complete copy for %s: %s\n", *c.in.Source.CopySourceString(), err)
		log.Println(c.parts)
		return err
	}
	log.Println("completed...", *uid)
	return nil

}

type copyPartResult struct {
	Part int64
	*s3.CopyPartResult
}

type multipartCopyInput struct {
	Part            int64
	Bucket          *string
	CopySource      *string
	CopySourceRange *string
	Key             *string
	UploadID        *string
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
			log.Printf(
				"Failed to get source object info for %s: %s\n",
				c.in.Source, aerr.Error())
		} else {
			log.Printf(
				"Failed to get source object info for %s: %s\n",
				c.in.Source, err)
		}
		return nil, err
	}
	return info, nil
}

// deleteObject deletes and object. We can use it after copy, say for a move.
func (c *copier) deleteObject(o object) {
	params := &s3.DeleteObjectInput{
		Bucket: o.Bucket(),
		Key:    o.Key(),
	}
	_, err := c.cfg.SrcS3.DeleteObject(params)
	if err != nil {
		log.Printf("Failed to delete %s: %s", o, err)
	}
}
