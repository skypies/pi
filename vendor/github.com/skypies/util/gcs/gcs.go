package gcs

import (
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
)

type RWHandle struct {
	Writer *storage.Writer
	Client *storage.Client
}

// IMPORTANT - if you don't call this (and see it return nil), your data is likely lost
func (h *RWHandle)Close() error {
	if err := h.Writer.Close(); err != nil { return err }
	if err := h.Client.Close(); err != nil { return err }
	return nil
}

func (h *RWHandle)IOWriter() io.Writer {
	return io.Writer(h.Writer)
}

func Exists(ctx context.Context, bucketname string, filename string) (bool,error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Errorf(ctx, "failed to get a client: %v", err)
		return false, err
	}
	
	bucket := client.Bucket(bucketname)
	if bucket == nil {
		return false, fmt.Errorf("GCS client.Bucket() was nil")
	}

	if _,err := bucket.Object(filename).NewReader(ctx); err == nil {
		return true,nil
	} else if err == storage.ErrObjectNotExist {
		return false,nil
	} else {
		return false,err
	}
}

func OpenRW(ctx context.Context, bucketname string, filename string, contentType string) (*RWHandle, error) {
	handle := RWHandle{}
	if c, err := storage.NewClient(ctx); err != nil {
		log.Errorf(ctx, "failed to get a client: %v", err)
		return nil, err
	} else {
		handle.Client = c
	}
	
	bucket := handle.Client.Bucket(bucketname)
	if bucket == nil {
		return nil, fmt.Errorf("GCS client.Bucket() was nil")
	}

	handle.Writer = bucket.Object(filename).NewWriter(ctx)
	handle.Writer.ContentType = contentType //"text/plain" // CSV?
	
	return &handle, nil
}
