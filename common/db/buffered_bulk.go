package db

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	MaxMessageSize = 47 * 1000 * 1000 // real = 48000000
)

type BufferedBulk struct {
	bulk            *mgo.Bulk
	collection      *mgo.Collection
	continueOnError bool
	docLimit        int

	byteCount int
	docCount  int

	// for testing purposes
	flushCount int
}

// NewBufferedBulk returns an initialized BufferedBulk writer
func NewBufferedBulk(collection *mgo.Collection, docLimit int,
	continueOnError bool) *BufferedBulk {

	bb := &BufferedBulk{
		collection:      collection,
		continueOnError: continueOnError,
		docLimit:        docLimit,
	}
	bb.refreshBulk()
	return bb
}

// throw away the old bulk and init a new one
func (bb *BufferedBulk) refreshBulk() {
	bb.bulk = bb.collection.Bulk()
	if bb.continueOnError {
		bb.bulk.Unordered()
	}
	bb.byteCount = 0
	bb.docCount = 0
}

// Insert buffers a document for bulk insertion. If the buffer is full, the bulk
// insert is made, returning any errors that occur.
func (bb *BufferedBulk) Insert(doc interface{}) error {
	rawBytes, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("bson encoding error: %v", err)
	}
	// flush if we are full
	if bb.docCount >= bb.docLimit || bb.byteCount+len(rawBytes) > MaxMessageSize {
		if err := bb.Flush(); err != nil {
			return fmt.Errorf("error writing bulk insert: %v", err)
		}
	}
	// buffer the document
	bb.docCount++
	bb.byteCount += len(rawBytes)
	bb.bulk.Insert(bson.Raw{Data: rawBytes})
	return nil
}

// Flush sends all buffered documents in one bulk insert
// then resets the bulk buffer
func (bb *BufferedBulk) Flush() error {
	bb.flushCount++
	if bb.docCount == 0 {
		return nil
	}
	if _, err := bb.bulk.Run(); err != nil {
		return err
	}
	bb.refreshBulk()
	return nil
}
