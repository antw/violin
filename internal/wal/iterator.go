package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidSkip = errors.New("wal: cannot skip, already at a later txid")
	ErrNoSuchTxid  = errors.New("wal: no such txid")
)

type Iterator struct {
	source  *bufio.Reader
	peeked  *Record
	started bool
}

// NewIterator creates a new iterator for the given reader.
func NewIterator(source io.Reader) *Iterator {
	return &Iterator{source: bufio.NewReader(source)}
}

// Next returns the next record in the WAL and advances the iterator.
func (r *Iterator) Next() (*Record, error) {
	if !r.started {
		r.started = true
		if err := verifySchemaVersion(r.source); err != nil {
			return nil, err
		}
	}

	if r.peeked != nil {
		rec := r.peeked
		r.peeked = nil
		return rec, nil
	}

	rec, err := r.nextRecord()
	if err == io.EOF {
		return nil, iterator.Done
	}

	return rec, nil
}

// SkipTo advances the iterator to the first record whose transaction ID is equal to or higher than
// txid.
func (r *Iterator) SkipTo(txid uint64) error {
	for {
		rec, err := r.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				return ErrNoSuchTxid
			}
			return err
		}

		if rec.GetTxid() > txid {
			return ErrInvalidSkip
		}

		if rec.GetTxid() == txid {
			r.peeked = rec
			return nil
		}
	}
}

// nextRecord fetches the next record from the source.
func (r *Iterator) nextRecord() (*Record, error) {
	var recordLen uint32
	if err := binary.Read(r.source, binary.LittleEndian, &recordLen); err != nil {
		return nil, err
	}

	recordBytes := make([]byte, recordLen)
	if _, err := r.source.Read(recordBytes); err != nil {
		return nil, err
	}

	var record Record
	if err := proto.Unmarshal(recordBytes, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

// verifySchemaVersion checks the schema version of the SSTable index file and returns an error if
// the version is not supported.
func verifySchemaVersion(r io.Reader) error {
	var version uint32

	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		if errors.Is(err, io.EOF) {
			// Allow completely empty indices to simplify testing.
			return nil
		}
		return err
	}

	if version != schemaVersion {
		return fmt.Errorf("unsupported WAL schema version: %d", version)
	}

	return nil
}
