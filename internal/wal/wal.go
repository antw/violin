package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"

	"google.golang.org/protobuf/proto"
)

var (
	ErrCausalityViolation = errors.New("wal: cannot insert into WAL due to causality violation")
	ErrWALNonEmpty        = errors.New("wal: cannot open a new WAL with a non-empty file")
	schemaVersion         = uint32(1)
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	latest uint64
}

// NewWAL creates a new write-ahead log and requires that the given file be empty. If the file may
// already have logged entries, use Open instead.
func New(f *os.File) (*WAL, error) {
	if stat, err := f.Stat(); err != nil {
		return nil, err
	} else if stat.Size() != 0 {
		return nil, ErrWALNonEmpty
	}

	buf := bufio.NewWriter(f)
	if err := binary.Write(buf, binary.LittleEndian, schemaVersion); err != nil {
		return nil, err
	}

	return &WAL{file: f, writer: buf}, nil
}

// NewWithPath creates a new write-ahead log at the given path. If the file may already have logged
// entries use OpenPath instead.
func NewWithPath(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return New(f)
}

// Open is used internally when opening a WAL file which may already contain entries. The second
// argument is a function which will be called with each existing record in the WAL.
func Open(f *os.File, fn func(rec *Record)) (*WAL, error) {
	if stat, err := f.Stat(); err != nil {
		return nil, err
	} else if stat.Size() == 0 {
		return New(f)
	}

	reader, err := os.Open(f.Name())
	if err != nil {
		return nil, err
	}

	iter := NewIterator(reader)
	var txid uint64

	err = iter.ForEach(func(rec *Record) {
		txid = rec.GetTxid()
		fn(rec)
	})
	if err != nil {
		return nil, err
	}

	return &WAL{file: f, writer: bufio.NewWriter(f), latest: txid}, nil
}

// OpenWithPath opens an existing WAL file at the given path. The second argument is a function
// which will be called with each existing record in the WAL.
func OpenWithPath(path string, fn func(rec *Record)) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	return Open(f, fn)
}

// MakeUpsert creates a new wal.Record representing an insertion or update.
func MakeUpsert(txid uint64, key string, value []byte) *Record {
	return &Record{Txid: txid, Record: &Record_Upsert{&Upsert{Key: key, Value: value}}}
}

// MakeUpsert creates a new wal.Record representing a deletion.
func MakeDelete(txid uint64, key string) *Record {
	return &Record{Txid: txid, Record: &Record_Delete{&Delete{Key: key}}}
}

// Upsert writes a new entry to the WAL that represents the insertion of a new key and value, or an
// update of an existing key.
func (w *WAL) Upsert(txid uint64, key string, value []byte) error {
	return w.write(MakeUpsert(txid, key, value))
}

// Delete writes a new entry to the WAL representing the deletion of a key
func (w *WAL) Delete(txid uint64, key string) error {
	return w.write(MakeDelete(txid, key))
}

func (w *WAL) Close() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}

// write adds a record to the WAL.
func (w *WAL) write(r *Record) error {
	if r.GetTxid() != w.latest+1 {
		return ErrCausalityViolation
	}

	marshalled, err := proto.Marshal(r)
	if err != nil {
		return err
	}

	// Write to a temporary buffer in case an error occurs writing the record, but after the record
	// length has been written.
	var buffer bytes.Buffer

	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(marshalled))); err != nil {
		return err
	}
	if _, err := buffer.Write(marshalled); err != nil {
		return err
	}

	if _, err := w.writer.Write(buffer.Bytes()); err != nil {
		return err
	}

	w.latest = r.GetTxid()

	return nil
}
