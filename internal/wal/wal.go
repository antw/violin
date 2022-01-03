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
	ErrCausalityViolation = errors.New("cannot insert into WAL due to causality violation")
	schemaVersion         = uint32(1)
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	latest uint64
}

func NewWAL(f *os.File) (*WAL, error) {
	buf := bufio.NewWriter(f)

	if err := binary.Write(buf, binary.LittleEndian, schemaVersion); err != nil {
		return nil, err
	}

	return &WAL{file: f, writer: buf}, nil
}

// Upsert writes a new entry to the WAL that represents the insertion of a new key and value, or an
// update of an existing key.
func (w *WAL) Upsert(txid uint64, key string, value []byte) error {
	return w.write(&Record{Txid: txid, Record: &Record_Upsert{&Upsert{Key: key, Value: value}}})
}

// Delete writes a new entry to the WAL representing the deletion of a key
func (w *WAL) Delete(txid uint64, key string) error {
	return w.write(&Record{Txid: txid, Record: &Record_Delete{&Delete{Key: key}}})
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
