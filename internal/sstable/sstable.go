package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/antw/violin/internal/storage"
	"google.golang.org/protobuf/proto"
)

var (
	// Size in bytes of an offset position (uint32).
	offsetSize = 4
)

// SSTable implements a sorted string table. SSTs are immutable and populated by values stored in
// a data file. The data file is a sequence of key-value pairs, where each key appears in
// lexographic order. An index file provides a mapping from keys to the position in the data file
// where the key-value pair is stored.
type SSTable struct {
	file  *os.File
	index *index
}

var _ storage.GettableStore = (*SSTable)(nil)

// NewSSTable rakes a reference to data and index os.File objects and returns an SSTable
// representing the data.
func NewSSTable(kvFile, indexFile *os.File) (*SSTable, error) {
	index, err := newIndex(indexFile)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		file:  kvFile,
		index: index,
	}, nil
}

// OpenSSTable takes a path to data and index files, and returns an SSTable representing the data.
func OpenSSTable(dataPath, indexPath string) (*SSTable, error) {
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}

	// Index file is not needed once it has been read.
	defer func() { _ = indexFile.Close() }()

	return NewSSTable(dataFile, indexFile)
}

// Get looks up a key in the table and returns the corresponding value. If the key does not exist,
// the second return value will be storage.ErrNoSuchKey.
func (s *SSTable) Get(key string) (value []byte, err error) {
	offset, ok := s.index.get(key)
	if !ok {
		return nil, storage.ErrNoSuchKey
	}

	recordLen := make([]byte, offsetSize)
	_, err = s.file.ReadAt(recordLen, int64(offset))
	if err != nil {
		return nil, err
	}

	recordBytes := make([]byte, binary.LittleEndian.Uint32(recordLen))
	if _, err = s.file.ReadAt(recordBytes, int64(offset+uint32(offsetSize))); err != nil {
		return nil, err
	}

	var kv KeyValue
	if err = proto.Unmarshal(recordBytes, &kv); err != nil {
		return nil, err
	}

	return kv.Value, nil
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

// -------------------------------------------------------------------------------------------------

// SerializableStore is an interface which describes any kind of key/value store whose values may
// be iterated and yielded to a function in lexographical order.
type SerializableStore interface {
	Ascend(func(key string, value []byte) bool)
}

type Writer struct {
	kvFile    *os.File
	indexFile *os.File
	source    SerializableStore
}

// Write the contents of the source to a sstable and index on disk. Both the kvFile and indexFile
// are closed after writing is completed.
func (w *Writer) Write() error {
	var ascendErr error

	index := indexWriter{bufio.NewWriter(w.indexFile)}
	kvBuf := bufio.NewWriter(w.kvFile)
	pos := 0

	w.source.Ascend(func(key string, value []byte) bool {
		marshalled, err := proto.Marshal(&KeyValue{Key: key, Value: value})
		if err != nil {
			ascendErr = err
			return false
		}

		err = binary.Write(kvBuf, binary.LittleEndian, uint32(len(marshalled)))
		if err != nil {
			ascendErr = err
			return false
		}

		_, err = kvBuf.Write(marshalled)
		if err != nil {
			ascendErr = err
			return false
		}

		err = index.Write(key, uint32(pos))
		if err != nil {
			ascendErr = err
			return false
		}

		pos += offsetSize + len(marshalled)

		return true
	})

	if ascendErr != nil {
		removeFileIfExists(w.kvFile)
		removeFileIfExists(w.indexFile)
		return ascendErr
	}

	err := kvBuf.Flush()
	if err != nil {
		return err
	}

	err = index.Flush()
	if err != nil {
		return err
	}

	err = w.indexFile.Close()
	if err != nil {
		return err
	}

	return w.kvFile.Close()
}

func removeFileIfExists(file *os.File) {
	stat, err := file.Stat()
	if err != nil {
		return
	}

	err = os.Remove(stat.Name())
	if err != nil {
		return
	}
}
