package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"google.golang.org/protobuf/proto"
)

var (
	// Size in bytes of an offset position (uint32).
	offsetSize = 4
)

type SSTable struct {
	file  *os.File
	index *index
}

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

// TODO This needs to be able to return an error instead of just an ok bool
func (s *SSTable) Get(key string) (value []byte, ok bool) {
	offset, ok := s.index.get(key)
	if !ok {
		return nil, false
	}

	recordLen := make([]byte, offsetSize)
	_, err := s.file.ReadAt(recordLen, int64(offset))
	if err != nil {
		return nil, false
	}

	recordBytes := make([]byte, binary.LittleEndian.Uint32(recordLen))
	if _, err = s.file.ReadAt(recordBytes, int64(offset+uint32(offsetSize))); err != nil {
		return nil, false
	}

	var kv KeyValue
	if err := proto.Unmarshal(recordBytes, &kv); err != nil {
		return nil, false
	}

	return kv.Value, true
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

//func writeUvarint(w io.Writer, x uint64) (int, error) {
//	b := make([]byte, binary.MaxVarintLen64)
//	n := binary.PutUvarint(b, x)
//	_, err := w.Write(b[:n])
//
//	return n, err
//}

// -------------------------------------------------------------------------------------------------

//type indexWriter struct {
//	buf *bufio.Writer
//}
//
//func (i *indexWriter) Write(key string, pos rune) error {
//	_, err := i.buf.WriteRune(rune(len(key)))
//	if err != nil {
//		return err
//	}
//
//	_, err = i.buf.Write([]byte(key))
//	if err != nil {
//		return err
//	}
//
//	_, err = i.buf.WriteRune(pos)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (i *indexWriter) Flush() error {
//	return i.buf.Flush()
//}