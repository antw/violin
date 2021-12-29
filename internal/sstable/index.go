package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/google/btree"
)

// index stores the offset of each key in the SSTable file.
type index struct {
	tree *btree.BTree
}

func newIndex(f *os.File) (*index, error) {
	tree := btree.New(2)
	buf := bufio.NewReader(f)

	if err := verifySchemaVersion(buf); err != nil {
		return nil, err
	}

	for {
		_, err := buf.Peek(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		keyLen := make([]byte, offsetSize)
		_, err = buf.Read(keyLen)
		if err != nil {
			return nil, err
		}

		key := make([]byte, binary.LittleEndian.Uint32(keyLen))
		_, err = buf.Read(key)
		if err != nil {
			return nil, err
		}

		pos := make([]byte, offsetSize)
		_, err = buf.Read(pos)
		if err != nil {
			return nil, err
		}

		tree.ReplaceOrInsert(indexEntry{
			key: string(key),
			pos: binary.LittleEndian.Uint32(pos),
		})
	}

	return &index{tree}, nil
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
		return fmt.Errorf("unsupported schema version: %d", version)
	}

	return nil
}

// Ascend iterates through each key in the index in ascending order, yielding to the function the
// key and corresponding position of the value in the data file.
func (i *index) Ascend(fn func(key string, pos uint32) bool) {
	i.tree.Ascend(func(item btree.Item) bool {
		return fn(item.(indexEntry).key, item.(indexEntry).pos)
	})
}

// AscendRange calls the iterator for every value in the index within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (i *index) AscendRange(
	greaterOrEqual string,
	lessThan string,
	iterator func(key string, pos uint32) bool,
) {
	from := indexEntry{key: greaterOrEqual}
	to := indexEntry{key: lessThan}

	i.tree.AscendRange(from, to, func(item btree.Item) bool {
		return iterator(item.(indexEntry).key, item.(indexEntry).pos)
	})
}

// get returns the position at which the key is stored in the file. The second return value
// indicates whether the key exists.
func (i *index) get(key string) (uint32, bool) {
	entry := i.tree.Get(indexEntry{key, 0})
	if entry == nil {
		return 0, false
	}

	return entry.(indexEntry).pos, true
}

// -------------------------------------------------------------------------------------------------

type indexEntry struct {
	key string
	pos uint32
}

func (ie indexEntry) Less(than btree.Item) bool {
	return ie.key < than.(indexEntry).key
}

// -------------------------------------------------------------------------------------------------

type indexWriter struct {
	buf     *bufio.Writer
	started bool
}

func newIndexWriter(w io.Writer) *indexWriter {
	return &indexWriter{
		buf: bufio.NewWriter(w),
	}
}

func (i *indexWriter) start() error {
	if i.started {
		return nil
	}

	i.started = true

	// Write a prefix which will be used to check that the file version can be read.
	return binary.Write(i.buf, binary.LittleEndian, schemaVersion)
}

func (i *indexWriter) Write(key string, pos uint32) error {
	if err := i.start(); err != nil {
		return err
	}

	err := binary.Write(i.buf, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		return err
	}

	_, err = i.buf.Write([]byte(key))
	if err != nil {
		return err
	}

	err = binary.Write(i.buf, binary.LittleEndian, pos)
	if err != nil {
		return err
	}

	return nil
}

func (i *indexWriter) Flush() error {
	if err := i.start(); err != nil {
		return err
	}
	return i.buf.Flush()
}
