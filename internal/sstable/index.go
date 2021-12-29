package sstable

import (
	"bufio"
	"encoding/binary"
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
	buf *bufio.Writer
}

func (i *indexWriter) Write(key string, pos uint32) error {
	err := binary.Write(i.buf, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		return err
	}

	_, err = i.buf.Write([]byte(key))
	if err != nil {
		return err
	}

	//_, err = writeUvarint(i.buf, pos)
	err = binary.Write(i.buf, binary.LittleEndian, pos)
	if err != nil {
		return err
	}

	return nil
}

func (i *indexWriter) Flush() error {
	return i.buf.Flush()
}
