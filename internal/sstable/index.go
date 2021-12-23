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
