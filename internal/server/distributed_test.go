package server

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"

	"github.com/antw/violin/internal/storage"
)

func TestMultipleNodes(t *testing.T) {
	stores, teardown := createDistributedStores(t, 3)
	defer teardown()

	kvs := []struct {
		key   string
		value []byte
	}{
		{key: "foo", value: []byte("bar")},
		{key: "baz", value: []byte("qux")},
	}
	for _, kv := range kvs {
		err := stores[0].Set(kv.key, kv.value)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < len(stores); j++ {
				value, err := stores[j].Get(kv.key)
				if errors.Is(err, storage.ErrNoSuchKey) {
					// Ignore missing keys which haven't been replicated yet.
					return false
				}

				require.NoError(t, err)

				if !bytes.Equal(kv.value, value) {
					return false
				}
			}

			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	err := stores[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = stores[0].Set("foo2", []byte("bar2"))
	require.NoError(t, err)

	// No writes to the follower.
	err = stores[1].Set("foo3", []byte("bar"))
	require.Error(t, err)

	err = stores[1].Delete("foo3")
	require.Error(t, err)

	time.Sleep(50 * time.Millisecond)

	// Test that disconnected node doesn't receive the KV.
	value, err := stores[1].Get("foo2")
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	require.Nil(t, value)

	// Test that the node which is still connected gets the KV.
	value, err = stores[2].Get("foo2")
	require.NoError(t, err)
	require.Equal(t, []byte("bar2"), value)
}

func TestDelete(t *testing.T) {
	stores, teardown := createDistributedStores(t, 3)
	defer teardown()

	err := stores[0].Set("foo", []byte("bar"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := stores[1].Get("foo")
		return err == nil
	}, 500*time.Millisecond, 25*time.Millisecond)

	err = stores[0].Delete("foo")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		val, err := stores[1].Get("foo")
		return errors.Is(err, storage.ErrNoSuchKey) && val == nil
	}, 500*time.Millisecond, 25*time.Millisecond)
}

// -------------------------------------------------------------------------------------------------

func createDistributedStores(t *testing.T, count int) ([]*DistributedStore, func()) {
	var stores []*DistributedStore
	var dirs []string

	ports, err := freeport.GetFreePorts(count)
	require.NoError(t, err)

	for i := 0; i < count; i++ {
		fmt.Println(i)
		baseDir, err := ioutil.TempDir("", "distributed-store-test")
		require.NoError(t, err)

		os.MkdirAll(filepath.Join(baseDir, "raft"), 0700)
		os.MkdirAll(filepath.Join(baseDir, "data"), 0700)

		dirs = append(dirs, baseDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := Config{}
		config.Raft.StreamLayer = &StreamLayer{ln: ln}
		config.Raft.LocalID = raft.ServerID(fmt.Sprint(i))
		config.Raft.HeartbeatTimeout = 500 * time.Millisecond
		config.Raft.ElectionTimeout = 500 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 500 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.DataDir = filepath.Join(baseDir, "raft")

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		controller, err := OpenController(ControllerConfig{Dir: filepath.Join(baseDir, "data")})
		require.NoError(t, err)

		store, err := NewDistributedStore(controller, config)
		require.NoError(t, err)

		if i == 0 {
			// Bootstrap the cluster.
			err = store.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		} else {
			// Join the follower node to the cluster.
			err = stores[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		}

		stores = append(stores, store)
	}

	return stores, func() {
		for _, dir := range dirs {
			_ = os.RemoveAll(dir)
		}
	}
}
