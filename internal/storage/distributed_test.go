package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
)

func TestMultipleNodes(t *testing.T) {
	var stores []*DistributedStore
	nodeCount := 3

	ports, err := freeport.GetFreePorts(nodeCount)
	require.NoError(t, err)

	for i := 0; i < nodeCount; i++ {
		fmt.Println(i)
		dataDir, err := ioutil.TempDir("", "distributed-store-test")
		require.NoError(t, err)

		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := Config{}
		config.Raft.StreamLayer = &StreamLayer{ln: ln}
		config.Raft.LocalID = raft.ServerID(fmt.Sprint(i))
		config.Raft.HeartbeatTimeout = 500 * time.Millisecond
		config.Raft.ElectionTimeout = 500 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 500 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		store, err := NewDistributedStore(dataDir, config)
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
			for j := 0; j < nodeCount; j++ {
				value, err := stores[j].Get(kv.key)
				if errors.Is(err, ErrNoSuchKey) {
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

	err = stores[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = stores[0].Set("foo2", []byte("bar2"))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Test that disconnected node doesn't receive the KV.
	value, err := stores[1].Get("foo2")
	require.ErrorIs(t, err, ErrNoSuchKey)
	require.Nil(t, value)

	// Test that the node which is still connected gets the KV.
	value, err = stores[2].Get("foo2")
	require.NoError(t, err)
	require.Equal(t, []byte("bar2"), value)
}
