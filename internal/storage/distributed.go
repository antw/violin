package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	"github.com/antw/violin/api"
)

const (
	// RaftRPC is a value used to indicate to cmux that a connection is intended for Raft RPC rather
	// than the gRPC server.
	RaftRPC byte = 1
)

var (
	ErrNotRaftRPC = errors.New("not a Raft RPC connection")
)

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		Bootstrap   bool
	}
}

type DistributedStore struct {
	config Config
	store  *Store
	raft   *raft.Raft
}

func NewDistributedStore(dataDir string, config Config) (*DistributedStore, error) {
	ds := &DistributedStore{
		store:  NewStore(),
		config: config,
	}

	if err := ds.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return ds, nil
}

func (ds *DistributedStore) setupRaft(dataDir string) error {
	fsm := &fsm{store: ds.store}

	if err := os.MkdirAll(filepath.Join(dataDir, "raft"), 0700); err != nil {
		return err
	}

	// Log store

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "log"))
	if err != nil {
		return err
	}

	// Stable store

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// Snapshot store

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		1, // retain
		os.Stderr,
	)
	if err != nil {
		return err
	}

	// Raft transport

	transport := raft.NewNetworkTransport(
		ds.config.Raft.StreamLayer,
		5,              // maxPool
		10*time.Second, // timeout
		os.Stderr,
	)

	// Config

	config := raft.DefaultConfig()
	config.LocalID = ds.config.Raft.LocalID

	if ds.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = ds.config.Raft.HeartbeatTimeout
	}
	if ds.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = ds.config.Raft.ElectionTimeout
	}
	if ds.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = ds.config.Raft.LeaderLeaseTimeout
	}
	if ds.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = ds.config.Raft.CommitTimeout
	}

	// Start Raft

	ds.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if ds.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = ds.raft.BootstrapCluster(config).Error()
	}

	return err
}

// Set appends the KV to the Raft log.
func (ds *DistributedStore) Set(key string, value []byte) error {
	_, err := ds.apply(&api.SetRequest{Register: &api.KV{Key: key, Value: value}})
	return err
}

// apply sends something to the Raft log.
func (ds *DistributedStore) apply(req proto.Message) (interface{}, error) {
	var buf bytes.Buffer

	// Marshal and write the message.
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	// Send to Raft
	future := ds.raft.Apply(buf.Bytes(), 10*time.Second)
	if future.Error() != nil {
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

// Get reads the value associated with key and returns a KV record.
func (ds *DistributedStore) Get(key string) (value []byte, ok bool) {
	return ds.store.Get(key)
}

// Join adds a node to the Raft cluster.
func (ds *DistributedStore) Join(id, addr string) error {
	configFuture := ds.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the existing server with the same ID, to be replaced by this one
			removeFuture := ds.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := ds.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// Leave removes a node from the Raft cluster.
func (ds *DistributedStore) Leave(id string) error {
	fmt.Printf("Leaving: %s %v\n", id, raft.ServerID(id))
	return ds.raft.RemoveServer(raft.ServerID(id), 0, 0).Error()
}

// WaitForLeader blocks until the cluster has elected a leader or times out.
func (ds *DistributedStore) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("raft cluster timed out")
		case <-ticker.C:
			if leader := ds.raft.Leader(); leader != "" {
				return nil
			}
		}
	}
}

// Close shuts down the Raft instance.
func (ds *DistributedStore) Close() error {
	if err := ds.raft.Shutdown().Error(); err != nil {
		return err
	}

	return nil
}

// FSM ---------------------------------------------------------------------------------------------

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	store *Store
}

func (f *fsm) Apply(record *raft.Log) interface{} {
	var req api.SetRequest

	err := proto.Unmarshal(record.Data, &req)
	if err != nil {
		return err
	}

	err = f.store.Set(req.GetRegister().GetKey(), req.GetRegister().GetValue())
	if err != nil {
		return err
	}

	return &api.SetResponse{}
}

// Snapshot returns a snapshot which represents the store at a point in time.
//
// For now this is encoded as JSON, but there are likely more efficient ways to encode this data
// (using api.KV, for example).
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	var data map[string][]byte

	// Convert the sync.Map into a map which can be encoded.
	f.store.data.Range(func(key, value interface{}) bool {
		if valueBytes, ok := value.([]byte); ok {
			data[fmt.Sprint(key)] = valueBytes
		}
		return true
	})

	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(data)
	if err != nil {
		return nil, err
	}

	return &snapshot{reader: buf}, nil
}

// Restore reads the snapshot store and fills the Store with keys and values.
func (f *fsm) Restore(r io.ReadCloser) error {
	var data map[string][]byte

	err := json.NewDecoder(r).Decode(&data)
	if err != nil {
		return err
	}

	for key, value := range data {
		err = f.store.Set(key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Snapshot ----------------------------------------------------------------------------------------

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist is called by Raft to persist the snapshot data into the snapshot store.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release is called when Raft no longer needs the snapshot.
func (s *snapshot) Release() {}

// StreamLayer -------------------------------------------------------------------------------------

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln net.Listener
}

func NewStreamLayer(ln net.Listener) *StreamLayer {
	return &StreamLayer{ln: ln}
}

// Dial establishes outgoing connections to other nodes in the Raft cluster.
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// Identify to cmux that this is a Raft RPC connection.
	_, err = conn.Write([]byte{RaftRPC})
	if err != nil {
		return nil, err
	}

	return conn, err
}

// Accept receives connections from other nodes in the Raft cluster.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(b, []byte{RaftRPC}) != 0 {
		return nil, ErrNotRaftRPC
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
