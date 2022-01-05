package agent

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

	"github.com/antw/violin/internal/discovery"
	"github.com/antw/violin/internal/server"
)

type Agent struct {
	Config

	mux        cmux.CMux
	store      *server.DistributedStore
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
	Bootstrap      bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupMux,
		a.setupStore,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go func() {
		_ = a.serve()
	}()

	return a, nil
}

// setupStore configures cmux to allow multiplexing Raft and the gRPC server on the same port.
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)

	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	a.mux = cmux.New(listener)
	return nil
}

// setupStore configures the distributed store.
func (a *Agent) setupStore() error {
	raftListener := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{server.RaftRPC})
	})

	storeConfig := server.Config{}
	storeConfig.Raft.StreamLayer = server.NewStreamLayer(raftListener)
	storeConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	storeConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error

	a.store, err = server.NewDistributedStore(a.Config.DataDir, storeConfig)
	if err != nil {
		return err
	}

	if a.Config.Bootstrap {
		err = a.store.WaitForLeader(3 * time.Second)
	}

	return err
}

// setupServer configures the server and starts a listener.
func (a *Agent) setupServer() error {
	a.server = server.New(a.store)

	grpcListener := a.mux.Match(cmux.Any())

	go func() {
		if err := a.server.Serve(grpcListener); err != nil {
			_ = a.Shutdown()
		}
	}()

	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	a.membership, err = discovery.New(a.store, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	if err := a.membership.Leave(); err != nil {
		return err
	}

	a.server.GracefulStop()

	return nil
}

// serve runs the server
func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}

	return nil
}
