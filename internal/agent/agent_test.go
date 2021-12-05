package agent

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/antw/violin/api"
)

func TestAgent(t *testing.T) {
	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports, err := freeport.GetFreePorts(2)
		require.NoError(t, err)

		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])

		dataDir, err := ioutil.TempDir("", "agent-test")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := New(Config{
			NodeName:       fmt.Sprintf("%d", i),
			Bootstrap:      i == 0,
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddr,
			RPCPort:        ports[1],
			DataDir:        dataDir,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0])
	_, err := leaderClient.Set(
		context.Background(),
		&api.SetRequest{Register: &api.KV{Key: "foo", Value: []byte("bar")}},
	)
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Get(
		context.Background(),
		&api.GetRequest{Key: "foo"},
	)
	require.NoError(t, err)
	require.Equal(t, "bar", string(consumeResponse.GetRegister().GetValue()))

	// wait until replication has finished
	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1])
	consumeResponse, err = followerClient.Get(
		context.Background(),
		&api.GetRequest{Key: "foo"},
	)
	require.NoError(t, err)
	require.Equal(t, "bar", string(consumeResponse.GetRegister().GetValue()))

	// Check accessing a register which does not exist.
	consumeResponse, err = leaderClient.Get(
		context.Background(),
		&api.GetRequest{Key: "baz"},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)

	// TODO: Check the gRPC error code?
}

// client creates a client for interacting with a server.
func client(t *testing.T, agent *Agent) api.RegisterClient {
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, grpc.WithInsecure())
	require.NoError(t, err)

	return api.NewRegisterClient(conn)
}
