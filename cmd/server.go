package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/antw/violin/internal/agent"
)

func init() {
	serverCmd.Flags().String("bind", "127.0.0.1:5000", "Bind address for Serf (service discovery)")
	serverCmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster")
	serverCmd.Flags().String("data-dir", "", "Path in which to store log and Raft data")
	serverCmd.Flags().String("node-name", "", "A unique name for the node in the cluster")
	serverCmd.Flags().Int("rpc-port", 5001, "Port for RPC and Raft connections")
	serverCmd.Flags().StringSlice("start-addrs", nil, "Serf addresses to join")

	rootCmd.AddCommand(serverCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Starts a Violin server",
	Long: `A longer description that spans multiple lines and likely contains
				examples and usage of using your application.`,
	Run: func(cmd *cobra.Command, args []string) {
		bindAddr, _ := cmd.Flags().GetString("bind")
		bootstrap, _ := cmd.Flags().GetBool("bootstrap")
		dataDir, _ := cmd.Flags().GetString("data-dir")
		nodeName, _ := cmd.Flags().GetString("node-name")
		rpcPort, _ := cmd.Flags().GetInt("rpc-port")
		startAddrs, _ := cmd.Flags().GetStringSlice("start-addrs")

		fmt.Println("Starting server...")
		fmt.Println(bindAddr, rpcPort, bootstrap, startAddrs)

		if nodeName == "" {
			nodeName = bindAddr
		}

		config := agent.Config{
			BindAddr:       bindAddr,
			Bootstrap:      bootstrap,
			DataDir:        dataDir,
			NodeName:       nodeName,
			RPCPort:        rpcPort,
			StartJoinAddrs: startAddrs,
		}

		agent, err := agent.New(config)
		if err != nil {
			fmt.Println("Error starting server:", err)
			os.Exit(1)
		}

		shutdownSig := make(chan os.Signal, 1)
		signal.Notify(shutdownSig, syscall.SIGINT, syscall.SIGTERM)
		<-shutdownSig

		_ = agent.Shutdown()
	},
}
