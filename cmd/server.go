package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/antw/violin/internal/agent"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	if err := setupFlags(); err != nil {
		log.Fatal(err)
	}

	rootCmd.AddCommand(serverCmd)
}

func setupFlags() error {
	serverCmd.Flags().String("bind", "127.0.0.1:5000", "Bind address for Serf (service discovery)")
	serverCmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster")
	serverCmd.Flags().String("config", "", "Path to a config file")
	serverCmd.Flags().String("data-dir", "", "Path in which to store log and Raft data")
	serverCmd.Flags().String("node-name", "", "A unique name for the node in the cluster")
	serverCmd.Flags().Int("rpc-port", 5001, "Port for RPC and Raft connections")
	serverCmd.Flags().StringSlice("start-addrs", nil, "Serf addresses to join")

	return viper.BindPFlags(serverCmd.Flags())
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Starts a Violin server",
	Long: `A longer description that spans multiple lines and likely contains
				examples and usage of using your application.`,
	Run: func(cmd *cobra.Command, args []string) {
		configPath, _ := cmd.Flags().GetString("config")

		if configPath != "" {
			viper.SetConfigFile(configPath)
		} else {
			viper.SetConfigName("server")
			viper.SetConfigType("toml")
			viper.AddConfigPath("/etc/violin/")
			viper.AddConfigPath("$HOME/.violin")
			viper.AddConfigPath(".")
		}

		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				if configPath != "" {
					panic(fmt.Errorf("config file %s not found\n", configPath))
				}
			} else {
				panic(fmt.Errorf("error in config file: %w\n", err))
			}
		}

		bindAddr := viper.GetString("bind")
		bootstrap := viper.GetBool("bootstrap")
		dataDir := viper.GetString("data-dir")
		nodeName := viper.GetString("node-name")
		rpcPort := viper.GetInt("rpc-port")
		startAddrs := viper.GetStringSlice("start-addrs")

		fmt.Println("Starting server...")

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
