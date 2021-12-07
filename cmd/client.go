package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/antw/violin/api"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.PersistentFlags().String(
		"host",
		"127.0.0.1:5001",
		"Host to which to send requests",
	)

	clientCmd.AddCommand(clientGetCmd)
	clientCmd.AddCommand(clientSetCmd)
}

func clientHost(cmd *cobra.Command) string {
	host, _ := cmd.Flags().GetString("host")
	if host == "" {
		host = viper.GetString("bind")
	}

	if host == "" {
		fmt.Println("No host specified")
		os.Exit(1)
	}

	return host
}

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Set and get values from a Violin server",
	Long: `A longer description that spans multiple lines and likely contains
				examples and usage of using your application.`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
}

var clientGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a value from a Violin server",
	Long:  `Gets a value from a Violin server.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ReadViolinConfig(cmd)

		client := createClient(clientHost(cmd))

		resp, err := client.Get(context.Background(), &api.GetRequest{Key: args[0]})
		if err != nil {
			fmt.Println("Failed to get value: ", err)
			os.Exit(1)
		}

		fmt.Println(string(resp.GetRegister().GetValue()))
	},
}

var clientSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Sets a value on a Violin server",
	Long:  `Sets a value on a Violin server.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		ReadViolinConfig(cmd)

		client := createClient(clientHost(cmd))

		_, err := client.Set(context.Background(), &api.SetRequest{
			Register: &api.KV{
				Key:   args[0],
				Value: []byte(args[1]),
			},
		})

		if err != nil {
			fmt.Println("Failed to get value: ", err)
			os.Exit(1)
		}
	},
}

// createClient establishes a connection to a Violin server at addr and returns a client which may
// be used to set or get values.
func createClient(addr string) api.RegisterClient {
	clientConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Could not establish connection to server: ", err)
		os.Exit(1)
	}

	return api.NewRegisterClient(clientConn)
}
