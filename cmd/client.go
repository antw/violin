package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/antw/violin/api"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(clientCmd)

	clientCmd.AddCommand(clientGetCmd)
	clientCmd.AddCommand(clientSetCmd)
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
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		client := createClient(args[0])

		resp, err := client.Get(context.Background(), &api.GetRequest{Key: args[1]})
		if err != nil {
			log.Fatal("Failed to get value: ", err)
		}

		fmt.Printf("%s = %s\n", args[1], string(resp.GetRegister().GetValue()))
	},
}

var clientSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Sets a value on a Violin server",
	Long:  `Sets a value on a Violin server.`,
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		client := createClient(args[0])

		_, err := client.Set(context.Background(), &api.SetRequest{
			Register: &api.KV{
				Key:   args[1],
				Value: []byte(args[2]),
			},
		})

		if err != nil {
			log.Fatal("Failed to get value: ", err)
		}

		fmt.Println("Value set")
	},
}

// createClient establishes a connection to a Violin server at addr and returns a client which may
// be used to set or get values.
func createClient(addr string) api.RegisterClient {
	clientConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("Could not establish connection to server: ", err)
	}

	return api.NewRegisterClient(clientConn)
}
