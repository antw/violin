package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "violin",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
				examples and usage of using your application.`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
}

func init() {
	rootCmd.PersistentFlags().String("config", "", "Path to a config file")
}

// ReadViolinConfig sets up the Viper config, reading the config file when specified, falling back
// to the default config paths otherwise.
func ReadViolinConfig(cmd *cobra.Command) {
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
}

// Execute runs the command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
