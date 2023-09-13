/*
Copyright © 2023 Redis Performance Group performance <at> redis <dot> com
*/
package cmd

import (
	"context"
	"fmt"
	"github.com/rueian/rueidis"

	"github.com/spf13/cobra"
)

// producerCmd represents the producer command
var producerCmd = &cobra.Command{
	Use:   "producer",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		h, _ := cmd.Flags().GetString("h")
		p, _ := cmd.Flags().GetInt("p")
		a, _ := cmd.Flags().GetString("a")

		initaddress := fmt.Sprintf("%s:%d", h, p)

		client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{initaddress}, Password: a, AlwaysPipelining: false, AlwaysRESP2: true})
		if err != nil {
			panic(err)
		}
		defer client.Close()

		ctx := context.Background()
		// SET key val NX
		err = client.Do(ctx, client.B().Set().Key("key").Value("val").Nx().Build()).Error()

		fmt.Println("producer called")
	},
}

func init() {
	rootCmd.AddCommand(producerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// producerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// producerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
