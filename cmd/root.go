/*
Copyright © 2023 Redis Performance Group performance <at> redis <dot> com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "openstreaming-benchmark",
	Short: "Make it easy to benchmark distributed streaming systems.",
	Long:  `Make it easy to benchmark distributed streaming systems.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		version, _ := cmd.Flags().GetBool("version")
		git_sha := toolGitSHA1()
		git_dirty_str := ""
		if toolGitDirty() {
			git_dirty_str = "-dirty"
		}
		if version {
			fmt.Fprintf(os.Stdout, "openstreaming-benchmark (git_sha1:%s%s)\n", git_sha, git_dirty_str)
			os.Exit(0)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().String("h", "127.0.0.1", "Server hostname.")
	rootCmd.PersistentFlags().Int("p", 6379, "Server port.")
	rootCmd.PersistentFlags().Int("rps", 0, "Max rps. If 0 no limit is applied and the DB is stressed up to maximum.")
	rootCmd.PersistentFlags().Int("rps-burst", 0, "Max rps burst. If 0 the allowed burst will be the ammount of clients.")
	rootCmd.PersistentFlags().String("a", "", "Password for Redis Auth.")
	rootCmd.PersistentFlags().Int64("random-seed", 12345, "random seed to be used.")
	rootCmd.PersistentFlags().Uint64("c", 50, "number of clients.")
	rootCmd.PersistentFlags().Int64("keyspace-len", 100, "number of streams.")
	rootCmd.PersistentFlags().Int64("stream-maxlen", 1000000, "stream max length.")
	rootCmd.PersistentFlags().Uint64("consumers-per-stream", 5, "per stream consumer count.")
	rootCmd.PersistentFlags().Int64("stream-maxlen-expire-secs", 60, "If a stream reached the max length, we expire it after the provided amount of seconds. If 0 will use DEL instead of expire.")
	rootCmd.PersistentFlags().String("stream-prefix", "", "stream prefix.")
	rootCmd.PersistentFlags().Int("d", 100, "Data size in bytes of the expanded string value sent in the message.")
	rootCmd.PersistentFlags().Uint64("n", 10000000, "Total number of requests")
	rootCmd.PersistentFlags().Bool("oss-cluster", false, "Enable OSS cluster mode.")
	rootCmd.PersistentFlags().Duration("between-clients-duration", time.Millisecond*0, "Between each client creation, wait this time.")
	rootCmd.PersistentFlags().Duration("test-time", time.Second*0, "test time in seconds.")
	rootCmd.PersistentFlags().Duration("client-keep-alive-time", time.Second*60, "keepalive time (in every keepalive we send a PING).")
	rootCmd.PersistentFlags().Bool("version", false, "Output version and exit")
	rootCmd.PersistentFlags().Bool("verbose", false, "Output verbose info")
	rootCmd.PersistentFlags().String("resp", "", "redis command response protocol (2 - RESP 2, 3 - RESP 3). If empty will not enforce it.")
	rootCmd.PersistentFlags().String("nameserver", "", "the IP address of the DNS name server. The IP address can be an IPv4 or an IPv6 address. If empty will use the default host namserver.")
	rootCmd.PersistentFlags().String("json-out-file", "", "Results file. If empty will not save.")

}
