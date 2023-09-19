/*
Copyright © 2023 Redis Performance Group performance <at> redis <dot> com
*/
package cmd

import (
	"context"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/rueian/rueidis"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
)

// consumerCmd represents the consumer command
var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "Consumer workload.",
	Long:  `.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("h")
		jsonOutFile, _ := cmd.Flags().GetString("json-out-file")
		port, _ := cmd.Flags().GetInt("p")
		keyspaceLen, _ := cmd.Flags().GetInt64("keyspace-len")
		nConsumersPerStream, _ := cmd.Flags().GetUint64("consumers-per-stream")
		readBlockMs, _ := cmd.Flags().GetInt64("stream-read-block-ms")
		numberRequests, _ := cmd.Flags().GetUint64("n")
		auth, _ := cmd.Flags().GetString("a")
		verbose, _ := cmd.Flags().GetBool("verbose")
		seed, _ := cmd.Flags().GetInt64("seed")
		nameserver, _ := cmd.Flags().GetString("nameserver")
		streamPrefix, _ := cmd.Flags().GetString("stream-prefix")
		betweenClientsDelay, _ := cmd.Flags().GetDuration("between-clients-duration")
		clientKeepAlive, _ := cmd.Flags().GetDuration("client-keep-alive-time")

		ctx := context.Background()
		ips := resolveHostnames(nameserver, host, ctx)

		//stopChan := make(chan struct{})
		// a WaitGroup for the goroutines to tell us they've stopped
		wg := sync.WaitGroup{}

		fmt.Printf("Using random seed: %d\n", seed)

		client_update_tick := 1
		latencies = hdrhistogram.New(1, 90000000, 3)
		latenciesTick = hdrhistogram.New(1, 90000000, 3)

		datapointsChan := make(chan datapoint, numberRequests)
		progressSize := keyspaceLen * int64(nConsumersPerStream)
		fmt.Printf("Setting up the consumer groups. On total we will have %d connections.\n", progressSize)
		bar := progressbar.Default(progressSize)
		startT := time.Now()
		for streamId := 1; int64(streamId) <= keyspaceLen; streamId++ {
			ctx := context.Background()
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			keyname := fmt.Sprintf("%s%d", streamPrefix, streamId)
			groupname := fmt.Sprintf("streamgroup:%s%d", streamPrefix, streamId)
			if verbose {
				fmt.Printf("Using connection string %s for stream %s\n", connectionStr, keyname)
			}

			clientOptions := rueidis.ClientOption{
				InitAddress:      []string{connectionStr},
				Password:         auth,
				AlwaysPipelining: false,
				AlwaysRESP2:      true,
				DisableCache:     true,
				BlockingPoolSize: int(nConsumersPerStream),
			}
			clientOptions.Dialer.KeepAlive = clientKeepAlive
			client, err := rueidis.NewClient(clientOptions)
			if err != nil {
				panic(err)
			}

			// ensure we destroy the group before starting
			err = client.Do(ctx, client.B().XgroupDestroy().Key(keyname).Group(groupname).Build()).Error()
			err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
			if err != nil {
				panic(err)
			}
			for consumerId := 1; uint64(consumerId) <= nConsumersPerStream; consumerId++ {
				consumername := fmt.Sprintf("streamconsumer:%s:%d", groupname, consumerId)
				err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
				bar.Add(1)
			}
			client.Close()
		}
		endT := time.Now()
		durationSetup := endT.Sub(startT)
		fmt.Printf("Finished setting up the consumer groups after %f seconds.\n", durationSetup.Seconds())

		// listen for C-c
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		startT = time.Now()
		for streamId := 1; int64(streamId) <= keyspaceLen; streamId++ {
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			keyname := fmt.Sprintf("%s%d", streamPrefix, streamId)
			groupname := fmt.Sprintf("streamgroup:%s%d", streamPrefix, streamId)
			if verbose {
				fmt.Printf("Using connection string %s for stream %s\n", connectionStr, keyname)
			}

			clientOptions := rueidis.ClientOption{
				InitAddress:      []string{connectionStr},
				Password:         auth,
				AlwaysPipelining: false,
				AlwaysRESP2:      true,
				DisableCache:     true,
				BlockingPoolSize: int(nConsumersPerStream),
			}
			clientOptions.Dialer.KeepAlive = clientKeepAlive
			client, err := rueidis.NewClient(clientOptions)
			if err != nil {
				panic(err)
			}
			defer client.Close()
			// ensure we destroy the group before starting
			if err != nil {
				panic(err)
			}
			for consumerId := 1; uint64(consumerId) <= nConsumersPerStream; consumerId++ {
				consumername := fmt.Sprintf("streamconsumer:%s:%d", groupname, consumerId)
				wg.Add(1)
				go benchmarkConsumerRoutine(client, c, datapointsChan, &wg, keyname, groupname, consumername, readBlockMs)
			}

			// delay the creation for each additional client
			time.Sleep(betweenClientsDelay)
		}
		endT = time.Now()

		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)

		closed, _, duration, totalMessages, messageRateTs, percentilesTs := updateCLI(tick, c, numberRequests, false, datapointsChan)
		messageRate := float64(totalMessages) / float64(duration.Seconds())
		avgMs := float64(latencies.Mean()) / 1000.0
		p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
		p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
		p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0

		fmt.Printf("\n")
		fmt.Printf("#################################################\n")
		fmt.Printf("Total Duration %.3f Seconds\n", duration.Seconds())
		fmt.Printf("Total Errors %d\n", totalErrors)
		fmt.Printf("Throughput summary: %.0f requests per second\n", messageRate)
		fmt.Printf("Latency summary (msec):\n")
		fmt.Printf("    %9s %9s %9s %9s\n", "avg", "p50", "p95", "p99")
		fmt.Printf("    %9.3f %9.3f %9.3f %9.3f\n", avgMs, p50IngestionMs, p95IngestionMs, p99IngestionMs)

		testResult := NewTestResult("", uint(progressSize), 0)
		testResult.FillDurationInfo(startT, endT, duration)
		testResult.OverallClientLatencies = percentilesTs
		testResult.OverallQueryRates = messageRateTs
		_, overallLatencies := generateLatenciesMap(latencies, duration)
		testResult.Totals = overallLatencies
		saveJsonResult(testResult, jsonOutFile)
		if closed {
			return
		}

	},
}

func benchmarkConsumerRoutine(client rueidis.Client, c chan os.Signal, datapointsChan chan datapoint, wg *sync.WaitGroup, keyname, groupname, consumername string, readBlockMs int64) {
	defer wg.Done()
	ctx := context.Background()
	for {
		select {
		case <-c:
			fmt.Println("\nreceived Ctrl-c on Consumer Routine - shutting down")
			return
		default:
			startT := time.Now()
			xreadEntries, err := client.Do(ctx, client.B().Xreadgroup().Group(groupname, consumername).Block(readBlockMs).Streams().Key(keyname).Id(">").Build()).AsXRead()
			if err != nil {
				err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
				err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
				xreadEntries, err = client.Do(ctx, client.B().Xreadgroup().Group(groupname, consumername).Block(readBlockMs).Streams().Key(keyname).Id(">").Build()).AsXRead()
			}
			if err == nil {
				xrangeEntries, found := xreadEntries[keyname]
				if found {
					for _, xrangeEntry := range xrangeEntries {
						err = client.Do(ctx, client.B().Xack().Key(keyname).Group(groupname).Id(xrangeEntry.ID).Build()).Error()
					}
				}
			}
			endT := time.Now()
			duration := endT.Sub(startT)
			datapointsChan <- datapoint{!(err != nil), duration.Microseconds()}
		}

	}
}

func init() {
	rootCmd.AddCommand(consumerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	consumerCmd.PersistentFlags().Int64("stream-read-block-ms", 30000, "Block the client for this amount of milliseconds.")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
