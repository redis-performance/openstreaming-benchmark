/*
Copyright © 2023 Redis Performance Group performance <at> redis <dot> com
*/
package cmd

import (
	"context"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/rueian/rueidis"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
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
		readBufferEachConn, _ := cmd.Flags().GetInt("read-buffer-each-conn")
		writeBufferEachConn, _ := cmd.Flags().GetInt("write-buffer-each-conn")
		keyspaceLen, _ := cmd.Flags().GetInt64("keyspace-len")
		nConsumersPerStreamMax, _ := cmd.Flags().GetUint64("consumers-per-stream-max")
		nConsumersPerStreamMin, _ := cmd.Flags().GetUint64("consumers-per-stream-min")
		readBlockMs, _ := cmd.Flags().GetInt64("stream-read-block-ms")
		readCount, _ := cmd.Flags().GetInt64("stream-read-count")
		numberRequests, _ := cmd.Flags().GetUint64("n")
		auth, _ := cmd.Flags().GetString("a")
		verbose, _ := cmd.Flags().GetBool("verbose")
		loop, _ := cmd.Flags().GetBool("loop")
		seed, _ := cmd.Flags().GetInt64("seed")
		nameserver, _ := cmd.Flags().GetString("nameserver")
		streamPrefix, _ := cmd.Flags().GetString("stream-prefix")
		betweenClientsDelay, _ := cmd.Flags().GetDuration("between-clients-duration")
		clientKeepAlive, _ := cmd.Flags().GetDuration("client-keep-alive-time")
		pprofPort, _ := cmd.Flags().GetInt64("pprof-port")
		rps, _ := cmd.Flags().GetInt("rps")
		rpsBurst, _ := cmd.Flags().GetInt("rps-burst")

		ctx := context.Background()
		ips := resolveHostnames(nameserver, host, ctx)

		//stopChan := make(chan struct{})
		// a WaitGroup for the goroutines to tell us they've stopped
		wg := sync.WaitGroup{}
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil)
		}()
		fmt.Printf("Using random seed: %d\n", seed)

		var requestRate = Inf
		var requestBurst = rps
		useRateLimiter := false
		if rps != 0 {
			requestRate = rate.Limit(rps)
			useRateLimiter = true
			if rpsBurst != 0 {
				requestBurst = rpsBurst
			}
		} else {
			if readBlockMs < 0 {
				fmt.Printf("Given you haven't specified a rate of messages/sec and the \"--stream-read-block-ms\" value is also < 0 this means you'll be stressing the server to the highest capacity. Be aware that this will influence the producer side as well!\n")
			}
		}

		var rateLimiter = rate.NewLimiter(requestRate, requestBurst)

		client_update_tick := 1
		latencies = hdrhistogram.New(1, 90000000, 3)
		latenciesTick = hdrhistogram.New(1, 90000000, 3)
		gen := generator.NewZipfianWithRange(int64(nConsumersPerStreamMin), int64(nConsumersPerStreamMax), float64(0.99))
		randSource := rand.New(rand.NewSource(seed))
		consumersPerStream := make([]int, keyspaceLen, keyspaceLen)
		progressSize := 0
		for i := 0; int64(i) < keyspaceLen; i++ {
			consumersPerStream[i] = int(gen.Next(randSource))
			progressSize += consumersPerStream[i]
		}
		datapointsChan := make(chan datapoint, numberRequests)
		fmt.Printf("Setting up the consumer groups. On total we will have %d connections.\n", progressSize)
		bar := progressbar.Default(int64(progressSize))
		startT := time.Now()
		for streamId := 1; int64(streamId) <= keyspaceLen; streamId++ {
			consumersForStream := consumersPerStream[streamId-1]
			ctx := context.Background()
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			keyname := fmt.Sprintf("%s%d", streamPrefix, streamId)
			groupname := fmt.Sprintf("streamgroup:%s%d", streamPrefix, streamId)
			if verbose {
				fmt.Printf("Using connection string %s for stream %s\n", connectionStr, keyname)
			}

			blockingPoolSize := int(consumersForStream)
			client := getClientWithOptions(connectionStr, auth, blockingPoolSize, readBufferEachConn, writeBufferEachConn, clientKeepAlive)

			// ensure we destroy the group before starting
			err := client.Do(ctx, client.B().XgroupDestroy().Key(keyname).Group(groupname).Build()).Error()
			err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
			if err != nil {
				panic(err)
			}
			for consumerId := 1; uint64(consumerId) <= uint64(consumersForStream); consumerId++ {
				consumername := fmt.Sprintf("streamconsumer:%s:%d", groupname, consumerId)
				err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
				bar.Add(1)
			}
			client.Close()
		}
		endT := time.Now()
		durationSetup := endT.Sub(startT)
		fmt.Printf("Finished setting up the consumer groups after %f seconds.\n", durationSetup.Seconds())
		fmt.Printf("Starting all consumer go-routines. On total we will have %d connections.\n", progressSize)
		bar = progressbar.Default(int64(progressSize))
		// listen for C-c
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		startT = time.Now()
		for streamId := 1; int64(streamId) <= keyspaceLen; streamId++ {
			consumersForStream := consumersPerStream[streamId-1]
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			keyname := fmt.Sprintf("%s%d", streamPrefix, streamId)
			groupname := fmt.Sprintf("streamgroup:%s%d", streamPrefix, streamId)
			if verbose {
				fmt.Printf("Using connection string %s for stream %s\n", connectionStr, keyname)
			}

			blockingPoolSize := int(consumersForStream)
			client := getClientWithOptions(connectionStr, auth, blockingPoolSize, readBufferEachConn, writeBufferEachConn, clientKeepAlive)

			defer client.Close()
			for consumerId := 1; uint64(consumerId) <= uint64(consumersForStream); consumerId++ {
				consumername := fmt.Sprintf("streamconsumer:%s:%d", groupname, consumerId)
				wg.Add(1)
				go benchmarkConsumerRoutine(client, c, datapointsChan, &wg, keyname, groupname, consumername, readBlockMs, readCount, useRateLimiter, rateLimiter)
				bar.Add(1)
			}

			// delay the creation for each additional client
			time.Sleep(betweenClientsDelay)
		}
		fmt.Printf("Finished starting all consumer go-routines.\n")
		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)

		closed, _, duration, totalMessages, messageRateTs, percentilesTs, cmdRateTs := updateCLI(tick, c, numberRequests, loop, datapointsChan)
		endT = time.Now()
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
		testResult.CmdRateTs = cmdRateTs
		_, overallLatencies := generateLatenciesMap(latencies, duration)
		testResult.Totals = overallLatencies
		saveJsonResult(testResult, jsonOutFile)
		if closed {
			return
		}

	},
}

func benchmarkConsumerRoutine(client rueidis.Client, c chan os.Signal, datapointsChan chan datapoint, wg *sync.WaitGroup, keyname, groupname, consumername string, readBlockMs, readCount int64, useRateLimiter bool, rateLimiter *rate.Limiter) {
	defer wg.Done()
	ctx := context.Background()
	var startT time.Time
	var endT time.Time

	for {
		select {
		case <-c:
			fmt.Println("\nreceived Ctrl-c on Consumer Routine - shutting down")
			return
		default:
			cmdsIssued := make([]int, 0, 1)
			cmdsIssued = append(cmdsIssued, XREADGROUP)
			processedEntries := 0
			if useRateLimiter {
				r := rateLimiter.ReserveN(time.Now(), int(1))
				time.Sleep(r.Delay())
			}
			startT = time.Now()
			res := client.Do(ctx, getXreadGroupCmd(client, groupname, consumername, readCount, readBlockMs, keyname))
			endT = time.Now()
			xreadEntries, err := res.AsXRead()

			if err != nil {
				if err != rueidis.Nil {
					cmdsIssued = append(cmdsIssued, XGROUPCREATE)
					cmdsIssued = append(cmdsIssued, XGROUPCREATECONSUMER)
					cmdsIssued = append(cmdsIssued, XREADGROUP)
					err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
					err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
					startT = time.Now()
					xreadEntries, err = client.Do(ctx, getXreadGroupCmd(client, groupname, consumername, readCount, readBlockMs, keyname)).AsXRead()
					endT = time.Now()
				}
			}
			if err == nil {
				xrangeEntries, found := xreadEntries[keyname]
				if found {
					for _, xrangeEntry := range xrangeEntries {
						cmdsIssued = append(cmdsIssued, XACK)
						err = client.Do(ctx, client.B().Xack().Key(keyname).Group(groupname).Id(xrangeEntry.ID).Build()).Error()
						processedEntries++
					}
				}
			}
			duration := endT.Sub(startT)
			datapointsChan <- datapoint{(err == nil || (err != nil && err == rueidis.Nil)), duration.Microseconds(), cmdsIssued, processedEntries}
		}
	}
}

func getXreadGroupCmd(client rueidis.Client, groupname string, consumername string, readCount int64, readBlockMs int64, keyname string) rueidis.Completed {
	cmd := client.B().Xreadgroup().Group(groupname, consumername).Count(readCount)
	if readBlockMs >= 0 {
		cmd.Block(readBlockMs)
	}
	xreadGroupCompletedCmd := cmd.Streams().Key(keyname).Id(">")
	return xreadGroupCompletedCmd.Build()
}

func init() {
	rootCmd.AddCommand(consumerCmd)
	consumerCmd.PersistentFlags().Int64("stream-read-block-ms", -1, "Block the client for this amount of milliseconds. If 0 will return immediatelly.")
	consumerCmd.PersistentFlags().Uint64("consumers-per-stream-min", 5, "per stream consumer count min.")
	consumerCmd.PersistentFlags().Uint64("consumers-per-stream-max", 50, "per stream consumer count max.")
	consumerCmd.PersistentFlags().Int64("stream-read-count", 1, "per command count of messages to be read.")

}
