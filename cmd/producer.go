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
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
)

var totalCommands uint64
var totalErrors uint64
var latencies *hdrhistogram.Histogram
var latenciesTick *hdrhistogram.Histogram

const Inf = rate.Limit(math.MaxFloat64)
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type datapoint struct {
	success          bool
	durationMs       int64
	commandsIssued   []int
	processedEntries int
}

func stringWithCharset(length int, charset string) string {

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// producerCmd represents the producer command
var producerCmd = &cobra.Command{
	Use:   "producer",
	Short: "Producer workload doing the actual data ingestion of streams.",
	Long:  `.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("h")
		port, _ := cmd.Flags().GetInt("p")
		rps, _ := cmd.Flags().GetInt("rps")
		dataLen, _ := cmd.Flags().GetInt("d")
		rpsBurst, _ := cmd.Flags().GetInt("rps-burst")
		numberRequests, _ := cmd.Flags().GetUint64("n")
		streamMaxlen, _ := cmd.Flags().GetInt64("stream-maxlen")
		streamMaxlenExpireSeconds, _ := cmd.Flags().GetInt64("stream-maxlen-expire-secs")
		keyspaceLen, _ := cmd.Flags().GetInt64("keyspace-len")
		nClients, _ := cmd.Flags().GetUint64("c")
		auth, _ := cmd.Flags().GetString("a")
		verbose, _ := cmd.Flags().GetBool("verbose")
		seed, _ := cmd.Flags().GetInt64("seed")
		nameserver, _ := cmd.Flags().GetString("nameserver")
		streamPrefix, _ := cmd.Flags().GetString("stream-prefix")
		betweenClientsDelay, _ := cmd.Flags().GetDuration("between-clients-duration")
		jsonOutFile, _ := cmd.Flags().GetString("json-out-file")
		clientKeepAlive, _ := cmd.Flags().GetDuration("client-keep-alive-time")
		loop, _ := cmd.Flags().GetBool("loop")
		readBufferEachConn, _ := cmd.Flags().GetInt("read-buffer-each-conn")
		writeBufferEachConn, _ := cmd.Flags().GetInt("write-buffer-each-conn")

		if nClients > uint64(keyspaceLen) {
			log.Fatalf("The number of clients needs to be smaller or equal to the number of streams")
		}
		ctx := context.Background()

		ips := resolveHostnames(nameserver, host, ctx)

		// a WaitGroup for the goroutines to tell us they've stopped
		wg := sync.WaitGroup{}

		fmt.Printf("Using random seed: %d. Each client will have seed of %d+<client id>\n", seed, seed)

		var requestRate = Inf
		var requestBurst = rps
		useRateLimiter := false
		if rps != 0 {
			requestRate = rate.Limit(rps)
			useRateLimiter = true
			if rpsBurst != 0 {
				requestBurst = rpsBurst
			}
		}

		var rateLimiter = rate.NewLimiter(requestRate, requestBurst)

		samplesPerClient := numberRequests / nClients
		streamsPerClient := int(keyspaceLen / int64(nClients))
		fmt.Printf("samplesPerClient %d\n", samplesPerClient)
		client_update_tick := 1
		latencies = hdrhistogram.New(1, 90000000, 3)
		latenciesTick = hdrhistogram.New(1, 90000000, 3)
		value := stringWithCharset(dataLen, charset)
		datapointsChan := make(chan datapoint, numberRequests)
		startT := time.Now()
		for clientId := 0; uint64(clientId) < nClients; clientId++ {
			randSource := rand.New(rand.NewSource(seed + int64(clientId)))
			clientStreamStart := (clientId * streamsPerClient) + 1
			clientStreamEnd := (clientId + 1) * streamsPerClient
			gen := generator.NewZipfianWithRange(int64(clientStreamStart), int64(clientStreamEnd), float64(0.99))
			wg.Add(1)
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			if verbose {
				fmt.Printf("Using connection string %s for client %d\n", connectionStr, clientId)
			}

			blockingPoolSize := 1
			client := getClientWithOptions(connectionStr, auth, blockingPoolSize, readBufferEachConn, writeBufferEachConn, clientKeepAlive)
			defer client.Close()

			go benchmarkRoutine(client, streamPrefix, value, datapointsChan, samplesPerClient, &wg, useRateLimiter, rateLimiter, gen, randSource, streamMaxlen, streamMaxlenExpireSeconds, loop)

			// delay the creation for each additional client
			time.Sleep(betweenClientsDelay)
		}

		// listen for C-c
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)
		closed, _, duration, totalMessages, messageRateTs, percentilesTs, cmdRateTs := updateCLI(tick, c, numberRequests, loop, datapointsChan)
		messageRate := float64(totalMessages) / float64(duration.Seconds())
		avgMs := float64(latencies.Mean()) / 1000.0
		p50IngestionMs := float64(latencies.ValueAtQuantile(50.0)) / 1000.0
		p95IngestionMs := float64(latencies.ValueAtQuantile(95.0)) / 1000.0
		p99IngestionMs := float64(latencies.ValueAtQuantile(99.0)) / 1000.0
		endT := time.Now()
		fmt.Printf("\n")
		fmt.Printf("#################################################\n")
		fmt.Printf("Total Duration %.3f Seconds\n", duration.Seconds())
		fmt.Printf("Total Errors %d\n", totalErrors)
		fmt.Printf("Throughput summary: %.0f requests per second\n", messageRate)
		fmt.Printf("Latency summary (msec):\n")
		fmt.Printf("    %9s %9s %9s %9s\n", "avg", "p50", "p95", "p99")
		fmt.Printf("    %9.3f %9.3f %9.3f %9.3f\n", avgMs, p50IngestionMs, p95IngestionMs, p99IngestionMs)

		testResult := NewTestResult("", uint(nClients), uint64(rps))
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

func benchmarkRoutine(client rueidis.Client, streamPrefix, value string, datapointsChan chan datapoint, samplesPerClient uint64, wg *sync.WaitGroup, useRateLimiter bool, rateLimiter *rate.Limiter, gen *generator.Zipfian, randSource *rand.Rand, streamMaxlen int64, streamMaxlenExpireSeconds int64, loop bool) {
	streamMessages := make(map[int64]int64, 0)
	defer wg.Done()
	for i := 0; uint64(i) < samplesPerClient || loop; i++ {
		ctx := context.Background()
		if useRateLimiter {
			r := rateLimiter.ReserveN(time.Now(), int(1))
			time.Sleep(r.Delay())
		}
		var streamId int64
		var err error = nil
		var keyname string
		var counter int64 = streamMaxlen
		cmdsIssued := make([]int, 0, 1)
		for counter >= streamMaxlen {
			streamId = gen.Next(randSource)
			counterV, found := streamMessages[streamId]
			keyname = fmt.Sprintf("%s%d", streamPrefix, streamId)
			if found {
				counter = counterV
			} else {
				cmdsIssued = append(cmdsIssued, XLEN)
				counter, err = client.Do(ctx, client.B().Xlen().Key(keyname).Build()).AsInt64()
				if counter > 0 {
					counter++
				}
			}
			if counter >= streamMaxlen {
				if streamMaxlenExpireSeconds == 0 {
					cmdsIssued = append(cmdsIssued, DEL)
					err = client.Do(ctx, client.B().Del().Key(keyname).Build()).Error()
					counter = 0
				} else {
					var ttl int64 = -1
					cmdsIssued = append(cmdsIssued, TTL)
					cc := client.Do(ctx, client.B().Ttl().Key(keyname).Build())
					ttl, err = cc.AsInt64()
					if ttl < 0 {
						cmdsIssued = append(cmdsIssued, EXPIRE)
						//fmt.Printf("Expiring %s in %d seconds given it surpasses stream max len %d\n", keyname, streamMaxlenExpireSeconds, streamMaxlen)
						err = client.Do(ctx, client.B().Expire().Key(keyname).Seconds(streamMaxlenExpireSeconds).Build()).Error()
					}
				}
			}

		}
		counter = counter + 1
		cmdsIssued = append(cmdsIssued, XADD)
		streamEntry := fmt.Sprintf("%d", counter)
		startT := time.Now()
		err = client.Do(ctx, client.B().Xadd().Key(keyname).Id(streamEntry).FieldValue().FieldValue("field", value).Build()).Error()
		endT := time.Now()
		duration := endT.Sub(startT)
		streamMessages[streamId] = counter
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds(), cmdsIssued, 1}
	}
}

func init() {
	rootCmd.AddCommand(producerCmd)
}
