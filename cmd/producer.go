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
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

var totalCommands uint64
var totalErrors uint64
var latencies *hdrhistogram.Histogram

const Inf = rate.Limit(math.MaxFloat64)
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type datapoint struct {
	success     bool
	duration_ms int64
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
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
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

		ips := make([]net.IP, 0)
		if nameserver != "" {
			fmt.Printf("Using %s to resolve hostname %s\n", nameserver, host)
			r := &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					d := net.Dialer{
						Timeout: time.Millisecond * time.Duration(10000),
					}
					return d.DialContext(ctx, network, nameserver)
				},
			}
			ips, _ = r.LookupIP(context.Background(), "ip", host)
		} else {
			ips, _ = net.LookupIP(host)
		}
		if len(ips) < 1 {
			log.Fatalf("Failed to resolve %s to any IP", host)
		}

		fmt.Printf("IPs %v\n", ips)

		stopChan := make(chan struct{})
		// a WaitGroup for the goroutines to tell us they've stopped
		wg := sync.WaitGroup{}

		fmt.Printf("Using random seed: %d\n", seed)
		randSource := rand.New(rand.NewSource(seed))

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
		value := stringWithCharset(dataLen, charset)
		datapointsChan := make(chan datapoint, numberRequests)
		for clientId := 0; uint64(clientId) < nClients; clientId++ {
			clientStreamStart := (clientId * streamsPerClient) + 1
			clientStreamEnd := (clientId + 1) * streamsPerClient
			gen := generator.NewZipfianWithRange(int64(clientStreamStart), int64(clientStreamEnd), float64(0.99))
			wg.Add(1)
			connectionStr := fmt.Sprintf("%s:%d", ips[rand.Int63n(int64(len(ips)))], port)
			if verbose {
				fmt.Printf("Using connection string %s for client %d\n", connectionStr, clientId)
			}

			clientOptions := rueidis.ClientOption{
				InitAddress:      []string{connectionStr},
				Password:         auth,
				AlwaysPipelining: false,
				AlwaysRESP2:      true,
				DisableCache:     true,
			}
			client, err := rueidis.NewClient(clientOptions)
			if err != nil {
				panic(err)
			}
			defer client.Close()

			go benchmarkRoutine(client, streamPrefix, value, datapointsChan, samplesPerClient, &wg, useRateLimiter, rateLimiter, gen, randSource, streamMaxlen, streamMaxlenExpireSeconds)

			// delay the creation for each additional client
			time.Sleep(betweenClientsDelay)
		}

		// listen for C-c
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		tick := time.NewTicker(time.Duration(client_update_tick) * time.Second)
		closed, _, duration, totalMessages, _ := updateCLI(tick, c, numberRequests, false, datapointsChan)
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

		if closed {
			return
		}

		// tell the goroutine to stop
		close(stopChan)
		// and wait for them both to reply back
		wg.Wait()
	},
}

func benchmarkRoutine(client rueidis.Client, streamPrefix, value string, datapointsChan chan datapoint, samplesPerClient uint64, wg *sync.WaitGroup, useRateLimiter bool, rateLimiter *rate.Limiter, gen *generator.Zipfian, randSource *rand.Rand, streamMaxlen int64, streamMaxlenExpireSeconds int64) {
	streamMessages := make(map[int64]int64, 0)
	defer wg.Done()
	for i := 0; uint64(i) < samplesPerClient; i++ {
		ctx := context.Background()
		if useRateLimiter {
			r := rateLimiter.ReserveN(time.Now(), int(1))
			time.Sleep(r.Delay())
		}
		var streamId int64
		var err error = nil
		var keyname string
		var counter int64 = streamMaxlen
		for counter >= streamMaxlen {
			streamId = gen.Next(randSource)
			counterV, found := streamMessages[streamId]
			keyname = fmt.Sprintf("%s%d", streamPrefix, streamId)
			if found {
				counter = counterV
			} else {
				counter, err = client.Do(ctx, client.B().Xlen().Key(keyname).Build()).AsInt64()
				if counter > 0 {
					counter++
					//fmt.Printf("The stream %s already contained data. starting by length %d\n", keyname, counter)
				}
			}
			if counter >= streamMaxlen {
				var ttl int64 = -1
				cc := client.Do(ctx, client.B().Ttl().Key(keyname).Build())
				ttl, err = cc.AsInt64()
				if ttl < 0 {
					fmt.Printf("Expiring %s in %d seconds given it surpasses stream max len %d\n", keyname, streamMaxlenExpireSeconds, streamMaxlen)
					err = client.Do(ctx, client.B().Expire().Key(keyname).Seconds(streamMaxlenExpireSeconds).Build()).Error()
				}
			}

		}
		counter = counter + 1
		streamEntry := fmt.Sprintf("%d", counter)
		startT := time.Now()
		err = client.Do(ctx, client.B().Xadd().Key(keyname).Id(streamEntry).FieldValue().FieldValue("field", value).Build()).Error()
		endT := time.Now()
		duration := endT.Sub(startT)
		streamMessages[streamId] = counter
		datapointsChan <- datapoint{!(err != nil), duration.Microseconds()}
	}
}

func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, loop bool, datapointsChan chan datapoint) (bool, time.Time, time.Duration, uint64, []float64) {
	var currentErr uint64 = 0
	var currentCount uint64 = 0
	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %7s %25s %25s\n", "Test time", " ", "Total Commands", "Total Errors", "", "Command Rate", "p50 lat. (msec)")
	for {
		select {
		case dp = <-datapointsChan:
			{
				latencies.RecordValue(dp.duration_ms)
				if !dp.success {
					currentErr++
				}
				currentCount++
			}
		case <-tick.C:
			{
				totalCommands += currentCount
				totalErrors += currentErr
				currentErr = 0
				currentCount = 0
				now := time.Now()
				took := now.Sub(prevTime)
				messageRate := float64(totalCommands-prevMessageCount) / float64(took.Seconds())
				completionPercentStr := "[----%]"
				if !loop {
					completionPercent := float64(totalCommands) / float64(message_limit) * 100.0
					completionPercentStr = fmt.Sprintf("[%3.1f%%]", completionPercent)
				}
				errorPercent := float64(totalErrors) / float64(totalCommands) * 100.0

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if prevMessageCount == 0 && totalCommands != 0 {
					start = time.Now()
				}
				if totalCommands != 0 {
					messageRateTs = append(messageRateTs, messageRate)
				}
				prevMessageCount = totalCommands
				prevTime = now

				fmt.Printf("%25.0fs %s %25d %25d [%3.1f%%] %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercentStr, totalCommands, totalErrors, errorPercent, messageRate, p50)
				fmt.Printf("\r")
				//w.Flush()
				if message_limit > 0 && totalCommands >= uint64(message_limit) && !loop {
					return true, start, time.Since(start), totalCommands, messageRateTs
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommands, messageRateTs
		}
	}
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
