/*
Copyright © 2023 Redis Performance Group performance <at> redis <dot> com
*/
package cmd

import (
	"context"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/rueian/rueidis"
	"github.com/spf13/cobra"
	"log"
	"math/rand"
	"net"
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
		port, _ := cmd.Flags().GetInt("p")
		keyspaceLen, _ := cmd.Flags().GetInt64("keyspace-len")
		nConsumersPerStream, _ := cmd.Flags().GetUint64("consumers-per-stream")
		numberRequests, _ := cmd.Flags().GetUint64("n")
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

		client_update_tick := 1
		latencies = hdrhistogram.New(1, 90000000, 3)
		datapointsChan := make(chan datapoint, numberRequests)
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
			client, err := rueidis.NewClient(clientOptions)
			if err != nil {
				panic(err)
			}
			defer client.Close()
			// ensure we destroy the group before starting
			err = client.Do(ctx, client.B().XgroupDestroy().Key(keyname).Group(groupname).Build()).Error()
			err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
			if err != nil {
				panic(err)
			}
			for consumerId := 1; uint64(consumerId) <= nConsumersPerStream; consumerId++ {
				consumername := fmt.Sprintf("streamconsumer:%s:%d", groupname, consumerId)
				err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
				wg.Add(1)
				go benchmarkConsumerRoutine(client, datapointsChan, &wg, keyname, groupname, consumername)
			}

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

func benchmarkConsumerRoutine(client rueidis.Client, datapointsChan chan datapoint, wg *sync.WaitGroup, keyname, groupname, consumername string) {
	defer wg.Done()
	ctx := context.Background()
	for true {
		startT := time.Now()
		xreadEntries, err := client.Do(ctx, client.B().Xreadgroup().Group(groupname, consumername).Block(0).Streams().Key(keyname).Id(">").Build()).AsXRead()
		if err != nil {
			err = client.Do(ctx, client.B().XgroupCreate().Key(keyname).Group(groupname).Id("0").Mkstream().Build()).Error()
			err = client.Do(ctx, client.B().XgroupCreateconsumer().Key(keyname).Group(groupname).Consumer(consumername).Build()).Error()
			xreadEntries, err = client.Do(ctx, client.B().Xreadgroup().Group(groupname, consumername).Block(0).Streams().Key(keyname).Id(">").Build()).AsXRead()
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

func init() {
	rootCmd.AddCommand(consumerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
