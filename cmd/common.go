package cmd

import (
	"context"
	"fmt"
	"github.com/rueian/rueidis"
	"log"
	"net"
	"os"
	"time"
)

const (
	XREADGROUP           int = 0
	XADD                     = 1
	XACK                     = 2
	EXPIRE                   = 3
	DEL                      = 4
	XGROUPCREATE             = 5
	XGROUPCREATECONSUMER     = 6
	TTL                      = 7
	XLEN                     = 8
)

const (
	DefaultReadBuffer  int = 2 << 13
	DefaultWriteBuffer int = 2 << 13
)

func getClientWithOptions(connectionStr, auth string, blockingPoolSize, readBufferEachConn, writeBufferEachConn int, clientKeepAlive time.Duration) rueidis.Client {
	clientOptions := rueidis.ClientOption{
		InitAddress:         []string{connectionStr},
		Password:            auth,
		AlwaysPipelining:    false,
		AlwaysRESP2:         true,
		DisableCache:        true,
		BlockingPoolSize:    blockingPoolSize,
		PipelineMultiplex:   0,
		RingScaleEachConn:   1,
		ReadBufferEachConn:  readBufferEachConn,
		WriteBufferEachConn: writeBufferEachConn,
	}
	clientOptions.Dialer.KeepAlive = clientKeepAlive
	client, err := rueidis.NewClient(clientOptions)
	if err != nil {
		panic(err)
	}
	return client
}

func resolveHostnames(nameserver, host string, ctx context.Context) []net.IP {
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
	return ips
}

func updateCLI(tick *time.Ticker, c chan os.Signal, message_limit uint64, loop bool, datapointsChan chan datapoint) (bool, time.Time, time.Duration, uint64, []float64, []map[string]float64, map[string][]int) {
	ALL_COMMANDS := []int{
		XREADGROUP,
		XADD,
		XACK,
		EXPIRE,
		DEL,
		XGROUPCREATE,
		XGROUPCREATECONSUMER,
		TTL,
		XLEN,
	}
	ALL_COMMANDS_STR := []string{
		"XREADGROUP",
		"XADD",
		"XACK",
		"EXPIRE",
		"DEL",
		"XGROUPCREATE",
		"XGROUPCREATECONSUMER",
		"TTL",
		"XLEN",
	}

	var currentErr uint64 = 0
	var currentCommandCount uint64 = 0
	var currentMessageCount uint64 = 0
	prevCommandCount := uint64(0)
	prevMessageCount := uint64(0)
	start := time.Now()
	prevTime := time.Now()

	messageRateTs := []float64{}
	cmdRateTs := map[string][]int{}
	cmdRateTick := make([]int, len(ALL_COMMANDS), len(ALL_COMMANDS))
	percentilesTs := []map[string]float64{}
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %25s %7s %25s %25s %25s\n", "Test time", " ", "Total Commands", "Total Messages", "Total Errors", "", "Command Rate", "Message Rate", "p50 lat. (msec)")
	for {
		select {
		case dp = <-datapointsChan:
			{
				currentCommandCount++
				// in case of blocking commands that did not processed entries we don't really account it
				if dp.processedEntries > 0 {
					latencies.RecordValue(dp.durationMs)
					latenciesTick.RecordValue(dp.durationMs)
					currentMessageCount += uint64(dp.processedEntries)
					for _, cmdType := range dp.commandsIssued {
						cmdRateTick[cmdType]++
					}
				}
				// in all cases we check for errors
				if !dp.success {
					currentErr++
				}

			}
		case <-tick.C:
			{
				totalCommands += currentCommandCount
				totalMessages += currentMessageCount
				totalErrors += currentErr
				currentErr = 0
				currentCommandCount = 0
				currentMessageCount = 0
				now := time.Now()
				took := now.Sub(prevTime)
				commandRate := float64(totalCommands-prevCommandCount) / float64(took.Seconds())
				messageRate := float64(totalMessages-prevMessageCount) / float64(took.Seconds())
				completionPercentStr := "[----%]"
				errorPercentStr := "[----%]"
				if !loop {
					completionPercent := float64(totalMessages) / float64(message_limit) * 100.0
					completionPercentStr = fmt.Sprintf("[%3.1f%%]", completionPercent)
				}
				if totalCommands > 0 {
					errorPercent := float64(totalErrors) / float64(totalCommands) * 100.0
					errorPercentStr = fmt.Sprintf("[%3.1f%%]", errorPercent)
				}

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if prevMessageCount == 0 && totalMessages != 0 {
					start = time.Now()
				}
				if totalMessages != 0 {
					messageRateTs = append(messageRateTs, messageRate)
					_, perTickLatencies := generateLatenciesMap(latenciesTick, took)
					percentilesTs = append(percentilesTs, perTickLatencies)
					latenciesTick.Reset()
				}

				// rotate cmdRateTick and update cmdRateTs
				for pos, value := range cmdRateTick {
					cmdNameStr := ALL_COMMANDS_STR[pos]
					cmdRateTs[cmdNameStr] = append(cmdRateTs[cmdNameStr], value)
					cmdRateTick[pos] = 0
				}

				prevMessageCount = totalMessages
				prevCommandCount = totalCommands
				prevTime = now

				fmt.Printf("%25.0fs %s %25d %25d %25d  %s %25.2f %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercentStr, totalCommands, totalMessages, totalErrors, errorPercentStr, commandRate, messageRate, p50)
				fmt.Printf("\r")
				//w.Flush()
				if message_limit > 0 && totalMessages >= uint64(message_limit) && !loop {
					return true, start, time.Since(start), totalMessages, messageRateTs, percentilesTs, cmdRateTs
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			return true, start, time.Since(start), totalMessages, messageRateTs, percentilesTs, cmdRateTs
		}
	}
}
