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
	var currentCount uint64 = 0
	start := time.Now()
	prevTime := time.Now()
	prevMessageCount := uint64(0)
	messageRateTs := []float64{}
	cmdRateTs := map[string][]int{}
	cmdRateTick := make([]int, len(ALL_COMMANDS), len(ALL_COMMANDS))
	percentilesTs := []map[string]float64{}
	var dp datapoint
	fmt.Printf("%26s %7s %25s %25s %7s %25s %25s\n", "Test time", " ", "Total Commands", "Total Errors", "", "Command Rate", "p50 lat. (msec)")
	for {
		select {
		case dp = <-datapointsChan:
			{
				latencies.RecordValue(dp.durationMs)
				latenciesTick.RecordValue(dp.durationMs)
				if !dp.success {
					currentErr++
				}
				currentCount++
				for _, cmdType := range dp.commandsIssued {
					cmdRateTick[cmdType]++
				}
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
				errorPercentStr := "[----%]"
				if !loop {
					completionPercent := float64(totalCommands) / float64(message_limit) * 100.0
					completionPercentStr = fmt.Sprintf("[%3.1f%%]", completionPercent)
				}
				if totalCommands > 0 {
					errorPercent := float64(totalErrors) / float64(totalCommands) * 100.0
					errorPercentStr = fmt.Sprintf("[%3.1f%%]", errorPercent)
				}

				p50 := float64(latencies.ValueAtQuantile(50.0)) / 1000.0

				if prevMessageCount == 0 && totalCommands != 0 {
					start = time.Now()
				}
				if totalCommands != 0 {
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

				prevMessageCount = totalCommands
				prevTime = now

				fmt.Printf("%25.0fs %s %25d %25d %s %25.2f %25.2f\t", time.Since(start).Seconds(), completionPercentStr, totalCommands, totalErrors, errorPercentStr, messageRate, p50)
				fmt.Printf("\r")
				//w.Flush()
				if message_limit > 0 && totalCommands >= uint64(message_limit) && !loop {
					return true, start, time.Since(start), totalCommands, messageRateTs, percentilesTs, cmdRateTs
				}

				break
			}

		case <-c:
			fmt.Println("\nreceived Ctrl-c - shutting down")
			return true, start, time.Since(start), totalCommands, messageRateTs, percentilesTs, cmdRateTs
		}
	}
}
