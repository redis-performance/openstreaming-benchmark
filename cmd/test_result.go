package cmd

import (
	"encoding/json"
	"github.com/HdrHistogram/hdrhistogram-go"
	"io/ioutil"
	"log"
	"time"
)

const resultFormatVersion = "0.0.1"

type GraphQueryDatapoint struct {
	ClientDurationMicros  int64
	Error                 bool
	Empty                 bool
	ConsumerGroupsCreated uint64
}

type TestResult struct {

	// Test Configs
	resultFormatVersion string `json:"ResultFormatVersion"`
	Metadata            string `json:"Metadata"`
	Clients             uint   `json:"Clients"`
	MaxRps              uint64 `json:"MaxRps"`
	RandomSeed          int64  `json:"RandomSeed"`

	StartTime      int64 `json:"StartTime"`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	// Populated after benchmark
	// Benchmark Totals
	Totals map[string]float64 `json:"Totals"`

	// Overall Rates
	OverallQueryRates []float64 `json:"OverallQueryRates"`

	// Overall Client Quantiles
	OverallClientLatencies []map[string]float64 `json:"OverallClientLatencies"`

	// Per second ( tick ) client stats
	ClientRunTimeStats map[int64]interface{} `json:"ClientRunTimeStats"`

	// Per second ( tick ) client stats
	CmdRateTs map[string][]int `json:"CmdRateTs"`
}

func NewTestResult(metadata string, clients uint, maxRps uint64) *TestResult {
	return &TestResult{resultFormatVersion: resultFormatVersion, Metadata: metadata, Clients: clients, MaxRps: maxRps}
}

func (r *TestResult) SetUsedRandomSeed(seed int64) *TestResult {
	r.RandomSeed = seed
	return r
}

func (r *TestResult) FillDurationInfo(startTime time.Time, endTime time.Time, duration time.Duration) {
	r.StartTime = startTime.UTC().UnixNano() / 1000000
	r.EndTime = endTime.UTC().UnixNano() / 1000000
	r.DurationMillis = duration.Milliseconds()
}

func saveJsonResult(testResult *TestResult, jsonOutputFile string) {
	if jsonOutputFile != "" {
		file, err := json.MarshalIndent(testResult, "", " ")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Saving JSON results file to %s\n", jsonOutputFile)
		err = ioutil.WriteFile(jsonOutputFile, file, 0644)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func calculateRateMetrics(current, prev int64, took time.Duration) (rate float64) {
	rate = float64(current-prev) / float64(took.Seconds())
	return
}

func generateLatenciesMap(hist *hdrhistogram.Histogram, tick time.Duration) (int64, map[string]float64) {
	ops := hist.TotalCount()
	percentilesTrack := []float64{0.0, 50.0, 99.0, 100.0}
	q0 := 0.0
	q50 := 0.0
	q99 := 0.0
	q100 := 0.0
	if ops > 0 {
		percentilesMap := hist.ValueAtPercentiles(percentilesTrack)
		q0 = float64(percentilesMap[0.0]) / 10e2
		q50 = float64(percentilesMap[50.0]) / 10e2
		q99 = float64(percentilesMap[99.0]) / 10e2
		q100 = float64(percentilesMap[100.0]) / 10e2
	}
	mp := map[string]float64{"q0": q0, "q50": q50, "q99": q99, "q100": q100, "ops/sec": float64(ops) / tick.Seconds()}
	return ops, mp
}
