
[![license](https://img.shields.io/github/license/redis-performance/openstreaming-benchmark.svg)](https://github.com/redis-performance/openstreaming-benchmark)
[![GitHub issues](https://img.shields.io/github/release/redis-performance/openstreaming-benchmark.svg)](https://github.com/redis-performance/openstreaming-benchmark/releases/latest)
[![codecov](https://codecov.io/github/redis-performance/openstreaming-benchmark/branch/main/graph/badge.svg?token=B6ISQSDK3Y)](https://codecov.io/github/redis-performance/openstreaming-benchmark)


# openstreaming-benchmark
make it easy to benchmark distributed streaming systems.


## Installation

### Download Standalone binaries ( no Golang needed )

If you don't have go on your machine and just want to use the produced binaries you can download the following prebuilt bins:

https://github.com/redis-performance/openstreaming-benchmark/releases/latest

| OS | Arch | Link |
| :---         |     :---:      |          ---: |
| Linux   | amd64  (64-bit X86)     | [openstreaming-benchmark-linux-amd64](https://github.com/redis-performance/openstreaming-benchmark/releases/latest/download/openstreaming-benchmark-linux-amd64.tar.gz)    |
| Linux   | arm64 (64-bit ARM)     | [openstreaming-benchmark-linux-arm64](https://github.com/redis-performance/openstreaming-benchmark/releases/latest/download/openstreaming-benchmark-linux-arm64.tar.gz)    |
| Darwin   | amd64  (64-bit X86)     | [openstreaming-benchmark-darwin-amd64](https://github.com/redis-performance/openstreaming-benchmark/releases/latest/download/openstreaming-benchmark-darwin-amd64.tar.gz)    |
| Darwin   | arm64 (64-bit ARM)     | [openstreaming-benchmark-darwin-arm64](https://github.com/redis-performance/openstreaming-benchmark/releases/latest/download/openstreaming-benchmark-darwin-arm64.tar.gz)    |

Here's how bash script to download and try it:

```bash
wget -c https://github.com/redis-performance/openstreaming-benchmark/releases/latest/download/openstreaming-benchmark-$(uname -mrs | awk '{ print tolower($1) }')-$(dpkg --print-architecture).tar.gz -O - | tar -xz

# give it a try
./openstreaming-benchmark --help
```


### Installation in a Golang env

To install the benchmark utility with a Go Env do as follow:

`go get` and then `go install`:
```bash
# Fetch this repo
go get github.com/redis-performance/openstreaming-benchmark
cd $GOPATH/src/github.com/redis-performance/openstreaming-benchmark
make
```