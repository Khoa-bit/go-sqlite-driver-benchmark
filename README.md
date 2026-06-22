# Benchmark
This is a benchmark between 2 SQLite driver doesn't don't need CGO:
  1. zombiezen
  2. modernc

# Run benchmark
```
go test -run='^$' -bench='.' -benchmem -benchtime=3s -count=1 ./benchmark/
```

# Results
## Windows
```ps1
goos: windows
goarch: amd64
pkg: khoa-sqlite-driver-benchmark/benchmark
cpu: AMD Ryzen 7 Pro 7735U with Radeon Graphics
BenchmarkInsert_Single/zombie-16           40906             97361 ns/op            1330 B/op         33 allocs/op
BenchmarkInsert_Single/modernc-16          39364            103126 ns/op            1815 B/op         41 allocs/op
BenchmarkInsert_Batch/zombie-16              432           9781886 ns/op          100493 B/op       5628 allocs/op
BenchmarkInsert_Batch/modernc-16             393          10417691 ns/op          469102 B/op      13664 allocs/op
BenchmarkSelect_ByPK/zombie-16            223544             13467 ns/op             704 B/op         15 allocs/op
BenchmarkSelect_ByPK/modernc-16           192608             18419 ns/op            1783 B/op         48 allocs/op
BenchmarkSelect_RangeScan/zombie-16         1851           1646055 ns/op          608818 B/op       6304 allocs/op
BenchmarkSelect_RangeScan/modernc-16                1449           2469023 ns/op          895646 B/op      17459 allocs/op
BenchmarkUpdate/zombie-16                          66280             60174 ns/op            1108 B/op         25 allocs/op
BenchmarkUpdate/modernc-16                         51297             60653 ns/op            1410 B/op         31 allocs/op
BenchmarkDelete/zombie-16                          57632             62376 ns/op            1004 B/op         20 allocs/op
BenchmarkDelete/modernc-16                         49735             72769 ns/op            1237 B/op         25 allocs/op
BenchmarkConcurrentReads/zombie-16                  6727            527530 ns/op          116726 B/op       2579 allocs/op
BenchmarkConcurrentReads/modernc-16                  445           8203949 ns/op          363860 B/op       9571 allocs/op
BenchmarkWriteThroughput/zombie-16                  2625           1497070 ns/op           22512 B/op        553 allocs/op
BenchmarkWriteThroughput/modernc-16                 1953           1821091 ns/op           35175 B/op        804 allocs/op
BenchmarkContention_WriteRead/zombie-16              660           5750275 ns/op          613790 B/op      13841 allocs/op
BenchmarkContention_WriteRead/modernc-16             144          24567135 ns/op         1656286 B/op      44042 allocs/op
BenchmarkTx_WriteOnly/zombie-16                    42460             79917 ns/op            1681 B/op         35 allocs/op
BenchmarkTx_WriteOnly/modernc-16                   32445            174108 ns/op            1920 B/op         50 allocs/op
BenchmarkTx_ReadWrite/zombie-16                    20659            167424 ns/op            1700 B/op         36 allocs/op
BenchmarkTx_ReadWrite/modernc-16                   14104            244302 ns/op            2043 B/op         55 allocs/op
PASS
ok      khoa-sqlite-driver-benchmark/benchmark  121.155s
```

|Zombie|CPU % faster|Memory % faster|
|---|---|---|
|BenchmarkInsert_Single/zombie-16|1.04|1.36|
|BenchmarkInsert_Batch/zombie-16|1.10|4.67|
|BenchmarkSelect_ByPK/zombie-16|1.16|2.53|
|BenchmarkSelect_RangeScan/zombie-16|1.28|1.47|
|BenchmarkUpdate/zombie-16|1.29|1.27|
|BenchmarkDelete/zombie-16|1.16|1.23|
|BenchmarkConcurrentReads/zombie-16|15.12|3.12|
|BenchmarkWriteThroughput/zombie-16|1.34|1.56|
|BenchmarkContention_WriteRead/zombie-16|4.58|2.70|
|BenchmarkTx_WriteOnly/zombie-16|1.31|1.14|
|BenchmarkTx_ReadWrite/zombie-16|1.46|1.20|