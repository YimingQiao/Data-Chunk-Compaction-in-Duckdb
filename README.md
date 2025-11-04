# Data Chunk Compaction in Vectorized Execution

This is the repository for the paper "Data Chunk Compaction in Vectorized Execution", accepted by SIGMOD'25.

The Supplementary Material of our paper includes three repositories:
1. [Problem formalization and simulation](https://github.com/YimingQiao/Chunk-Compaction-Formalization)
2. [Some Microbenchmarks to compare various compaction strategies](https://github.com/YimingQiao/Chunk-Compaction-in-Vectorized-Execution)
3. **Integrate the Leaning and Logical Compaction into the Duckdb, evaluating the End-to-end performance (Current Repository)**

**Updates: The implementation of Logical Compaction has been successfully [merged into DuckDB](https://github.com/duckdb/duckdb/pull/14956)!**


---

## Chunk Compaction in the DuckDB
Since we have showed that the chunk compaction is important for vectorized execution in [repository](https://github.com/YimingQiao/Chunk-Compaction-in-Vectorized-Execution), we then integrate our solution into [duckdb](https://github.com/duckdb/duckdb). Our solution consists of Dynamic/Learning compaction and the compacted vectorized hash join.

## Important Modified Files
We modify many files of the original duckdb, but files around the Hash Join Operator are the most important, including:
- `physical_hash_join.h/cpp`, which contains the hash join operator implemetation.
- `join_hashtable.h/cpp`, which contains the used hash table in hash join.
- `physical_operator.hpp`, which contains a class called CachingPhysicalOperator/CompactingPhysicalOperator. 

This operator introduces the compaction strategy used in the original duckdb.
And, we disable the column compression and the perfect hash techinique in our end-to-end benchmark. We use the benchmark code provided by DuckDB, but adjust the scale factor used in the TPC-H and the TPC-DS.

## Compile and Execution
We use the same way as the orignal duckdb to compile and execute. Please refer to this [document](https://duckdb.org/docs/stable/dev/building/overview.html) and the [benchmark suite document](https://duckdb.org/docs/stable/dev/benchmark.html).

### System Requirements
**Important:** Compiling this project requires a significant amount of main memory. The compilation process may require **more than 50 GB of RAM** and has been successfully tested on systems with 64 GB of memory. If you encounter compilation failures due to the compiler being killed, this is likely due to insufficient memory. Consider using a machine with more RAM or enabling swap space if compilation fails.

#### Build the benchmark
`BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 BUILD_HTTPFS=1 CORE_EXTENSIONS='tpch' make`

**Note:** The `BUILD_HTTPFS=1` flag enables the HTTPFS extension, which is required to automatically download benchmark datasets from remote sources.

#### List all available benchmarks
`build/release/benchmark/benchmark_runner --list`

#### Run a single benchmark
`build/release/benchmark/benchmark_runner --threads=1 'benchmark/imdb/19d.benchmark'`

The output will be printed to `stdout` in CSV format, in the following format:

```
name	run	timing
benchmark/imdb/19d.benchmark	1	2.305139
benchmark/imdb/19d.benchmark	2	2.317836
benchmark/imdb/19d.benchmark	3	2.305804
benchmark/imdb/19d.benchmark	4	2.312833
benchmark/imdb/19d.benchmark	5	2.267040
```

**Note:**
1. When running IMDB benchmarks for the first time, downloading the IMDB dataset may take a considerable amount of time depending on your internet connection.
2. This code version is experimental and currently does not support multi-threading. For multi-threaded performance benchmarking, please use the official DuckDB release.
#### Regex
You can also use a regex to specify which benchmarks to run. Be careful of shell expansion of certain regex characters (e.g. `*` will likely be expanded by your shell, hence this requires proper quoting or escaping).

`build/release/benchmark/benchmark_runner '(benchmark/imdb/.*)' `

#### Run other benchmarks
You can use the following commands to run TPC-H and TPC-DS, respectively.

`build/release/benchmark/benchmark_runner '(benchmark/tpch/sf1/.*)'`

`build/release/benchmark/benchmark_runner '(benchmark/tpcds/sf1/.*)'`

To run all benchmarks:

`build/release/benchmark/benchmark_runner`


