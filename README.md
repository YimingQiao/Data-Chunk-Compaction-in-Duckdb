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
 - `physical_operator.hpp`, which contains a class called CachingPhysicalOperator/CompactingPhysicalOperator. This operator introduces the compaction strategy used in the original duckdb.
 - `data_chunk.hpp`, which contains the design of the data chunk.
 - `profiler.hpp`, which contains several profilers that we use to record the chunk number and chunk size.

And, we disable the column compression and the perfect hash techinique in our end-to-end benchmark. We use the benchmark code provided by DuckDB, but adjust the scale factor used in the TPC-H and the TPC-DS. 

## Compile and Execution
We use the same way as the orignal duckdb to compile and execute. Please refer to this [document](https://duckdb.org/docs/dev/building/overview.html). 

#### Build the benchmark
`BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make`

#### List all available benchmarks
`build/release/benchmark/benchmark_runner --list`

#### Run a single benchmark
`build/release/benchmark/benchmark_runner 'benchmark/imdb/19d.benchmark'`

The output will be printed to `stdout` in CSV format, in the following format:

```
name	run	timing
benchmark/imdb/19d.benchmark	1	2.305139
benchmark/imdb/19d.benchmark	2	2.317836
benchmark/imdb/19d.benchmark	3	2.305804
benchmark/imdb/19d.benchmark	4	2.312833
benchmark/imdb/19d.benchmark	5	2.267040
```

#### Regex
You can also use a regex to specify which benchmarks to run. Be careful of shell expansion of certain regex characters (e.g. `*` will likely be expanded by your shell, hence this requires proper quoting or escaping).

`build/release/benchmark/benchmark_runner --threads=1 '(benchmark/imdb/.*)' `

#### Run all benchmarks
Not specifying any argument will run all benchmarks.

`build/release/benchmark/benchmark_runner`
