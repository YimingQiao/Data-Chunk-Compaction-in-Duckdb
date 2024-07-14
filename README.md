# Chunk Compaction in the DuckDB
Since we have showed that the chunk compaction is important the the vectorized execution in this [repository](https://github.com/YimingQiao/Chunk-Compaction-in-Vectorized-Execution), we the integrate our compaction solution into the [duckdb](https://github.com/duckdb/duckdb). Our solution consists of the dynamic/learning compaction and the compacted vectorized hash join. 

## Important Modified Files
We modify many files of the original duckdb, but files around the Hash Join Operator are the most important, including:
 - `physical_hash_join.h/cpp`, which contains the hash join operator implemetation.
 - `join_hashtable.h/cpp`, which contains the used hash table in hash join.
 - `physical_operator.hpp`, which contains a class called CachingPhysicalOperator/CompactingPhysicalOperator. This operator introduces the compaction strategy used in the original duckdb.
 - `data_chunk.hpp`, which contains the design of the data chunk.
 - `profiler.hpp`, which contains several profilers that we use to record the chunk number and chunk size.

And, we disable the column compression in our end-to-end benchmark. We use the benchmark code provided by DuckDB, but adjust the scale factor used in the TPC-H and the TPC-DS. 

## Compile and Execution
We use the same way as the orignal duckdb to compile and execute. Please refer to this [docuement](https://duckdb.org/docs/dev/building/overview.html). 

### Build the benchmark
`BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_TPCDS=1 make`

#### List all available benchmarks
`build/release/benchmark/benchmark_runner --list`

#### Run a single benchmark
`build/release/benchmark/benchmark_runner benchmark/micro/nulls/no_nulls_addition.benchmark`

The output will be printed to `stdout` in CSV format, in the following format:

```
name	run	timing
benchmark/micro/nulls/no_nulls_addition.benchmark	1	0.121234
benchmark/micro/nulls/no_nulls_addition.benchmark	2	0.121702
benchmark/micro/nulls/no_nulls_addition.benchmark	3	0.122948
benchmark/micro/nulls/no_nulls_addition.benchmark	4	0.122534
benchmark/micro/nulls/no_nulls_addition.benchmark	5	0.124102
```

You can also specify an output file using the `--out` flag. This will write only the timings (delimited by newlines) to that file.

```
build/release/benchmark/benchmark_runner benchmark/micro/nulls/no_nulls_addition.benchmark --out=timings.out
cat timings.out
0.182472
0.185027
0.184163
0.185281
0.182948
```

#### Regex
You can also use a regex to specify which benchmarks to run. Be careful of shell expansion of certain regex characters (e.g. `*` will likely be expanded by your shell, hence this requires proper quoting or escaping).

`build/release/benchmark/benchmark_runner "benchmark/micro/nulls/.*" `

#### Run all benchmarks
Not specifying any argument will run all benchmarks.

`build/release/benchmark/benchmark_runner`

#### Other options
`--info` gives you some other information about the benchmark.

```
build/release/benchmark/benchmark_runner benchmark/micro/nulls/no_nulls_addition.benchmark --info
display_name:NULL Addition (no nulls)
group:micro
subgroup:nulls
```

`--query` will print the query that is run by the benchmark.

```
SELECT MIN(i + 1) FROM integers
```

`--profile` will output a query tree (pretty printed), primarily intended for interactive use.

```
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
SELECT MIN(i + 1) FROM integers
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││         Total Time: 0.176s        ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
┌───────────────────────────┐
│    UNGROUPED_AGGREGATE    │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          min(#0)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             1             │
│          (0.03s)          │
└─────────────┬─────────────┘                             
┌─────────────┴─────────────┐
│         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          +(i, 1)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         100000000         │
│          (0.05s)          │
└─────────────┬─────────────┘                             
┌─────────────┴─────────────┐
│          SEQ_SCAN         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          integers         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             i             │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         100000000         │
│          (0.08s)          │
└───────────────────────────┘      
```

