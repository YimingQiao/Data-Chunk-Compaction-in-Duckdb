<div align="center">
  <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-stacked.svg" height="120">
</div>
<br>




<p align="center">
  <a href="https://github.com/duckdb/duckdb/actions">
    <img src="https://github.com/duckdb/duckdb/actions/workflows/Main.yml/badge.svg?branch=main" alt="Github Actions Badge">
  </a>
  <a href="https://discord.gg/tcvwpjfnZx">
    <img src="https://shields.io/discord/909674491309850675" alt="discord" />
  </a>
  <a href="https://github.com/duckdb/duckdb/releases/">
    <img src="https://img.shields.io/github/v/release/duckdb/duckdb?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release">
  </a>
</p>

## Chunk Compaction in the DuckDB
Since we have showed that the [chunk compaction is important the the vectorized execution](https://github.com/YimingQiao/Chunk-Compaction-in-Vectorized-Execution), we the integrate our compaction solution into the [duckdb](https://github.com/duckdb/duckdb). Our solution consists of the dynamic/learning compaction and the compacted vectorized hash join. 

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
