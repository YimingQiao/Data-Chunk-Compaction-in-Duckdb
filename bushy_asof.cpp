#include <fstream>
#include <iostream>
#include <random>

#include "duckdb.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"

inline bool FileExisted(const std::string &name) {
	std::ifstream f(name.c_str());
	return f.good();
}

void ExecuteQuery(duckdb::Connection &con, const std::string &query, size_t running_times, size_t showing_times);

void Log2File(std::vector<std::vector<double>> &the_stats, const std::string &log_name);

int main() {
	// nullptr means in-memory database.
	std::string db_name = "";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);
	auto &scheduler = duckdb::ThreadScheduler::Get();
	using VecStr = std::vector<std::string>;

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	{
		// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
		con.Query("SET threads TO 64;");

		// set the allocator flush threshold
		auto res = con.Query("SET allocator_flush_threshold=\"300gb\"; ");

		// disable the object cache
		con.Query("PRAGMA disable_object_cache;");
	}

	// ------------------------------------ Create Tables -------------------------------------------------
	const size_t scale = 5e6;
	const size_t n_kind = 256;
	if (FileExisted("./build_ts.parquet") && FileExisted("./probe_ts.parquet")) {
		con.Query("CREATE TEMPORARY TABLE build AS SELECT * FROM read_parquet('build_ts.parquet');");
		con.Query("CREATE TEMPORARY TABLE probe AS SELECT * FROM read_parquet('probe_ts.parquet');");
	} else {
		con.Query(
		    "CREATE OR REPLACE TABLE build AS (\n"
		    "  SELECT k, '2001-01-01 00:00:00'::TIMESTAMP + INTERVAL (v) MINUTE AS t, v\n"
		    "  FROM range(0," +
		    std::to_string(scale) + ") vals(v), range(0," + std::to_string(n_kind) +
		    ") keys(k)\n"
		    ");");
		con.Query(
		    "CREATE OR REPLACE TABLE probe AS (\n"
		    "  SELECT k * 2 AS k, t - INTERVAL (30) SECOND AS t\n"
		    "  FROM build\n"
		    ");");
		con.Query("COPY build TO 'build_ts.parquet' (FORMAT PARQUET);");
		con.Query("COPY probe TO 'probe_ts.parquet' (FORMAT PARQUET);");
	}
	// ---------------------------- ------- Threads Settings -----------------------------------------------
	{
		scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {""});
		scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"ASOF_JOIN"});
		scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {"EXPLAIN_ANALYZE"});
		scheduler.SetThreadSetting(32, VecStr {"PARTITION_MERGE"}, VecStr {"PARTITION_MERGE"}, false);
	}

	// ------------------------------------------ Query -----------------------------------------------------

	std::vector<std::vector<double>> the_stats;
	std::string query = "EXPLAIN ANALYZE SELECT v FROM probe ASOF JOIN build USING(k, t);";
	// warm up the cache and memory allocator.
	ExecuteQuery(con, query, 2, 0);
	duckdb::CatProfiler::Get().Clear();

	for (int64_t i = 64; i > 0; i -= 4) {
		for (int64_t j = 64; j > 0; j -= 4) {
			double n_building = i;
			double n_combine = i;
			double n_probing = j;

			// asof join
			scheduler.SetThreadSetting(n_building, VecStr {"SEQ_SCAN ", "READ_PARQUET"}, VecStr {"EXPLAIN_ANALYZE"});
			scheduler.SetThreadSetting(n_building, VecStr {""}, VecStr {"ASOF_JOIN"});
			scheduler.SetThreadSetting(n_building, VecStr {"PARTITION_MERGE"}, VecStr {"PARTITION_MERGE"}, false);
			scheduler.SetThreadSetting(n_probing, VecStr {"ASOF_JOIN"}, VecStr {"EXPLAIN_ANALYZE"});

			ExecuteQuery(con, query, 1, 0);

			std::vector<double> record {n_building, n_combine, n_probing};
			auto &timing = duckdb::CatProfiler::Get().GetStageTimings();
			for (auto &time : timing) {
				std::cerr << time.second << ",";
				record.push_back(time.second);
			}
			std::cerr << "\n";
			the_stats.push_back(record);
			duckdb::CatProfiler::Get().Clear();
		}
	}
	Log2File(the_stats, "asof_join_scalability.log");

	return 0;
}

void ExecuteQuery(duckdb::Connection &con, const std::string &query, size_t running_times, size_t showing_times) {
	if (running_times < showing_times) {
		std::cerr << "running_times < showing_times\n";
		return;
	}

	for (size_t i = 0; i < running_times; ++i) {
		auto result = con.Query(query);

		duckdb::BeeProfiler::Get().EndProfiling();
		std::cerr << "-------------------------------------------------------------------------------------------------"
		             "-------------------\n";

		if (i >= running_times - showing_times) {
			if (!result->HasError()) {
				std::string plan = result->GetValue(1, 0).ToString();
				std::cerr << plan << "\n";
				// std::cerr << result->ToString() << "\n";
			} else {
				std::cerr << result->GetError() << "\n";
			}
		}
	}
}

void Log2File(std::vector<std::vector<double>> &the_stats, const std::string &log_name) {
	std::ofstream log_file(log_name);
	if (!log_file.is_open()) {
		std::cerr << "Error opening file: " << log_name << std::endl;
	}
	for (auto &record : the_stats) {
		for (auto &v : record) {
			log_file << v << ",";
		}
		log_file << "\n";
	}
	log_file.close();
}
