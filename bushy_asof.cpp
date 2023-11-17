#include <iostream>
#include <random>

#include "duckdb.hpp"

void ExecuteQuery(duckdb::Connection &con, std::string query, size_t running_times, size_t showing_times) {
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

int main() {
	// nullptr means in-memory database.
	std::string db_name = "";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 64;"); }

	// set the allocator flush threshold
	{ auto res = con.Query("SET allocator_flush_threshold=\"64gb\"; "); }

	// disable the object cache
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ------------------------------------ Create Tables -------------------------------------------------
	con.Query(
	    "CREATE OR REPLACE TABLE build AS (\n"
	    "  SELECT k, '2001-01-01 00:00:00'::TIMESTAMP + INTERVAL (v) MINUTE AS t, v\n"
	    "  FROM range(0,2000000) vals(v), range(0,16) keys(k)\n"
	    ");");
	con.Query(
	    "CREATE OR REPLACE TABLE probe AS (\n"
	    "  SELECT k * 2 AS k, t - INTERVAL (30) SECOND AS t\n"
	    "  FROM build\n"
	    ");");

	std::string query = "EXPLAIN ANALYZE SELECT v FROM probe ASOF JOIN build USING(k, t);";
	ExecuteQuery(con, query, 2, 1);

	return 0;
}