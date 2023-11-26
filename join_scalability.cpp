#include <fstream>
#include <iostream>
#include <random>

#include "duckdb.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"

void Log2File(std::vector<std::vector<double>> &the_stats, const std::string &log_name);

//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
void GenDatabase(duckdb::Connection &con);

void ExecuteQuery(duckdb::Connection &con, std::string query, size_t running_times, size_t showing_times);

int main() {
	// nullptr means in-memory database.
	std::string db_name = "";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);
	auto &scheduler = duckdb::ThreadScheduler::Get();
	using VecStr = std::vector<std::string>;

	// GenDatabase(con);

	// ---------------------------------------- Load Data --------------------------------------------------
	{
		// loading table into memory, using the temp table (so that we are sure the data is in memory, even if DuckDB is
		// not in in-memory mode.)
		{
			con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('probe.parquet');");
			con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('build.parquet');");
		}

		// Or, leave tables in disk, we create the views
		{
			// con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('student.parquet');");
			// con.Query("CREATE VIEW department AS SELECT * FROM read_parquet('department.parquet');");
		}
		duckdb::BeeProfiler::Get().Clear();
	}

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	{
		// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
		con.Query("SET threads TO 64;");

		// set the allocator flush threshold
		auto res = con.Query("SET allocator_flush_threshold=\"300gb\"; ");

		// disable the object cache
		con.Query("PRAGMA disable_object_cache;");
	}

	// ---------------------------- ------- Threads Settings -----------------------------------------------
	{
		// [HashJoin]
		{
			// Build Hash Table
			scheduler.SetThreadSetting(64, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(64, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
			// Probe Hash Table
			scheduler.SetThreadSetting(64, VecStr {"SEQ_SCAN "}, VecStr {"EXPLAIN_ANALYZE"}, true);
		}
		// [Sort-Merge Join]
		{
			scheduler.SetThreadSetting(32, VecStr {"PIECEWISE_MERGE_JOIN"}, VecStr {""}, false);
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"}, true);
		}
		// [BREAKER]
		{
			scheduler.SetThreadSetting(64, VecStr {"BREAKER"}, VecStr {""});
			scheduler.SetThreadSetting(12, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"BREAKER"});
			scheduler.SetThreadSetting(52, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, true);
		}
		// [AsOf Join]
		{
			scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {""});
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"ASOF_JOIN"});
			scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {"EXPLAIN_ANALYZE"});
			scheduler.SetThreadSetting(32, VecStr {"PARTITION_MERGE"}, VecStr {"PARTITION_MERGE"}, false);
		}
		// [IE Join]
		{
			scheduler.SetThreadSetting(32, VecStr {"IE_JOIN"}, VecStr {"IE_JOIN", "EXPLAIN_ANALYZE", "BREAKER"});
			scheduler.SetThreadSetting(4, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"IE_JOIN"});
			scheduler.SetThreadSetting(28, VecStr {"IE_JOIN"}, VecStr {"HASH_JOIN"});
		}
	}

	// ------------------------------------------ Query -----------------------------------------------------
	std::vector<std::vector<double>> the_stats;
	con.Query("SET prefer_range_joins=true;");

	std::string query =
	    "EXPLAIN ANALYZE "
	    "SELECT student.stu_id, department.major_id, department.name "
	    "FROM student, department "
	    "WHERE student.major_id >= department.major_id AND student.major_id <= department.major_id AND "
	    "department.major_id <= 10000000;";

	// the first time is to warm up the cache and memory allocator.
	ExecuteQuery(con, query, 2, 0);
	duckdb::CatProfiler::Get().Clear();

	for (size_t i = 64; i > 0; i -= 4) {
		for (size_t j = 64; j > 0; j -= 4) {
			if (i != 64 && j != 64 && i != j) continue;
			double n_building = i;
			double n_combine = i;
			double n_probing = j;

			// Hash Join
			scheduler.SetThreadSetting(n_building, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(n_building, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
			scheduler.SetThreadSetting(n_probing, VecStr {"SEQ_SCAN "}, VecStr {"EXPLAIN_ANALYZE"}, true);

			// Sort-Merge Join
			scheduler.SetThreadSetting(n_building, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"});
			scheduler.SetThreadSetting(n_building, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
			scheduler.SetThreadSetting(n_probing, VecStr {"PIECEWISE_MERGE_JOIN"}, VecStr {""});

			// ie join
			scheduler.SetThreadSetting(n_building, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
			scheduler.SetThreadSetting(n_building, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"IE_JOIN"});
			scheduler.SetThreadSetting(n_probing, VecStr {"IE_JOIN"}, VecStr {"IE_JOIN", "EXPLAIN_ANALYZE", "BREAKER"});

			// asof join
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
	Log2File(the_stats, "iejoin_scalability.log");

	return 0;
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

void GenDatabase(duckdb::Connection &con) {
	// database setting
	const size_t probing_size = 3e9;
	const size_t building_size = 5e8;

	// sequential queries
	{
		auto res = con.Query(
		    "CREATE OR REPLACE TABLE student AS "
		    "SELECT "
		    "    CAST(stu_id AS BIGINT) AS stu_id, "
		    "    CAST(RANDOM() * " +
		    std::to_string(building_size) +
		    " AS INT) AS major_id, "
		    "    CAST(RANDOM() * 100 AS TINYINT) AS age "
		    "FROM generate_series(1, " +
		    std::to_string(probing_size) + ") vals(stu_id);");
		if (res->HasError()) std::cerr << res->GetError() << "\n";

		res = con.Query(
		    "CREATE OR REPLACE TABLE department AS "
		    "SELECT "
		    "    CAST(RANDOM() * " +
		    std::to_string(building_size) +
		    " AS INT) AS major_id, "
		    "    'major_' || (major_id) AS name "
		    "FROM generate_series(1, " +
		    std::to_string(building_size) + ") vals(major_id);");
		if (res->HasError()) std::cerr << res->GetError() << "\n";
	}

	// We export the tables to disk in parquet format, separately.
	{
		con.Query("COPY student TO 'probe.parquet' (FORMAT PARQUET);");
		con.Query("COPY department TO 'build.parquet' (FORMAT PARQUET);");
	}
}

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
