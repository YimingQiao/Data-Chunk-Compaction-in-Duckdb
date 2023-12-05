#include <iostream>
#include <random>

#include "duckdb.hpp"
#include "duckdb/common/negative_feedback.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"

void ExecuteQuery(duckdb::Connection &con, std::string query, size_t running_times, size_t showing_times) {
	if (running_times < showing_times) {
		std::cerr << "running_times < showing_times\n";
		return;
	}

	for (size_t i = 0; i < running_times; ++i) {
		auto result = con.Query(query);

		duckdb::BeeProfiler::Get().EndProfiling();

		duckdb::ZebraProfiler::Get().ToCSV();
		duckdb::ZebraProfiler::Get().Clear();

		duckdb::CompactionController::Get().Reset();

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
	auto &scheduler = duckdb::ThreadScheduler::Get();
	using VecStr = std::vector<std::string>;

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 96;"); }

	// set the allocator flush threshold
	{ auto res = con.Query("SET allocator_flush_threshold=\"300gb\"; "); }

	// disable the object cache
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ---------------------------------------- Load Data --------------------------------------------------
	{
		con.Query(
		    "CREATE OR REPLACE TABLE student AS "
		    "SELECT "
		    "    CAST(stu_id AS INT) AS stu_id, "
		    "    CAST((RANDOM() * 5e6) AS INT) AS major_id, "
		    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
		    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(stu_id);");

		std::string a_long_string = "abc";
		for (size_t i = 0; i < 2; i++) {
			a_long_string += "abc";
		}

		con.Query(
		    "CREATE OR REPLACE TABLE department AS "
		    "SELECT "
		    "    CAST(major_id * 4 % 5e6 AS INT) AS major_id, "
		    "	'major_" +
		    a_long_string +
		    "' || (major_id) AS name, "
		    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(major_id);");

		con.Query(
		    "CREATE OR REPLACE TABLE room AS "
		    "SELECT "
		    "	 room_id AS room_id, "
		    "    CAST(room_id * 4 % 5e7 AS INT) AS stu_id, "
		    "    CAST((RANDOM() * 5e6) AS INT) AS type "
		    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(room_id);");

		con.Query(
		    "CREATE OR REPLACE TABLE type AS "
		    "SELECT "
		    "    CAST(type % 5e6 AS INT) AS type, "
		    "    'room_type_' || type AS info "
		    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(type);");
	}

	// ---------------------------- ------- Threads Settings -----------------------------------------------
	{
		// [HashJoin]
		{
			// Build Hash Table
			scheduler.SetThreadSetting(4, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(32, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
			// Probe Hash Table
			scheduler.SetThreadSetting(16, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"EXPLAIN_ANALYZE"}, true);
		}
		// [BREAKER]
		{
			scheduler.SetThreadSetting(8, VecStr {"BREAKER"}, VecStr {""});
			scheduler.SetThreadSetting(4, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"BREAKER"});
			scheduler.SetThreadSetting(4, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, true);
		}
		// [Sort-Merge Join]
		{
			scheduler.SetThreadSetting(32, VecStr {"PIECEWISE_MERGE_JOIN"}, VecStr {""}, false);
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"}, true);
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"}, false);
			scheduler.SetThreadSetting(32, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
		}
	}

	// ---------------------------------------- Compaction Setting -----------------------------------------
	std::vector<size_t> arms = {8, 16, 32, 64, 128, 256, 512, 1024};
	std::vector<double> means(arms.size(), 0);
	duckdb::CompactionController::Get().Initialize(arms, means);

	// ------------------------------------------ Query -----------------------------------------------------
	{
		// BUSHY join query
		std::string bushy_query =
		    "EXPLAIN ANALYZE "
		    "SELECT t1.stu_id, t1.name, t2.type, t2.room_id "
		    "FROM "
		    "(SELECT student.stu_id, department.name, department.name "
		    "FROM student, department WHERE student.major_id = department.major_id) AS t1, "
		    "(SELECT room.stu_id, room.room_id, type.type FROM room INNER JOIN type ON room.type = type.type) AS t2, "
		    "WHERE t1.stu_id = t2.stu_id;";

		// SEQ join query
		std::string query =
		    "EXPLAIN ANALYZE "
		    "SELECT student.stu_id, department.name, room.room_id, type.type,  "
		    "FROM student, room, department, type "
		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = type.type;";

		// ExecuteQuery(con, bushy_query, 2, 1);
		ExecuteQuery(con, query, 2, 2);
	}
}
