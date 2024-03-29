#include <iostream>
#include <random>

#include "duckdb.hpp"
#include "duckdb/common/negative_feedback.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"

void GenerateTablesSkewedDist(duckdb::Connection &con);

void GenerateTablesChunkFactor(duckdb::Connection &con);

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

		duckdb::CompactTuner::Get().Reset();

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
	{ con.Query("SET threads TO 32;"); }

	// set the allocator flush threshold
	{ auto res = con.Query("SET allocator_flush_threshold=\"300gb\"; "); }

	// disable the object cache
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ---------------------------------------- Load Data --------------------------------------------------
	{
		GenerateTablesChunkFactor(con);

		con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student_simd.parquet');");
		con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department_simd.parquet');");
		con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room_simd.parquet');");
		con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type_simd.parquet');");
	}

	// ------------------------------------ Threads Settings -----------------------------------------------
	{
		// [HashJoin]
		{
			// Build Hash Table
			scheduler.SetThreadSetting(4, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(8, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
			// Probe Hash Table
			scheduler.SetThreadSetting(1, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"EXPLAIN_ANALYZE"}, true);
		}
	}

	// ------------------------------------------ Query -----------------------------------------------------
	{
		// SEQ join query
		std::string query =
		    "EXPLAIN ANALYZE "
		    "SELECT student.stu_id, department.name, room.room_id, type.type,  "
		    "FROM student, room, department, type "
		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = type.type;";

		// enable auto-tuning
		scheduler.SetThreadSetting(1, "CompactTuner", "CompactTuner");
		ExecuteQuery(con, query, 2, 1);

		// disable auto-tuning
		scheduler.SetThreadSetting(0, "CompactTuner", "CompactTuner");
		ExecuteQuery(con, query, 2, 1);  // 5 times, show 4 times because the first time is warm-up
	}
}

void GenerateTablesSkewedDist(duckdb::Connection &con) {
	// Table student
	con.Query(
	    "CREATE OR REPLACE TABLE student AS "
	    "SELECT "
	    "    CAST(stu_id AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6 / 3) AS INT) AS major_id, "
	    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
	    "FROM generate_series(1,  CAST(5e7 / 3 AS INT)) vals(stu_id);");

	con.Query(
	    "INSERT INTO student "
	    "SELECT "
	    "    CAST(stu_id AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6 / 3 + 5e6 / 3) AS INT) AS major_id, "
	    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
	    "FROM generate_series(CAST(5e7 / 3 AS INT),  CAST(2 * 5e7 / 3 AS INT)) vals(stu_id);");

	con.Query(
	    "INSERT INTO student "
	    "SELECT "
	    "    CAST(stu_id AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6 / 3 + 2 * 5e6 / 3) AS INT) AS major_id, "
	    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
	    "FROM generate_series(CAST(2 * 5e7 / 3 AS INT),  CAST(5e7 AS INT)) vals(stu_id);");

	std::string str_1, str_2;
	for (size_t i = 0; i < 0; i++)
		str_1 += "abcd";
	for (size_t i = 0; i < 0; i++)
		str_2 += "abcd";

	// Table: department
	con.Query(
	    "CREATE OR REPLACE TABLE department AS "
	    "SELECT "
	    "    CAST(major_id % 5e6 AS INT) AS major_id, "
	    "	'_" +
	    str_1 +
	    "' || (major_id) AS name, "
	    "FROM generate_series(1,  CAST(5e6 / 3 AS INT)) vals(major_id);");

	con.Query(
	    "INSERT INTO department "
	    "SELECT "
	    "	CAST(major_id % 5e6 AS INT) AS major_id, "
	    "	'_" +
	    str_2 +
	    "' || (major_id) AS name, "
	    "FROM generate_series(CAST(5e6 / 3 AS INT), CAST(2 * 5e6 / 3 AS INT)) vals(major_id);");

	con.Query(
	    "INSERT INTO department "
	    "SELECT "
	    "	CAST(major_id % 5e6 AS INT) AS major_id, "
	    "	'_" +
	    str_1 +
	    "' || (major_id) AS name, "
	    "FROM generate_series(CAST(2 * 5e6 / 3 AS INT), CAST(5e6 AS INT)) vals(major_id);");

	// Table room
	con.Query(
	    "CREATE OR REPLACE TABLE room AS "
	    "SELECT "
	    "	 room_id AS room_id, "
	    "    CAST(room_id % 5e7 AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6) AS INT) AS type "
	    "FROM generate_series(1,  CAST(5e7 / 3 AS INT)) vals(room_id);");

	con.Query(
	    "INSERT INTO room "
	    "SELECT "
	    "	 room_id AS room_id, "
	    "    CAST(room_id % 5e7 AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6) AS INT) AS type "
	    "FROM generate_series(CAST(5e7 / 3 AS INT),  CAST(2 * 5e7 / 3 AS INT)) vals(room_id);");

	con.Query(
	    "INSERT INTO room "
	    "SELECT "
	    "	 room_id AS room_id, "
	    "    CAST(room_id % 5e7 AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6) AS INT) AS type "
	    "FROM generate_series(CAST(2 * 5e7 / 3 AS INT),  CAST(5e7 AS INT)) vals(room_id);");

	// Table type
	con.Query(
	    "CREATE OR REPLACE TABLE type AS "
	    "SELECT "
	    "    CAST(type % 5e6 AS INT) AS type, "
	    "    'room_type_' || type AS info "
	    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(type);");

	con.Query("COPY student TO 'student_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY department TO 'department_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY room TO 'room_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY type TO 'type_simd.parquet' (FORMAT PARQUET);");
}

void GenerateTablesChunkFactor(duckdb::Connection &con) {
	// Table student
	con.Query(
	    "CREATE OR REPLACE TABLE student AS "
	    "SELECT "
	    "    CAST(stu_id AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6) AS INT) AS major_id, "
	    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
	    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(stu_id);");

	std::string str_1, str_2;
	for (size_t i = 0; i < 0; i++)
		str_1 += "abcd";

	// Table: department
	con.Query(
	    "CREATE OR REPLACE TABLE department AS "
	    "SELECT "
	    "    CAST(major_id * 8 % 5e6 AS INT) AS major_id, "
	    "	'_" +
	    str_1 +
	    "' || (major_id) AS name, "
	    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(major_id);");

	// Table room
	con.Query(
	    "CREATE OR REPLACE TABLE room AS "
	    "SELECT "
	    "	 room_id AS room_id, "
	    "    CAST(room_id * 8 % 5e7 AS INT) AS stu_id, "
	    "    CAST((RANDOM() * 5e6) AS INT) AS type "
	    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(room_id);");

	// Table type
	con.Query(
	    "CREATE OR REPLACE TABLE type AS "
	    "SELECT "
	    "    CAST(type * 8 % 5e6 AS INT) AS type, "
	    "    'room_type_' || type AS info "
	    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(type);");

	con.Query("COPY student TO 'student_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY department TO 'department_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY room TO 'room_simd.parquet' (FORMAT PARQUET);");
	con.Query("COPY type TO 'type_simd.parquet' (FORMAT PARQUET);");
}
