#include <valgrind/callgrind.h>

#include <iostream>

#include "duckdb.hpp"

int main() {
	std::string db_name = "student_uncompressed.db";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 126;"); }

	// set the allocator flush threshold to 1GB
	{ auto res = con.Query("SET allocator_flush_threshold=\"16gb\"; "); }

	// disable the object cache for all parquet metadata
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ------------------------------------ Load Data -------------------------------------------------
	// loading table into memory, using the temp table (to make sure the data is in memory, even if DuckDB is not
	// in in-memory mode.)
	{
		//		con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student.parquet');");
		//		con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department.parquet');");
		//		con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room.parquet');");
		//		con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type.parquet');");
	}
	// Or, leave tables in disk, we create the views
	//	{
	//		con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('student.parquet');");
	//		con.Query("CREATE VIEW department AS SELECT * FROM read_parquet('department.parquet');");
	//		con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('room.parquet');");
	//		con.Query("CREATE VIEW type AS SELECT * FROM read_parquet('type.parquet');");
	//	}
	// clean the Profiler
	duckdb::BeeProfiler::Get().Clear();

	//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
	//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
	//  "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
	//  "CREATE TABLE type (type INTEGER, info VARCHAR);"
	// ------------------------------------ Query -------------------------------------------------
	// A query with a very long probing pipeline.
	{
		std::string query =
		    "EXPLAIN ANALYZE "
		    "SELECT student.stu_id, student.major_id, room.room_id, room.type "
		    "FROM student, room WHERE student.stu_id = room.stu_id AND student.stu_id < 50000000 AND room.stu_id < "
		    "50000000;";

		for (size_t i = 0; i < 1; ++i) {
			CALLGRIND_START_INSTRUMENTATION;
			auto result = con.Query(query);
			CALLGRIND_STOP_INSTRUMENTATION;
			CALLGRIND_DUMP_STATS;

			duckdb::BeeProfiler::Get().EndProfiling();
			std::cerr << "----------------------------------------------------------\n";

			if (i >= 0) {
				if (!result->HasError()) {
					std::string plan = result->GetValue(1, 0).ToString();
					std::cerr << plan << "\n";
				} else
					std::cerr << result->GetError() << "\n";
			}
		}
	}

	return 0;
}
