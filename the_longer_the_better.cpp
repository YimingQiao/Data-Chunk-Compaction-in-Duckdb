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
	//	{
	//		con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type.parquet');");
	//	}
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
		std::string joins =
		    "SELECT student.stu_id, student.major_id, t2.room_id, t2.type "
		    "FROM student, room AS t2 WHERE student.stu_id = t2.stu_id AND t2.stu_id < 5000000";

		joins = "SELECT t1.stu_id, t1.type, t2.major_id FROM (" + joins +
		        ") AS t1, department AS t2 WHERE t1.major_id = t2.major_id";
		joins = "SELECT t1.stu_id, t1.major_id, t2.type FROM (" + joins + ") AS t1, type AS t2 WHERE t1.type = t2.type";
		joins = "SELECT t1.stu_id, t1.type, t2.major_id, t2.name FROM (" + joins +
		        ") AS t1, department AS t2 WHERE t1.major_id = t2.major_id";

		std::string query = "EXPLAIN ANALYZE " + joins + ";";

		for (size_t i = 0; i < 2; ++i) {
			auto result = con.Query(query);

			duckdb::BeeProfiler::Get().EndProfiling();
			std::cerr << "-----------------------------------------------------------------------------------------\n";

			if (i >= 1) {
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
