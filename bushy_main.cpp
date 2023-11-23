#include <iostream>
#include <random>

#include "duckdb.hpp"

std::string s3_access_key_id = "AKIARZ5TMPGJHQ4PFLDP";
std::string s3_access_key = "+uSXS1yGwBP+wfoaqJrQ71/Mu7WPZbUNABDy2c0h";

//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
//  "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
//  "CREATE TABLE type (type INTEGER, info VARCHAR);"
void GenDatabase(duckdb::Connection &con) {
	// database setting
	const int probing_size = 5e7;
	const int building_size = 5e6;
	{
		// random queries
		//		con.Query(
		//		    "CREATE OR REPLACE TABLE student AS "
		//		    "SELECT "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS stu_id, "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS major_id, "
		//		    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
		//		    "FROM generate_series(1, CAST(%d AS INT)) vals(stu_id);",
		//		    probing_size, building_size, probing_size);
		//
		//		con.Query(
		//		    "CREATE OR REPLACE TABLE department AS "
		//		    "SELECT "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS major_id, "
		//		    "    'major_' || (major_id) AS name "
		//		    "FROM generate_series(1, CAST(%d AS INT)) vals(major_id);",
		//		    building_size, building_size);
		//
		//		con.Query(
		//		    "CREATE OR REPLACE TABLE room AS "
		//		    "SELECT "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS room_id, "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS stu_id, "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS type "
		//		    "FROM generate_series(1, CAST(%d AS INT)) vals(room_id);",
		//		    probing_size, probing_size, building_size, probing_size);
		//
		//		con.Query(
		//		    "CREATE OR REPLACE TABLE type AS "
		//		    "SELECT "
		//		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS type, "
		//		    "    'room_type_' || type AS info "
		//		    "FROM generate_series(1, CAST(%d AS INT)) vals(type);",
		//		    building_size, building_size);
	}
	{
		// sequential queries
		con.Query(
		    "CREATE OR REPLACE TABLE student AS "
		    "SELECT "
		    "    CAST(stu_id AS INT) AS stu_id, "
		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS major_id, "
		    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
		    "FROM generate_series(1,  CAST(%d AS INT)) vals(stu_id);",
		    building_size, probing_size);

		con.Query(
		    "CREATE OR REPLACE TABLE department AS "
		    "SELECT "
		    "    CAST(major_id AS INT) AS major_id, "
		    "    'major_' || (major_id) AS name "
		    "FROM generate_series(1,  CAST(%d AS INT)) vals(major_id);",
		    building_size);

		con.Query(
		    "CREATE OR REPLACE TABLE room AS "
		    "SELECT "
		    "    CAST(room_id AS INT) AS room_id, "
		    "    CAST(room_id AS INT) AS stu_id, "
		    "    CAST((RANDOM() * CAST(%d AS INT)) AS INT) AS type "
		    "FROM generate_series(1,  CAST(%d AS INT)) vals(room_id);",
		    building_size, probing_size);

		con.Query(
		    "CREATE OR REPLACE TABLE type AS "
		    "SELECT "
		    "    CAST(type AS INT) AS type, "
		    "    'room_type_' || type AS info "
		    "FROM generate_series(1,  CAST(%d AS INT)) vals(type);",
		    building_size);
	}
	// We export the tables to disk in parquet format, separately.
	{
		con.Query("COPY student TO 'student.parquet' (FORMAT PARQUET);");
		con.Query("COPY department TO 'department.parquet' (FORMAT PARQUET);");
		con.Query("COPY room TO 'room.parquet' (FORMAT PARQUET);");
		con.Query("COPY type TO 'type.parquet' (FORMAT PARQUET);");
	}
	// export to S3, in parquet format
	{
		con.Query("SET s3_region='ap-southeast-1';");
		con.Query("SET s3_access_key_id=" + s3_access_key_id + ";");
		con.Query("SET s3_secret_access_key=" + s3_access_key + ";");

		con.Query("COPY student TO 's3://parquets/student.parquet';");
		con.Query("COPY department TO 's3://parquets/department.parquet';");
		con.Query("COPY room TO 's3://parquets/room.parquet';");
		con.Query("COPY type TO 's3://parquets/type.parquet';");
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

int main() {
	// nullptr means in-memory database.
	std::string db_name = "";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	GenDatabase(con);

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 102;"); }

	// set the allocator flush threshold
	{ auto res = con.Query("SET allocator_flush_threshold=\"64gb\"; "); }

	// disable the object cache
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ---------------------------------------- Load Data --------------------------------------------------
	// loading table into memory, using the temp table (so that we are sure the data is in memory, even if DuckDB is not
	// in in-memory mode.)
	{
		con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student.parquet');");
		con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department.parquet');");
		con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room.parquet');");
		con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type.parquet');");
	}

	// Or, leave tables in disk, we create the views
	{
		// con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('student.parquet');");
		// con.Query("CREATE VIEW department AS SELECT * FROM read_parquet('department.parquet');");
		// con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('room.parquet');");
		// con.Query("CREATE VIEW type AS SELECT * FROM read_parquet('type.parquet');");
	}
	duckdb::BeeProfiler::Get().Clear();

	// Or, leave tables in S3, we create the views
	{
		//		con.Query("SET s3_region='ap-southeast-1';");
		//		con.Query("SET s3_access_key_id=" + s3_access_key_id + ";");
		//		con.Query("SET s3_secret_access_key=" + s3_access_key + ";");
		//
		//		con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('s3://parquets/student.parquet');");
		//		con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('s3://parquets/room.parquet');");
	}

	// ------------------------------------------ Query -----------------------------------------------------
	// Hash Join & Sort-Merge Join
	{
		// hash join or sort-merge join?
		// con.Query("SET prefer_range_joins=true;");
		// BUSHY join query
		{
			std::string bushy_query =
			    "EXPLAIN ANALYZE "
			    "SELECT t1.stu_id, t1.major_id, t2.type, t2.room_id "
			    "FROM "
			    "(SELECT student.stu_id, department.major_id "
			    "	FROM student, department WHERE student.major_id = department.major_id) AS t1, "
			    "(SELECT room.stu_id, room.room_id, type.type FROM room, type WHERE room.type = type.type) "
			    "	AS t2, WHERE t1.stu_id = t2.stu_id;";

			ExecuteQuery(con, bushy_query, 2, 1);
		}

		// SEQ join query
		{
			std::string query =
			    "EXPLAIN ANALYZE "
			    "SELECT student.stu_id, department.major_id, room.room_id, type.type "
			    "FROM student, department, room, type "
			    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = "
			    "type.type;";

			ExecuteQuery(con, query, 2, 1);
		}
	}

	// range join
	{
		// SEQ join query
		{
			std::string query =
			    "EXPLAIN ANALYZE "
			    "SELECT student.stu_id, department.major_id, room.room_id, type.type "
			    "FROM student, room, department, type "
			    "WHERE student.stu_id >= room.stu_id AND student.stu_id <= room.stu_id "
			    "AND student.major_id >= department.major_id AND student.major_id <= department.major_id "
			    "AND room.type >= type.type AND room.type <= type.type AND student.stu_id <= 10000000;";

			// ExecuteQuery(con, query, 2, 1);
		}

		// BUSHY join query
		{
			std::string bushy_query =
			    "EXPLAIN ANALYZE "
			    "SELECT t1.stu_id, t1.major_id, t2.type, t2.room_id "
			    "FROM "
			    "(SELECT student.stu_id, department.major_id "
			    "FROM student, department WHERE student.major_id >= department.major_id AND student.major_id <= "
			    "department.major_id) AS t1, "
			    "(SELECT room.stu_id, room.room_id, type.type FROM room, type WHERE room.type >= type.type AND "
			    "room.type <= type.type) AS t2, "
			    "WHERE t1.stu_id >= t2.stu_id AND t1.stu_id <= t2.stu_id AND t1.stu_id <= 10000000;";

			// ExecuteQuery(con, bushy_query, 2, 1);
		}
	}

	return 0;
}
