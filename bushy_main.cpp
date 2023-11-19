#include <iostream>
#include <random>

#include "duckdb.hpp"

const bool kGenerateData = false;

//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
//  "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
//  "CREATE TABLE type (type INTEGER, info VARCHAR);"
void GenDatabase(duckdb::Connection &con);

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

	if (kGenerateData) GenDatabase(con);

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

	std::string s3_access_key_id = "AKIARZ5TMPGJHQ4PFLDP";
	std::string s3_access_key = "+uSXS1yGwBP+wfoaqJrQ71/Mu7WPZbUNABDy2c0h";

	// export to S3, in parquet format
	//	{
	//		con.Query("SET s3_region='ap-southeast-1';");
	//		con.Query("SET s3_access_key_id=" + s3_access_key_id + ";");
	//		con.Query("SET s3_secret_access_key=" + s3_access_key + ";"
	//
	//		con.Query("COPY student TO 's3://parquets/student.parquet';");
	//		con.Query("COPY department TO 's3://parquets/department.parquet';");
	//		con.Query("COPY room TO 's3://parquets/room.parquet';");
	//		con.Query("COPY type TO 's3://parquets/type.parquet';");
	//	}

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

	// SEQ join query
	{
		std::string query =
		    "EXPLAIN ANALYZE "
		    "SELECT student.stu_id, department.major_id, room.room_id, type.type "
		    "FROM student, department, room, type "
		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = type.type;";

		ExecuteQuery(con, query, 2, 1);
	}

	// BUSHY join query
	{
		std::string bushy_query =
		    "EXPLAIN ANALYZE "
		    "SELECT t1.stu_id, t1.major_id, t2.type, t2.room_id "
		    "FROM "
		    "(SELECT student.stu_id, department.major_id "
		    "FROM student, department WHERE student.major_id = department.major_id) AS t1, "
		    "(SELECT room.stu_id, room.room_id, type.type FROM room INNER JOIN type ON room.type = type.type) AS t2, "
		    "WHERE t1.stu_id = t2.stu_id;";

		ExecuteQuery(con, bushy_query, 2, 1);
	}

	// I want to find the lock contension in the join building phase.
	{
		// Right-Deep
		{
			std::string right_deep_query =
			    "EXPLAIN ANALYZE "
			    "SELECT student.stu_id, student.major_id, t0.room_id, t0.type FROM student, "
			    "(SELECT student.stu_id, t2.room_id, t2.type FROM student, "
			    "(SELECT room.stu_id, room.room_id, t3.type FROM room, type AS t3 WHERE room.type = t3.type) "
			    "AS t2 WHERE student.stu_id = t2.stu_id) "
			    "AS t0 WHERE student.stu_id = t0.stu_id";

			// ExecuteQuery(con, right_deep_query, 2, 1);
		}

		// BUSHY
		{
			std::string bushy_join =
			    "EXPLAIN ANALYZE "
			    "SELECT t1.stu_id, t1.major_id, t2.room_id, t2.type FROM "
			    "(SELECT student.stu_id, t.major_id "
			    "FROM student, student AS t WHERE student.stu_id = t.stu_id) AS t1, "
			    "(SELECT room.stu_id, room.room_id, type.type FROM room INNER JOIN type ON room.type = type.type) AS "
			    "t2 WHERE t1.stu_id = t2.stu_id;";

			// ExecuteQuery(con, bushy_join, 2, 1);
		}

		// Left-Deep
		{
			std::string query =
			    "EXPLAIN ANALYZE "
			    "SELECT student.stu_id, student.major_id, t0.room_id, t0.type FROM "
			    "(SELECT student.stu_id, t2.room_id, t2.type FROM "
			    "(SELECT room.stu_id, room.room_id, t3.type FROM room, type AS t3 WHERE room.type = t3.type) "
			    "AS t2, student WHERE student.stu_id = t2.stu_id) "
			    "AS t0, student WHERE student.stu_id = t0.stu_id;";

			// ExecuteQuery(con, query, 2, 1);
		}
	}

	// how about the range join?
	{
		con.Query(
		    "CREATE TEMPORARY TABLE room_2 AS "
		    "SELECT t.room_id, t.room_id + 1 AS next_id, t.stu_id, t.type "
		    "FROM room AS t "
		    "WHERE t.room_id < 1000000;");
		con.Query(
		    "CREATE VIEW room_ie AS "
		    "SELECT room.room_id, room.stu_id, room_2.type, room_2.next_id "
		    "FROM room ASOF JOIN room_2 "
		    "ON room.room_id >= room_2.room_id;");

		con.Query(
		    "CREATE TEMPORARY TABLE student_2 AS "
		    "SELECT t.stu_id, t.stu_id + 1 AS next_id, t.major_id, t.age "
		    "FROM student AS t "
		    "WHERE t.stu_id < 1000000;");
		con.Query(
		    "CREATE VIEW student_ie AS "
		    "SELECT student.stu_id, student.major_id, student.age, student_2.next_id "
		    "FROM student ASOF JOIN student_2 "
		    "ON student.stu_id >= student_2.stu_id;");

		// left deep
		{
			std::string left_deep_query =
			    "EXPLAIN ANALYZE "
			    "SELECT student_ie.stu_id, department.major_id, room.room_id, type.type "
			    "FROM student_ie, department, room, type "
			    "WHERE student_ie.stu_id = room.stu_id AND student_ie.major_id = department.major_id "
			    "	AND room.type = type.type;";

			// ExecuteQuery(con, left_deep_query, 2, 1);
		}
		// bushy
		{
			std::string bushy_query =
			    "EXPLAIN ANALYZE "
			    "SELECT t1.stu_id, t1.major_id, t2.room_id, t2.type FROM "
			    "(SELECT student_ie.stu_id, department.major_id, FROM student_ie, department "
			    "	WHERE student_ie.major_id = department.major_id) AS t1, "
			    "(SELECT room.stu_id, room.room_id, type.type, type.info FROM room INNER JOIN type "
			    "	ON room.type = type.type) AS t2 "
			    "WHERE t1.stu_id = t2.stu_id;";

			// ExecuteQuery(con, bushy_query, 2, 1);
		}
	}

	// Left-deep join Bushy
	//	{
	//		std::string left_deep =
	//		    "(SELECT student.stu_id, department.name, room.type, type.info FROM student, department, room, type
	// WHERE " 		    "student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type =
	// type.type)";
	//
	//		for (size_t i = 0; i < 3; i++) {
	//			left_deep = "(SELECT * FROM " + left_deep + " AS ls INNER JOIN student ON ls.stu_id = student.stu_id)";
	//		}
	//		left_deep = "(SELECT * FROM " + left_deep +
	//		            " AS ls INNER JOIN student ON ls.stu_id = student.stu_id WHERE student.major_id <= 500000)";
	//
	//		std::string left_side = "(SELECT * FROM " + left_deep + " AS t)";
	//		std::string right_side = "(SELECT * FROM " + left_deep + " ORDER BY stu_id LIMIT 1)";
	//		std::string complex_join = "EXPLAIN ANALYZE SELECT * FROM " + left_side + " AS ls INNER JOIN " + right_side
	//+ 		                           " AS rs ON ls.stu_id = rs.stu_id;";
	//
	//		ExecuteQuery(con, complex_join, 2, 1);
	//	}

	return 0;
}

void GenDatabase(duckdb::Connection &con) {
	// database setting
	uint64_t stu_n = 5e7;
	uint64_t major_n = 5e6;
	uint64_t room_n = 5e6;

	// insert into student, use mt to generate tuples
	{
		con.Query(
		    "CREATE OR REPLACE TABLE student AS "
		    "SELECT "
		    "    CAST(stu_id AS INT) AS stu_id, "
		    "    CAST((RANDOM() * CAST(5e6 AS INT)) AS INT) AS major_id, "
		    "    CAST((RANDOM() * 100) AS TINYINT) AS age "
		    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(stu_id);");

		con.Query(
		    "CREATE OR REPLACE TABLE department AS "
		    "SELECT "
		    "    CAST(major_id AS INT) AS major_id, "
		    "    'major_' || (major_id) AS name "
		    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(major_id);");

		con.Query(
		    "CREATE OR REPLACE TABLE room AS "
		    "SELECT "
		    "    CAST(room_id AS INT) AS room_id, "
		    "    CAST(room_id AS INT) AS stu_id, "
		    "    CAST((RANDOM() * CAST(5e6 AS INT)) AS INT) AS type "
		    "FROM generate_series(1,  CAST(5e7 AS INT)) vals(room_id);");

		con.Query(
		    "CREATE OR REPLACE TABLE type AS "
		    "SELECT "
		    "    CAST(type AS INT) AS type, "
		    "    'room_type_' || type AS info "
		    "FROM generate_series(1,  CAST(5e6 AS INT)) vals(type);");
	}
	// We export the tables to disk in parquet format, separately.
	{
		con.Query("COPY student TO 'student.parquet' (FORMAT PARQUET);");
		con.Query("COPY department TO 'department.parquet' (FORMAT PARQUET);");
		con.Query("COPY room TO 'room.parquet' (FORMAT PARQUET);");
		con.Query("COPY type TO 'type.parquet' (FORMAT PARQUET);");
	}
}
