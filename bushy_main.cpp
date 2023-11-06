#include <iostream>
#include <random>

#include "duckdb.hpp"

const bool kGenerateData = false;

//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER);",
//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
//  "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
//  "CREATE TABLE type (type INTEGER, info VARCHAR);"
void GenDatabase(duckdb::Connection &con);

int main() {
	// nullptr means in-memory database.
	std::string db_name = "";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	if (kGenerateData) GenDatabase(con);

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 126;"); }

	// set the allocator flush threshold to 1GB
	{ auto res = con.Query("SET allocator_flush_threshold=\"16gb\"; "); }

	// disable the object cache
	{ con.Query("PRAGMA disable_object_cache;"); }

	// ---------------------------------------- Load Data --------------------------------------------------
	// loading table into memory, using the temp table (so that we are sure the data is in memory, even if DuckDB is not
	// in in-memory mode.)
	//	{
	//		con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room.parquet');");
	//		con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type.parquet');");
	//	}

	// Or, leave tables in disk, we create the views
	{
		// con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('student.parquet');");
		con.Query("CREATE VIEW department AS SELECT * FROM read_parquet('department.parquet');");
		// con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('room.parquet');");
		con.Query("CREATE VIEW type AS SELECT * FROM read_parquet('type.parquet');");
	}
	duckdb::BeeProfiler::Get().Clear();

	// export to S3, in parquet format
	//	{
	//		con.Query("SET s3_region='ap-southeast-1';");
	//		con.Query("SET s3_access_key_id='AKIARZ5TMPGJAWWRWZYB';");
	//		con.Query("SET s3_secret_access_key='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';");
	//
	//		con.Query("COPY student TO 's3://parquets/student.parquet';");
	//		con.Query("COPY department TO 's3://parquets/department.parquet';");
	//		con.Query("COPY room TO 's3://parquets/room.parquet';");
	//		con.Query("COPY type TO 's3://parquets/type.parquet';");
	//	}

	// Or, leave tables in S3, we create the views
	{
		con.Query("SET s3_region='ap-southeast-1';");
		con.Query("SET s3_access_key_id='xxxx';");
		con.Query("SET s3_secret_access_key='xxxx';");

		con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('s3://parquets/student.parquet');");
		con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('s3://parquets/room.parquet');");
	}

	// ------------------------------------------ Query -----------------------------------------------------

	// SEQ join query
	{
		std::string seq_sql_join =
		    "EXPLAIN ANALYZE "
		    "SELECT student.stu_id, department.major_id, room.room_id, type.type "
		    "FROM student, department, room, type "
		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = type.type;";

		//		for (size_t i = 0; i < 5; ++i) {
		//			auto result = con.Query(seq_sql_join);
		//
		//			duckdb::BeeProfiler::Get().EndProfiling();
		//			std::cerr << "----------------------------------------------------------\n";
		//
		//			if (i >= 3) {
		//				if (!result->HasError()) {
		//					std::string plan = result->GetValue(1, 0).ToString();
		//					std::cerr << plan << "\n";
		//					// std::cerr << result->ToString() << "\n";
		//				} else {
		//					std::cerr << result->GetError() << "\n";
		//				}
		//			}
		//		}
	}

	// BUSHY join query
	{
		std::string bushy_sql_join =
		    "EXPLAIN ANALYZE "
		    "SELECT t1.stu_id, t1.major_id, t2.type, t2.room_id "
		    "FROM "
		    "(SELECT student.stu_id, department.major_id "
		    "FROM student, department WHERE student.major_id = department.major_id) AS t1, "
		    "(SELECT room.stu_id, room.room_id, type.type FROM room INNER JOIN type ON room.type = type.type) AS t2, "
		    "WHERE t1.stu_id = t2.stu_id;";

		for (size_t i = 0; i < 5; ++i) {
			auto result = con.Query(bushy_sql_join);

			duckdb::BeeProfiler::Get().EndProfiling();
			std::cerr << "----------------------------------------------------------\n";

			if (i >= 3) {
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

	// Left-deep join Bushy
	//		{
	//			std::string left_deep =
	//			    "(SELECT student.stu_id, department.name, room.type, type.info FROM student, department, room, type
	//" 			    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id " "AND room.type
	// = type.type)";
	//
	//			for (size_t i = 0; i < 3; i++) {
	//				left_deep = "(SELECT * FROM " + left_deep + " AS ls INNER JOIN student ON ls.stu_id =
	// student.stu_id)";
	//			}
	//			left_deep = "(SELECT * FROM " + left_deep +
	//			            " AS ls INNER JOIN student ON ls.stu_id = student.stu_id WHERE student.major_id <= 500000)";
	//
	//			std::string left_side = "(SELECT * FROM " + left_deep + " AS t)";
	//			std::string right_side = "(SELECT * FROM " + left_deep + " ORDER BY stu_id LIMIT 1)";
	//			std::string complex_join = "EXPLAIN ANALYZE SELECT * FROM " + left_side + " AS ls INNER JOIN " +
	// right_side
	//	+ 		                           " AS rs ON ls.stu_id = rs.stu_id;";
	//
	//			auto result = con.Query(complex_join);
	//			if (!result->HasError()) {
	//				std::string plan = result->GetValue(1, 0).ToString();
	//				std::cerr << plan << "\n";
	//				// std::cerr << result->ToString() << "\n";
	//			} else {
	//				std::cerr << result->GetError() << "\n";
	//			}
	//		}

	return 0;
}
void GenDatabase(duckdb::Connection &con) {
	std::vector<std::string> sql_create = {"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
	                                       "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
	                                       "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
	                                       "CREATE TABLE type (type INTEGER, info VARCHAR);"};

	for (const auto &sql : sql_create) {
		con.Query(sql);
	}

	// database setting
	uint64_t stu_n = 5 * 1e7;
	uint64_t major_n = 5 * 1e6;
	uint64_t room_n = 5 * 1e6;

	// random generator
	std::mt19937 mt(42);

	// drop previous data
	con.Query("DELETE FROM student; DELETE FROM department; DELETE FROM room; DELETE FROM type;");

	// insert into student, use mt to generate tuples
	{
		std::string sql_insert = "INSERT INTO student VALUES ";
		for (uint64_t i = 0; i < stu_n; i++) {
			uint64_t major_id = mt() % major_n;
			uint16_t age = mt() % 100;
			sql_insert += "(" + std::to_string(i) + ", " + std::to_string(major_id) + ", " + std::to_string(age) + ")";
			if (i != stu_n - 1) sql_insert += ", ";

			if (i % (5 * stu_n / 100) == 0) {
				con.Query(sql_insert + ";");
				sql_insert = "INSERT INTO student VALUES ";
				std::cout << "Student Table Batch " + std::to_string(double(i) / stu_n * 100) + "%" + " inserted\n";
			}
		}
		if (sql_insert != "INSERT INTO student VALUES ") {
			con.Query(sql_insert + ";");
		}
		std::cout << "Student table inserted\n";
	}
	// insert into department, use majors
	{
		std::string sql_insert = "INSERT INTO department VALUES ";
		for (uint64_t i = 0; i < major_n; i++) {
			sql_insert += "(" + std::to_string(i) + ", '" + "major_" + std::to_string(i) + "')";
			if (i != major_n - 1) sql_insert += ", ";

			if (i % (5 * major_n / 100) == 0) {
				con.Query(sql_insert + ";");
				sql_insert = "INSERT INTO department VALUES ";
				std::cout << "Department Table Batch " + std::to_string(double(i) / major_n * 100) + "%" +
				                 " inserted\n";
			}
		}
		if (sql_insert != "INSERT INTO department VALUES ") {
			con.Query(sql_insert + ";");
		}
		std::cout << "Department table inserted\n";
	}
	// insert into room, use mt to generate tuples, each student has a room
	{
		std::string sql_insert = "INSERT INTO room VALUES ";
		for (uint64_t i = 0; i < stu_n; i++) {
			uint64_t type = mt() % room_n;
			sql_insert += "(" + std::to_string(i) + ", " + std::to_string(i) + ", " + std::to_string(type) + ")";
			if (i != stu_n - 1) sql_insert += ", ";

			if (i % (5 * stu_n / 100) == 0) {
				con.Query(sql_insert + ";");
				sql_insert = "INSERT INTO room VALUES ";
				std::cout << "Room Table Batch " + std::to_string(double(i) / stu_n * 100) + "%" + " inserted\n";
			}
		}
		if (sql_insert != "INSERT INTO room VALUES ") {
			con.Query(sql_insert + ";");
		}
		std::cout << "Room table inserted\n";
	}

	// insert into type, use room_types
	{
		std::string sql_insert = "INSERT INTO type VALUES ";
		for (uint64_t i = 0; i < room_n; i++) {
			sql_insert += "(" + std::to_string(i) + ", '" + "room_type_" + std::to_string(i) + "')";
			if (i != room_n - 1) sql_insert += ", ";

			if (i % (5 * room_n / 100) == 0) {
				con.Query(sql_insert + ";");
				sql_insert = "INSERT INTO type VALUES ";
				std::cout << "Type Table Batch " + std::to_string(double(i) / room_n * 100) + "%" + " inserted\n";
			}
		}
		if (sql_insert != "INSERT INTO type VALUES ") {
			con.Query(sql_insert + ";");
		}
		std::cout << "Type table inserted\n";
	}
	// We export the tables to disk in parquet format, separately.
	{
		con.Query("COPY student TO 'student.parquet' (FORMAT PARQUET);");
		con.Query("COPY department TO 'department.parquet' (FORMAT PARQUET);");
		con.Query("COPY room TO 'room.parquet' (FORMAT PARQUET);");
		con.Query("COPY type TO 'type.parquet' (FORMAT PARQUET);");
	}
}
