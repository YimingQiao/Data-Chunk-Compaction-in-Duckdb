#include <iostream>
#include <random>

#include "duckdb.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"

//	"CREATE TABLE student (stu_id INTEGER, major_id INTEGER, age TINYINT);",
//  "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
//  "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
//  "CREATE TABLE type (type INTEGER, info VARCHAR);"
void GenDatabase(duckdb::Connection &con) {
	// database setting
	const int probing_size = 5e7;
	const int building_size = 5e6;
	const int collision = 1;

	{
		auto res = con.Query(
		    "CREATE OR REPLACE TABLE student AS "
		    "SELECT "
		    "    CAST(stu_id AS INT) AS stu_id, "
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
		    "    CAST(major_id AS INT) AS major_id, "
		    "    'major_' || (major_id) AS name "
		    "FROM generate_series(1, " +
		    std::to_string(building_size) + ") vals(major_id);");
		if (res->HasError()) std::cerr << res->GetError() << "\n";

		res = con.Query(
		    "CREATE OR REPLACE TABLE room AS "
		    "SELECT "
		    "    CAST(room_id AS INT) AS room_id, "
		    "    CAST(RANDOM() * " +
		    std::to_string(probing_size) +
		    " AS INT) AS stu_id, "
		    "    CAST(RANDOM() * " +
		    std::to_string(building_size) +
		    " AS INT) AS type "
		    "FROM generate_series(1, " +
		    std::to_string(probing_size) + ") vals(room_id);");
		if (res->HasError()) std::cerr << res->GetError() << "\n";

		res = con.Query(
		    "CREATE OR REPLACE TABLE type AS "
		    "SELECT "
		    "    CAST(type AS INT) AS type, "
		    "    'room_type_' || type AS info "
		    "FROM generate_series(1, " +
		    std::to_string(building_size) + ") vals(type);");
		if (res->HasError()) std::cerr << res->GetError() << "\n";
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
		//		con.Query("SET s3_region='ap-southeast-1';");
		//		con.Query("SET s3_access_key_id=" + s3_access_key_id + ";");
		//		con.Query("SET s3_secret_access_key=" + s3_access_key + ";");
		//
		//		con.Query("COPY student TO 's3://parquets/student.parquet';");
		//		con.Query("COPY department TO 's3://parquets/department.parquet';");
		//		con.Query("COPY room TO 's3://parquets/room.parquet';");
		//		con.Query("COPY type TO 's3://parquets/type.parquet';");
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
	auto &scheduler = duckdb::ThreadScheduler::Get();
	using VecStr = std::vector<std::string>;

	// GenDatabase(con);

	// ---------------------------------------- Load Data --------------------------------------------------
	{
		// loading table into memory, using the temp table (so that we are sure the data is in memory, even if DuckDB is
		// not in in-memory mode.)
		{
			con.Query("CREATE TEMPORARY TABLE student AS SELECT * FROM read_parquet('student.parquet');");
			con.Query("CREATE TEMPORARY TABLE department AS SELECT * FROM read_parquet('department.parquet');");
			con.Query("CREATE TEMPORARY TABLE room AS SELECT * FROM read_parquet('room.parquet');");
			con.Query("CREATE TEMPORARY TABLE type AS SELECT * FROM read_parquet('type.parquet');");
		}

		// Or, leave tables in disk, we create the views
		{
			//			con.Query("CREATE VIEW student AS SELECT * FROM read_parquet('student.parquet');");
			//			con.Query("CREATE VIEW department AS SELECT * FROM read_parquet('department.parquet');");
			//			con.Query("CREATE VIEW room AS SELECT * FROM read_parquet('room.parquet');");
			//			con.Query("CREATE VIEW type AS SELECT * FROM read_parquet('type.parquet');");
		}

		// Or, leave tables in S3, we create the views
		{
			//			con.Query("SET s3_region='ap-southeast-1';");
			//			con.Query("SET s3_access_key_id=" + s3_access_key_id + ";");
			//			con.Query("SET s3_secret_access_key=" + s3_access_key + ";");
			//
			//			con.Query("CREATE VIEW student AS SELECT * FROM
			// read_parquet('s3://parquets/a_student.parquet');"); 			con.Query("CREATE VIEW room AS SELECT * FROM
			// read_parquet('s3://parquets/b_room.parquet');"); 			con.Query("CREATE VIEW department AS SELECT
			// * FROM read_parquet('s3://parquets/department.parquet');"); 			con.Query("CREATE VIEW type AS
			// SELECT * FROM read_parquet('s3://parquets/type.parquet');");
		}
		duckdb::BeeProfiler::Get().Clear();
	}

	// ------------------------------------ DuckDB Settings -------------------------------------------------
	{
		// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
		con.Query("SET threads TO 102;");

		// set the allocator flush threshold
		auto res = con.Query("SET allocator_flush_threshold=\"300gb\"; ");

		// disable the object cache
		con.Query("PRAGMA disable_object_cache;");
	}

	// ------------------------------------ Threads Settings -----------------------------------------------
	{
		scheduler.SetThreadSetting(64, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"EXPLAIN_ANALYZE"}, true);
		// [HashJoin]
		{
			scheduler.SetThreadSetting(16, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(32, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
		}
		// [Sort-Merge Join]
		{
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"}, true);
			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"PIECEWISE_MERGE_JOIN"}, false);
			scheduler.SetThreadSetting(16, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
		}
		// [BREAKER]
		{
			scheduler.SetThreadSetting(64, VecStr {"BREAKER"}, VecStr {""});
			scheduler.SetThreadSetting(32, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"BREAKER"});
			scheduler.SetThreadSetting(32, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, true);
		}
		//		// [AsOf Join]
		//		{
		//			scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {""});
		//			scheduler.SetThreadSetting(32, VecStr {""}, VecStr {"ASOF_JOIN"});
		//			scheduler.SetThreadSetting(32, VecStr {"ASOF_JOIN"}, VecStr {"EXPLAIN_ANALYZE"});
		//		}
		//		// [IE Join]
		//		{
		//			scheduler.SetThreadSetting(16, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"IE_JOIN"});
		//			scheduler.SetThreadSetting(16, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
		//			scheduler.SetThreadSetting(16, VecStr {"IE_JOIN"}, VecStr {"IE_JOIN", "BREAKER"});
		//			scheduler.SetThreadSetting(16, VecStr {"RANGE_JOIN_MERGE"}, VecStr {"RANGE_JOIN_MERGE"}, false);
		//
		//			scheduler.SetThreadSetting(32, VecStr {"IE_JOIN"}, VecStr {"EXPLAIN_ANALYZE"});
		//		}
	}

	// ------------------------------------------ Query -----------------------------------------------------
	// Hash Join & Sort-Merge Join
	{
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

			// ExecuteQuery(con, bushy_query, 3, 1);
		}

		// SEQ join query
		{
			std::string query =
			    "EXPLAIN ANALYZE "
			    "SELECT student.stu_id, department.major_id, room.room_id, type.type "
			    "FROM student, department, room, type "
			    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id AND room.type = "
			    "type.type;";

			ExecuteQuery(con, query, 3, 1);
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
			    "AND room.type >= type.type AND room.type <= type.type;";

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
			    "WHERE t1.stu_id >= t2.stu_id AND t1.stu_id <= t2.stu_id;";

			// ExecuteQuery(con, bushy_query, 2, 1);
		}
	}

	return 0;
}
