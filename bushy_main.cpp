#include <iostream>
#include <random>

#include "duckdb.hpp"

int main() {
	std::string db_name = "student.db";
	// nullptr means in-memory database.
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	//	std::vector<std::string> sql_create = {"CREATE TABLE student (stu_id INTEGER, major_id INTEGER);",
	//	                                       "CREATE TABLE department(major_id INTEGER, name VARCHAR);"
	//	                                       "CREATE TABLE room (room_id INTEGER, stu_id INTEGER, type INTEGER);"
	//	                                       "CREATE TABLE type (type INTEGER, info VARCHAR);"};
	//
	//	for (const auto &sql : sql_create) {
	//		con.Query(sql);
	//	}
	//
	//	// database setting
	//	uint64_t stu_n = 5 * 1e7;
	//	uint64_t major_n = 5 * 1e6;
	//	uint64_t room_n = 5 * 1e6;
	//
	//	// random generator
	//	std::mt19937 mt(42);
	//
	//	// drop previous data
	//	con.Query("DELETE FROM student; DELETE FROM department; DELETE FROM room; DELETE FROM type;");
	//
	//	// insert into student, use mt to generate tuples
	//	{
	//		std::string sql_insert = "INSERT INTO student VALUES ";
	//		for (uint64_t i = 0; i < stu_n; i++) {
	//			uint64_t major_id = mt() % major_n;
	//			sql_insert += "(" + std::to_string(i) + ", " + std::to_string(major_id) + ")";
	//			if (i != stu_n - 1) sql_insert += ", ";
	//
	//			if (i % (5 * stu_n / 100) == 0) {
	//				con.Query(sql_insert + ";");
	//				sql_insert = "INSERT INTO student VALUES ";
	//				std::cout << "Student Table Batch " + std::to_string(double(i) / stu_n * 100) + "%" + " inserted\n";
	//			}
	//		}
	//		if (sql_insert != "INSERT INTO student VALUES ") {
	//			con.Query(sql_insert + ";");
	//		}
	//		std::cout << "Student table inserted\n";
	//	}
	//	// insert into department, use majors
	//	{
	//		std::string sql_insert = "INSERT INTO department VALUES ";
	//		for (uint64_t i = 0; i < major_n; i++) {
	//			sql_insert += "(" + std::to_string(i) + ", '" + "major_" + std::to_string(i) + "')";
	//			if (i != major_n - 1) sql_insert += ", ";
	//
	//			if (i % (5 * major_n / 100) == 0) {
	//				con.Query(sql_insert + ";");
	//				sql_insert = "INSERT INTO department VALUES ";
	//				std::cout << "Department Table Batch " + std::to_string(double(i) / major_n * 100) + "%" +
	//				                 " inserted\n";
	//			}
	//		}
	//		if (sql_insert != "INSERT INTO department VALUES ") {
	//			con.Query(sql_insert + ";");
	//		}
	//		std::cout << "Department table inserted\n";
	//	}
	//	// insert into room, use mt to generate tuples, each student has a room
	//	{
	//		std::string sql_insert = "INSERT INTO room VALUES ";
	//		for (uint64_t i = 0; i < stu_n; i++) {
	//			uint64_t type = mt() % room_n;
	//			sql_insert += "(" + std::to_string(i) + ", " + std::to_string(i) + ", " + std::to_string(type) + ")";
	//			if (i != stu_n - 1) sql_insert += ", ";
	//
	//			if (i % (5 * stu_n / 100) == 0) {
	//				con.Query(sql_insert + ";");
	//				sql_insert = "INSERT INTO room VALUES ";
	//				std::cout << "Room Table Batch " + std::to_string(double(i) / stu_n * 100) + "%" + " inserted\n";
	//			}
	//		}
	//		if (sql_insert != "INSERT INTO room VALUES ") {
	//			con.Query(sql_insert + ";");
	//		}
	//		std::cout << "Room table inserted\n";
	//	}
	//
	//	// insert into type, use room_types
	//	{
	//		std::string sql_insert = "INSERT INTO type VALUES ";
	//		for (uint64_t i = 0; i < room_n; i++) {
	//			sql_insert += "(" + std::to_string(i) + ", '" + "room_type_" + std::to_string(i) + "')";
	//			if (i != room_n - 1) sql_insert += ", ";
	//
	//			if (i % (5 * room_n / 100) == 0) {
	//				con.Query(sql_insert + ";");
	//				sql_insert = "INSERT INTO type VALUES ";
	//				std::cout << "Type Table Batch " + std::to_string(double(i) / room_n * 100) + "%" + " inserted\n";
	//			}
	//		}
	//		if (sql_insert != "INSERT INTO type VALUES ") {
	//			con.Query(sql_insert + ";");
	//		}
	//		std::cout << "Type table inserted\n";
	//	}

	// set num of thread, we cannot use 128 threads because 2 threads are left for Perf.
	{ con.Query("SET threads TO 128;"); }

	// SEQ join query
	//	{
	//		std::string seq_sql_join =
	//		    "EXPLAIN ANALYZE "
	//		    "SELECT student.stu_id, department.name, room.type, type.info FROM student, department, room, type "
	//		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id "
	//		    "AND room.type = type.type;";
	//		auto result = con.Query(seq_sql_join);
	//		if (!result->HasError()) {
	//			std::string plan = result->GetValue(1, 0).ToString();
	//			std::cerr << plan << "\n";
	//			// std::cerr << result->ToString() << "\n";
	//		} else {
	//			std::cerr << result->GetError() << "\n";
	//		}
	//	}

	// BUSHY join query
	//	{
	//		std::string bushy_sql_join =
	//		    "EXPLAIN ANALYZE "
	//		    "SELECT t2.stu_id, t2.name, t1.type, t1.info "
	//		    "FROM "
	//		    "(SELECT room.stu_id, room.type, type.info "
	//		    " FROM room INNER JOIN type ON room.type = type.type) AS t1,"
	//		    "(SELECT student.stu_id, department.name "
	//		    " FROM student, department WHERE student.major_id = department.major_id) AS t2,"
	//		    "WHERE t1.stu_id = t2.stu_id;";
	//		for (size_t i = 0; i < 1; ++i) {
	//			auto result = con.Query(bushy_sql_join);
	//			if (!result->HasError()) {
	//				std::string plan = result->GetValue(1, 0).ToString();
	//				std::cerr << plan << "\n";
	//				// std::cerr << result->ToString() << "\n";
	//			} else {
	//				std::cerr << result->GetError() << "\n";
	//			}
	//		}
	//	}

	// Left-deep join Bushy
	{
		std::string left_deep =
		    "(SELECT student.stu_id, department.name, room.type, type.info FROM student, department, room, type "
		    "WHERE student.stu_id = room.stu_id AND student.major_id = department.major_id "
		    "AND room.type = type.type)";
		std::string bushy =
		    "(SELECT t2.stu_id, t2.name, t1.type, t1.info "
		    "FROM "
		    "(SELECT room.stu_id, room.type, type.info "
		    " FROM room INNER JOIN type ON room.type = type.type) AS t1,"
		    "(SELECT student.stu_id, department.name "
		    " FROM student, department WHERE student.major_id = department.major_id) AS t2,"
		    "WHERE t1.stu_id = t2.stu_id ORDER BY t1.stu_id LIMIT 1)";

		//		std::string complex_join = "EXPLAIN ANALYZE SELECT * FROM " + left_deep + " AS ld INNER JOIN " + bushy +
		//		                           " AS b ON ld.stu_id = b.stu_id";

		for (size_t i = 0; i < 3; i++) {
			left_deep = "(SELECT * FROM " + left_deep + " AS ls INNER JOIN student ON ls.stu_id = student.stu_id)";
		}
		left_deep = "(SELECT * FROM " + left_deep +
		            " AS ls INNER JOIN student ON ls.stu_id = student.stu_id WHERE student.major_id <= 500000)";

		std::string left_side = "(SELECT * FROM " + left_deep + " AS t)";
		std::string right_side = "(SELECT * FROM " + left_deep + " ORDER BY stu_id LIMIT 1)";
		std::string complex_join = "EXPLAIN ANALYZE SELECT * FROM " + left_side + " AS ls INNER JOIN " + right_side +
		                           " AS rs ON ls.stu_id = rs.stu_id;";

		auto result = con.Query(complex_join);
		if (!result->HasError()) {
			std::string plan = result->GetValue(1, 0).ToString();
			std::cerr << plan << "\n";
			// std::cerr << result->ToString() << "\n";
		} else {
			std::cerr << result->GetError() << "\n";
		}
	}

	return 0;
}