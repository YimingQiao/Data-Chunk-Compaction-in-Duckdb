#include <iostream>

#include "duckdb.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "third_party/imdb/include/imdb.hpp"

class IMDBDatabase {
public:
	explicit IMDBDatabase(duckdb::DuckDB db) : connection_(db), client_(db.instance), config_() {
		auto result = Query("SHOW TABLES", nullptr, false);
		if (result->RowCount() == 0) {
			std::cout << "Database is empty, importing data...\n";
			imdb::dbgen(db);
			std::cout << "Database imported.\n";
		}
		std::cout << "Database Loaded.\n";
	}

	std::unique_ptr<duckdb::MaterializedQueryResult> Query(const std::string &query, double *time, bool print = true) {
		// Measure the time.
		profiler_.Start();
		auto result = connection_.Query(query);
		if (time != nullptr) *time = profiler_.Elapsed();

		if (!result->HasError()) {
			if (print) {
				std::cout << result->ToBox(client_, config_) << "\n";
				std::cout << ExplainQuery(query);
			}
			return std::move(result);
		} else {
			std::cerr << result->GetError() << "\n";
			return nullptr;
		}
	}

private:
	duckdb::Connection connection_;

	// For drawing the query result.
	duckdb::ClientContext client_;
	duckdb::BoxRendererConfig config_;

	duckdb::Profiler profiler_;

	std::string ExplainQuery(const std::string &query) {
		auto result = connection_.Query("EXPLAIN ANALYZE " + query);
		std::string plan = result->GetValue(1, 0).ToString();

		return plan;
	}
};

void IMDBFinder(IMDBDatabase &imdb);

int main() {
	std::string db_name = "third_party/imdb/data/imdb.db";
	// nullptr means in-memory database.
	duckdb::DuckDB db(db_name);
	IMDBDatabase imdb(db);

	imdb.Query("SET threads TO 64;", nullptr, false);
	std::vector<size_t> query_id = {1};
	double time;
	for (auto id : query_id) {
		std::string query = imdb::get_query(id);
		imdb.Query(query, &time, false);
		std::cout << "Query " << id << " time: " << time << " s\n";
	}

	return 0;
}

void IMDBFinder(IMDBDatabase &imdb) {  // Number of Queries
	int num_queries = 114;

	std::vector<uint32_t> interesting_queries;
	for (size_t i = 1; i <= num_queries; ++i)
		interesting_queries.push_back(i);

	double time;
	std::vector<double> single_thread_times;
	std::vector<double> multi_thread_times;
	std::vector<size_t> promising_queries;

	for (auto i : interesting_queries) {
		std::string query = imdb::get_query(i);
		std::cout << "----------------------------------------------------------- " + std::to_string(i) +
		                 " -----------------------------------------------------------\n";

		// warm up
		imdb.Query(query, nullptr, false);

		// single thread
		imdb.Query("SET threads TO 1;", nullptr, false);
		imdb.Query(query, &time, false);
		single_thread_times.push_back(time);

		// multi thread
		imdb.Query("SET threads TO 16;", nullptr, false);
		imdb.Query(query, &time, false);
		multi_thread_times.push_back(time);

		if ((single_thread_times.back() - multi_thread_times.back()) > 0.05) {
			promising_queries.push_back(i);
			std::cout << "Query " << i << " single thread time: " << single_thread_times.back() << " s\t"
			          << " multi thread time: " << multi_thread_times.back() << " s\t"
			          << " Promising\n";
		} else {
			std::cout << "Query " << i << " single thread time: " << single_thread_times.back() << " s\t"
			          << " multi thread time: " << multi_thread_times.back() << " s\n";
		}
	}

	for (auto i : promising_queries) {
		std::cout << "----------------------------------------------------------- Promising queries " +
		                 std::to_string(i) + "-----------------------------------------------------------\n";
		std::string query = imdb::get_query(i);
		std::cout << query << "\n";

		// warm up
		imdb.Query(query, nullptr, false);

		imdb.Query("SET threads TO 1;", nullptr, false);
		imdb.Query(query, nullptr, true);

		imdb.Query("SET threads TO 16;", nullptr, false);
		imdb.Query(query, nullptr, true);
	}
}
