#include <iostream>

#include "duckdb.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/optimizer/thread_scheduler.hpp"
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
		profiler_.End();
		if (time) *time = profiler_.Elapsed();

		if (!result->HasError()) {
			if (print) {
				std::cerr << result->ToBox(client_, config_) << "\n";
				std::cerr << ExplainQuery(query);
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

int main() {
	// std::string db_name = "./imdb.db";
	std::string db_name = "third_party/imdb/data/imdb.db";
	// nullptr means in-memory database.
	duckdb::DuckDB db(db_name);
	IMDBDatabase imdb(db);

	// ------------------------------------ Threads Settings -----------------------------------------------
	imdb.Query("SET threads TO 1;", nullptr, false);

	// ------------------------------------ Execution -----------------------------------------------
	std::vector<size_t> query_id(114);
	for (size_t i = 0; i < query_id.size(); ++i)
		query_id[i] = i + 1;
	double time;
	for (auto id : query_id) {
		std::cerr << "-------------------------\n";
		std::string query = imdb::get_query(id);
		for (size_t i = 0; i < 6; ++i) {
			auto result = imdb.Query(query, &time, false);

			if (i == 0)
				std::cerr << "Result: " << result->GetValue(0, 0) << "\n";
			else
				std::cerr << "Query: " << id << " Time: " << time << "s\n";

			duckdb::HashJoinProfiler::Get().PrintProfile();
			duckdb::HashJoinProfiler::Get().Clear();
		}
	}

	return 0;
}
