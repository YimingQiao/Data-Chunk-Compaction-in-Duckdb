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

int main() {
	std::string db_name = "./imdb.db";
	// std::string db_name = "third_party/imdb/data/imdb.db";
	// nullptr means in-memory database.
	duckdb::DuckDB db(db_name);
	IMDBDatabase imdb(db);

	// ------------------------------------ Threads Settings -----------------------------------------------
	{
		auto &scheduler = duckdb::ThreadScheduler::Get();
		using VecStr = std::vector<std::string>;
		// [HashJoin]
		{
			// Build Hash Table
			scheduler.SetThreadSetting(1, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, false);
			scheduler.SetThreadSetting(1, VecStr {"HT_FINALIZE"}, VecStr {"HT_FINALIZE"}, false);
			// Probe Hash Table
			scheduler.SetThreadSetting(1, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"EXPLAIN_ANALYZE"}, true);
		}
		// [BREAKER]
		{
			scheduler.SetThreadSetting(1, VecStr {"BREAKER"}, VecStr {""});
			scheduler.SetThreadSetting(1, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"BREAKER"});
			scheduler.SetThreadSetting(1, VecStr {"SEQ_SCAN ", "READ_PARQUET "}, VecStr {"HASH_JOIN"}, true);
		}

		scheduler.SetThreadSetting(0, "CompactTuner", "CompactTuner");
	}

	imdb.Query("SET threads TO 1;", nullptr, false);

	// ------------------------------------ Execution -----------------------------------------------
	std::vector<size_t> query_id(114);
	for (size_t i = 0; i < query_id.size(); ++i)
		query_id[i] = i + 1;
	double time;
	for (auto id : query_id) {
		std::string query = imdb::get_query(id);
		auto result = imdb.Query(query, &time, false);

		std::cerr << "-------------------------\n";
		std::cerr << "Query " << id << " time: " << time << " s\n";
		std::cerr << result->ToString() << "\n";
		duckdb::HashJoinProfiler::Get().PrintProfile();
		duckdb::HashJoinProfiler::Get().Clear();
	}

	return 0;
}
