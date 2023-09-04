#include <iostream>

#include "duckdb/common/box_renderer.hpp"
#include "third_party/imdb/include/imdb.hpp"

class IMDB {
public:
	explicit IMDB(duckdb::DuckDB db) : connection_(db), client_(db.instance), config_() {
		auto result = Query("SHOW TABLES", false);
		if (result->RowCount() == 0) {
			std::cout << "Database is empty, importing data...\n";
			imdb::dbgen(db);
		}
	}

	std::unique_ptr<duckdb::MaterializedQueryResult> Query(const std::string &query, bool print = true) {
		auto result = connection_.Query(query);

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

	std::string ExplainQuery(const std::string &query) {
		auto result = connection_.Query("EXPLAIN ANALYZE " + query);
		std::string plan = result->GetValue(1, 0).ToString();

		return plan;
	}
};

int main() {
	std::string db_name = "./third_party/imdb/data/imdb.db";
	duckdb::DuckDB db(db_name);
	IMDB imdb(db);

	// IMDB queries, the query idx starts from 1.
	std::string query = imdb::get_query(1);
	std::cout << "Query: " << query << "\n";
	imdb.Query(query);

	return 0;
}