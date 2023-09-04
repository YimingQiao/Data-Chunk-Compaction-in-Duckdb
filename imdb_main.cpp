#include "third_party/imdb/include/imdb.hpp"

#include <iostream>

int main() {
	std::string db_name = "./third_party/imdb/data/imdb.db";
	duckdb::DuckDB db(db_name);
	duckdb::Connection con(db);

	auto result = con.Query("Show tables;");
	if (!result->HasError()) {
		result->Print();
	} else {
		std::cout << result->GetError() << "\n";
	}

	return 0;
}