//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/split_long_pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class SplitPipelineOptimizer {
public:
	explicit SplitPipelineOptimizer(ClientContext &context) : context(context), num_left_joins(0) {
	}

	//! Perform splitting
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

protected:
private:
	ClientContext &context;

	size_t num_left_joins;
};

}  // namespace duckdb
