//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/bushy_join_order/bushy_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class BushyOrderOptimizer {
public:
	explicit BushyOrderOptimizer(ClientContext &context) : context(context) {
	}

	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	ClientContext &context;
};

}  // namespace duckdb
