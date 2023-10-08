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
	explicit BushyOrderOptimizer(ClientContext &context) : context(context), can_break(false) {
	}

	//! Perform pipeline breaker
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

protected:
private:
	ClientContext &context;

	bool can_break = false;
};

}  // namespace duckdb
