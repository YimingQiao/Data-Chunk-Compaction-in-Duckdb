#include "duckdb/optimizer/bushy_join_order/bushy_order_optimizer.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> BushyOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	return plan;
}
}  // namespace duckdb