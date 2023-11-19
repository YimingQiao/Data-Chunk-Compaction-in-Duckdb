#include "duckdb/optimizer/bushy_join_order/bushy_order_optimizer.hpp"

#include "duckdb/planner/operator/logical_pipeline_breaker.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> BushyOrderOptimizer::Rewrite(unique_ptr<duckdb::LogicalOperator> op) {
	switch (op->type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			bool can_break_record = can_break;

			can_break = true;
			// if the RHS is a get, we do not break the pipeline
			if (op->children[1]->type == LogicalOperatorType::LOGICAL_GET) {
				can_break = false;
			} else if (op->children[1]->type == LogicalOperatorType::LOGICAL_PROJECTION &&
			           op->children[1]->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
				can_break = false;
			} else if (op->children[1]->type == LogicalOperatorType::LOGICAL_FILTER &&
			           op->children[1]->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
				can_break = false;
			}
			op->children[0] = Rewrite(move(op->children[0]));

			// the RHS does not break the pipeline
			can_break = false;
			op->children[1] = Rewrite(move(op->children[1]));

			// handle current node
			if (can_break_record) {
				auto breaker = make_uniq<LogicalPipelineBreaker>();
				breaker->children.push_back(move(op));
				op = move(breaker);
			}
			return op;
		}
		default: {
			for (auto &child : op->children) {
				child = Rewrite(move(child));
			}
			return op;
		}
	}
}
}  // namespace duckdb