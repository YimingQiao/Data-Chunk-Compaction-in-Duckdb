#include "duckdb/optimizer/split_long_pipeline.hpp"

#include "duckdb/planner/operator/logical_pipeline_breaker.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> SplitPipelineOptimizer::Rewrite(unique_ptr<duckdb::LogicalOperator> op) {
	switch (op->type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			num_left_joins += 1;

			// if the number of left joins is 2 or more, we break the pipeline to ensure hash table can be cached.
			if (num_left_joins >= 7 && op->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
				auto breaker = make_uniq<LogicalPipelineBreaker>();
				breaker->children.push_back(move(op->children[0]));
				op->children[0] = move(breaker);
				num_left_joins = 0;
			}
			op->children[0] = Rewrite(move(op->children[0]));

			// reset the counter for the right side of the join
			num_left_joins = 0;
			op->children[1] = Rewrite(move(op->children[1]));
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