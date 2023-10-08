#include "duckdb/execution//operator/helper/physical_pipeline_breaker.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPipelineBreaker &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(op.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	auto types = plan->types;
	auto estimated_cardinality = op.estimated_cardinality;
	auto breaker = make_uniq<PhysicalPipelineBreaker>(std::move(plan), types, estimated_cardinality);
	plan = std::move(breaker);
	return plan;
}

}  // namespace duckdb