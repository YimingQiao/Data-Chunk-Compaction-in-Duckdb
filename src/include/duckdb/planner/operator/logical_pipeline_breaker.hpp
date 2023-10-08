//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalPipelineBreaker : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_BREAKER;

public:
	LogicalPipelineBreaker() : LogicalOperator(LogicalOperatorType::LOGICAL_BREAKER) {
	}

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

}  // namespace duckdb
