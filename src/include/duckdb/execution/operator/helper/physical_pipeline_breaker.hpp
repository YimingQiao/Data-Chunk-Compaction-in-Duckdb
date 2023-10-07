//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalPipelineBreaker represents a physical operator that is used to break up pipelines
class PhysicalPipelineBreaker : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PIPELINE_BREAKER;

public:
	PhysicalPipelineBreaker(unique_ptr<PhysicalOperator> join, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality) {
		children.push_back(std::move(join));
	};

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}
};
}  // namespace duckdb
