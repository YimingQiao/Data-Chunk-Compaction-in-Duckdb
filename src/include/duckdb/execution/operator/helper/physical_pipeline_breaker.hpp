//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

// [It is not a good idea.] A DataChunk pool to store the intermediate results of the pipeline breaker
class ChunkPool {
public:
	// The size of the pool, sizeof(tuple) is about 40 bytes, one chunk has 2048 tuples, so 40 * 2048 * 2**16 = 5GB
	static constexpr const idx_t POOL_SIZE = 1 << 16;
	static constexpr const idx_t NUM_THREADS = 6;
	static constexpr const idx_t NUM_CHUNKS_PER_THREAD = POOL_SIZE / NUM_THREADS;

public:
	explicit ChunkPool(vector<LogicalType> types) : types(types), thread_idx(0), chunk_pools(POOL_SIZE) {
		chunk_pools.resize(NUM_THREADS);
		for (idx_t i = 0; i < NUM_THREADS; i++) {
			auto &chunks = chunk_pools[i];
			chunks.resize(NUM_CHUNKS_PER_THREAD);
			for (idx_t j = 0; j < NUM_CHUNKS_PER_THREAD; j++) {
				chunks[j] = make_uniq<DataChunk>();
				chunks[j]->Initialize(Allocator::DefaultAllocator(), types);
			}
		}
	}

	vector<unique_ptr<DataChunk>> *Chunks() {
		idx_t idx = thread_idx.fetch_add(1);
		if (idx >= NUM_THREADS) {
			std::cerr << "[ChunkPool] ChunkPool is Empty!\n";
			return nullptr;
		}

		return &chunk_pools[idx];
	}

private:
	vector<LogicalType> types;

	std::atomic<idx_t> thread_idx;
	vector<vector<unique_ptr<DataChunk>>> chunk_pools;
};

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
