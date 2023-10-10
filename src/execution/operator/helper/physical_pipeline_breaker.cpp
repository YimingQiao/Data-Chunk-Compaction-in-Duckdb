#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

#include "../extension/jemalloc/include/jemalloc_extension.hpp"

namespace duckdb {

PhysicalPipelineBreaker::PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> join,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality) {
	children.push_back(std::move(join));
}

PhysicalPipelineBreaker::~PhysicalPipelineBreaker() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalState : public GlobalSinkState {
public:
	std::mutex glock;
	unique_ptr<ColumnDataCollection> intermediate_table;
	ColumnDataScanState scan_state;
	bool initialized = false;
};

class PipelineBreakerLocalState : public LocalSinkState {
public:
	explicit PipelineBreakerLocalState(vector<LogicalType> types) {
		intermediate_table = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		intermediate_table->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> intermediate_table;
	ColumnDataAppendState append_state;
	//	idx_t sink_times = 0;
	//	int64_t sink_time_us = 0;
};

duckdb::SinkResultType duckdb::PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context,
                                                             duckdb::DataChunk &chunk,
                                                             duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalState>();

	// auto start_time = std::chrono::high_resolution_clock::now();
	{ lstate.intermediate_table->Append(chunk); }
	//	auto end_time = std::chrono::high_resolution_clock::now();
	//	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
	//	lstate.sink_time_us += duration.count();
	//	lstate.sink_times++;

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalState>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalState>();

	if (lstate.intermediate_table->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.intermediate_table) {
		gstate.intermediate_table = std::move(lstate.intermediate_table);
	} else {
		gstate.intermediate_table->Combine(*lstate.intermediate_table);
	}

	//	auto now = std::chrono::system_clock::now();
	//	auto duration = now.time_since_epoch();
	//	auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
	//	auto avg_sink_time = lstate.sink_time_us / lstate.sink_times;
	//	std::cerr << "[Breaker Sink Combine] time: " + std::to_string(milliseconds) +
	//	                 "\tAverage Chunk Initialization Time: " + std::to_string(avg_sink_time) + " us\n";

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPipelineBreaker::Finalize(duckdb::Pipeline &pipeline, duckdb::Event &event,
                                                   duckdb::ClientContext &context,
                                                   duckdb::OperatorSinkFinalizeInput &input) const {
	// JemallocExtension::Print();
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PipelineBreakerLocalState>(types);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &sink = sink_state->Cast<PipelineBreakerGlobalState>();

	if (!sink.initialized) {
		sink.intermediate_table->InitializeScan(sink.scan_state);
		sink.initialized = true;
	}

	std::lock_guard<std::mutex> lock(sink.glock);
	sink.intermediate_table->Scan(sink.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}
}  // namespace duckdb
