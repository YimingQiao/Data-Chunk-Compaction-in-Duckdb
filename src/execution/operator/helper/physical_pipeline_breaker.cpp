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
	explicit PipelineBreakerLocalState(vector<LogicalType> types, ClientContext &context) {
		// intermediate_table = make_uniq<ColumnDataCollection>(allocator, types);
		intermediate_table = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		intermediate_table->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> intermediate_table;
	ColumnDataAppendState append_state;
};

duckdb::SinkResultType PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context, duckdb::DataChunk &chunk,
                                                     duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalState>();

	Profiler profiler;
	profiler.Start();
	{ lstate.intermediate_table->Append(lstate.append_state, chunk); }
	BeeProfiler::Get().InsertRecord("[PhysicalPipelineBreaker::Sink] append", profiler.Elapsed());
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

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPipelineBreaker::Finalize(duckdb::Pipeline &pipeline, duckdb::Event &event,
                                                   duckdb::ClientContext &context,
                                                   duckdb::OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PipelineBreakerLocalState>(types, context.client);
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

	Profiler profiler;
	profiler.Start();

	std::lock_guard<std::mutex> lock(sink.glock);
	sink.intermediate_table->Scan(sink.scan_state, chunk);

	BeeProfiler::Get().InsertRecord("[PhysicalPipelineBreaker::GetData] scan", profiler.Elapsed());

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}
}  // namespace duckdb
