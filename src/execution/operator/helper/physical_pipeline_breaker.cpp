#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSinkState : public GlobalSinkState {
public:
	mutex glock;
	unique_ptr<ColumnDataCollection> collection;
	shared_ptr<ClientContext> context;
};

class PipelineBreakerLocalSinkState : public LocalSinkState {
public:
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
};

duckdb::SinkResultType duckdb::PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context,
                                                             duckdb::DataChunk &chunk,
                                                             duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSinkState>();
	lstate.collection->Append(lstate.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSinkState>();
	if (lstate.collection->Count() == 0) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	if (!gstate.collection) {
		gstate.collection = move(lstate.collection);
	} else {
		gstate.collection->Combine(*lstate.collection);
	}

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<PipelineBreakerGlobalSinkState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<PipelineBreakerLocalSinkState>();
	state->collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	state->collection->InitializeAppend(state->append_state);
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSourceState : public GlobalSourceState {
public:
	explicit PipelineBreakerGlobalSourceState() : initialized(false) {
	}

	//! The current position in the scan
	ColumnDataParallelScanState global_scan_state;
	bool initialized;
};

class PipelineBreakerLocalSourceState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSourceState>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSourceState>();
	auto &sink = sink_state->Cast<PipelineBreakerGlobalSinkState>();

	// todo: this is not thread safe
	if (sink.collection->Count() == 0) {
		return SourceResultType::FINISHED;
	}
	if (!gstate.initialized) {
		sink.collection->InitializeScan(gstate.global_scan_state);
		gstate.initialized = true;
	}
	sink.collection->Scan(gstate.global_scan_state, lstate.local_scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalSourceState>();
}

unique_ptr<LocalSourceState> PhysicalPipelineBreaker::GetLocalSourceState(ExecutionContext &context,
                                                                          GlobalSourceState &gstate) const {
	return make_uniq<PipelineBreakerLocalSourceState>();
}

}  // namespace duckdb
