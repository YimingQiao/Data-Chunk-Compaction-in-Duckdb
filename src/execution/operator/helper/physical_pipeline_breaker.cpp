#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSinkState : public GlobalSinkState {
public:
	mutex glock;
	vector<unique_ptr<DataChunk>> chunks;
	shared_ptr<ClientContext> context;
};

class PipelineBreakerLocalSinkState : public LocalSinkState {
public:
	vector<unique_ptr<DataChunk>> chunks;
};

duckdb::SinkResultType duckdb::PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context,
                                                             duckdb::DataChunk &chunk,
                                                             duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSinkState>();

	lstate.chunks.push_back(make_uniq<DataChunk>());
	auto &stored_chunk = lstate.chunks.back();
	stored_chunk->Move(chunk);
	chunk.Initialize(Allocator::DefaultAllocator(), stored_chunk->GetTypes());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSinkState>();

	if (lstate.chunks.empty()) {
		return SinkCombineResultType::FINISHED;
	}

	lock_guard<mutex> l(gstate.glock);
	std::move(std::begin(lstate.chunks), std::end(lstate.chunks), std::back_inserter(gstate.chunks));

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<PipelineBreakerGlobalSinkState>();
	state->context = context.shared_from_this();
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<PipelineBreakerLocalSinkState>();
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSourceState : public GlobalSourceState {
public:
	explicit PipelineBreakerGlobalSourceState(const PhysicalPipelineBreaker &op) : chunk_idx(0), max_threads(0) {
		auto &sink = op.sink_state->Cast<PipelineBreakerGlobalSinkState>();
		// max_threads = sink.chunks.size() / 512;
		max_threads = 1;
	}

	std::atomic<size_t> chunk_idx;
	idx_t max_threads;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class PipelineBreakerLocalSourceState : public LocalSourceState {
public:
};

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<PipelineBreakerGlobalSourceState>();
	auto &sink = sink_state->Cast<PipelineBreakerGlobalSinkState>();

	size_t idx = gstate.chunk_idx.fetch_add(1);
	if (idx >= sink.chunks.size()) {
		return SourceResultType::FINISHED;
	}

	chunk.Move(*sink.chunks[idx]);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalPipelineBreaker::GetLocalSourceState(ExecutionContext &context,
                                                                          GlobalSourceState &gstate) const {
	return make_uniq<PipelineBreakerLocalSourceState>();
}

}  // namespace duckdb
