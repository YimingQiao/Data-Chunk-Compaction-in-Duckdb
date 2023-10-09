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

	DataChunk gchunk;
};

class PipelineBreakerLocalSinkState : public LocalSinkState {
public:
	vector<unique_ptr<DataChunk>> chunks;

	idx_t sink_times = 0;
	idx_t sink_time_us = 0;

	DataChunk lchunk;
};

duckdb::SinkResultType duckdb::PhysicalPipelineBreaker::Sink(duckdb::ExecutionContext &context,
                                                             duckdb::DataChunk &chunk,
                                                             duckdb::OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSinkState>();

	auto start_time = std::chrono::high_resolution_clock::now();

	lstate.chunks.push_back(make_uniq<DataChunk>());
	auto &stored_chunk = lstate.chunks.back();
	stored_chunk->Move(chunk);
	chunk.Initialize(Allocator::DefaultAllocator(), stored_chunk->GetTypes());

	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
	lstate.sink_time_us += duration.count();
	lstate.sink_times++;

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
	gstate.chunks.reserve(gstate.chunks.size() + lstate.chunks.size());
	gstate.chunks.insert(gstate.chunks.end(), std::make_move_iterator(lstate.chunks.begin()),
	                     std::make_move_iterator(lstate.chunks.end()));

	auto now = std::chrono::system_clock::now();
	auto duration = now.time_since_epoch();
	auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
	auto avg_sink_time = lstate.sink_time_us / lstate.sink_times;

	std::cerr << "[Breaker Sink Combine] time: " + std::to_string(milliseconds) +
	                 "\tAverage Chunk Initialization Time: " + std::to_string(avg_sink_time) + " us\n";

	return SinkCombineResultType::FINISHED;
}

unique_ptr<GlobalSinkState> PhysicalPipelineBreaker::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<PipelineBreakerGlobalSinkState>();
	state->context = context.shared_from_this();
	state->gchunk.Initialize(Allocator::DefaultAllocator(), types);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<PipelineBreakerLocalSinkState>();
	state->lchunk.Initialize(Allocator::DefaultAllocator(), types);
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
