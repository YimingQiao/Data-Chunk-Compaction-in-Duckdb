//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

//! The profiler can be used to measure elapsed time
template <typename T>
class BaseProfiler {
public:
	//! Starts the timer
	void Start() {
		finished = false;
		start = Tick();
	}
	//! Finishes timing
	void End() {
		end = Tick();
		finished = true;
	}

	//! Returns the elapsed time in seconds. If End() has been called, returns
	//! the total elapsed time. Otherwise returns how far along the timer is
	//! right now.
	double Elapsed() const {
		auto _end = finished ? end : Tick();
		return std::chrono::duration_cast<std::chrono::duration<double>>(_end - start).count();
	}

private:
	time_point<T> Tick() const {
		return T::now();
	}
	time_point<T> start;
	time_point<T> end;
	bool finished = false;
};

using Profiler = BaseProfiler<system_clock>;

// Singleton class to aggregate results from all SpikeProfiler instances
class ResultAggregator {
public:
	static ResultAggregator &GetInstance() {
		static ResultAggregator instance;
		return instance;
	}

	void AddResult(const std::string &name, double time, size_t calling_times) {
		std::lock_guard<std::mutex> lock(mtx);
		if (agg_times.count(name) == 0) {
			agg_times[name] = time;
			agg_calling_times[name] = calling_times;
		} else {
			agg_times[name] += time;
			agg_calling_times[name] += calling_times;
		}
	}

	~ResultAggregator() {
		PrintResults();
	}

	void PrintResults() const {
		// Extract keys from the unordered_map and store in a vector
		std::vector<std::string> keys;
		for (const auto &pair : agg_times) {
			keys.push_back(pair.first);
		}

		// Sort the keys
		std::sort(keys.begin(), keys.end());

		// Print the results in alphabetical order
		for (const auto &key : keys) {
			double time = agg_times.at(key);
			size_t calling_times = agg_calling_times.at(key);
			double avg = time / calling_times;

			std::cout << "Total: " << time << " s\tCalls: " << calling_times << "\tAvg: " << avg << " s\t" << key
			          << '\n';
		}
	}

private:
	unordered_map<std::string, double> agg_times;
	unordered_map<string, size_t> agg_calling_times;
	mutable std::mutex mtx;

	// Private constructor
	ResultAggregator() {
	}
};

class SpikeProfiler {
public:
	SpikeProfiler() {};

	void StartTiming(const std::string &name) {
		profiler.Start();
		// Assuming you don't start the same timer without stopping it
	}

	void EndTiming(const std::string &name) {
		double elapsedTime = profiler.Elapsed();

		if (times.count(name) == 0) {
			times[name] = elapsedTime;
			calling_times[name] = 1;
		} else {
			times[name] += elapsedTime;
			calling_times[name]++;
		}
	}

	~SpikeProfiler() {
		for (const auto &pair : times) {
			auto &key = pair.first;
			double time = pair.second;
			size_t calling = calling_times[key];

			ResultAggregator::GetInstance().AddResult(key, time, calling);
		}
	}

private:
	Profiler profiler;
	unordered_map<string, double> times;
	unordered_map<string, size_t> calling_times;
};

class BeeProfiler {
public:
	static BeeProfiler &Get() {
		static BeeProfiler instance;
		return instance;
	}

	void InsertRecord(string name, double time) {
		std::lock_guard<std::mutex> lock(mtx);
		if (times_.count(name) == 0) {
			times_[name] = time;
			calling_times_[name] = 1;
		} else {
			times_[name] += time;
			calling_times_[name] += 1;
		}
	}

	void EndProfiling() {
		PrintResults();
		Clear();
	}

	void PrintResults() const {
		// Extract keys from the unordered_map and store in a vector
		std::vector<std::string> keys;
		for (const auto &pair : times_) {
			keys.push_back(pair.first);
		}

		// Sort the keys
		std::sort(keys.begin(), keys.end());

		// Print the results in alphabetical order
		for (const auto &key : keys) {
			if (key.find("TableScan") != std::string::npos && key.find("in_mem") == std::string::npos) {
				continue;
			}
			double time = times_.at(key);
			size_t calling_times = calling_times_.at(key);
			double avg = time / calling_times;

			std::cerr << "Total: " << time << " s\tCalls: " << calling_times << "\tAvg: " << avg << " s\t" << key
			          << '\n';
		}
	}

	void Clear() {
		std::lock_guard<std::mutex> lock(mtx);
		times_.clear();
		calling_times_.clear();
	}

private:
	unordered_map<std::string, double> times_;
	unordered_map<string, size_t> calling_times_;
	mutable std::mutex mtx;
};
}  // namespace duckdb
