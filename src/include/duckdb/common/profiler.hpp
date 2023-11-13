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

class BeeProfiler {
public:
	static BeeProfiler &Get() {
		static BeeProfiler instance;
		return instance;
	}

	void InsertStatRecord(string name, double value) {
		if (values_.find(name) == values_.end()) {
			values_[name] = value;
			calling_times_[name] = 1;
		} else {
			values_[name].fetch_add(value * 1e9, std::memory_order_relaxed);
			calling_times_[name].fetch_add(1, std::memory_order_relaxed);
		}
	}

	void InsertStatRecord(string name, size_t value) {
		if (values_.find(name) == values_.end()) {
			values_[name] = value;
			calling_times_[name] = 1;
		} else {
			values_[name].fetch_add(value, std::memory_order_relaxed);
			calling_times_[name].fetch_add(1, std::memory_order_relaxed);
		}
	}

	void InsertHTRecord(string name, size_t tuple_sz, size_t point_table_sz, size_t num_terms) {
		std::lock_guard<std::mutex> lock(mtx);
		if (ht_records_.count(name) == 0) {
			ht_records_[name] = HTInfo(tuple_sz, point_table_sz, num_terms);
		}
	}

	void EndProfiling() {
		PrintResults();
		Clear();
	}

	void PrintResults() const {
		// -------------------------------- Print Timing Results --------------------------------
		// Extract keys from the unordered_map and store in a vector
		std::vector<std::string> keys;
		for (const auto &pair : values_) {
			keys.push_back(pair.first);
		}

		// Sort the keys
		std::sort(keys.begin(), keys.end());

		// Print the results in alphabetical order
		std::cerr << "-------\n";
		for (const auto &key : keys) {
			if (key.find("TableScan") != std::string::npos && key.find("in_mem") == std::string::npos) {
				continue;
			}
			if (key.find("#Tuple") != std::string::npos) {
				continue;
			}
			double time = values_.at(key) / double(1e9);
			size_t calling_times = calling_times_.at(key);
			double avg = time / calling_times;

			std::cerr << "Total: " << time << " s\tCalls: " << calling_times << "\tAvg: " << avg << " s\t" << key
			          << '\n';
		}
		// -------------------------------- Print HT Results --------------------------------
		std::cerr << "-------\n";
		for (const auto &key : keys) {
			if (key.find("#Tuple") != std::string::npos) {
				size_t total_tuples = values_.at(key);
				size_t calling_times = calling_times_.at(key);
				double avg = total_tuples / double(calling_times);

				std::cerr << "Total: " << total_tuples << "\tCalls: " << calling_times << "\tAvg: " << avg << "\t"
				          << key << '\n';
			}
		}

		// -------------------------------- Print HT Results --------------------------------
		std::vector<string> ht_keys;
		for (const auto &pair : ht_records_) {
			ht_keys.push_back(pair.first);
		}

		std::sort(ht_keys.begin(), ht_keys.end());

		std::cerr << "-------\n";
		for (const auto &key : ht_keys) {
			auto ht_info = ht_records_.at(key);
			std::cerr << "Tuples Size: " << (double)ht_info.tuple_size / (1 << 20) << " MB\t"
			          << "Point Size: " << (double)ht_info.point_table_size / (1 << 20) << " MB\t"
			          << "#Term: " << ht_info.num_terms << "\t" << key << '\n';
		}
	}

	void Clear() {
		std::lock_guard<std::mutex> lock(mtx);
		values_.clear();
		calling_times_.clear();
		ht_records_.clear();
	}

private:
	unordered_map<string, std::atomic<size_t>> values_;
	unordered_map<string, std::atomic<size_t>> calling_times_;

	struct HTInfo {
		size_t tuple_size;
		size_t point_table_size;
		size_t num_terms;

		HTInfo(size_t ts = 0, size_t pts = 0, size_t nt = 0) : tuple_size(ts), point_table_size(pts), num_terms(nt) {
		}
	};
	unordered_map<string, HTInfo> ht_records_;
	mutable std::mutex mtx;
};
}  // namespace duckdb
