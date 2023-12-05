//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/negative_feedback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <math.h>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

// We use UCB1 (https://cse442-17f.github.io/LinUCB/) to select the best
class MultiArmedBandit {
public:
	MultiArmedBandit(size_t n_arms, const std::vector<double> &means)
	    : n_arms_(n_arms), est_means_(means), n_select_(n_arms, 0), n_update_(n_arms, 0), times_(0) {
	}

	// Selects an arm based on the UCB1 algorithm
	inline size_t SelectArm() {
		std::lock_guard<std::mutex> lock(mutex_);

		if (times_ < n_arms_) {
			// initialize experimental means by pulling each arm once
			size_t arm = times_;

			times_++;
			n_select_[arm]++;
			return arm;
		}

		// select the arm with the highest estimated_mean + UCB value
		double max_value = -1;
		size_t max_arm = 0;
		for (size_t i = 0; i < n_arms_; i++) {
			double value = est_means_[i] + UpperConfidenceBound(i);

			if (value > max_value) {
				max_value = value;
				max_arm = i;
			}
		}
		times_++;
		n_select_[max_arm]++;
		return max_arm;
	}

	// Updates the arm with the given weight
	inline void UpdateArm(size_t arm, double reward) {
		std::lock_guard<std::mutex> lock(mutex_);
		double ratio = n_update_[arm] / double(1 + n_update_[arm]);
		est_means_[arm] = est_means_[arm] * ratio + reward * (1 - ratio);
		n_update_[arm]++;
	}

	inline void Reset() {
		est_means_ = std::vector<double>(est_means_.size(), 0);
		n_select_ = std::vector<size_t>(n_select_.size(), 0);
		n_update_ = std::vector<size_t>(n_update_.size(), 0);
		times_ = 0;
	}

	inline void Print(const std::vector<size_t> &values) {
		for (size_t i = 0; i < est_means_.size(); i++) {
			std::cerr << " [PARAMETERS] Estimated mean for arm " << values[i] << " is " << to_string(est_means_[i])
			          << " - Sampling times is " << n_select_[i] << "\n";
		}
	}

private:
	inline double UpperConfidenceBound(size_t arm) {
		return sqrt(2 * log(times_) / (n_select_[arm] + 1));
	}

	std::mutex mutex_;

	size_t n_arms_;
	std::vector<double> est_means_;
	// select times and update times should be recorded separately, because we are multithreaded.
	std::vector<size_t> n_select_;
	std::vector<size_t> n_update_;
	size_t times_;
};

class CompactionController {
public:
	static CompactionController &Get() {
		static CompactionController instance;
		return instance;
	}

	void Initialize(const std::vector<size_t> &arms, const std::vector<double> &means) {
		bandit_ = make_uniq<MultiArmedBandit>(arms.size(), means);
		value_ = arms;
		for (size_t i = 0; i < arms.size(); i++) {
			value_index_[arms[i]] = i;
		}
	}

	// Selects an arm based on the UCB1 algorithm
	inline size_t SelectArm() {
		size_t ret = value_[bandit_->SelectArm()];
		return ret;
	}

	// Updates the arm with the given weight
	inline void UpdateArm(size_t arm, double weight) {
		if (value_index_.count(arm) == 0) return;
		bandit_->UpdateArm(value_index_[arm], weight);
	}

	inline void Reset() {
		bandit_->Print(value_);
		bandit_->Reset();
		cnt_ = 0;
	}

private:
	std::unique_ptr<MultiArmedBandit> bandit_;

	std::vector<size_t> value_;
	std::unordered_map<size_t, idx_t> value_index_;

	size_t cnt_ = 0;
};
}  // namespace duckdb
