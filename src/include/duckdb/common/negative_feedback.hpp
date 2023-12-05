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

// I use UCB1 (https://cse442-17f.github.io/LinUCB/) to select the best
class MultiArmedBandit {
public:
	// The reward is always 1
	static constexpr double kReward = 1;

public:
	MultiArmedBandit(size_t n_arms, const std::vector<double> &means)
	    : n_arms_(n_arms), est_means_(means), n_select_(n_arms, 0), n_update_(n_arms, 0), times_(0), best_reward_(0) {
	}

	// Selects an arm based on the UCB1 algorithm
	inline size_t SelectArm() {
		std::lock_guard<std::mutex> lock(mutex_);

		if (times_ < n_arms_ * 10) {
			// initialize experimental means by pulling each arm once
			size_t arm = times_ % n_arms_;

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
		double ratio = n_update_[arm] / (n_update_[arm] + 1.0);
		est_means_[arm] = est_means_[arm] * ratio + reward * (1 - ratio);
		n_update_[arm]++;
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

	double best_reward_;
};

class CompactTuner {
public:
	static CompactTuner &Get() {
		static CompactTuner instance;
		return instance;
	}

	inline void Initialize(size_t address,
	                       const std::vector<size_t> &arms = {8, 16, 32, 64, 128, 256, 384, 512, 768, 1024}) {
		D_ASSERT(package_index_.count(address) == 0);

		package_index_[address] = bandit_packages_.size();
		bandit_packages_.emplace_back(arms, std::vector<double>(arms.size(), 0));
	}

	// Selects an arm based on the UCB1 algorithm
	inline size_t SelectArm(idx_t id) {
		auto &bandit = bandit_packages_[id].bandit;
		auto &value = bandit_packages_[id].value;

		size_t ret = value[bandit->SelectArm()];
		return ret;
	}

	// Updates the arm with the given weight
	inline void UpdateArm(idx_t id, size_t arm, double reward) {
		auto &bandit = bandit_packages_[id].bandit;
		auto &value_index = bandit_packages_[id].value_index;

		if (value_index.count(arm) == 0) return;
		bandit->UpdateArm(value_index[arm], reward);
	}

	inline void Reset() {
		// output the parameters
		std::cerr << "-------\n";
		for (auto &pair : package_index_) {
			auto &addr = pair.first;
			auto &id = pair.second;

			auto &bandit = bandit_packages_[id].bandit;
			auto &value = bandit_packages_[id].value;

			std::cerr << " [PARAMETERS] Compaction Address - 0x" << addr << "\tId - " << id << "\n";
			bandit->Print(value);
		}

		package_index_.clear();
		bandit_packages_.clear();
	}

	inline int64_t GetId(size_t address) {
		if (package_index_.count(address) == 0) {
			// not found
			return -1;
		}

		return package_index_[address];
	}

	inline size_t GetBanditSize() {
		return bandit_packages_.size();
	}

private:
	struct BanditPackage {
		std::unique_ptr<MultiArmedBandit> bandit;
		std::vector<size_t> value;
		std::unordered_map<size_t, idx_t> value_index;

		BanditPackage(const std::vector<size_t> &arms, const std::vector<double> &means) {
			bandit = make_uniq<MultiArmedBandit>(arms.size(), means);
			value = arms;
			for (size_t i = 0; i < arms.size(); i++) {
				value_index[arms[i]] = i;
			}
		}
	};

	std::unordered_map<size_t, idx_t> package_index_;
	std::vector<BanditPackage> bandit_packages_;
};
}  // namespace duckdb
