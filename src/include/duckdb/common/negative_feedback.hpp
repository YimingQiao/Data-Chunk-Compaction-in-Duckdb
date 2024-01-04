//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/negative_feedback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <filesystem>
#include <random>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

// I use UCB1 (https://cse442-17f.github.io/LinUCB/) to select the best
class MultiArmedBandit {
public:
	MultiArmedBandit(size_t n_arms, const std::vector<double> &means)
	    : n_arms_(n_arms),
	      est_rewards_(means),
	      est_square_rewards_(n_arms, 0),
	      n_select_(n_arms, 0),
	      n_update_(n_arms, 0),
	      select_times_(0),
	      update_times_(0),
	      dis_update_(0),
	      dis_n_update_(n_arms, 0) {
	}

	// Selects an arm based on the UCB1 algorithm
	inline size_t SelectArm() {
		std::lock_guard<std::mutex> lock(mutex_);
		Logging();

		if (select_times_ < n_arms_ * 8) {
			// initialize experimental means by pulling each arm once
			size_t arm = select_times_ % n_arms_;

			select_times_++;
			n_select_[arm]++;
			return arm;
		}

		// select the arm with the highest estimated_mean + UCB value
		double max_value = -1;
		size_t max_arm = 0;
		for (size_t i = 0; i < n_arms_; i++) {
			// double value = est_rewards_[i] + UpperConfidenceBound(i);
			double value = est_rewards_[i] + UCBTuned(i);

			if (value > max_value) {
				max_value = value;
				max_arm = i;
			}
		}
		select_times_++;
		n_select_[max_arm]++;
		return max_arm;
	}

	// Updates the arm with the given weight
	inline void UpdateArm(size_t arm, double reward) {
		std::lock_guard<std::mutex> lock(mutex_);

		// update discount rewards
		size_t update_factor = std::min(n_update_[arm], size_t(7));
		double ratio = update_factor / (update_factor + 1.0);
		est_rewards_[arm] = est_rewards_[arm] * ratio + reward * (1 - ratio);
		est_square_rewards_[arm] = est_square_rewards_[arm] * ratio + reward * reward * (1 - ratio);
		n_update_[arm]++;
		update_times_++;

		// update discount times
		dis_update_ = dis_update_ * kFactor + 1;
		for (size_t i = 0; i < n_arms_; ++i)
			dis_n_update_[i] *= kFactor;
		dis_n_update_[arm] += 1;
	}

	inline void Print(const std::vector<size_t> &values) {
		for (size_t i = 0; i < est_rewards_.size(); i++) {
			std::cerr << " [PARAMETERS] Estimated reward for arm " << values[i] << " is " << to_string(est_rewards_[i])
			          << " - Sampling times is " << n_select_[i] << "\n";
		}
	}

	inline void Log2Csv(std::string addr) {
		std::ofstream file(addr);

		// Check if file is open
		if (!file.is_open()) {
			throw std::runtime_error("Unable to open file");
		}

		// Iterating over history to write each record
		for (size_t i = 0; i < history_.size(); ++i) {
			const auto &record = history_[i];
			file << i * kHeart << ", ";
			for (size_t j = 0; j < record.his_rewards_.size(); ++j)
				file << record.his_rewards_[j] << ", ";

			for (size_t j = 0; j < record.his_select_.size(); ++j)
				file << record.his_select_[j] << ", ";
			file << "\n";
		}

		file.close();
	}

private:
	inline double UpperConfidenceBound(size_t arm) {
		return sqrt(2 * log(select_times_) / (n_select_[arm] + 1));
	}

	inline double UCBTuned(size_t arm) {
		double ucb_var = est_square_rewards_[arm] - est_rewards_[arm] * est_rewards_[arm] +
		                 sqrt(2 * log(dis_update_) / (dis_n_update_[arm] + kEpsilon));
		return sqrt(log(dis_update_) / (dis_n_update_[arm] + kEpsilon) * std::min(0.25, ucb_var));
	}

	inline void Logging() {
		if (select_times_ % kHeart == 0) history_.push_back(Record(est_rewards_, n_select_));
	}

	std::mutex mutex_;

	// stats
	size_t n_arms_;
	std::vector<size_t> n_select_;
	std::vector<size_t> n_update_;
	size_t update_times_;
	size_t select_times_;

	// UCB
	std::vector<double> est_rewards_;
	std::vector<double> est_square_rewards_;

	double kFactor = 0.99;
	double kEpsilon = 0.001;
	size_t dis_update_;
	std::vector<size_t> dis_n_update_;

	// logging
	size_t kHeart = 256;
	struct Record {
		std::vector<double> his_rewards_;
		std::vector<size_t> his_select_;

		Record(const std::vector<double> &rewards, const std::vector<size_t> selects)
		    : his_rewards_(rewards), his_select_(selects) {};
	};
	std::vector<Record> history_;
};

class CompactTuner {
public:
	static CompactTuner &Get() {
		static CompactTuner instance;
		return instance;
	}

	inline void Initialize(size_t address, const std::vector<size_t> &arms = {32, 64, 128, 256, 384, 512, 768, 1024}) {
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
		if (!bandit_packages_.empty()) {
			// output the parameters
			std::cerr << "-------\n";

			std::string folder_name = "./bandit_log_0x" + std::to_string(RandomInteger());
			std::filesystem::create_directories(folder_name);
			for (auto &pair : package_index_) {
				auto &addr = pair.first;
				auto &id = pair.second;

				auto &bandit = bandit_packages_[id].bandit;
				auto &value = bandit_packages_[id].value;

				std::string bandit_name = "0x" + std::to_string(addr) + "\tId-" + std::to_string(id);
				std::cerr << " [PARAMETERS] Compaction Address - " << bandit_name << "\n";
				bandit->Log2Csv("./" + folder_name + "/" + bandit_name + ".log");
				bandit->Print(value);
			}

			package_index_.clear();
			bandit_packages_.clear();
		}
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
	inline size_t RandomInteger() {
		return integers(gen_);
	}

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

	// random
	std::mt19937 gen_;
	std::uniform_int_distribution<int> integers;
};
}  // namespace duckdb
