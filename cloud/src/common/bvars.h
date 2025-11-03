// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <aws/core/external/cjson/cJSON.h>
#include <bthread/bthread.h>
#include <bthread/mutex.h>
#include <bthread/unstable.h>
#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>
#include <bvar/multi_dimension.h>
#include <bvar/passive_status.h>
#include <bvar/reducer.h>
#include <bvar/status.h>
#include <cpp/sync_point.h>
#include <gmock/gmock-actions.h>

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>

#include "common/logging.h"

/**
 * Manage bvars that with similar names (identical prefix)
 * ${module}_${name}_${tag}
 * where `tag` is added automatically when calling `get` or `put`
 */
template <typename Bvar, bool is_status = false>
class BvarWithTag {
public:
    BvarWithTag(std::string module, std::string name)
            : module_(std::move(module)), name_(std::move(name)) {}

    template <typename ValType>
        requires std::is_integral_v<ValType>
    void put(const std::string& tag, ValType value) {
        std::shared_ptr<Bvar> instance = nullptr;
        {
            std::lock_guard<bthread::Mutex> l(mutex_);
            auto it = bvar_map_.find(tag);
            if (it == bvar_map_.end()) {
                instance = std::make_shared<Bvar>(module_, name_ + "_" + tag, ValType());
                bvar_map_[tag] = instance;
            } else {
                instance = it->second;
            }
        }
        // FIXME(gavin): check bvar::Adder and more
        if constexpr (std::is_same_v<Bvar, bvar::LatencyRecorder>) {
            (*instance) << value;
        } else if constexpr (is_status) {
            instance->set_value(value);
        } else {
            // This branch mean to be unreachable, add an assert(false) here to
            // prevent missing branch match.
            // Postpone deduction of static_assert by evaluating sizeof(T)
            static_assert(!sizeof(Bvar), "all types must be matched with if constexpr");
        }
    }

    std::shared_ptr<Bvar> get(const std::string& tag) {
        std::shared_ptr<Bvar> instance = nullptr;
        std::lock_guard<bthread::Mutex> l(mutex_);

        auto it = bvar_map_.find(tag);
        if (it == bvar_map_.end()) {
            instance = std::make_shared<Bvar>(module_, name_ + "_" + tag);
            bvar_map_[tag] = instance;
            return instance;
        }
        return it->second;
    }

    void remove(const std::string& tag) {
        std::lock_guard<bthread::Mutex> l(mutex_);
        bvar_map_.erase(tag);
    }

private:
    bthread::Mutex mutex_;
    std::string module_;
    std::string name_;
    std::map<std::string, std::shared_ptr<Bvar>> bvar_map_;
};

using BvarLatencyRecorderWithTag = BvarWithTag<bvar::LatencyRecorder>;

template <typename T>
    requires std::is_integral_v<T>
using BvarStatusWithTag = BvarWithTag<bvar::Status<T>, true>;

/**
@brief: A wrapper class for multidimensional bvar metrics.
This template class provides a convenient interface for managing multidimensional
bvar metrics. It supports various bvar types including Adder, IntRecorder,
LatencyRecorder, Maxer, and Status.
@param: BvarType The type of bvar metric to use (must be one of the supported types)
@output: Based on the bvar multidimensional counter implementation,
the metrics output format would typically follow this structure:
{metric_name}{dimension1="value1",dimension2="value2",...} value
@example: Basic usage with an Adder:
// Create a 2-dimensional counter with dimensions "region" and "service"
mBvarWrapper<bvar::Adder<int>> request_counter("xxx_request_count", {"region", "service"});
// Increment the counter for specific dimension values
request_counter.put({"east", "login"}, 1);
request_counter.put({"west", "search"}, 1);
request_counter.put({"east", "login"}, 1); // Now east/login has value 2
// the output of above metrics:
xxx_request_count{region="east",service="login"} 2
xxx_request_count{region="west",service="search"} 1
@note: The dimensions provided in the constructor and the values provided to
put() and get() methods must match in count. Also, all supported bvar types
have different behaviors for how values are processed and retrieved.
*/

template <typename T>
struct is_valid_bvar_type : std::false_type {};
template <typename T>
struct is_valid_bvar_type<bvar::Adder<T>> : std::true_type {};
template <>
struct is_valid_bvar_type<bvar::IntRecorder> : std::true_type {};
template <typename T>
struct is_valid_bvar_type<bvar::Maxer<T>> : std::true_type {};
template <typename T>
struct is_valid_bvar_type<bvar::Status<T>> : std::true_type {};
template <>
struct is_valid_bvar_type<bvar::LatencyRecorder> : std::true_type {};
template <typename T>
struct is_bvar_status : std::false_type {};
template <typename T>
struct is_bvar_status<bvar::Status<T>> : std::true_type {};

template <typename BvarType>
class mBvarWrapper {
public:
    mBvarWrapper(const std::string& metric_name,
                 const std::initializer_list<std::string>& dim_names)
            : counter_(metric_name, std::list<std::string>(dim_names)) {
        static_assert(is_valid_bvar_type<BvarType>::value,
                      "BvarType must be one of the supported bvar types (Adder, IntRecorder, "
                      "LatencyRecorder, Maxer, Status)");
    }

    template <typename ValType>
    void put(const std::initializer_list<std::string>& dim_values, ValType value) {
        BvarType* stats = counter_.get_stats(std::list<std::string>(dim_values));
        if (stats) {
            if constexpr (is_bvar_status<BvarType>::value) {
                stats->set_value(value);
            } else {
                *stats << value;
            }
        }
    }

    auto get(const std::initializer_list<std::string>& dim_values) {
        BvarType* stats = counter_.get_stats(std::list<std::string>(dim_values));
        using ReturnType = decltype(stats->get_value());
        if (stats) {
            return stats->get_value();
        }
        return ReturnType {};
    }

private:
    bvar::MultiDimension<BvarType> counter_;
};
/**
 * @class BvarLatencyRecorderWithStatus
 * @brief A latency recorder with auto-exposed max and average metrics
 *
 * This class wraps a bvar::LatencyRecorder and automatically creates two
 * additional PassiveStatus metrics to expose the maximum and average latency.
 * This makes it convenient to track these key metrics in monitoring systems.
 * 
 * @tparam N Window size in seconds for the latency recorder, defaults to 60 seconds
 *
 * @note Unlike mBvarLatencyRecorderWithStatus, this class doesn't support multi-dimensional
 * metrics and doesn't use a timer to update status. It uses PassiveStatus to calculate
 * statistics in real-time when queried.
 *
 * @example Basic usage:
 *   // Create a latency recorder
 *   BvarLatencyRecorderWithStatus<> my_latency("my_service_latency");
 *   // Record a latency value (in microseconds)
 *   my_latency << 1500;  // or my_latency.put(1500);
 *   // This will create three metrics:
 *   // - The original latency recorder (hidden)
 *   // - my_service_latency_max (showing maximum latency)
 *   // - my_service_latency_avg (showing average latency)
 */
template <int N = 60>
class BvarLatencyRecorderWithStatus {
public:
    /**
     * @brief Constructor
     * @param metric_name Base name for the metrics, _max and _avg suffixes will be added
     */
    BvarLatencyRecorderWithStatus(const std::string& metric_name)
            : recorder_(N),
              max_status_(metric_name + "_max", get_max_latency, this),
              avg_status_(metric_name + "_avg", get_avg_latency, this),
              count_status_(metric_name + "_count", get_count_latency, this) {
        recorder_.hide();
    }

    /**
     * @brief Constructor with prefix
     * @param prefix Prefix for the metric name
     * @param metric_name Base name for the metrics
     */
    BvarLatencyRecorderWithStatus(const std::string& prefix, const std::string& metric_name)
            : BvarLatencyRecorderWithStatus(prefix + "_" + metric_name) {}

    /**
     * @brief Record a latency value
     * @param value Latency value to record (in microseconds)
     */
    void put(int64_t value) { recorder_ << value; }

    /**
     * @brief Stream operator for recording latency values
     * @param value Latency value to record (in microseconds)
     */
    void operator<<(int64_t value) { recorder_ << value; }

    int64_t max() const { return recorder_.max_latency(); }

    int64_t avg() const { return recorder_.latency(); }

    int64_t count() const { return recorder_.count(); }

private:
    bvar::LatencyRecorder recorder_;            // The underlying latency recorder
    bvar::PassiveStatus<int64_t> max_status_;   // Passive status for maximum latency
    bvar::PassiveStatus<int64_t> avg_status_;   // Passive status for average latency
    bvar::PassiveStatus<int64_t> count_status_; // Passive status for count latency

    /**
     * @brief Callback function to get maximum latency
     * @param arg Pointer to the BvarLatencyRecorderWithStatus instance
     * @return Maximum latency value, or 0 if negative
     */
    static int64_t get_max_latency(void* arg) {
        auto* self = static_cast<BvarLatencyRecorderWithStatus*>(arg);
        int64_t value = self->recorder_.max_latency();
        return value >= 0 ? value : 0;
    }

    /**
     * @brief Callback function to get average latency
     * @param arg Pointer to the BvarLatencyRecorderWithStatus instance
     * @return Average latency value, or 0 if negative
     */
    static int64_t get_avg_latency(void* arg) {
        auto* self = static_cast<BvarLatencyRecorderWithStatus*>(arg);
        int64_t value = self->recorder_.latency();
        return value >= 0 ? value : 0;
    }

    /**
     * @brief Callback function to get count latency
     * @param arg Pointer to the BvarLatencyRecorderWithStatus instance
     * @return Count latency value, or 0 if negative
     */
    static int64_t get_count_latency(void* arg) {
        auto* self = static_cast<BvarLatencyRecorderWithStatus*>(arg);
        int64_t value = self->recorder_.count();
        return value >= 0 ? value : 0;
    }
};

/**
 * @class MBvarLatencyRecorderWithStatus
 * @brief A multi-dimensional latency recorder with status metrics
 * 
 * This class provides a way to record latency metrics across multiple dimensions and
 * automatically update status metrics (max and average) at regular intervals.
 * It leverages bvar's MultiDimension capability to track metrics per dimension combination.
 * 
 * @tparam N Window size in seconds for the latency recorder (default: 60)
 */
template <int N = 60>
class MBvarLatencyRecorderWithStatus {
private:
    /**
    * @class ScheduledLatencyUpdater
    * @brief A helper class to schedule deferred execution of tasks using bthread timer.
    * 
    * This class provides a way to execute a callback function after a specified time interval.
    * It takes care of safely managing the lifecycle of the timer and ensures the callback
    * is only executed if the timer and its arguments are still valid.
    * 
    * @note This class requires bthread to be initialized before use. If bthread is not
    * initialized, timer creation will fail.
    */
    class ScheduledLatencyUpdater : public bvar::LatencyRecorder {
    public:
        /**
        * @brief Constructor for ScheduledLatencyUpdater
        * 
        * @param interval_s The time interval in seconds after which the callback should be executed
        * @param arg Optional argument to pass to the callback function
        */
        ScheduledLatencyUpdater(size_t interval_s, void* arg = nullptr)
                : bvar::LatencyRecorder(interval_s), _interval_s(interval_s), _arg(arg) {
            hide();
        }

        /**
        * @brief Destructor
        * 
        * Stops the timer if it's still running to prevent any callbacks after destruction
        */
        ~ScheduledLatencyUpdater() { stop(); }

        /**
        * @brief Start the timer
        * 
        * Schedules the callback function to be executed after the specified interval.
        * Does nothing if the timer has already been started.
        * 
        * @return true if the timer was successfully started, false otherwise
        */
        bool start() {
            if (!_started.load()) {
                {
                    std::lock_guard<bthread::Mutex> l(init_mutex_);
                    if (!_started.load()) {
                        if (!schedule()) {
                            return false;
                        }
                        _started.store(true);
                    }
                    return true;
                }
            }
            return true;
        }

        /*** @brief Reschedule the timer
        * 
        * Scheduling a one-time task.
        * This is useful if you want to reset the timer interval.
        */
        bool schedule() {
            if (bthread_timer_add(&_timer, butil::seconds_from_now(_interval_s), update, this) !=
                0) {
                LOG(WARNING) << "Failed to add bthread timer for ScheduledLatencyUpdater";
                return false;
            }
            return true;
        }

        /**
        * @brief Background update function
        * 
        * This function is called periodically by the timer to update the status metrics
        * with the current values from the latency recorders.
        * 
        * @param arg Pointer to the mBvarLatencyRecorderWithStatus instance
        */
        static void update(void* arg) {
            auto* latency_updater = static_cast<ScheduledLatencyUpdater*>(arg);
            if (!latency_updater || !latency_updater->_started) {
                LOG(WARNING) << "Invalid ScheduledLatencyUpdater in timer callback";
                return;
            }

            VLOG_DEBUG << "Timer triggered for ScheduledLatencyUpdater, interval: "
                       << latency_updater->_interval_s << "s";

            auto* parent = static_cast<MBvarLatencyRecorderWithStatus*>(latency_updater->_arg);
            if (!parent) {
                LOG(WARNING) << "Invalid parent container in timer callback";
                return;
            }

            std::list<std::string> current_dim_list;
            {
                std::lock_guard<bthread::Mutex> l(parent->recorder_mutex_);
                for (const auto& it : parent->recorder_) {
                    if (it.second.get() == latency_updater) {
                        current_dim_list = it.first;
                        break;
                    }
                }
            }

            if (current_dim_list.empty()) {
                LOG(WARNING) << "Could not find dimension for ScheduledLatencyUpdater";
                return;
            }

            {
                std::lock_guard<bthread::Mutex> l(parent->timer_mutex_);

                bvar::Status<int64_t>* max_status = parent->max_status_.get_stats(current_dim_list);
                bvar::Status<int64_t>* avg_status = parent->avg_status_.get_stats(current_dim_list);
                bvar::Status<int64_t>* count_status =
                        parent->count_status_.get_stats(current_dim_list);

                VLOG_DEBUG << "Updating latency recorder status for dimension, "
                           << "max_latency: " << latency_updater->max_latency()
                           << ", avg_latency: " << latency_updater->latency();
                TEST_SYNC_POINT("mBvarLatencyRecorderWithStatus::update");

                if (max_status) {
                    max_status->set_value(latency_updater->max_latency());
                }
                if (avg_status) {
                    avg_status->set_value(latency_updater->latency());
                }
                if (count_status) {
                    count_status->set_value(latency_updater->count());
                }
            }

            if (latency_updater->_started && !latency_updater->schedule()) {
                LOG(WARNING) << "Failed to reschedule timer for ScheduledLatencyUpdater";
                latency_updater->_started = false;
            }
        }

        /**
        * @brief Stop the timer
        * 
        * Cancels the timer if it's running and marks arguments as invalid to prevent
        * any pending callbacks from accessing potentially freed resources.
        */
        void stop() {
            if (_started.load()) {
                bthread_timer_del(_timer);
                _started = false;
            }
        }

    private:
        int _interval_s;                   // Timer interval in seconds
        void* _arg;                        // Argument to pass to the callback
        bthread_timer_t _timer;            // The bthread timer handle
        std::atomic_bool _started {false}; // Whether the timer has been started
        bthread::Mutex init_mutex_;        // Mutex for timer_map_
    };

public:
    /**
     * @brief Constructor
     * 
     * @param metric_name Base name for the metrics
     * @param dim_names List of dimension names
     */
    MBvarLatencyRecorderWithStatus(const std::string& metric_name,
                                   const std::initializer_list<std::string>& dim_names)
            : _metric_name(metric_name),
              max_status_(metric_name + "_max", std::list<std::string>(dim_names)),
              avg_status_(metric_name + "_avg", std::list<std::string>(dim_names)),
              count_status_(metric_name + "_count", std::list<std::string>(dim_names)) {}

    MBvarLatencyRecorderWithStatus(const std::string& prefix, const std::string& metric_name,
                                   const std::initializer_list<std::string>& dim_names)
            : MBvarLatencyRecorderWithStatus(prefix + "_" + metric_name, dim_names) {}

    /**
     * @brief Record a latency value
     * 
     * @param dim_values List of dimension values (must match the number of dimensions)
     * @param value The latency value to record
     */
    void put(const std::initializer_list<std::string>& dim_values, int64_t value) {
        std::list<std::string> dim_list(dim_values);
        std::shared_ptr<ScheduledLatencyUpdater> latency = nullptr;
        {
            std::lock_guard<bthread::Mutex> l(recorder_mutex_);
            auto it = recorder_.find(dim_list);
            if (it == recorder_.end()) {
                int inteval_s = N;
                TEST_SYNC_POINT_CALLBACK("mBvarLatencyRecorderWithStatus::put", &inteval_s);
                latency = std::make_shared<ScheduledLatencyUpdater>(inteval_s, this);
                recorder_[dim_list] = latency;
            } else {
                latency = it->second;
            }
        }

        auto* latency_ptr = latency.get();
        latency->start();
        *latency_ptr << value;
    }

    int64_t get_max(const std::initializer_list<std::string>& dim_values) {
        return max_status_.get_stats(std::list<std::string>(dim_values))->get_value();
    }

    int64_t get_avg(const std::initializer_list<std::string>& dim_values) {
        return avg_status_.get_stats(std::list<std::string>(dim_values))->get_value();
    }

    int64_t get_count(const std::initializer_list<std::string>& dim_values) {
        return count_status_.get_stats(std::list<std::string>(dim_values))->get_value();
    }

private:
    std::string _metric_name;
    // dim_names -> recorder
    std::map<std::list<std::string>, std::shared_ptr<ScheduledLatencyUpdater>> recorder_;
    bvar::MultiDimension<bvar::Status<int64_t>> max_status_;
    bvar::MultiDimension<bvar::Status<int64_t>> avg_status_;
    bvar::MultiDimension<bvar::Status<int64_t>> count_status_;
    bthread::Mutex recorder_mutex_; // Mutex for recorder_
    bthread::Mutex timer_mutex_;    // Mutex for timer_map_
};

using mBvarIntAdder = mBvarWrapper<bvar::Adder<int>>;
using mBvarInt64Adder = mBvarWrapper<bvar::Adder<int64_t>>;
using mBvarDoubleAdder = mBvarWrapper<bvar::Adder<double>>;
using mBvarIntRecorder = mBvarWrapper<bvar::IntRecorder>;
using mBvarLatencyRecorder = mBvarWrapper<bvar::LatencyRecorder>;
using mBvarIntMaxer = mBvarWrapper<bvar::Maxer<int>>;
using mBvarDoubleMaxer = mBvarWrapper<bvar::Maxer<double>>;
template <typename T>
using mBvarStatus = mBvarWrapper<bvar::Status<T>>;

// meta-service's bvars
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_precommit_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_txn_eventually;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_current_max_txn_id;
extern BvarLatencyRecorderWithTag g_bvar_ms_check_txn_conflict;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_txn_with_coordinator;
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_sub_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_sub_txn;
extern BvarLatencyRecorderWithTag g_bvar_ms_clean_txn_label;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_version;
extern BvarLatencyRecorderWithTag g_bvar_ms_batch_get_version;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_tablets;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_tablet;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_tablet;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_tmp_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_rowset;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_index;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_partition;
extern BvarLatencyRecorderWithTag g_bvar_ms_prepare_restore_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_restore_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_finish_restore_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_tablet_stats;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_obj_store_info;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_obj_store_info;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_storage_vault;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_cluster;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_cluster;
extern BvarLatencyRecorderWithTag g_bvar_ms_create_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_stage;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_iam;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_ak_sk;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_iam;
extern BvarLatencyRecorderWithTag g_bvar_ms_alter_ram_user;
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_copy;
extern BvarLatencyRecorderWithTag g_bvar_ms_finish_copy;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_copy_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_copy_files;
extern BvarLatencyRecorderWithTag g_bvar_ms_filter_copy_files;
extern BvarLatencyRecorderWithTag g_bvar_ms_start_tablet_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_finish_tablet_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_delete_bitmap_update_lock;
extern BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap;
extern BvarLatencyRecorderWithTag g_bvar_ms_remove_delete_bitmap_update_lock;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_cluster_status;
extern BvarLatencyRecorderWithTag g_bvar_ms_set_cluster_status;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_instance;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_rl_task_commit_attach;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_streaming_task_commit_attach;
extern BvarLatencyRecorderWithTag g_bvar_ms_delete_streaming_job;
extern BvarLatencyRecorderWithTag g_bvar_ms_reset_streaming_job_offset;
extern BvarLatencyRecorderWithTag g_bvar_ms_reset_rl_progress;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_txn_id;
extern BvarLatencyRecorderWithTag g_bvar_ms_check_kv;
extern BvarLatencyRecorderWithTag g_bvar_ms_get_schema_dict;
extern BvarLatencyRecorderWithTag g_bvar_ms_begin_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_update_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_commit_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_abort_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_drop_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_list_snapshot;
extern BvarLatencyRecorderWithTag g_bvar_ms_clone_instance;
extern bvar::Adder<int64_t> g_bvar_update_delete_bitmap_fail_counter;
extern bvar::Adder<int64_t> g_bvar_get_delete_bitmap_fail_counter;
extern BvarLatencyRecorderWithStatus<60> g_bvar_ms_txn_commit_with_tablet_count;
extern BvarLatencyRecorderWithStatus<60> g_bvar_ms_txn_commit_with_partition_count;
extern MBvarLatencyRecorderWithStatus<60> g_bvar_instance_txn_commit_with_partition_count;
extern MBvarLatencyRecorderWithStatus<60> g_bvar_instance_txn_commit_with_tablet_count;
extern bvar::LatencyRecorder g_bvar_ms_scan_instance_update;

// recycler's bvars
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_index_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_partition_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_rowset_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_tmp_rowset_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_expired_txn_label_earlest_ts;
extern BvarStatusWithTag<int64_t> g_bvar_recycler_recycle_restore_job_earlest_ts;

// recycler's mbvars
extern bvar::Status<int64_t> g_bvar_recycler_task_max_concurrency;
extern mBvarIntAdder g_bvar_recycler_instance_recycle_task_status;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycle_duration;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_next_ts;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_start_ts;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_end_ts;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_recycle_last_success_ts;

extern mBvarIntAdder g_bvar_recycler_vault_recycle_task_status;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycled_num;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_to_recycle_num;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_recycled_bytes;
extern mBvarStatus<int64_t> g_bvar_recycler_instance_last_round_to_recycle_bytes;
extern mBvarStatus<double> g_bvar_recycler_instance_last_round_recycle_elpased_ts;
extern mBvarInt64Adder g_bvar_recycler_instance_recycle_total_num_since_started;
extern mBvarInt64Adder g_bvar_recycler_instance_recycle_total_bytes_since_started;
extern mBvarIntAdder g_bvar_recycler_instance_recycle_round;
extern mBvarStatus<double> g_bvar_recycler_instance_recycle_time_per_resource;
extern mBvarStatus<double> g_bvar_recycler_instance_recycle_bytes_per_ms;

// txn_kv's bvars
extern bvar::LatencyRecorder g_bvar_txn_kv_get;
extern bvar::LatencyRecorder g_bvar_txn_kv_range_get;
extern bvar::LatencyRecorder g_bvar_txn_kv_put;
extern bvar::LatencyRecorder g_bvar_txn_kv_commit;
extern bvar::LatencyRecorder g_bvar_txn_kv_watch_key;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_key;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_set_ver_value;
extern bvar::LatencyRecorder g_bvar_txn_kv_atomic_add;
extern bvar::LatencyRecorder g_bvar_txn_kv_remove;
extern bvar::LatencyRecorder g_bvar_txn_kv_range_remove;
extern bvar::LatencyRecorder g_bvar_txn_kv_get_read_version;
extern bvar::LatencyRecorder g_bvar_txn_kv_get_committed_version;
extern bvar::LatencyRecorder g_bvar_txn_kv_batch_get;

extern bvar::Adder<int64_t> g_bvar_txn_kv_commit_error_counter;
extern bvar::Adder<int64_t> g_bvar_txn_kv_commit_conflict_counter;
extern bvar::Adder<int64_t> g_bvar_txn_kv_get_count_normalized;

extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_put_conflict_counter;
extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_fail_counter;
extern bvar::Adder<int64_t> g_bvar_delete_bitmap_lock_txn_remove_conflict_by_load_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_commit_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_lease_counter;
extern bvar::Adder<int64_t>
        g_bvar_delete_bitmap_lock_txn_remove_conflict_by_compaction_abort_counter;

extern const int64_t BVAR_FDB_INVALID_VALUE;
extern bvar::Status<int64_t> g_bvar_fdb_client_count;
extern bvar::Status<int64_t> g_bvar_fdb_configuration_coordinators_count;
extern bvar::Status<int64_t> g_bvar_fdb_configuration_usable_regions;
extern bvar::Status<int64_t> g_bvar_fdb_coordinators_unreachable_count;
extern bvar::Status<int64_t> g_bvar_fdb_fault_tolerance_count;
extern bvar::Status<int64_t> g_bvar_fdb_data_average_partition_size_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_log_server_space_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_highest_priority;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_flight_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_data_in_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_moving_total_written_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_partition_count;
extern bvar::Status<int64_t> g_bvar_fdb_data_storage_server_space_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_state_min_replicas_remaining;
extern bvar::Status<int64_t> g_bvar_fdb_data_total_kv_size_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_data_total_disk_used_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_generation;
extern bvar::Status<int64_t> g_bvar_fdb_incompatible_connections;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_transaction_start_ns;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_commit_ns;
extern bvar::Status<int64_t> g_bvar_fdb_latency_probe_read_ns;
extern bvar::Status<int64_t> g_bvar_fdb_machines_count;
extern bvar::Status<int64_t> g_bvar_fdb_process_count;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_data_lag_storage_server_ns;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_durability_lag_storage_server_ns;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_log_server_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_qos_worst_storage_server_queue_bytes;
extern bvar::Status<int64_t> g_bvar_fdb_workload_conflict_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_location_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_keys_read_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_read_bytes_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_read_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_write_rate_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_written_bytes_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_started_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_committed_hz;
extern bvar::Status<int64_t> g_bvar_fdb_workload_transactions_rejected_hz;
extern bvar::Status<int64_t> g_bvar_fdb_client_thread_busyness_percent;
extern mBvarStatus<int64_t> g_bvar_fdb_process_status_int;
extern mBvarStatus<double> g_bvar_fdb_process_status_float;

// checker
extern BvarStatusWithTag<long> g_bvar_checker_num_scanned;
extern BvarStatusWithTag<long> g_bvar_checker_num_scanned_with_segment;
extern BvarStatusWithTag<long> g_bvar_checker_num_check_failed;
extern BvarStatusWithTag<long> g_bvar_checker_check_cost_s;
extern BvarStatusWithTag<long> g_bvar_checker_enqueue_cost_s;
extern BvarStatusWithTag<long> g_bvar_checker_last_success_time_ms;
extern BvarStatusWithTag<long> g_bvar_checker_instance_volume;
extern BvarStatusWithTag<long> g_bvar_inverted_checker_num_scanned;
extern BvarStatusWithTag<long> g_bvar_inverted_checker_num_check_failed;

extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_leaked_delete_bitmaps;
extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_abnormal_delete_bitmaps;
extern BvarStatusWithTag<int64_t> g_bvar_inverted_checker_delete_bitmaps_scanned;
extern BvarStatusWithTag<int64_t> g_bvar_max_rowsets_with_useless_delete_bitmap_version;

extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_prepared_state;
extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_committed_state;
extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_dropped_state;
extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_completed_state;
extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_recycling_state;
extern BvarStatusWithTag<int64_t> g_bvar_checker_restore_job_cost_many_time;

// rpc kv
extern mBvarInt64Adder g_bvar_rpc_kv_get_rowset_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_version_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_schema_dict_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_tablets_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_tablets_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tablet_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tablet_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_tablet_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_tablet_stats_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_index_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_index_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_index_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_index_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_partition_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_partition_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_restore_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_restore_job_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_restore_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_restore_job_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_restore_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_restore_job_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_check_kv_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_obj_store_info_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_instance_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_instance_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_instance_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_cluster_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_stage_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_create_stage_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_stage_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_iam_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_iam_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_iam_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_copy_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_copy_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_copy_job_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_copy_files_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_filter_copy_files_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_status_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_rl_task_commit_attach_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_streaming_task_commit_attach_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_delete_streaming_job_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_current_max_txn_id_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_with_coordinator_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_check_txn_conflict_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_get_txn_id_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_del_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_get_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_put_counter;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_del_counter;

extern mBvarInt64Adder g_bvar_rpc_kv_get_rowset_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_version_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_schema_dict_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_tablets_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_tablets_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tablet_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tablet_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_tablet_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_rowset_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_rowset_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_tmp_rowset_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_tablet_stats_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_delete_bitmap_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_delete_bitmap_update_lock_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_update_lock_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_remove_delete_bitmap_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_start_tablet_job_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_tablet_job_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_index_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_index_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_index_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_index_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_index_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_partition_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_partition_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_partition_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_partition_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_restore_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_prepare_restore_job_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_restore_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_restore_job_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_restore_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_restore_job_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_check_kv_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_obj_store_info_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_storage_vault_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_obj_store_info_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_ak_sk_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_instance_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_instance_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_instance_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_cluster_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_stage_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_create_stage_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_stage_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_iam_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_iam_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_iam_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_alter_ram_user_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_copy_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_copy_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_finish_copy_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_copy_job_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_copy_files_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_filter_copy_files_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_cluster_status_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_precommit_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_rl_task_commit_attach_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_streaming_task_commit_attach_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_delete_streaming_job_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_streaming_job_offset_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_reset_rl_progress_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_txn_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_current_max_txn_id_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_sub_txn_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_sub_txn_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_txn_with_coordinator_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_check_txn_conflict_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clean_txn_label_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_get_txn_id_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_begin_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_update_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_commit_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_abort_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_drop_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_list_snapshot_del_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_get_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_put_bytes;
extern mBvarInt64Adder g_bvar_rpc_kv_clone_instance_del_bytes;

// meta ranges
extern mBvarStatus<int64_t> g_bvar_fdb_kv_ranges_count;
