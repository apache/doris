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
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>

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
 *
 * @tparam N Window size in seconds for the latency recorder, defaults to 60 seconds
 */
template <int N = 60>
class BvarLatencyRecorderWithStatus {
public:
    BvarLatencyRecorderWithStatus(const std::string& metric_name)
            : recorder_(N),
              max_status_(metric_name + "_max", get_max_latency, this),
              avg_status_(metric_name + "_avg", get_avg_latency, this),
              count_status_(metric_name + "_count", get_count_latency, this) {
        recorder_.hide();
    }

    BvarLatencyRecorderWithStatus(const std::string& prefix, const std::string& metric_name)
            : BvarLatencyRecorderWithStatus(prefix + "_" + metric_name) {}

    void put(int64_t value) { recorder_ << value; }

    void operator<<(int64_t value) { recorder_ << value; }

    int64_t max() const { return recorder_.max_latency(); }

    int64_t avg() const { return recorder_.latency(); }

    int64_t count() const { return recorder_.count(); }

private:
    bvar::LatencyRecorder recorder_;
    bvar::PassiveStatus<int64_t> max_status_;
    bvar::PassiveStatus<int64_t> avg_status_;
    bvar::PassiveStatus<int64_t> count_status_;

    static int64_t get_max_latency(void* arg) {
        auto* self = static_cast<BvarLatencyRecorderWithStatus*>(arg);
        int64_t value = self->recorder_.max_latency();
        return value >= 0 ? value : 0;
    }

    static int64_t get_avg_latency(void* arg) {
        auto* self = static_cast<BvarLatencyRecorderWithStatus*>(arg);
        int64_t value = self->recorder_.latency();
        return value >= 0 ? value : 0;
    }

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
 * @tparam N Window size in seconds for the latency recorder (default: 60)
 */
template <int N = 60>
class MBvarLatencyRecorderWithStatus {
private:
    class ScheduledLatencyUpdater : public bvar::LatencyRecorder {
    public:
        ScheduledLatencyUpdater(size_t interval_s, void* arg = nullptr)
                : bvar::LatencyRecorder(interval_s), _interval_s(interval_s), _arg(arg) {
            hide();
        }

        ~ScheduledLatencyUpdater() { stop(); }

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

        bool schedule() {
            if (bthread_timer_add(&_timer, butil::seconds_from_now(_interval_s), update, this) !=
                0) {
                LOG(WARNING) << "Failed to add bthread timer for ScheduledLatencyUpdater";
                return false;
            }
            return true;
        }

        static void update(void* arg) {
            auto* latency_updater = static_cast<ScheduledLatencyUpdater*>(arg);
            if (!latency_updater || !latency_updater->_started) {
                LOG(WARNING) << "Invalid ScheduledLatencyUpdater in timer callback";
                return;
            }

            VLOG(5) << "Timer triggered for ScheduledLatencyUpdater, interval: "
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

                VLOG(5) << "Updating latency recorder status for dimension, "
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

        void stop() {
            if (_started.load()) {
                bthread_timer_del(_timer);
                _started = false;
            }
        }

    private:
        int _interval_s;
        void* _arg;
        bthread_timer_t _timer;
        std::atomic_bool _started {false};
        bthread::Mutex init_mutex_;
    };

public:
    MBvarLatencyRecorderWithStatus(const std::string& metric_name,
                                   const std::initializer_list<std::string>& dim_names)
            : _metric_name(metric_name),
              max_status_(metric_name + "_max", std::list<std::string>(dim_names)),
              avg_status_(metric_name + "_avg", std::list<std::string>(dim_names)),
              count_status_(metric_name + "_count", std::list<std::string>(dim_names)) {}

    MBvarLatencyRecorderWithStatus(const std::string& prefix, const std::string& metric_name,
                                   const std::initializer_list<std::string>& dim_names)
            : MBvarLatencyRecorderWithStatus(prefix + "_" + metric_name, dim_names) {}

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
    std::map<std::list<std::string>, std::shared_ptr<ScheduledLatencyUpdater>> recorder_;
    bvar::MultiDimension<bvar::Status<int64_t>> max_status_;
    bvar::MultiDimension<bvar::Status<int64_t>> avg_status_;
    bvar::MultiDimension<bvar::Status<int64_t>> count_status_;
    bthread::Mutex recorder_mutex_;
    bthread::Mutex timer_mutex_;
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
