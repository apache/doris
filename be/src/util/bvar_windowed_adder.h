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

#include <bthread/mutex.h>
#include <bvar/bvar.h>
#include <bvar/multi_dimension.h>
#include <bvar/window.h>

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace doris {

/**
 * Multi-dimension windowed adder.
 *
 * For each dimension value combination (e.g., job_id), automatically creates:
 *   - A bvar::Adder (cumulative counter managed by MultiDimension)
 *   - Multiple bvar::Window instances (sliding window views at different time scales)
 *
 * Windows are lazily created on first write to a dimension value.
 *
 * @example
 *   MBvarWindowedAdder requested_seg_num(
 *       "warmup_ed_requested_segment_num",
 *       {"job_id"},
 *       {300, 1800, 7200}
 *   );
 *   requested_seg_num.put({"13419"}, 1);
 */
class MBvarWindowedAdder {
public:
    MBvarWindowedAdder(const std::string& name, const std::initializer_list<std::string>& dim_names,
                       std::vector<int> window_seconds, bool expose = true)
            : name_(name),
              window_seconds_(std::move(window_seconds)),
              md_total_(std::list<std::string>(dim_names)),
              expose_(expose) {
        if (expose_) {
            md_total_.expose(name_ + "_total");
        }
    }

    void put(const std::initializer_list<std::string>& dim_values, int64_t value) {
        auto* adder = md_total_.get_stats(std::list<std::string>(dim_values));
        if (!adder) return;
        ensure_windows(dim_values, adder);
        *adder << value;
    }

    /** Get the current window value for the specified dimension and window index. */
    int64_t get_window_value(const std::initializer_list<std::string>& dim_values,
                             size_t window_idx) {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        auto it = dims_.find(make_key(dim_values));
        if (it == dims_.end() || window_idx >= it->second.windows.size()) {
            return 0;
        }
        return it->second.windows[window_idx]->get_value();
    }

    /** Overload accepting a pre-built key string (e.g., "job_id,table_id"). */
    int64_t get_window_value(const std::string& dim_key, size_t window_idx) {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        auto it = dims_.find(dim_key);
        if (it == dims_.end() || window_idx >= it->second.windows.size()) {
            return 0;
        }
        return it->second.windows[window_idx]->get_value();
    }

    /** List all dimension key strings that have been seen. */
    std::vector<std::string> list_dimensions() const {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        std::vector<std::string> result;
        result.reserve(dims_.size());
        for (auto& [key, _] : dims_) {
            result.push_back(key);
        }
        return result;
    }

    void hide() {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (!expose_) {
            return;
        }
        expose_ = false;
        md_total_.hide();
        for (auto& [_, entry] : dims_) {
            for (auto& window : entry.windows) {
                window->hide();
            }
        }
    }

private:
    struct DimEntry {
        bvar::Adder<int64_t>* adder; // owned by MultiDimension
        std::vector<std::unique_ptr<bvar::Window<bvar::Adder<int64_t>>>> windows;
    };

    void ensure_windows(const std::initializer_list<std::string>& dim_values,
                        bvar::Adder<int64_t>* adder) {
        std::string key = make_key(dim_values);
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (dims_.count(key)) return;
        DimEntry entry;
        entry.adder = adder;
        for (int ws : window_seconds_) {
            if (expose_) {
                std::string wname = name_ + "_" + std::to_string(ws) + "s_" + key;
                entry.windows.emplace_back(
                        std::make_unique<bvar::Window<bvar::Adder<int64_t>>>(wname, adder, ws));
            } else {
                entry.windows.emplace_back(
                        std::make_unique<bvar::Window<bvar::Adder<int64_t>>>(adder, ws));
            }
        }
        dims_[key] = std::move(entry);
    }

    static std::string make_key(const std::initializer_list<std::string>& dim_values) {
        std::string result;
        for (auto& v : dim_values) {
            if (!result.empty()) result += ",";
            result += v;
        }
        return result;
    }

    std::string name_;
    std::vector<int> window_seconds_;
    bvar::MultiDimension<bvar::Adder<int64_t>> md_total_;
    bool expose_;
    mutable bthread::Mutex mutex_;
    std::map<std::string, DimEntry> dims_;
};

} // namespace doris
