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
// This file is copied from
#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "common/compiler_util.h"
#include "util/pretty_printer.h"

namespace doris {

/*
 * A counter that keeps track of the current and peak memory usage seen.
 * Relaxed ordering, not accurate in real time.
 *
 * This class is thread-safe.
*/
class MemCounter {
public:
    MemCounter() = default;

    void add(int64_t delta) {
        if (UNLIKELY(delta == 0)) {
            return;
        }
        int64_t value = _current_value.fetch_add(delta, std::memory_order_relaxed) + delta;
        update_peak(value);
    }

    void add_no_update_peak(int64_t delta) { // need extreme fast
        _current_value.fetch_add(delta, std::memory_order_relaxed);
    }

    bool try_add(int64_t delta, int64_t max) {
        if (UNLIKELY(delta == 0)) {
            return true;
        }
        int64_t cur_val = _current_value.load(std::memory_order_relaxed);
        int64_t new_val = 0;
        do {
            new_val = cur_val + delta;
            if (UNLIKELY(new_val > max)) {
                return false;
            }
        } while (UNLIKELY(!_current_value.compare_exchange_weak(cur_val, new_val,
                                                                std::memory_order_relaxed)));
        update_peak(new_val);
        return true;
    }

    void sub(int64_t delta) { _current_value.fetch_sub(delta, std::memory_order_relaxed); }

    void set(int64_t v) {
        _current_value.store(v, std::memory_order_relaxed);
        update_peak(v);
    }

    void update_peak(int64_t value) {
        int64_t pre_value = _peak_value.load(std::memory_order_relaxed);
        while (value > pre_value &&
               !_peak_value.compare_exchange_weak(pre_value, value, std::memory_order_relaxed)) {
        }
    }

    int64_t current_value() const { return _current_value.load(std::memory_order_relaxed); }
    int64_t peak_value() const { return _peak_value.load(std::memory_order_relaxed); }

    static std::string print_bytes(int64_t bytes) {
        return bytes >= 0 ? PrettyPrinter::print(bytes, TUnit::BYTES)
                          : "-" + PrettyPrinter::print(std::abs(bytes), TUnit::BYTES);
    }

private:
    std::atomic<int64_t> _current_value {0};
    std::atomic<int64_t> _peak_value {0};
};

} // namespace doris
