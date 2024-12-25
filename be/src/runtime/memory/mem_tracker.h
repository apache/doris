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

#include <cstdint>
#include <string>

#include "runtime/memory/mem_counter.h"

namespace doris {

/*
 * can be consumed manually by consume()/release(), or put into SCOPED_CONSUME_MEM_TRACKER,
 * which will automatically track all memory usage of the code segment where it is located.
 *
 * This class is thread-safe.
*/
class MemTracker final {
public:
    MemTracker() = default;
    MemTracker(std::string label) : _label(std::move(label)) {};
    ~MemTracker() = default;

    void consume(int64_t bytes) { _mem_counter.add(bytes); }
    void consume_no_update_peak(int64_t bytes) { _mem_counter.add_no_update_peak(bytes); }
    void release(int64_t bytes) { _mem_counter.sub(bytes); }
    void set_consumption(int64_t bytes) { _mem_counter.set(bytes); }
    int64_t consumption() const { return _mem_counter.current_value(); }
    int64_t peak_consumption() const { return _mem_counter.peak_value(); }

    const std::string& label() const { return _label; }
    std::string log_usage() const {
        return fmt::format("MemTracker name={}, Used={}({} B), Peak={}({} B)", _label,
                           MemCounter::print_bytes(consumption()), consumption(),
                           MemCounter::print_bytes(peak_consumption()), peak_consumption());
    }

private:
    MemCounter _mem_counter;
    std::string _label {"None"};
};

} // namespace doris
