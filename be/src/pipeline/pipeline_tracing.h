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

#include <concurrentqueue.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <parallel_hashmap/phmap.h>

#include <cstdint>

#include "common/config.h"
#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris::pipeline {

struct ScheduleRecord {
    TUniqueId query_id;
    std::string task_id;
    uint32_t core_id;
    uint64_t thread_id;
    uint64_t start_time;
    uint64_t end_time;
    std::string_view state_name;

    bool operator<(const ScheduleRecord& rhs) const { return start_time < rhs.start_time; }
    std::string to_string(uint64_t append_value) const {
        return fmt::format("{}|{}|{}|{}|{}|{}|{}|{}\n", doris::to_string(query_id), task_id,
                           core_id, thread_id, start_time, end_time, state_name, append_value);
    }
};

// all tracing datas of ONE specific query
using OneQueryTraces = moodycamel::ConcurrentQueue<ScheduleRecord>;

// belongs to exec_env, for all query, if enabled
class PipelineTracerContext {
public:
    enum class RecordType {
        None,     // disable
        PerQuery, // record per query. one query one file.
        Periodic  // record per times. one timeslice one file.
    };
    void record(ScheduleRecord record); // record one schedule record
    void end_query(TUniqueId query_id,
                   uint64_t workload_group); // tell context this query is end. may leads to dump.
    Status change_record_params(const std::map<std::string, std::string>& params);

    bool enabled() const { return !(_dump_type == RecordType::None); }

private:
    // dump data to disk. one query or all.
    void _dump_query(TUniqueId query_id);
    void _dump_timeslice();

    std::mutex _data_lock; // lock for map, not map items.
    phmap::flat_hash_map<TUniqueId, OneQueryTraces> _datas;
    std::mutex _tg_lock; //TODO: use an lockfree DS
    phmap::flat_hash_map<TUniqueId, uint64_t>
            _id_to_workload_group; // save query's workload group number

    RecordType _dump_type = RecordType::None;
    decltype(MonotonicSeconds()) _last_dump_time;
    decltype(MonotonicSeconds()) _dump_interval_s =
            60; // effective iff Periodic mode. 1 minute default.
};
} // namespace doris::pipeline
