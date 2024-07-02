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

#include "pipeline_tracing.h"

#include <absl/time/clock.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <boost/algorithm/string/predicate.hpp>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "io/fs/local_file_writer.h"
#include "util/time.h"

namespace doris::pipeline {

void PipelineTracerContext::record(ScheduleRecord record) {
    if (_dump_type == RecordType::None) [[unlikely]] {
        return;
    }

    auto map_ptr = std::atomic_load_explicit(&_data, std::memory_order_relaxed);
    auto it = map_ptr->find({record.query_id});
    if (it != map_ptr->end()) {
        it->second->enqueue(record);
    } else {
        _update([&](QueryTracesMap& new_map) {
            if (!new_map.contains({record.query_id})) {
                new_map[{record.query_id}].reset(new OneQueryTraces());
            }
            new_map[{record.query_id}]->enqueue(record);
        });
    }
}

void PipelineTracerContext::_update(std::function<void(QueryTracesMap&)>&& handler) {
    auto map_ptr = std::atomic_load_explicit(&_data, std::memory_order_relaxed);
    while (true) {
        auto new_map = std::make_shared<QueryTracesMap>(*map_ptr);
        handler(*new_map);
        if (std::atomic_compare_exchange_strong_explicit(&_data, &map_ptr, new_map,
                                                         std::memory_order_relaxed,
                                                         std::memory_order_relaxed)) {
            break;
        }
    }
}

void PipelineTracerContext::end_query(TUniqueId query_id, uint64_t workload_group) {
    {
        std::unique_lock<std::mutex> l(_tg_lock);
        _id_to_workload_group[query_id] = workload_group;
    }
    if (_dump_type == RecordType::PerQuery) {
        _dump_query(query_id);
    } else if (_dump_type == RecordType::Periodic) {
        auto now = MonotonicSeconds();
        auto interval = now - _last_dump_time;
        if (interval > _dump_interval_s) {
            _dump_timeslice();
        }
    }
}

Status PipelineTracerContext::change_record_params(
        const std::map<std::string, std::string>& params) {
    bool effective = false;
    if (auto it = params.find("type"); it != params.end()) {
        if (boost::iequals(it->second, "disable") || boost::iequals(it->second, "none")) {
            _dump_type = RecordType::None;
            effective = true;
        } else if (boost::iequals(it->second, "per_query") ||
                   boost::iequals(it->second, "perquery")) {
            _dump_type = RecordType::PerQuery;
            effective = true;
        } else if (boost::iequals(it->second, "periodic")) {
            _dump_type = RecordType::Periodic;
            _last_dump_time = MonotonicSeconds();
            effective = true;
        }
    }

    if (auto it = params.find("dump_interval"); it != params.end()) {
        _dump_interval_s = std::stoll(it->second); // s as unit
        effective = true;
    }

    return effective ? Status::OK()
                     : Status::InvalidArgument(
                               "No qualified param in changing tracing record method");
}

void PipelineTracerContext::_dump_query(TUniqueId query_id) {
    auto map_ptr = std::atomic_load_explicit(&_data, std::memory_order_relaxed);
    auto path = _log_dir / fmt::format("query{}", to_string(query_id));
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                    S_ISGID | S_ISUID | S_IWUSR | S_IRUSR | S_IWGRP | S_IRGRP | S_IWOTH | S_IROTH);
    if (fd < 0) [[unlikely]] {
        throw Exception(Status::Error<ErrorCode::CREATE_FILE_ERROR>(
                "create tracing log file {} failed", path.c_str()));
    }
    auto writer = io::LocalFileWriter {path, fd};

    ScheduleRecord record;
    while ((*map_ptr)[QueryID {query_id}]->try_dequeue(record)) {
        uint64_t v = 0;
        {
            std::unique_lock<std::mutex> l(_tg_lock);
            v = _id_to_workload_group.at(query_id);
        }
        auto tmp_str = record.to_string(v);
        auto text = Slice {tmp_str};
        THROW_IF_ERROR(writer.appendv(&text, 1));
    }

    THROW_IF_ERROR(writer.close());

    _last_dump_time = MonotonicSeconds();

    _update([&](QueryTracesMap& new_map) { _data->erase(QueryID {query_id}); });

    {
        std::unique_lock<std::mutex> l(_tg_lock);
        _id_to_workload_group.erase(query_id);
    }
}

void PipelineTracerContext::_dump_timeslice() {
    auto new_map = std::make_shared<QueryTracesMap>();
    new_map.swap(_data);
    //TODO: if long time, per timeslice per file
    auto path = _log_dir /
                fmt::format("until{}", std::chrono::steady_clock::now().time_since_epoch().count());
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                    S_ISGID | S_ISUID | S_IWUSR | S_IRUSR | S_IWGRP | S_IRGRP | S_IWOTH | S_IROTH);
    if (fd < 0) [[unlikely]] {
        throw Exception(Status::Error<ErrorCode::CREATE_FILE_ERROR>(
                "create tracing log file {} failed", path.c_str()));
    }
    auto writer = io::LocalFileWriter {path, fd};

    // dump all query traces in this time window to one file.
    for (auto& [query_id, trace] : (*new_map)) {
        ScheduleRecord record;
        while (trace->try_dequeue(record)) {
            uint64_t v = 0;
            {
                std::unique_lock<std::mutex> l(_tg_lock);
                v = _id_to_workload_group.at(query_id.query_id);
            }
            auto tmp_str = record.to_string(v);
            auto text = Slice {tmp_str};
            THROW_IF_ERROR(writer.appendv(&text, 1));
        }
    }
    THROW_IF_ERROR(writer.close());

    _last_dump_time = MonotonicSeconds();

    _id_to_workload_group.clear();
}
} // namespace doris::pipeline
