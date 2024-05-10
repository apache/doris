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
    if (_datas.contains(record.query_id)) {
        _datas[record.query_id].enqueue(record);
    } else {
        // dump per timeslice may cause this. lead perv records broken. that's acceptable
        std::unique_lock<std::mutex> l(_data_lock); // add new item, may rehash
        _datas[record.query_id].enqueue(record);
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
    //TODO: when dump, now could append records but can't add new query. try use better grained locks.
    std::unique_lock<std::mutex> l(_data_lock); // can't rehash
    auto path = _log_dir / fmt::format("query{}", to_string(query_id));
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                    S_ISGID | S_ISUID | S_IWUSR | S_IRUSR | S_IWGRP | S_IRGRP | S_IWOTH | S_IROTH);
    if (fd < 0) [[unlikely]] {
        throw Exception(Status::Error<ErrorCode::CREATE_FILE_ERROR>(
                "create tracing log file {} failed", path.c_str()));
    }
    auto writer = io::LocalFileWriter {path, fd};

    ScheduleRecord record;
    while (_datas[query_id].try_dequeue(record)) {
        uint64_t v = 0;
        {
            std::unique_lock<std::mutex> l(_tg_lock);
            v = _id_to_workload_group.at(query_id);
        }
        auto tmp_str = record.to_string(v);
        auto text = Slice {tmp_str};
        THROW_IF_ERROR(writer.appendv(&text, 1));
    }

    THROW_IF_ERROR(writer.finalize());
    THROW_IF_ERROR(writer.close());

    _last_dump_time = MonotonicSeconds();

    _datas.erase(query_id);
    {
        std::unique_lock<std::mutex> l(_tg_lock);
        _id_to_workload_group.erase(query_id);
    }
}

void PipelineTracerContext::_dump_timeslice() {
    std::unique_lock<std::mutex> l(_data_lock); // can't rehash

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
    for (auto& [query_id, trace] : _datas) {
        ScheduleRecord record;
        while (trace.try_dequeue(record)) {
            uint64_t v = 0;
            {
                std::unique_lock<std::mutex> l(_tg_lock);
                v = _id_to_workload_group.at(query_id);
            }
            auto tmp_str = record.to_string(v);
            auto text = Slice {tmp_str};
            THROW_IF_ERROR(writer.appendv(&text, 1));
        }
    }
    THROW_IF_ERROR(writer.finalize());
    THROW_IF_ERROR(writer.close());

    _last_dump_time = MonotonicSeconds();

    _datas.clear();
    _id_to_workload_group.clear();
}
} // namespace doris::pipeline
