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

#include "io/fs/hdfs_file_reader.h"

#include <stdint.h>

#include <algorithm>
#include <filesystem>
#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "io/fs/err_utils.h"
// #include "io/fs/hdfs_file_system.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/hdfs_util.h"

namespace doris {
namespace io {

HdfsFileReader::HdfsFileReader(Path path, const std::string& name_node,
                               FileHandleCache::Accessor accessor, RuntimeProfile* profile)
        : _path(std::move(path)),
          _name_node(name_node),
          _accessor(std::move(accessor)),
          _profile(profile) {
    _handle = _accessor.get();

    DorisMetrics::instance()->hdfs_file_open_reading->increment(1);
    DorisMetrics::instance()->hdfs_file_reader_total->increment(1);
    if (_profile != nullptr && is_hdfs(_name_node)) {
#ifdef USE_HADOOP_HDFS
        const char* hdfs_profile_name = "HdfsIO";
        ADD_TIMER(_profile, hdfs_profile_name);
        _hdfs_profile.total_bytes_read =
                ADD_CHILD_COUNTER(_profile, "TotalBytesRead", TUnit::BYTES, hdfs_profile_name);
        _hdfs_profile.total_local_bytes_read =
                ADD_CHILD_COUNTER(_profile, "TotalLocalBytesRead", TUnit::BYTES, hdfs_profile_name);
        _hdfs_profile.total_short_circuit_bytes_read = ADD_CHILD_COUNTER(
                _profile, "TotalShortCircuitBytesRead", TUnit::BYTES, hdfs_profile_name);
        _hdfs_profile.total_total_zero_copy_bytes_read = ADD_CHILD_COUNTER(
                _profile, "TotalZeroCopyBytesRead", TUnit::BYTES, hdfs_profile_name);

        _hdfs_profile.total_hedged_read =
                ADD_CHILD_COUNTER(_profile, "TotalHedgedRead", TUnit::UNIT, hdfs_profile_name);
        _hdfs_profile.hedged_read_in_cur_thread = ADD_CHILD_COUNTER(
                _profile, "HedgedReadInCurThread", TUnit::UNIT, hdfs_profile_name);
        _hdfs_profile.hedged_read_wins =
                ADD_CHILD_COUNTER(_profile, "HedgedReadWins", TUnit::UNIT, hdfs_profile_name);
#endif
    }
}

HdfsFileReader::~HdfsFileReader() {
    static_cast<void>(close());
}

Status HdfsFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->hdfs_file_open_reading->increment(-1);
        if (_profile != nullptr && is_hdfs(_name_node)) {
#ifdef USE_HADOOP_HDFS
            struct hdfsReadStatistics* hdfs_statistics = nullptr;
            auto r = hdfsFileGetReadStatistics(_handle->file(), &hdfs_statistics);
            if (r != 0) {
                return Status::InternalError(
                        fmt::format("Failed to run hdfsFileGetReadStatistics(): {}", r));
            }
            COUNTER_UPDATE(_hdfs_profile.total_bytes_read, hdfs_statistics->totalBytesRead);
            COUNTER_UPDATE(_hdfs_profile.total_local_bytes_read,
                           hdfs_statistics->totalLocalBytesRead);
            COUNTER_UPDATE(_hdfs_profile.total_short_circuit_bytes_read,
                           hdfs_statistics->totalShortCircuitBytesRead);
            COUNTER_UPDATE(_hdfs_profile.total_total_zero_copy_bytes_read,
                           hdfs_statistics->totalZeroCopyBytesRead);
            hdfsFileFreeReadStatistics(hdfs_statistics);

            struct hdfsHedgedReadMetrics* hdfs_hedged_read_statistics = nullptr;
            r = hdfsGetHedgedReadMetrics(_handle->fs(), &hdfs_hedged_read_statistics);
            if (r != 0) {
                return Status::InternalError(
                        fmt::format("Failed to run hdfsGetHedgedReadMetrics(): {}", r));
            }

            COUNTER_UPDATE(_hdfs_profile.total_hedged_read,
                           hdfs_hedged_read_statistics->hedgedReadOps);
            COUNTER_UPDATE(_hdfs_profile.hedged_read_in_cur_thread,
                           hdfs_hedged_read_statistics->hedgedReadOpsInCurThread);
            COUNTER_UPDATE(_hdfs_profile.hedged_read_wins,
                           hdfs_hedged_read_statistics->hedgedReadOpsWin);

            hdfsFreeHedgedReadMetrics(hdfs_hedged_read_statistics);
            hdfsFileClearReadStatistics(_handle->file());
#endif
        }
    }
    return Status::OK();
}

#ifdef USE_HADOOP_HDFS
Status HdfsFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    if (offset > _handle->file_size()) {
        return Status::IOError("offset exceeds file size(offset: {}, file size: {}, path: {})",
                               offset, _handle->file_size(), _path.native());
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, (size_t)(_handle->file_size() - offset));
    *bytes_read = 0;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }

    size_t has_read = 0;
    while (has_read < bytes_req) {
        tSize loop_read = hdfsPread(_handle->fs(), _handle->file(), offset + has_read,
                                    to + has_read, bytes_req - has_read);
        if (loop_read < 0) {
            // invoker maybe just skip Status.NotFound and continue
            // so we need distinguish between it and other kinds of errors
            std::string _err_msg = hdfs_error();
            if (_err_msg.find("No such file or directory") != std::string::npos) {
                return Status::NotFound(_err_msg);
            }
            return Status::InternalError(
                    "Read hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                    BackendOptions::get_localhost(), _name_node, _path.string(), _err_msg);
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    return Status::OK();
}

#else
// The hedged read only support hdfsPread().
// TODO: rethink here to see if there are some difference between hdfsPread() and hdfsRead()
Status HdfsFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    if (offset > _handle->file_size()) {
        return Status::IOError("offset exceeds file size(offset: {}, file size: {}, path: {})",
                               offset, _handle->file_size(), _path.native());
    }

    int res = hdfsSeek(_handle->fs(), _handle->file(), offset);
    if (res != 0) {
        // invoker maybe just skip Status.NotFound and continue
        // so we need distinguish between it and other kinds of errors
        std::string _err_msg = hdfs_error();
        if (_err_msg.find("No such file or directory") != std::string::npos) {
            return Status::NotFound(_err_msg);
        }
        return Status::InternalError("Seek to offset failed. (BE: {}) offset={}, err: {}",
                                     BackendOptions::get_localhost(), offset, _err_msg);
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, (size_t)(_handle->file_size() - offset));
    *bytes_read = 0;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }

    size_t has_read = 0;
    while (has_read < bytes_req) {
        int64_t loop_read =
                hdfsRead(_handle->fs(), _handle->file(), to + has_read, bytes_req - has_read);
        if (loop_read < 0) {
            // invoker maybe just skip Status.NotFound and continue
            // so we need distinguish between it and other kinds of errors
            std::string _err_msg = hdfs_error();
            if (_err_msg.find("No such file or directory") != std::string::npos) {
                return Status::NotFound(_err_msg);
            }
            return Status::InternalError(
                    "Read hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                    BackendOptions::get_localhost(), _name_node, _path.string(), _err_msg);
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    return Status::OK();
}
#endif
} // namespace io
} // namespace doris
