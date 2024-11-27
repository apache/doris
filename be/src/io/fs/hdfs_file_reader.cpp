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

#include "bvar/latency_recorder.h"
#include "bvar/reducer.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_util.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"

namespace doris::io {

bvar::Adder<uint64_t> hdfs_bytes_read_total("hdfs_file_reader", "bytes_read");
bvar::LatencyRecorder hdfs_bytes_per_read("hdfs_file_reader", "bytes_per_read"); // also QPS
bvar::PerSecond<bvar::Adder<uint64_t>> hdfs_read_througthput("hdfs_file_reader",
                                                             "hdfs_read_throughput",
                                                             &hdfs_bytes_read_total);

namespace {

Result<FileHandleCache::Accessor> get_file(const hdfsFS& fs, const Path& file, int64_t mtime,
                                           int64_t file_size) {
    static FileHandleCache cache(config::max_hdfs_file_handle_cache_num, 16,
                                 config::max_hdfs_file_handle_cache_time_sec * 1000L);
    bool cache_hit;
    FileHandleCache::Accessor accessor;
    RETURN_IF_ERROR_RESULT(cache.get_file_handle(fs, file.native(), mtime, file_size, false,
                                                 &accessor, &cache_hit));
    return accessor;
}

} // namespace

Result<FileReaderSPtr> HdfsFileReader::create(Path full_path, const hdfsFS& fs, std::string fs_name,
                                              const FileReaderOptions& opts,
                                              RuntimeProfile* profile) {
    auto path = convert_path(full_path, fs_name);
    return get_file(fs, path, opts.mtime, opts.file_size).transform([&](auto&& accessor) {
        return std::make_shared<HdfsFileReader>(std::move(path), std::move(fs_name),
                                                std::move(accessor), profile);
    });
}

HdfsFileReader::HdfsFileReader(Path path, std::string fs_name, FileHandleCache::Accessor accessor,
                               RuntimeProfile* profile)
        : _path(std::move(path)),
          _fs_name(std::move(fs_name)),
          _accessor(std::move(accessor)),
          _profile(profile) {
    _handle = _accessor.get();

    DorisMetrics::instance()->hdfs_file_open_reading->increment(1);
    DorisMetrics::instance()->hdfs_file_reader_total->increment(1);
    if (_profile != nullptr && is_hdfs(_fs_name)) {
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
    }
    return Status::OK();
}

#ifdef USE_HADOOP_HDFS
Status HdfsFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    if (closed()) [[unlikely]] {
        return Status::InternalError("read closed file: {}", _path.native());
    }

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

    LIMIT_REMOTE_SCAN_IO(bytes_read);

    size_t has_read = 0;
    while (has_read < bytes_req) {
        tSize loop_read = hdfsPread(_handle->fs(), _handle->file(), offset + has_read,
                                    to + has_read, bytes_req - has_read);
        {
            [[maybe_unused]] Status error_ret;
            TEST_INJECTION_POINT_RETURN_WITH_VALUE("HdfsFileReader:read_error", error_ret);
        }
        if (loop_read < 0) {
            // invoker maybe just skip Status.NotFound and continue
            // so we need distinguish between it and other kinds of errors
            std::string _err_msg = hdfs_error();
            if (_err_msg.find("No such file or directory") != std::string::npos) {
                return Status::NotFound(_err_msg);
            }
            return Status::InternalError(
                    "Read hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                    BackendOptions::get_localhost(), _fs_name, _path.string(), _err_msg);
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    hdfs_bytes_read_total << *bytes_read;
    hdfs_bytes_per_read << *bytes_read;
    return Status::OK();
}

#else
// The hedged read only support hdfsPread().
// TODO: rethink here to see if there are some difference between hdfsPread() and hdfsRead()
Status HdfsFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    if (closed()) [[unlikely]] {
        return Status::InternalError("read closed file: ", _path.native());
    }

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

    LIMIT_REMOTE_SCAN_IO(bytes_read);

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
                    BackendOptions::get_localhost(), _fs_name, _path.string(), _err_msg);
        }
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    *bytes_read = has_read;
    hdfs_bytes_read_total << *bytes_read;
    hdfs_bytes_per_read << *bytes_read;
    return Status::OK();
}
#endif

void HdfsFileReader::_collect_profile_before_close() {
    if (_profile != nullptr && is_hdfs(_fs_name)) {
#ifdef USE_HADOOP_HDFS
        struct hdfsReadStatistics* hdfs_statistics = nullptr;
        auto r = hdfsFileGetReadStatistics(_handle->file(), &hdfs_statistics);
        if (r != 0) {
            LOG(WARNING) << "Failed to run hdfsFileGetReadStatistics(): " << r
                         << ", name node: " << _fs_name;
            return;
        }
        COUNTER_UPDATE(_hdfs_profile.total_bytes_read, hdfs_statistics->totalBytesRead);
        COUNTER_UPDATE(_hdfs_profile.total_local_bytes_read, hdfs_statistics->totalLocalBytesRead);
        COUNTER_UPDATE(_hdfs_profile.total_short_circuit_bytes_read,
                       hdfs_statistics->totalShortCircuitBytesRead);
        COUNTER_UPDATE(_hdfs_profile.total_total_zero_copy_bytes_read,
                       hdfs_statistics->totalZeroCopyBytesRead);
        hdfsFileFreeReadStatistics(hdfs_statistics);

        struct hdfsHedgedReadMetrics* hdfs_hedged_read_statistics = nullptr;
        r = hdfsGetHedgedReadMetrics(_handle->fs(), &hdfs_hedged_read_statistics);
        if (r != 0) {
            LOG(WARNING) << "Failed to run hdfsGetHedgedReadMetrics(): " << r
                         << ", name node: " << _fs_name;
            return;
        }

        COUNTER_UPDATE(_hdfs_profile.total_hedged_read, hdfs_hedged_read_statistics->hedgedReadOps);
        COUNTER_UPDATE(_hdfs_profile.hedged_read_in_cur_thread,
                       hdfs_hedged_read_statistics->hedgedReadOpsInCurThread);
        COUNTER_UPDATE(_hdfs_profile.hedged_read_wins,
                       hdfs_hedged_read_statistics->hedgedReadOpsWin);

        hdfsFreeHedgedReadMetrics(hdfs_hedged_read_statistics);
        hdfsFileClearReadStatistics(_handle->file());
#endif
    }
}

} // namespace doris::io
