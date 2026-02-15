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

#include "io/fs/oss_file_reader.h"

#include <alibabacloud/oss/OssClient.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>
#include <utility>

#include "common/compiler_util.h"
#include "common/config.h"
#include "io/fs/err_utils.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/bvar_helper.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

namespace doris::io {

bvar::Adder<uint64_t> oss_file_reader_read_counter("oss_file_reader", "read_at");
bvar::Adder<uint64_t> oss_file_reader_total("oss_file_reader", "total_num");
bvar::Adder<uint64_t> oss_bytes_read_total("oss_file_reader", "bytes_read");
bvar::Adder<uint64_t> oss_file_being_read("oss_file_reader", "file_being_read");
bvar::LatencyRecorder oss_bytes_per_read("oss_file_reader", "bytes_per_read");
bvar::PerSecond<bvar::Adder<uint64_t>> oss_read_throughput("oss_file_reader", "oss_read_throughput",
                                                            &oss_bytes_read_total);
bvar::PerSecond<bvar::Adder<uint64_t>> oss_get_request_qps("oss_file_reader", "oss_get_request",
                                                            &oss_file_reader_read_counter);
bvar::LatencyRecorder oss_file_reader_latency("oss_file_reader", "oss_latency");

Result<FileReaderSPtr> OSSFileReader::create(std::shared_ptr<OSSClientHolder> client,
                                              std::string bucket, std::string key,
                                              int64_t file_size, RuntimeProfile* profile) {
    if (file_size < 0) {
        auto res = client->object_file_size(bucket, key);
        if (!res.has_value()) {
            return ResultError(std::move(res.error()));
        }
        file_size = res.value();
    }

    return std::make_shared<OSSFileReader>(std::move(client), std::move(bucket), std::move(key),
                                           file_size, profile);
}

OSSFileReader::OSSFileReader(std::shared_ptr<OSSClientHolder> client, std::string bucket,
                             std::string key, size_t file_size, RuntimeProfile* profile)
        : _path(fmt::format("oss://{}/{}", bucket, key)),
          _file_size(file_size),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _client(std::move(client)),
          _profile(profile) {
    DorisMetrics::instance()->s3_file_open_reading->increment(1);
    DorisMetrics::instance()->s3_file_reader_total->increment(1);
    oss_file_reader_total << 1;
    oss_file_being_read << 1;
}

OSSFileReader::~OSSFileReader() {
    static_cast<void>(close());
    oss_file_being_read << -1;
}

Status OSSFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->s3_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status OSSFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                   const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::InternalError(
                "offset exceeds file size(offset: {}, file size: {}, path: {})", offset,
                _file_size, _path.native());
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);

    VLOG_DEBUG << "OSS read_at_impl: offset=" << offset << " bytes_req=" << bytes_req
               << " result.size=" << result.size << " file_size=" << _file_size;

    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    int retry_count = 0;
    const int base_wait_time = config::s3_read_base_wait_time_ms;
    const int max_wait_time = config::s3_read_max_wait_time_ms;
    const int max_retries = config::max_s3_client_retry;

    int64_t begin_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    LIMIT_REMOTE_SCAN_IO(bytes_read);
    SCOPED_RAW_TIMER(&_oss_stats.total_get_request_time_ns);
    Defer defer_latency {[&]() {
        int64_t end_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        oss_file_reader_latency << (end_ts - begin_ts);
    }};

    int total_sleep_time = 0;
    while (retry_count <= max_retries) {
        *bytes_read = 0;
        oss_file_reader_read_counter << 1;

        AlibabaCloud::OSS::GetObjectRequest request(_bucket, _key);
        request.setRange(static_cast<int64_t>(offset),
                         static_cast<int64_t>(offset + bytes_req - 1));

        auto outcome = client->GetObject(request);
        _oss_stats.total_get_request_counter++;

        if (!outcome.isSuccess()) {
            std::string error_code = outcome.error().Code();

            // Handle rate limiting with exponential backoff
            if (error_code == "TooManyRequests" || error_code == "SlowDown") {
                retry_count++;
                if (retry_count > max_retries) {
                    std::string err_msg = fmt::format(
                            "OSS GetObject failed after {} retries for {}: {} - {}",
                            max_retries, _path.native(), error_code, outcome.error().Message());
                    LOG(WARNING) << err_msg;
                    return Status::IOError(err_msg);
                }

                // Exponential backoff with jitter to prevent thundering herd
                int wait_time = std::min(base_wait_time * (1 << retry_count), max_wait_time);
                int jitter = rand() % 100;
                wait_time += jitter;

                std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
                _oss_stats.too_many_request_err_counter++;
                _oss_stats.too_many_request_sleep_time_ms += wait_time;
                total_sleep_time += wait_time;

                VLOG_DEBUG << "OSS rate limited, retry " << retry_count << "/" << max_retries
                           << " after " << wait_time << "ms (jitter: " << jitter << "ms), path: "
                           << _path.native();
                continue;
            }

            // Other errors - fail immediately
            std::string err_msg = fmt::format("OSS GetObject failed for {}: {} - {}",
                                              _path.native(), error_code,
                                              outcome.error().Message());
            LOG(WARNING) << err_msg;
            return Status::IOError(err_msg);
        }

        // Read content stream into buffer
        auto& content_stream = outcome.result().Content();
        content_stream->read(to, bytes_req);
        *bytes_read = content_stream->gcount();

        if (*bytes_read != bytes_req) {
            std::string msg = fmt::format(
                    "OSS read size mismatch: path={} offset={} bytes_req={} bytes_read={} "
                    "file_size={}",
                    _path.native(), offset, bytes_req, *bytes_read, _file_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }

        _oss_stats.total_bytes_read += bytes_req;
        oss_bytes_read_total << bytes_req;
        oss_bytes_per_read << bytes_req;
        DorisMetrics::instance()->s3_bytes_read_total->increment(bytes_req);

        if (retry_count > 0) {
            LOG(INFO) << fmt::format(
                    "OSS read {} succeeded after {} retries with {} ms sleeping", _path.native(),
                    retry_count, total_sleep_time);
        }

        return Status::OK();
    }

    return Status::IOError("OSS read failed: max retries exceeded");
}

} // namespace doris::io
