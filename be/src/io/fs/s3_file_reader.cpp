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

#include "io/fs/s3_file_reader.h"

#include <aws/core/http/URI.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <bvar/window.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <future>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/fs/err_utils.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/bvar_helper.h"
#include "util/concurrency_stats.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/s3_util.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"

namespace doris::io {

bvar::Adder<uint64_t> s3_file_reader_read_counter("s3_file_reader", "read_at");
bvar::Adder<uint64_t> s3_file_reader_total("s3_file_reader", "total_num");
bvar::Adder<uint64_t> s3_bytes_read_total("s3_file_reader", "bytes_read");
bvar::Adder<uint64_t> s3_file_being_read("s3_file_reader", "file_being_read");
bvar::Adder<uint64_t> s3_file_reader_read_active_counter("s3_file_reader", "read_active_num");
bvar::PerSecond<bvar::Adder<uint64_t>> s3_file_reader_read_active_qps(
        "s3_file_reader", "read_active_qps", &s3_file_reader_read_active_counter);
bvar::Adder<uint64_t> s3_file_reader_too_many_request_counter("s3_file_reader", "too_many_request");
bvar::LatencyRecorder s3_bytes_per_read("s3_file_reader", "bytes_per_read"); // also QPS
bvar::PerSecond<bvar::Adder<uint64_t>> s3_read_througthput("s3_file_reader", "s3_read_throughput",
                                                           &s3_bytes_read_total);
// Although we can get QPS from s3_bytes_per_read, but s3_bytes_per_read only
// record successfull request, and s3_get_request_qps will record all request.
bvar::PerSecond<bvar::Adder<uint64_t>> s3_get_request_qps("s3_file_reader", "s3_get_request",
                                                          &s3_file_reader_read_counter);

bvar::LatencyRecorder parallel_s3_reader_read_latency("parallel_s3_reader", "read_latency");

Result<FileReaderSPtr> S3FileReader::create(std::shared_ptr<const ObjClientHolder> client,
                                            std::string bucket, std::string key, int64_t file_size,
                                            RuntimeProfile* profile) {
    if (file_size < 0) {
        auto res = client->object_file_size(bucket, key);
        if (!res.has_value()) {
            return ResultError(std::move(res.error()));
        }

        file_size = res.value();
    }

    if (config::enable_s3_parallel_read) {
        return std::make_shared<ParallelS3FileReader>(std::move(client), std::move(bucket),
                                                      std::move(key), file_size, profile);
    }

    return std::make_shared<S3FileReader>(std::move(client), std::move(bucket), std::move(key),
                                          file_size, profile);
}

S3FileReader::S3FileReader(std::shared_ptr<const ObjClientHolder> client, std::string bucket,
                           std::string key, size_t file_size, RuntimeProfile* profile)
        : _path(fmt::format("s3://{}/{}", bucket, key)),
          _file_size(file_size),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _client(std::move(client)),
          _profile(profile) {
    DorisMetrics::instance()->s3_file_open_reading->increment(1);
    DorisMetrics::instance()->s3_file_reader_total->increment(1);
    s3_file_reader_total << 1;
    s3_file_being_read << 1;

    Aws::Http::SetCompliantRfc3986Encoding(true);
}

S3FileReader::~S3FileReader() {
    static_cast<void>(close());
    s3_file_being_read << -1;
}

Status S3FileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        DorisMetrics::instance()->s3_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status S3FileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                  const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::InternalError(
                "offset exceeds file size(offset: {}, file size: {}, path: {})", offset, _file_size,
                _path.native());
    }
    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    auto client = _client->get();
    if (!client) {
        return Status::InternalError("init s3 client error");
    }

    s3_file_reader_read_active_counter << 1;
    Defer _ = [&]() { s3_file_reader_read_active_counter << -1; };
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().s3_file_reader_read);

    int retry_count = 0;
    const int base_wait_time = config::s3_read_base_wait_time_ms; // Base wait time in milliseconds
    const int max_wait_time = config::s3_read_max_wait_time_ms; // Maximum wait time in milliseconds
    const int max_retries = config::max_s3_client_retry; // wait 1s, 2s, 4s, 8s for each backoff

    LIMIT_REMOTE_SCAN_IO(bytes_read);

    DBUG_EXECUTE_IF("S3FileReader::read_at_impl.io_slow", {
        auto sleep_time = dp->param("sleep", 3);
        LOG_INFO("S3FileReader::read_at_impl.io_slow inject sleep {} s", sleep_time)
                .tag("bucket", _bucket)
                .tag("key", _key);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
    });

    int total_sleep_time = 0;
    while (retry_count <= max_retries) {
        *bytes_read = 0;
        s3_file_reader_read_counter << 1;
        // clang-format off
        auto resp = client->get_object( { .bucket = _bucket, .key = _key, },
                to, offset, bytes_req, bytes_read);
        // clang-format on
        _s3_stats.total_get_request_counter++;
        if (resp.status.code != ErrorCode::OK) {
            if (resp.http_code ==
                static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS)) {
                s3_file_reader_too_many_request_counter << 1;
                retry_count++;
                int wait_time = std::min(base_wait_time * (1 << retry_count),
                                         max_wait_time); // Exponential backoff
                std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
                _s3_stats.too_many_request_err_counter++;
                _s3_stats.too_many_request_sleep_time_ms += wait_time;
                total_sleep_time += wait_time;
                continue;
            } else {
                // Handle other errors
                return std::move(Status(resp.status.code, std::move(resp.status.msg))
                                         .append("failed to read"));
            }
        }
        if (*bytes_read != bytes_req) {
            std::string msg = fmt::format(
                    "failed to get object, path={} offset={} bytes_req={} bytes_read={} "
                    "file_size={} tries={}",
                    _path.native(), offset, bytes_req, *bytes_read, _file_size, (retry_count + 1));
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        _s3_stats.total_bytes_read += bytes_req;
        s3_bytes_read_total << bytes_req;
        s3_bytes_per_read << bytes_req;
        DorisMetrics::instance()->s3_bytes_read_total->increment(bytes_req);
        if (retry_count > 0) {
            LOG(INFO) << fmt::format("read s3 file {} succeed after {} times with {} ms sleeping",
                                     _path.native(), retry_count, total_sleep_time);
        }
        return Status::OK();
    }
    std::string msg = fmt::format(
            "failed to get object, path={} offset={} bytes_req={} bytes_read={} file_size={} "
            "tries={}",
            _path.native(), offset, bytes_req, *bytes_read, _file_size, (max_retries + 1));
    LOG(WARNING) << msg;
    return Status::InternalError(msg);
}

void S3FileReader::_collect_profile_before_close() {
    if (_profile != nullptr) {
        const char* s3_profile_name = "S3Profile";
        ADD_TIMER(_profile, s3_profile_name);
        RuntimeProfile::Counter* total_get_request_counter =
                ADD_CHILD_COUNTER(_profile, "TotalGetRequest", TUnit::UNIT, s3_profile_name);
        RuntimeProfile::Counter* too_many_request_err_counter =
                ADD_CHILD_COUNTER(_profile, "TooManyRequestErr", TUnit::UNIT, s3_profile_name);
        RuntimeProfile::Counter* too_many_request_sleep_time = ADD_CHILD_COUNTER(
                _profile, "TooManyRequestSleepTime", TUnit::TIME_MS, s3_profile_name);
        RuntimeProfile::Counter* total_bytes_read =
                ADD_CHILD_COUNTER(_profile, "TotalBytesRead", TUnit::BYTES, s3_profile_name);

        COUNTER_UPDATE(total_get_request_counter, _s3_stats.total_get_request_counter);
        COUNTER_UPDATE(too_many_request_err_counter, _s3_stats.too_many_request_err_counter);
        COUNTER_UPDATE(too_many_request_sleep_time, _s3_stats.too_many_request_sleep_time_ms);
        COUNTER_UPDATE(total_bytes_read, _s3_stats.total_bytes_read);
    }
}

ParallelS3FileReader::ParallelS3FileReader(std::shared_ptr<const ObjClientHolder> client,
                                           std::string bucket, std::string key, size_t file_size,
                                           RuntimeProfile* profile)
        : S3FileReader(std::move(client), std::move(bucket), std::move(key), file_size, profile) {}

Status ParallelS3FileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                          const IOContext* io_ctx) {
    DCHECK(!closed());
    if (offset > _file_size) {
        return Status::InternalError(
                "offset exceeds file size(offset: {}, file size: {}, path: {})", offset, _file_size,
                _path.native());
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    MonotonicStopWatch watch;
    watch.start();
    Defer _ = [&]() { parallel_s3_reader_read_latency << watch.elapsed_time_microseconds(); };

    // If request size is smaller than chunk size, use base implementation
    size_t chunk_size = config::s3_parallel_read_chunk_size;
    if (bytes_req <= chunk_size) {
        return S3FileReader::read_at_impl(offset, result, bytes_read, io_ctx);
    }

    // Calculate number of chunks
    size_t num_chunks = (bytes_req + chunk_size - 1) / chunk_size;

    // Structure to hold chunk read information
    struct ChunkRead {
        size_t offset;
        size_t size;
        char* buffer;
        size_t bytes_read;
        Status status;
        std::shared_ptr<std::promise<void>> promise;
        std::future<void> future;
    };

    std::vector<ChunkRead> chunks(num_chunks);

    // Split the read into multiple chunks and initialize promise/future
    for (size_t i = 0; i < num_chunks; ++i) {
        size_t chunk_offset = offset + i * chunk_size;
        size_t chunk_bytes = std::min(chunk_size, bytes_req - i * chunk_size);

        chunks[i].offset = chunk_offset;
        chunks[i].size = chunk_bytes;
        chunks[i].buffer = to + i * chunk_size;
        chunks[i].bytes_read = 0;
        chunks[i].status = Status::OK();
        chunks[i].promise = std::make_shared<std::promise<void>>();
        chunks[i].future = chunks[i].promise->get_future();
    }

    // Get thread pool from ExecEnv
    ThreadPool* thread_pool = ExecEnv::GetInstance()->s3_parallel_read_thread_pool();
    if (!thread_pool) {
        // Fallback to base implementation if thread pool is not available
        return S3FileReader::read_at_impl(offset, result, bytes_read, io_ctx);
    }

    // Launch parallel reads
    for (size_t i = 0; i < num_chunks; ++i) {
        Status submit_status = thread_pool->submit_func([this, &chunks, i, io_ctx]() {
            Slice chunk_slice(chunks[i].buffer, chunks[i].size);
            chunks[i].status = S3FileReader::read_at_impl(chunks[i].offset, chunk_slice,
                                                          &chunks[i].bytes_read, io_ctx);
            chunks[i].promise->set_value();
        });

        if (!submit_status.ok()) {
            return Status::InternalError("Failed to submit parallel read task: {}",
                                         submit_status.to_string());
        }
    }

    // Wait for all reads to complete
    for (size_t i = 0; i < num_chunks; ++i) {
        chunks[i].future.get();
    }

    // Check status and aggregate bytes read
    *bytes_read = 0;
    for (size_t i = 0; i < num_chunks; ++i) {
        if (!chunks[i].status.ok()) {
            return Status::InternalError("Parallel read chunk {} failed: {}", i,
                                         chunks[i].status.to_string());
        }
        if (chunks[i].bytes_read != chunks[i].size) {
            std::string msg = fmt::format(
                    "parallel read chunk {} failed: expected {} bytes, got {} bytes, path={} "
                    "offset={}",
                    i, chunks[i].size, chunks[i].bytes_read, _path.native(), chunks[i].offset);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        *bytes_read += chunks[i].bytes_read;
    }

    return Status::OK();
}

} // namespace doris::io
