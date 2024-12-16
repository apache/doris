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
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "io/fs/err_utils.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_common.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/bvar_helper.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/s3_util.h"

namespace doris::io {

bvar::Adder<uint64_t> s3_file_reader_read_counter("s3_file_reader", "read_at");
bvar::Adder<uint64_t> s3_file_reader_total("s3_file_reader", "total_num");
bvar::Adder<uint64_t> s3_bytes_read_total("s3_file_reader", "bytes_read");
bvar::Adder<uint64_t> s3_file_being_read("s3_file_reader", "file_being_read");
bvar::Adder<uint64_t> s3_file_reader_too_many_request_counter("s3_file_reader", "too_many_request");
bvar::LatencyRecorder s3_bytes_per_read("s3_file_reader", "bytes_per_read"); // also QPS
bvar::PerSecond<bvar::Adder<uint64_t>> s3_read_througthput("s3_file_reader", "s3_read_throughput",
                                                           &s3_bytes_read_total);
// Although we can get QPS from s3_bytes_per_read, but s3_bytes_per_read only
// record successfull request, and s3_get_request_qps will record all request.
bvar::PerSecond<bvar::Adder<uint64_t>> s3_get_request_qps("s3_file_reader", "s3_get_request",
                                                          &s3_file_reader_read_counter);

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

    int retry_count = 0;
    const int base_wait_time = config::s3_read_base_wait_time_ms; // Base wait time in milliseconds
    const int max_wait_time = config::s3_read_max_wait_time_ms; // Maximum wait time in milliseconds
    const int max_retries = config::max_s3_client_retry; // wait 1s, 2s, 4s, 8s for each backoff

    LIMIT_REMOTE_SCAN_IO(bytes_read);

    int total_sleep_time = 0;
    while (retry_count <= max_retries) {
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
            return Status::InternalError("failed to read (bytes read: {}, bytes req: {})",
                                         *bytes_read, bytes_req);
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
    return Status::InternalError("failed to read from s3, exceeded maximum retries");
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

} // namespace doris::io
