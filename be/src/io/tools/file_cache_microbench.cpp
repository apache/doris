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
#include "io/cache/file_cache_common.h"
#if defined(BE_TEST) && defined(BUILD_FILE_CACHE_MICROBENCH_TOOL)
#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/server.h>
#include <brpc/uri.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem> // Add this header file
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "build/proto/microbench.pb.h"
#include "cloud/cloud_internal_service.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/obj-storage/rate_limit/token_bucket_rate_limiter.h"
#include "gflags/gflags.h"
#include "io/cache/async_cache_write_manager.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/cache/peer_file_cache_reader.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/fs/obj_storage_file_writer.h"
#include "io/object_storage/be_obj_storage_config_parser.h"
#include "io/object_storage/obj_storage_rate_limiter_manager.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/options.h"
#include "storage/utils.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/s3_uri.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#endif

#include "runtime/exec_env.h"

#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <gen_cpp/cloud_version.h>

#include "util/bvar_helper.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/string_util.h"
#include "util/threadpool.h"

using doris::io::FileCacheFactory;
using doris::io::BlockFileCache;
using namespace doris;

bvar::LatencyRecorder microbench_write_latency("file_cache_microbench_append");
bvar::LatencyRecorder microbench_read_latency("file_cache_microbench_read_at");
bvar::LatencyRecorder microbench_get_or_set_latency("file_cache_microbench_get_or_set");
bvar::LatencyRecorder microbench_peer_seed_latency("file_cache_microbench_peer_seed");
bvar::LatencyRecorder microbench_peer_read_latency("file_cache_microbench_peer_read");

const std::string HIDDEN_PREFIX = "test_file_cache_microbench/";
const char PAD_CHAR = 'x';
const size_t BUFFER_SIZE = 1024 * 1024;
// Just 10^9.
static constexpr auto NS = 1000000000UL;

static std::string normalize_benchmark_prefix(std::string_view raw_prefix) {
    std::string normalized {doris::trim(raw_prefix)};
    while (!normalized.empty() && normalized.front() == '/') {
        normalized.erase(normalized.begin());
    }
    while (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    return normalized;
}

static std::string get_prefix() {
    std::string prefix = HIDDEN_PREFIX;
    std::string subdir = normalize_benchmark_prefix(doris::config::test_s3_prefix);
    if (!subdir.empty()) {
        prefix += subdir;
        prefix += "/";
    }
    return prefix;
}

DEFINE_int32(port, 8888, "Http Port of this server");
DEFINE_int32(brpc_port, 0,
             "Optional BRPC port for peer cache server (CloudInternalServiceImpl). 0 disables it.");

static std::string build_info() {
    std::stringstream ss;
    ss << R"(
    Version: {)";
    ss << DORIS_CLOUD_BUILD_VERSION;

#if defined(NDEBUG)
    ss << R"(-release})";
#else
    ss << R"(-debug})";
#endif

    ss << R"(
    Code_version: {commit=)" DORIS_CLOUD_BUILD_HASH R"( time=)" DORIS_CLOUD_BUILD_VERSION_TIME R"(
    Build_info: {initiator=)" DORIS_CLOUD_BUILD_INITIATOR R"( build_at=)" DORIS_CLOUD_BUILD_TIME R"(
    Build_on: )" DORIS_CLOUD_BUILD_OS_VERSION R"(})";
    return ss.str();
}

// Modify DataGenerator class to generate more standard data blocks
class DataGenerator {
public:
    DataGenerator(size_t total_size) : _total_size(total_size), _generated_size(0) {
        _buffer.resize(BUFFER_SIZE);
    }

    // Get the next chunk of data
    doris::Slice next_chunk(const std::string& key) {
        if (_generated_size >= _total_size) {
            // Return an empty slice to indicate the end
            return doris::Slice();
        }

        size_t remaining = _total_size - _generated_size;
        size_t chunk_size = std::min(remaining, BUFFER_SIZE);

        // Generate the tag for this block
        std::string tag = fmt::format("key={},offset={}\n", key, _generated_size);
        size_t tag_size = tag.size();

        // Ensure chunk_size is not less than tag_size
        if (chunk_size < tag_size) {
            std::memcpy(_buffer.data(), tag.data(), chunk_size);
        } else {
            // Fill the buffer with key:offset
            std::memcpy(_buffer.data(), tag.data(), tag_size);
            // Fill the remaining part
            std::fill(_buffer.data() + tag_size, _buffer.data() + chunk_size, PAD_CHAR);
        }

        _generated_size += chunk_size;
        return doris::Slice(_buffer.data(), chunk_size);
    }

    bool has_more() const { return _generated_size < _total_size; }

private:
    const size_t _total_size;
    size_t _generated_size;
    std::vector<char> _buffer;
};

class DataVerifier {
public:
    static bool verify_data(const std::string& key, size_t file_size, size_t read_offset,
                            const std::string& data, size_t data_size) {
        size_t current_block_start = (read_offset / BUFFER_SIZE) * BUFFER_SIZE;
        size_t data_pos = 0;

        while (data_pos < data_size) {
            // Calculate the offset in the current block
            size_t block_offset = read_offset + data_pos - current_block_start;

            // Check if it exceeds the total file size
            if (current_block_start >= file_size) {
                break;
            }

            // Generate the expected tag
            std::string expected_tag = fmt::format("key={},offset={}\n", key, current_block_start);

            // If within the tag range, need to verify the tag
            if (block_offset < expected_tag.size()) {
                // Calculate the length of the tag that can be read in the current data
                size_t available_tag_len =
                        std::min(expected_tag.size() - block_offset, data_size - data_pos);

                // If already at the end of the file, only verify the actual existing data
                if (read_offset + data_pos + available_tag_len > file_size) {
                    available_tag_len = file_size - (read_offset + data_pos);
                }

                if (available_tag_len == 0) break;
                std::string_view actual_tag(data.data() + data_pos, available_tag_len);
                std::string_view expected_tag_part(expected_tag.data() + block_offset,
                                                   available_tag_len);

                if (actual_tag != expected_tag_part) {
                    LOG(ERROR) << "Tag mismatch at offset " << (read_offset + data_pos)
                               << "\nExpected: " << expected_tag_part << "\nGot: " << actual_tag;
                    return false;
                }
                data_pos += available_tag_len;
            } else {
                char expected_byte = static_cast<char>(PAD_CHAR);
                if (data[data_pos] != expected_byte) {
                    LOG(ERROR) << "Data mismatch at offset " << (read_offset + data_pos)
                               << "\nExpected byte: " << (char)expected_byte
                               << "\nGot byte: " << (char)data[data_pos];
                    return false;
                }
                data_pos++;
            }

            // If reaching the end of the block, move to the next block
            if ((read_offset + data_pos) % BUFFER_SIZE == 0) {
                current_block_start += BUFFER_SIZE;
            }
        }

        return true;
    }
};

static void fill_deterministic_data(const std::string& key, size_t file_size, size_t offset,
                                    char* dst, size_t len) {
    size_t written = 0;
    while (written < len && offset + written < file_size) {
        size_t abs_pos = offset + written;
        size_t chunk_start = (abs_pos / BUFFER_SIZE) * BUFFER_SIZE;
        size_t chunk_end = std::min(chunk_start + BUFFER_SIZE, file_size);
        size_t chunk_offset = abs_pos - chunk_start;
        size_t remain_in_chunk = std::min(len - written, chunk_end - abs_pos);

        std::string tag = fmt::format("key={},offset={}\n", key, chunk_start);
        if (chunk_offset < tag.size()) {
            size_t tag_copy = std::min(tag.size() - chunk_offset, remain_in_chunk);
            std::memcpy(dst + written, tag.data() + chunk_offset, tag_copy);
            written += tag_copy;
            remain_in_chunk -= tag_copy;
        }
        if (remain_in_chunk > 0) {
            std::memset(dst + written, PAD_CHAR, remain_in_chunk);
            written += remain_in_chunk;
        }
    }
}

class PeerOnlyFallbackFileReader final : public doris::io::FileReader {
public:
    PeerOnlyFallbackFileReader(doris::io::Path path, size_t size)
            : _path(std::move(path)), _size(size) {}

    doris::Status close() override {
        _closed.store(true, std::memory_order_relaxed);
        return doris::Status::OK();
    }

    const doris::io::Path& path() const override { return _path; }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed.load(std::memory_order_relaxed); }

    int64_t mtime() const override { return 0; }

protected:
    doris::Status read_at_impl(size_t offset, doris::Slice result, size_t* bytes_read,
                               const doris::io::IOContext* io_ctx) override {
        return doris::Status::NotSupported(
                "Remote S3 fallback is disabled in peer_read job (set use_s3_fallback=true to "
                "enable S3 fallback): path={}, offset={}, size={}",
                _path.native(), offset, result.size);
    }

private:
    doris::io::Path _path;
    size_t _size;
    std::atomic<bool> _closed {false};
};

// Define a struct to store file information
struct FileInfo {
    std::string filename; // File name
    size_t data_size;     // Data size
    std::string job_id;   // Associated job ID
};

class S3FileRecords {
public:
    void add_file_info(const std::string& source_id, const ::FileInfo& file_info) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& files = records_[source_id];
        for (auto& existing : files) {
            if (existing.filename == file_info.filename) {
                existing = file_info;
                return;
            }
        }
        files.emplace_back(file_info);
    }

    int64_t get_exist_job_perfile_size_by_prefix(const std::string& file_prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& pair : records_) {
            const std::vector<::FileInfo>& file_infos = pair.second;
            for (const auto& file_info : file_infos) {
                if (file_info.filename.compare(0, file_prefix.length(), file_prefix) == 0) {
                    return file_info.data_size;
                }
            }
        }
        return -1;
    }

    void get_exist_job_files_by_prefix(const std::string& file_prefix,
                                       std::vector<std::string>& result, int file_number = -1) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& pair : records_) {
            const std::vector<::FileInfo>& file_infos = pair.second;
            for (const auto& file_info : file_infos) {
                if (file_info.filename.starts_with(file_prefix)) {
                    if (file_number == -1 || result.size() < file_number) {
                        result.push_back(file_info.filename);
                    }
                    if (file_number != -1 && result.size() >= file_number) {
                        return;
                    }
                }
            }
        }
    }

    std::string find_job_id_by_prefix(const std::string& file_prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& pair : records_) {
            const std::vector<::FileInfo>& file_infos = pair.second;
            for (const auto& file_info : file_infos) {
                if (file_info.filename.starts_with(file_prefix)) {
                    return pair.first;
                }
            }
        }
        return "";
    }

private:
    std::mutex mutex_;
    std::map<std::string, std::vector<::FileInfo>> records_;
};

// Create a global S3FileRecords instance
S3FileRecords s3_file_records;

class MicrobenchS3FileWriter {
public:
    MicrobenchS3FileWriter(std::shared_ptr<doris::io::ObjStorageClientHolder> client,
                           const std::string& bucket, const std::string& key,
                           const doris::io::FileWriterOptions* options,
                           std::shared_ptr<doris::S3RateLimiterHolder> rate_limiter)
            : _writer(client, bucket, key, options), _rate_limiter(std::move(rate_limiter)) {}

    doris::Status appendv(const doris::Slice* slices, size_t slices_size,
                          const std::shared_ptr<bvar::LatencyRecorder>& write_bvar) {
        if (_rate_limiter) {
            _rate_limiter->add(1); // Consume a token
        }
        using namespace doris;
        if (write_bvar) {
            SCOPED_BVAR_LATENCY(*write_bvar);
        }
        SCOPED_BVAR_LATENCY(microbench_write_latency);
        return _writer.appendv(slices, slices_size);
    }

    doris::Status close() { return _writer.close(); }

private:
    doris::io::ObjStorageFileWriter _writer;
    std::shared_ptr<doris::S3RateLimiterHolder> _rate_limiter;
};

class MicrobenchFileReader {
public:
    MicrobenchFileReader(std::shared_ptr<doris::io::FileReader> base_reader,
                         std::shared_ptr<doris::S3RateLimiterHolder> rate_limiter)
            : _base_reader(std::move(base_reader)), _rate_limiter(std::move(rate_limiter)) {}

    doris::Status read_at(size_t offset, const doris::Slice& result, size_t* bytes_read,
                          const doris::io::IOContext* io_ctx,
                          std::shared_ptr<bvar::LatencyRecorder> read_bvar) {
        if (_rate_limiter) {
            _rate_limiter->add(1); // Consume a token
        }
        using namespace doris;
        if (read_bvar) {
            SCOPED_BVAR_LATENCY(*read_bvar);
        }
        SCOPED_BVAR_LATENCY(microbench_read_latency);
        return _base_reader->read_at(offset, result, bytes_read, io_ctx);
    }

    size_t size() const { return _base_reader->size(); }

    doris::Status close() { return _base_reader->close(); }

    bool closed() { return _base_reader->closed(); }

private:
    std::shared_ptr<doris::io::FileReader> _base_reader;
    std::shared_ptr<doris::S3RateLimiterHolder> _rate_limiter;
};

class BenchThreadPool {
public:
    BenchThreadPool(size_t num_threads) : stop(false) {
        try {
            for (size_t i = 0; i < num_threads; ++i) {
                workers.emplace_back([this] {
                    SCOPED_INIT_THREAD_CONTEXT();
                    try {
                        while (true) {
                            std::function<void()> task;
                            {
                                std::unique_lock<std::mutex> lock(queue_mutex);
                                condition.wait(lock, [this] { return stop || !tasks.empty(); });
                                if (stop && tasks.empty()) {
                                    return;
                                }
                                task = std::move(tasks.front());
                                tasks.pop();
                            }
                            task();
                        }
                    } catch (const std::exception& e) {
                        LOG(ERROR) << "Exception in thread pool worker: " << e.what();
                    } catch (...) {
                        LOG(ERROR) << "Unknown exception in thread pool worker";
                    }
                });
            }
        } catch (...) {
            // Ensure proper cleanup in case of exception during construction
            stop = true;
            condition.notify_all();
            throw;
        }
    }

    template <class F>
    std::future<void> enqueue(F&& f) {
        auto task = std::make_shared<std::packaged_task<void()>>(std::forward<F>(f));
        std::future<void> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop) {
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }
            tasks.emplace([task]() {
                try {
                    (*task)();
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Exception in task: " << e.what();
                } catch (...) {
                    LOG(ERROR) << "Unknown exception in task";
                }
            });
        }
        condition.notify_one();
        return res;
    }

    void stop_and_wait() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();

        for (auto& worker : workers) {
            try {
                if (worker.joinable()) {
                    worker.join();
                }
            } catch (const std::system_error& e) {
                LOG(WARNING) << "Failed to join thread: " << e.what();
            }
        }
    }

    ~BenchThreadPool() {
        if (!stop) {
            try {
                stop_and_wait();
            } catch (const std::exception& e) {
                LOG(WARNING) << "Error stopping thread pool: " << e.what();
            }
        }
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

class FileCompletionTracker {
public:
    void mark_completed(const std::string& key) {
        std::lock_guard<std::mutex> lock(_mutex);
        _completed_files.insert(key);
        _cv.notify_all(); // Notify all waiting threads
    }

    bool is_completed(const std::string& key) {
        return _completed_files.find(key) != _completed_files.end();
    }

    void wait_for_completion(const std::string& key) {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [&] { return is_completed(key); });
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::unordered_set<std::string> _completed_files;
};

std::string get_usage(const std::string& progname) {
    std::string usage = R"(
    )" + progname + R"( is the Doris microbench tool for testing file cache in cloud.

    Usage:
      Start the server:
        )" + progname + R"( --port=<port_number> [--brpc_port=<peer_brpc_port>]
        
    API Endpoints:
      POST /submit_job
        Submit a job with the following JSON body:
        {
          "size_bytes_perfile": <size>,        // Number of bytes to write per segment file
          "write_iops": <limit>,               // IOPS limit for writing per segment files
          "read_iops": <limit>,                // IOPS limit for reading per segment files
          "num_threads": <count>,              // Number of threads in the thread pool, default 200
          "num_files": <count>,                // Number of segments to write/read
          "file_prefix": "<prefix>",           // Prefix for segment files, key prefix is test_file_cache_microbench/<test_s3_prefix>/
          "write_batch_size": <size>,          // Size of data to write in each write operation
          "cache_type": <type>,                // Write or Read data enter file cache queue type, support NORMAL | TTL | INDEX | DISPOSABLE, default NORMAL
          "expiration": <timestamp>,           // File cache ttl expire time, value is a unix timestamp
          "repeat": <count>,                   // Read repeat times, default 1
          "read_offset": [<left>, <right>],    // Range for reading (left inclusive, right exclusive)
          "read_length": [<left>, <right>]     // Range for reading length (left inclusive, right exclusive)
        }

        Submit a direct file cache get_or_set benchmark job:
        {
          "job_type": "file_cache_get_or_set",
          "file_prefix": "<prefix>",
          "num_threads": <count>,
          "num_files": <count>,
          "repeat": <count>,                   // get_or_set calls per generated cache key
          "size_bytes_perfile": <size>,        // Virtual file size used to clip offset + size
          "get_or_set_offset": [<left>, <right>],
          "get_or_set_size": [<left>, <right>],
          "cache_type": <type>,                // NORMAL | TTL | INDEX | DISPOSABLE
          "expiration": <timestamp>            // Required for TTL cache type
        }

        Submit a local file cache seed job for peer server (no S3 required):
        {
          "job_type": "peer_cache_seed",
          "file_prefix": "<prefix>",
          "source_job_id": "<optional_id>",    // Optional key id (defaults to job_id)
          "num_threads": <count>,
          "num_files": <count>,
          "size_bytes_perfile": <size>,
          "tablet_id": <id>,                   // Default 1
          "cache_type": <type>,                // NORMAL | TTL | INDEX | DISPOSABLE
          "expiration": <timestamp>,           // Required for TTL cache type
          "overwrite_existing": <true|false>   // Default true
        }

        Submit an end-to-end peer read benchmark job:
        {
          "job_type": "peer_read",
          "file_prefix": "<prefix>",
          "source_job_id": "<seed_or_write_job_id>",
          "num_threads": <count>,
          "num_files": <count>,
          "size_bytes_perfile": <size>,
          "repeat": <count>,                   // Read repeat times per file, default 1
          "read_iops": <limit>,                // Optional IOPS limit (0 = unlimited)
          "read_offset": [<left>, <right>],    // [left, right), left+1==right for fixed offset
          "read_length": [<left>, <right>],    // [left, right), left+1==right for fixed length
          "read_chunk_size": <bytes>,          // Chunk size per read_at, default 4MB
          "tablet_id": <id>,                   // Default 1
          "peer_endpoints": [                  // Optional; also configurable via /set_peer_candidates
            {"host": "<peer_ip>", "port": <peer_brpc_port>, "compute_group_id": "<cg>"}
          ],
          "use_s3_fallback": <true|false>,     // Default false (pure peer read without S3)
          "bypass_peer_read": <true|false>,    // Default false
          "direct_peer_rpc": <true|false>,     // Default false (true bypasses local cache layer)
          "cache_write_mode": "<DEFAULT|SYNC_WRITE|ASYNC_WRITE>",
          "clear_local_cache_before_read": <true|false>, // Default true
          "clear_local_cache_each_repeat": <true|false>, // Default false
          "verify_data": <true|false>          // Default true
        }

      POST /set_peer_candidates
        Configure peer read candidates for a tablet_id:
        {
          "tablet_id": <id>,
          "peers": [{"host": "<ip>", "port": <brpc_port>, "compute_group_id": "<cg>"}]
        }

      GET /get_peer_candidates[?tablet_id=<id>]
        Inspect configured peer read candidates in CloudWarmUpManager.

      GET /get_job_status/<job_id>
        Retrieve the status of a submitted job.
        Parameters:
          - job_id: The ID of the job to retrieve status for.
          - files (optional): If provided, returns the associated file records for the job.
            Example: /get_job_status/job_id?files=10

      GET /list_jobs
        List all submitted jobs and their statuses.

      GET /get_help
        Get this help information.

      GET /file_cache_clear
        Clear the file cache with the following query parameters:
        {
          "sync": <true|false>,                // Whether to synchronize the cache clear operation
          "segment_path": "<path>"             // Optional path of the segment to clear from the cache
        }
        If "segment_path" is not provided, all caches will be cleared based on the "sync" parameter.

      GET /file_cache_reset
        Reset the file cache with the following query parameters:
        {
          "capacity": <new_capacity>,          // New capacity for the specified path
          "path": "<path>"                     // Path of the segment to reset
        }

      GET /file_cache_release
        Release the file cache with the following query parameters:
        {
          "base_path": "<base_path>"           // Optional base path to release specific caches
        }

      GET /update_config
        Update the configuration with the following JSON body:
        {
          "config_key": "<key>",               // The configuration key to update
          "config_value": "<value>",            // The new value for the configuration key
          "persist": <true|false>              // Whether to persist the configuration change
        }

      GET /show_config
        Retrieve the current configuration settings.

    Notes:
      - Ensure that the S3 configuration is set correctly in the environment.
      - The program will create and read files in the specified S3 bucket.
      - Monitor the logs for detailed execution information and errors.
    )" + build_info();

    return usage;
}

// Job configuration structure
enum class JobType { FILE_CACHE, FILE_CACHE_GET_OR_SET, PEER_CACHE_SEED, PEER_READ };

static std::string job_type_to_string(JobType type) {
    switch (type) {
    case JobType::FILE_CACHE:
        return "file_cache";
    case JobType::FILE_CACHE_GET_OR_SET:
        return "file_cache_get_or_set";
    case JobType::PEER_CACHE_SEED:
        return "peer_cache_seed";
    case JobType::PEER_READ:
        return "peer_read";
    }
    return "unknown";
}

struct PeerEndpointConfig {
    std::string host;
    int32_t port = 0;
    std::string compute_group_id;
};

struct JobConfig {
    // Default value initialization
    JobType job_type = JobType::FILE_CACHE;
    int64_t size_bytes_perfile = 1024 * 1024;
    int32_t write_iops = 0;
    int32_t read_iops = 0;
    int32_t num_threads = 200;
    int32_t num_files = 1;
    std::string file_prefix;
    std::string cache_type = "NORMAL";
    int64_t expiration = 0;
    int32_t repeat = 1;
    int64_t write_batch_size = doris::config::s3_write_buffer_size;
    int64_t read_offset_left = 0;
    int64_t read_offset_right = 0;
    int64_t read_length_left = 0;
    int64_t read_length_right = 0;
    int64_t get_or_set_offset_left = 0;
    int64_t get_or_set_offset_right = 1;
    int64_t get_or_set_size_left = 4096;
    int64_t get_or_set_size_right = 4097;
    bool write_file_cache = true;
    bool bvar_enable = false;
    // Additive peer-read / peer-seed configuration fields
    std::string source_job_id;
    int64_t tablet_id = 1;
    std::vector<PeerEndpointConfig> peer_endpoints;
    bool bypass_peer_read = false;
    bool use_s3_fallback = false;
    bool direct_peer_rpc = false;
    bool clear_local_cache_before_read = true;
    bool clear_local_cache_each_repeat = false;
    bool overwrite_existing = true;
    bool verify_data = true;
    std::string cache_write_mode = "DEFAULT";
    int64_t read_chunk_size = 4 * 1024 * 1024;

    // Parse configuration from JSON
    static JobConfig from_json(const std::string& json_str) {
        JobConfig config;
        rapidjson::Document d;
        d.Parse(json_str.c_str());

        if (d.HasParseError()) {
            throw std::runtime_error("JSON parse error json args=" + json_str);
        }

        config.job_type = parse_job_type(d);

        // Basic validation
        validate(d);

        // Use helper functions to parse each field
        parse_basic_fields(d, config);
        parse_cache_settings(d, config);

        if (config.job_type == JobType::FILE_CACHE_GET_OR_SET) {
            parse_get_or_set_fields(d, config);
            validate_get_or_set_config(config);
        } else if (config.job_type == JobType::PEER_CACHE_SEED) {
            parse_peer_common_fields(d, config);
            validate_peer_seed_config(config);
        } else if (config.job_type == JobType::PEER_READ) {
            parse_peer_common_fields(d, config);
            parse_peer_read_fields(d, config);
            validate_peer_read_config(config);
        } else {
            parse_read_settings(d, config);

            // Additional validation
            validate_config(config);
        }

        return config;
    }

private:
    static JobType parse_job_type(const rapidjson::Document& d) {
        if (!d.HasMember("job_type")) {
            return JobType::FILE_CACHE;
        }
        if (!d["job_type"].IsString()) {
            throw std::runtime_error("job_type must be a string");
        }
        std::string type = d["job_type"].GetString();
        if (type == "file_cache") {
            return JobType::FILE_CACHE;
        }
        if (type == "file_cache_get_or_set") {
            return JobType::FILE_CACHE_GET_OR_SET;
        }
        if (type == "peer_cache_seed") {
            return JobType::PEER_CACHE_SEED;
        }
        if (type == "peer_read") {
            return JobType::PEER_READ;
        }
        throw std::runtime_error("unsupported job_type: " + type);
    }

    // Validate the JSON document
    static void validate(const rapidjson::Document& json_data) {
        if (!json_data.HasMember("file_prefix") || !json_data["file_prefix"].IsString() ||
            strlen(json_data["file_prefix"].GetString()) == 0) {
            throw std::runtime_error("file_prefix is required and cannot be empty");
        }
    }

    // Parse basic fields
    static void parse_basic_fields(const rapidjson::Document& d, JobConfig& config) {
        // Parse file_prefix (required field)
        config.file_prefix = d["file_prefix"].GetString();

        // Parse optional fields
        if (d.HasMember("num_files") && d["num_files"].IsInt()) {
            config.num_files = d["num_files"].GetInt();
        }

        if (d.HasMember("size_bytes_perfile") && d["size_bytes_perfile"].IsInt64()) {
            config.size_bytes_perfile = d["size_bytes_perfile"].GetInt64();
        }

        if (d.HasMember("write_iops") && d["write_iops"].IsInt()) {
            config.write_iops = d["write_iops"].GetInt();
        }

        if (d.HasMember("read_iops") && d["read_iops"].IsInt()) {
            config.read_iops = d["read_iops"].GetInt();
        }

        if (d.HasMember("num_threads") && d["num_threads"].IsInt()) {
            config.num_threads = d["num_threads"].GetInt();
        }

        if (d.HasMember("repeat") && d["repeat"].IsInt64()) {
            config.repeat = d["repeat"].GetInt();
        }

        if (d.HasMember("write_batch_size") && d["write_batch_size"].IsInt64()) {
            config.write_batch_size = d["write_batch_size"].GetInt64();
        }

        if (d.HasMember("write_file_cache") && d["write_file_cache"].IsBool()) {
            config.write_file_cache = d["write_file_cache"].GetBool();
        }

        if (d.HasMember("bvar_enable") && d["bvar_enable"].IsBool()) {
            config.bvar_enable = d["bvar_enable"].GetBool();
        }
    }

    // Parse cache-related settings
    static void parse_cache_settings(const rapidjson::Document& d, JobConfig& config) {
        if (d.HasMember("cache_type") && d["cache_type"].IsString()) {
            config.cache_type = d["cache_type"].GetString();
        }

        // Check for TTL cache type
        if (config.cache_type == "TTL") {
            if (!d.HasMember("expiration") || !d["expiration"].IsInt64()) {
                throw std::runtime_error(
                        "expiration is required and must be an integer when cache type is TTL");
            }
            config.expiration = d["expiration"].GetInt64();
        }
    }

    static void parse_get_or_set_fields(const rapidjson::Document& d, JobConfig& config) {
        if (d.HasMember("get_or_set_offset") && d["get_or_set_offset"].IsArray() &&
            d["get_or_set_offset"].Size() == 2) {
            const rapidjson::Value& offset_array = d["get_or_set_offset"];
            config.get_or_set_offset_left = offset_array[0].GetInt64();
            config.get_or_set_offset_right = offset_array[1].GetInt64();
        } else if (d.HasMember("read_offset") && d["read_offset"].IsArray() &&
                   d["read_offset"].Size() == 2) {
            const rapidjson::Value& offset_array = d["read_offset"];
            config.get_or_set_offset_left = offset_array[0].GetInt64();
            config.get_or_set_offset_right = offset_array[1].GetInt64();
        }

        if (d.HasMember("get_or_set_size") && d["get_or_set_size"].IsArray() &&
            d["get_or_set_size"].Size() == 2) {
            const rapidjson::Value& size_array = d["get_or_set_size"];
            config.get_or_set_size_left = size_array[0].GetInt64();
            config.get_or_set_size_right = size_array[1].GetInt64();
        } else if (d.HasMember("read_length") && d["read_length"].IsArray() &&
                   d["read_length"].Size() == 2) {
            const rapidjson::Value& size_array = d["read_length"];
            config.get_or_set_size_left = size_array[0].GetInt64();
            config.get_or_set_size_right = size_array[1].GetInt64();
        }
    }

    // Parse read-related settings
    static void parse_read_settings(const rapidjson::Document& d, JobConfig& config) {
        if (config.read_iops > 0) {
            // Parse read_offset
            if (d.HasMember("read_offset") && d["read_offset"].IsArray() &&
                d["read_offset"].Size() == 2) {
                const rapidjson::Value& read_offset_array = d["read_offset"];
                config.read_offset_left = read_offset_array[0].GetInt64();
                config.read_offset_right = read_offset_array[1].GetInt64();
            } else {
                throw std::runtime_error("Invalid read_offset format, expected array of size 2");
            }

            // Parse read_length
            if (d.HasMember("read_length") && d["read_length"].IsArray() &&
                d["read_length"].Size() == 2) {
                const rapidjson::Value& read_length_array = d["read_length"];
                config.read_length_left = read_length_array[0].GetInt64();
                config.read_length_right = read_length_array[1].GetInt64();
            } else {
                throw std::runtime_error("Invalid read_length format, expected array of size 2");
            }
        }
    }

    // Validate the validity of the configuration
    static void validate_config(const JobConfig& config) {
        if (config.num_threads <= 0 || config.num_threads > 10000) {
            throw std::runtime_error("num_threads must be between 1 and 10000");
        }

        if (config.size_bytes_perfile <= 0) {
            throw std::runtime_error("size_bytes_perfile must be positive");
        }

        if (config.read_iops > 0) {
            if (config.read_offset_left >= config.read_offset_right) {
                throw std::runtime_error("read_offset_left must be less than read_offset_right");
            }

            if (config.read_length_left >= config.read_length_right) {
                throw std::runtime_error("read_length_left must be less than read_length_right");
            }
        }

        if (config.cache_type == "TTL" && config.expiration <= 0) {
            throw std::runtime_error("expiration must be positive when cache type is TTL");
        }
    }

    static void validate_get_or_set_config(const JobConfig& config) {
        if (config.num_threads <= 0 || config.num_threads > 10000) {
            throw std::runtime_error("num_threads must be between 1 and 10000");
        }
        if (config.num_files <= 0) {
            throw std::runtime_error("num_files must be positive");
        }
        if (config.repeat <= 0) {
            throw std::runtime_error("repeat must be positive");
        }
        if (config.size_bytes_perfile <= 0) {
            throw std::runtime_error("size_bytes_perfile must be positive");
        }
        if (config.get_or_set_offset_left < 0 ||
            config.get_or_set_offset_left >= config.get_or_set_offset_right) {
            throw std::runtime_error(
                    "get_or_set_offset must be [left, right) with 0 <= left < right");
        }
        if (config.get_or_set_size_left <= 0 ||
            config.get_or_set_size_left >= config.get_or_set_size_right) {
            throw std::runtime_error("get_or_set_size must be [left, right) with 0 < left < right");
        }
        if (config.cache_type != "NORMAL" && config.cache_type != "TTL" &&
            config.cache_type != "INDEX" && config.cache_type != "DISPOSABLE") {
            throw std::runtime_error(
                    "cache_type must be NORMAL, TTL, INDEX, or DISPOSABLE for get_or_set jobs");
        }
        if (config.cache_type == "TTL" && config.expiration <= 0) {
            throw std::runtime_error("expiration must be positive when cache type is TTL");
        }
    }

    static std::vector<PeerEndpointConfig> parse_peer_endpoints_array(const rapidjson::Value& arr) {
        if (!arr.IsArray()) {
            throw std::runtime_error("peer_endpoints / peers must be an array");
        }
        std::vector<PeerEndpointConfig> endpoints;
        endpoints.reserve(arr.Size());
        for (rapidjson::SizeType i = 0; i < arr.Size(); ++i) {
            const auto& item = arr[i];
            if (!item.IsObject()) {
                throw std::runtime_error("each peer endpoint must be a JSON object");
            }
            if (!item.HasMember("host") || !item["host"].IsString() ||
                strlen(item["host"].GetString()) == 0) {
                throw std::runtime_error("peer endpoint requires non-empty string field 'host'");
            }
            int32_t port = 0;
            if (item.HasMember("port") && item["port"].IsInt()) {
                port = item["port"].GetInt();
            } else if (item.HasMember("brpc_port") && item["brpc_port"].IsInt()) {
                port = item["brpc_port"].GetInt();
            }
            if (port <= 0 || port > 65535) {
                throw std::runtime_error("peer endpoint requires valid port (1..65535)");
            }
            PeerEndpointConfig ep;
            ep.host = item["host"].GetString();
            ep.port = port;
            if (item.HasMember("compute_group_id") && item["compute_group_id"].IsString()) {
                ep.compute_group_id = item["compute_group_id"].GetString();
            }
            endpoints.emplace_back(std::move(ep));
        }
        return endpoints;
    }

    static void parse_peer_common_fields(const rapidjson::Document& d, JobConfig& config) {
        if (d.HasMember("source_job_id") && d["source_job_id"].IsString()) {
            config.source_job_id = d["source_job_id"].GetString();
        }
        if (d.HasMember("tablet_id") && d["tablet_id"].IsInt64()) {
            config.tablet_id = d["tablet_id"].GetInt64();
        }
        if (d.HasMember("overwrite_existing") && d["overwrite_existing"].IsBool()) {
            config.overwrite_existing = d["overwrite_existing"].GetBool();
        }
    }

    static void parse_peer_read_fields(const rapidjson::Document& d, JobConfig& config) {
        if (d.HasMember("read_offset") && d["read_offset"].IsArray() &&
            d["read_offset"].Size() == 2 && d["read_offset"][0].IsInt64() &&
            d["read_offset"][1].IsInt64()) {
            config.read_offset_left = d["read_offset"][0].GetInt64();
            config.read_offset_right = d["read_offset"][1].GetInt64();
        } else {
            throw std::runtime_error(
                    "Invalid read_offset format, expected array of 2 int64 values");
        }
        if (d.HasMember("read_length") && d["read_length"].IsArray() &&
            d["read_length"].Size() == 2 && d["read_length"][0].IsInt64() &&
            d["read_length"][1].IsInt64()) {
            config.read_length_left = d["read_length"][0].GetInt64();
            config.read_length_right = d["read_length"][1].GetInt64();
        } else {
            throw std::runtime_error(
                    "Invalid read_length format, expected array of 2 int64 values");
        }
        if (d.HasMember("peer_endpoints")) {
            config.peer_endpoints = parse_peer_endpoints_array(d["peer_endpoints"]);
        } else if (d.HasMember("peers")) {
            config.peer_endpoints = parse_peer_endpoints_array(d["peers"]);
        }
        if (d.HasMember("bypass_peer_read") && d["bypass_peer_read"].IsBool()) {
            config.bypass_peer_read = d["bypass_peer_read"].GetBool();
        }
        if (d.HasMember("use_s3_fallback") && d["use_s3_fallback"].IsBool()) {
            config.use_s3_fallback = d["use_s3_fallback"].GetBool();
        }
        if (d.HasMember("direct_peer_rpc") && d["direct_peer_rpc"].IsBool()) {
            config.direct_peer_rpc = d["direct_peer_rpc"].GetBool();
        }
        if (d.HasMember("clear_local_cache_before_read") &&
            d["clear_local_cache_before_read"].IsBool()) {
            config.clear_local_cache_before_read = d["clear_local_cache_before_read"].GetBool();
        }
        if (d.HasMember("clear_local_cache_each_repeat") &&
            d["clear_local_cache_each_repeat"].IsBool()) {
            config.clear_local_cache_each_repeat = d["clear_local_cache_each_repeat"].GetBool();
        }
        if (d.HasMember("verify_data") && d["verify_data"].IsBool()) {
            config.verify_data = d["verify_data"].GetBool();
        }
        if (d.HasMember("cache_write_mode") && d["cache_write_mode"].IsString()) {
            config.cache_write_mode = d["cache_write_mode"].GetString();
        }
        if (d.HasMember("read_chunk_size") && d["read_chunk_size"].IsInt64()) {
            config.read_chunk_size = d["read_chunk_size"].GetInt64();
        }
    }

    static void validate_peer_seed_config(const JobConfig& config) {
        if (config.num_threads <= 0 || config.num_threads > 10000) {
            throw std::runtime_error("num_threads must be between 1 and 10000");
        }
        if (config.num_files <= 0) {
            throw std::runtime_error("num_files must be positive");
        }
        if (config.size_bytes_perfile <= 0) {
            throw std::runtime_error("size_bytes_perfile must be positive");
        }
        if (config.tablet_id <= 0) {
            throw std::runtime_error("tablet_id must be positive");
        }
        if (config.cache_type != "NORMAL" && config.cache_type != "TTL" &&
            config.cache_type != "INDEX" && config.cache_type != "DISPOSABLE") {
            throw std::runtime_error("cache_type must be NORMAL, TTL, INDEX, or DISPOSABLE");
        }
        if (config.cache_type == "TTL" && config.expiration <= 0) {
            throw std::runtime_error("expiration must be positive when cache type is TTL");
        }
    }

    static void validate_peer_read_config(const JobConfig& config) {
        if (config.num_threads <= 0 || config.num_threads > 10000) {
            throw std::runtime_error("num_threads must be between 1 and 10000");
        }
        if (config.num_files <= 0) {
            throw std::runtime_error("num_files must be positive");
        }
        if (config.repeat <= 0) {
            throw std::runtime_error("repeat must be positive");
        }
        if (config.size_bytes_perfile <= 0) {
            throw std::runtime_error("size_bytes_perfile must be positive");
        }
        if (config.tablet_id <= 0) {
            throw std::runtime_error("tablet_id must be positive");
        }
        if (config.read_offset_left < 0 || config.read_offset_left >= config.read_offset_right) {
            throw std::runtime_error("read_offset must be [left, right) with 0 <= left < right");
        }
        if (config.read_length_left <= 0 || config.read_length_left >= config.read_length_right) {
            throw std::runtime_error("read_length must be [left, right) with 0 < left < right");
        }
        if (config.read_chunk_size <= 0) {
            throw std::runtime_error("read_chunk_size must be positive");
        }
        if (config.cache_type != "NORMAL" && config.cache_type != "TTL" &&
            config.cache_type != "INDEX" && config.cache_type != "DISPOSABLE") {
            throw std::runtime_error("cache_type must be NORMAL, TTL, INDEX, or DISPOSABLE");
        }
        if (config.cache_type == "TTL" && config.expiration <= 0) {
            throw std::runtime_error("expiration must be positive when cache type is TTL");
        }
        if (config.cache_write_mode != "DEFAULT" && config.cache_write_mode != "SYNC_WRITE" &&
            config.cache_write_mode != "ASYNC_WRITE") {
            throw std::runtime_error(
                    "cache_write_mode must be DEFAULT, SYNC_WRITE, or ASYNC_WRITE");
        }
    }

public:
    static std::vector<PeerEndpointConfig> parse_endpoints_from_json(const rapidjson::Value& arr) {
        return parse_peer_endpoints_array(arr);
    }

    std::string to_string() const {
        if (job_type == JobType::FILE_CACHE_GET_OR_SET) {
            return fmt::format(
                    "job_type: {}, num_threads: {}, num_files: {}, file_prefix: {}, "
                    "size_bytes_perfile: {}, repeat: {}, cache_type: {}, expiration: {}, "
                    "get_or_set_offset: [{}, {}), get_or_set_size: [{}, {})",
                    job_type_to_string(job_type), num_threads, num_files,
                    get_prefix() + file_prefix, size_bytes_perfile, repeat, cache_type, expiration,
                    get_or_set_offset_left, get_or_set_offset_right, get_or_set_size_left,
                    get_or_set_size_right);
        }
        if (job_type == JobType::PEER_CACHE_SEED) {
            return fmt::format(
                    "job_type: {}, num_threads: {}, num_files: {}, file_prefix: {}, "
                    "source_job_id: {}, size_bytes_perfile: {}, tablet_id: {}, cache_type: {}, "
                    "expiration: {}, overwrite_existing: {}",
                    job_type_to_string(job_type), num_threads, num_files,
                    get_prefix() + file_prefix, source_job_id, size_bytes_perfile, tablet_id,
                    cache_type, expiration, overwrite_existing);
        }
        if (job_type == JobType::PEER_READ) {
            return fmt::format(
                    "job_type: {}, num_threads: {}, num_files: {}, file_prefix: {}, "
                    "source_job_id: {}, size_bytes_perfile: {}, repeat: {}, read_iops: {}, "
                    "tablet_id: {}, peer_endpoints: {}, use_s3_fallback: {}, bypass_peer_read: {}, "
                    "direct_peer_rpc: {}, cache_write_mode: {}, clear_before: {}, "
                    "clear_each_repeat: {}, verify_data: {}, read_chunk_size: {}, "
                    "read_offset: [{}, {}), read_length: [{}, {})",
                    job_type_to_string(job_type), num_threads, num_files,
                    get_prefix() + file_prefix, source_job_id, size_bytes_perfile, repeat,
                    read_iops, tablet_id, peer_endpoints.size(), use_s3_fallback, bypass_peer_read,
                    direct_peer_rpc, cache_write_mode, clear_local_cache_before_read,
                    clear_local_cache_each_repeat, verify_data, read_chunk_size, read_offset_left,
                    read_offset_right, read_length_left, read_length_right);
        }
        return fmt::format(
                "size_bytes_perfile: {}, write_iops: {}, read_iops: {}, num_threads: {}, "
                "num_files: {}, file_prefix: {}, write_file_cache: {}, write_batch_size: {}, "
                "repeat: {}, expiration: {}, cache_type: {}, read_offset: [{}, {}), "
                "read_length: [{}, {})",
                size_bytes_perfile, write_iops, read_iops, num_threads, num_files,
                get_prefix() + file_prefix, write_file_cache, write_batch_size, repeat, expiration,
                cache_type, read_offset_left, read_offset_right, read_length_left,
                read_length_right);
    }
};

// Job status
enum class JobStatus { PENDING, RUNNING, COMPLETED, FAILED };

// Job structure
struct Job {
    std::string job_id;
    JobConfig config;
    JobStatus status;
    std::string error_message;
    std::chrono::system_clock::time_point create_time;
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;

    std::shared_ptr<doris::S3RateLimiterHolder> write_limiter;
    std::shared_ptr<doris::S3RateLimiterHolder> read_limiter;

    // Job execution result statistics
    struct Statistics {
        std::string total_write_time;
        std::string total_read_time;
        // struct FileCacheStatistics
        int64_t num_local_io_total = 0;
        int64_t num_remote_io_total = 0;
        int64_t num_inverted_index_remote_io_total = 0;
        int64_t local_io_timer = 0;
        int64_t bytes_read_from_local = 0;
        int64_t bytes_read_from_remote = 0;
        int64_t remote_io_timer = 0;
        int64_t write_cache_io_timer = 0;
        int64_t bytes_write_into_cache = 0;
        int64_t num_skip_cache_io_total = 0;
        int64_t read_cache_file_directly_timer = 0;
        int64_t cache_get_or_set_timer = 0;
        int64_t lock_wait_timer = 0;
        int64_t get_timer = 0;
        int64_t set_timer = 0;
        int64_t get_or_set_ops = 0;
        int64_t get_or_set_blocks = 0;
        double get_or_set_qps = 0;
        double avg_get_or_set_us = 0;
        // Additive peer read / peer seed statistics
        int64_t num_peer_io_total = 0;
        int64_t bytes_read_from_peer = 0;
        int64_t peer_wire_bytes_read = 0;
        int64_t peer_io_timer = 0;
        int64_t peer_read_ops = 0;
        int64_t peer_total_bytes_read = 0;
        double peer_read_qps = 0;
        double peer_rpc_qps = 0;
        double peer_throughput_mb_s = 0;
        double peer_wire_throughput_mb_s = 0;
        double peer_avg_read_us = 0;
        double peer_p50_read_us = 0;
        double peer_p95_read_us = 0;
        double peer_p99_read_us = 0;
        double peer_max_read_us = 0;
        int64_t peer_seed_files = 0;
        int64_t peer_seed_blocks = 0;
        double peer_seed_throughput_mb_s = 0;
    } stats;

    // Record associated file information for the job
    std::vector<::FileInfo> file_records;

    mutable std::mutex resolved_source_mutex;
    std::string resolved_source_job_id;

    void set_resolved_source_job_id(std::string id) {
        std::lock_guard<std::mutex> lock(resolved_source_mutex);
        resolved_source_job_id = std::move(id);
    }

    std::string effective_source_job_id() const {
        std::lock_guard<std::mutex> lock(resolved_source_mutex);
        return resolved_source_job_id.empty() ? config.source_job_id : resolved_source_job_id;
    }

    // Add completion_tracker
    std::shared_ptr<FileCompletionTracker> completion_tracker;

    std::shared_ptr<bvar::LatencyRecorder> write_latency;
    std::shared_ptr<bvar::Adder<int64_t>> write_rate_limit_s;
    std::shared_ptr<bvar::LatencyRecorder> read_latency;
    std::shared_ptr<bvar::Adder<int64_t>> read_rate_limit_s;

    // Default constructor
    Job() : job_id(""), status(JobStatus::PENDING), create_time(std::chrono::system_clock::now()) {
        init_latency_recorders("");
        completion_tracker = std::make_shared<FileCompletionTracker>();
    }

    // Constructor with parameters
    Job(const std::string& id, const JobConfig& cfg)
            : job_id(id),
              config(cfg),
              status(JobStatus::PENDING),
              create_time(std::chrono::system_clock::now()) {
        init_latency_recorders(id);
        if (cfg.write_iops > 0 && cfg.read_iops > 0) {
            completion_tracker = std::make_shared<FileCompletionTracker>();
        }
        init_limiters(cfg);
    }

private:
    void init_latency_recorders(const std::string& id) {
        if (config.write_iops > 0 && config.bvar_enable) {
            write_latency =
                    std::make_shared<bvar::LatencyRecorder>("file_cache_microbench_append_" + id);
            write_rate_limit_s = std::make_shared<bvar::Adder<int64_t>>(
                    "file_cache_microbench_append_rate_limit_ns_" + id);
        }

        if ((config.read_iops > 0 || config.job_type == JobType::PEER_READ) && config.bvar_enable) {
            read_latency =
                    std::make_shared<bvar::LatencyRecorder>("file_cache_microbench_read_at_" + id);
            read_rate_limit_s = std::make_shared<bvar::Adder<int64_t>>(
                    "file_cache_microbench_read_rate_limit_ns_" + id);
        }
    }

    void init_limiters(const JobConfig& cfg) {
        if (cfg.write_iops > 0) {
            write_limiter = std::make_shared<doris::S3RateLimiterHolder>(
                    cfg.write_iops, // max_speed (IOPS)
                    cfg.write_iops, // max_burst
                    0,              // no limit
                    [this](int64_t wait_time_ns) {
                        if (wait_time_ns > 0 && write_rate_limit_s) {
                            *write_rate_limit_s << wait_time_ns / NS;
                        }
                    });
        }

        if (cfg.read_iops > 0) {
            read_limiter = std::make_shared<doris::S3RateLimiterHolder>(
                    cfg.read_iops, // max_speed (IOPS)
                    cfg.read_iops, // max_burst
                    0,             // no limit
                    [this](int64_t wait_time_ns) {
                        if (wait_time_ns > 0 && read_rate_limit_s) {
                            *read_rate_limit_s << wait_time_ns / NS;
                        }
                    });
        }
    }
};

namespace microbenchService {
class JobManager;

class BenchEnvManager : public std::enable_shared_from_this<BenchEnvManager> {
public:
    BenchEnvManager(std::string_view doris_home) : _doris_home(doris_home) {}

    ~BenchEnvManager() { stop_reload_worker(); }

    doris::Status load_config() {
        std::string conffile = std::string(_doris_home) + "/conf/be.conf";
        if (!doris::config::init(conffile.c_str(), true, true, true)) {
            return Status::InternalError("Error reading config file");
        }

        std::string custom_conffile = doris::config::custom_config_dir + "/be_custom.conf";
        if (!doris::config::init(custom_conffile.c_str(), true, false, false)) {
            return Status::InternalError("Error reading custom config file");
        }

        if (!doris::config::enable_file_cache) {
            return Status::InternalError("config::enbale_file_cache should be true!");
        }

        config::group_commit_wal_max_disk_limit = "100M";
        LOG(INFO) << "Obj config. region=" << doris::config::test_s3_region
                  << " endpoint=" << doris::config::test_s3_endpoint
                  << " bucket=" << doris::config::test_s3_bucket;

        LOG(INFO) << "File cache config. enable_file_cache=" << doris::config::enable_file_cache
                  << " file_cache_path=" << doris::config::file_cache_path
                  << " file_cache_each_block_size=" << doris::config::file_cache_each_block_size
                  << " clear_file_cache=" << doris::config::clear_file_cache
                  << " enable_file_cache_query_limit="
                  << doris::config::enable_file_cache_query_limit
                  << " file_cache_enter_disk_resource_limit_mode_percent="
                  << doris::config::file_cache_enter_disk_resource_limit_mode_percent
                  << " file_cache_exit_disk_resource_limit_mode_percent="
                  << doris::config::file_cache_exit_disk_resource_limit_mode_percent
                  << " enable_read_cache_file_directly="
                  << doris::config::enable_read_cache_file_directly
                  << " file_cache_enable_evict_from_other_queue_by_size="
                  << doris::config::file_cache_enable_evict_from_other_queue_by_size
                  << " file_cache_error_log_limit_bytes="
                  << doris::config::file_cache_error_log_limit_bytes
                  << " cache_lock_wait_long_tail_threshold_us="
                  << doris::config::cache_lock_wait_long_tail_threshold_us
                  << " cache_lock_held_long_tail_threshold_us="
                  << doris::config::cache_lock_held_long_tail_threshold_us
                  << " file_cache_remove_block_qps_limit="
                  << doris::config::file_cache_remove_block_qps_limit
                  << " enable_evict_file_cache_in_advance="
                  << doris::config::enable_evict_file_cache_in_advance
                  << " file_cache_enter_need_evict_cache_in_advance_percent="
                  << doris::config::file_cache_enter_need_evict_cache_in_advance_percent
                  << " file_cache_exit_need_evict_cache_in_advance_percent="
                  << doris::config::file_cache_exit_need_evict_cache_in_advance_percent
                  << " file_cache_evict_in_advance_interval_ms="
                  << doris::config::file_cache_evict_in_advance_interval_ms
                  << " file_cache_evict_in_advance_batch_bytes="
                  << doris::config::file_cache_evict_in_advance_batch_bytes;

        LOG(INFO) << "S3 writer config. s3_file_writer_log_interval_second="
                  << doris::config::s3_file_writer_log_interval_second
                  << " s3_write_buffer_size=" << doris::config::s3_write_buffer_size
                  << " enable_flush_file_cache_async="
                  << doris::config::enable_flush_file_cache_async;

        return Status::OK();
    }

    doris::Status load_bench_exec_env() {
        SCOPED_INIT_THREAD_CONTEXT();

        doris::CpuInfo::init();
        doris::DiskInfo::init();
        doris::MemInfo::init();

        LOG(INFO) << doris::CpuInfo::debug_string();
        LOG(INFO) << doris::DiskInfo::debug_string();
        LOG(INFO) << doris::MemInfo::debug_string();

        std::vector<doris::StorePath> paths;
        auto olap_res = doris::parse_conf_store_paths(doris::config::storage_root_path, &paths);
        if (!olap_res) {
            LOG(ERROR) << "parse config storage path failed, path="
                       << doris::config::storage_root_path;
            exit(-1);
        }

        std::vector<doris::StorePath> spill_paths;
        if (doris::config::spill_storage_root_path.empty()) {
            doris::config::spill_storage_root_path = doris::config::storage_root_path;
        }
        olap_res =
                doris::parse_conf_store_paths(doris::config::spill_storage_root_path, &spill_paths);
        if (!olap_res) {
            LOG(ERROR) << "parse config spill storage path failed, path="
                       << doris::config::spill_storage_root_path;
            exit(-1);
        }
        std::set<std::string> broken_paths;
        doris::parse_conf_broken_store_paths(doris::config::broken_storage_path, &broken_paths);

        auto it = paths.begin();
        for (; it != paths.end();) {
            if (broken_paths.contains(it->path)) {
                if (doris::config::ignore_broken_disk) {
                    LOG(WARNING) << "ignore broken disk, path = " << it->path;
                    it = paths.erase(it);
                } else {
                    LOG(ERROR) << "a broken disk is found " << it->path;
                    exit(-1);
                }
            } else if (!doris::check_datapath_rw(it->path)) {
                if (doris::config::ignore_broken_disk) {
                    LOG(WARNING) << "read write test file failed, path=" << it->path;
                    it = paths.erase(it);
                } else {
                    LOG(ERROR) << "read write test file failed, path=" << it->path;
                    // if only one disk and the disk is full, also need exit because rocksdb will open failed
                    exit(-1);
                }
            } else {
                ++it;
            }
        }

        if (paths.empty()) {
            LOG(ERROR) << "All disks are broken, exit.";
            exit(-1);
        }

        it = spill_paths.begin();
        for (; it != spill_paths.end();) {
            if (!doris::check_datapath_rw(it->path)) {
                if (doris::config::ignore_broken_disk) {
                    LOG(WARNING) << "read write test file failed, path=" << it->path;
                    it = spill_paths.erase(it);
                } else {
                    LOG(ERROR) << "read write test file failed, path=" << it->path;
                    exit(-1);
                }
            } else {
                ++it;
            }
        }
        if (spill_paths.empty()) {
            LOG(ERROR) << "All spill disks are broken, exit.";
            exit(-1);
        }

        auto* exec_env = doris::ExecEnv::GetInstance();
        auto status = doris::ExecEnv::init(exec_env, paths, spill_paths, broken_paths);
        if (!status.ok()) {
            return status;
        }

        if (doris::config::is_cloud_mode() &&
            exec_env->obj_storage_upload_thread_pool() != nullptr) {
            // Keep the ExecEnv-owned pool in cloud mode so AdaptiveThreadPoolController does not
            // hold a dangling pointer to a replaced ThreadPool.
            auto* pool = exec_env->obj_storage_upload_thread_pool();
            if (pool->min_threads() > 512) {
                RETURN_IF_ERROR(pool->set_min_threads(256));
                RETURN_IF_ERROR(pool->set_max_threads(512));
            } else {
                RETURN_IF_ERROR(pool->set_max_threads(512));
                RETURN_IF_ERROR(pool->set_min_threads(256));
            }
        } else {
            std::unique_ptr<doris::ThreadPool> obj_storage_upload_pool;
            static_cast<void>(doris::ThreadPoolBuilder("MicrobenchS3FileUploadThreadPool")
                                      .set_min_threads(256)
                                      .set_max_threads(512)
                                      .build(&obj_storage_upload_pool));
            exec_env->set_obj_storage_upload_thread_pool(std::move(obj_storage_upload_pool));
        }
        if (exec_env->file_cache_open_fd_cache() == nullptr) {
            exec_env->set_file_cache_open_fd_cache(std::make_unique<doris::io::FDCache>());
        }
        return Status::OK();
    }

    doris::Status reload_cache_in_config() {
        std::unordered_set<std::string> cache_path_set;
        std::vector<doris::CachePath> cache_paths;
        RETURN_IF_ERROR(doris::parse_conf_cache_paths(doris::config::file_cache_path, cache_paths));

        std::vector<CachePath> cache_paths_no_dup;
        cache_paths_no_dup.reserve(cache_paths.size());
        for (const auto& cache_path : cache_paths) {
            if (cache_path_set.contains(cache_path.path)) {
                LOG(WARNING) << fmt::format("cache path {} is duplicate", cache_path.path);
                continue;
            }
            cache_path_set.emplace(cache_path.path);
            cache_paths_no_dup.emplace_back(cache_path);
        }
        RETURN_IF_ERROR(doris::io::FileCacheFactory::instance()->reload_file_cache(cache_paths));
        return Status::OK();
    }

    std::unique_ptr<JobManager> create_job_manager_from_current_config() {
        return std::make_unique<JobManager>(shared_from_this());
    }

    void start_reload_worker() {
        std::lock_guard<std::mutex> lock(_reload_mt);
        if (_reload_thread.joinable()) {
            return;
        }

        _reloading.store(false);
        _stop_thread = false;
        _reload_thread = std::thread(&BenchEnvManager::_reload_worker_func, this);
        LOG(INFO) << "Reload worker thread started";
    }

    void stop_reload_worker() {
        {
            std::unique_lock<std::mutex> lock(_reload_mt);
            if (_stop_thread) {
                return;
            }

            _stop_thread = true;
            _reload_cv.notify_all();
        }
        if (_reload_thread.joinable()) {
            _reload_thread.join();
        }
    }

    void reload_request() {
        _reloading.store(true);
        _reload_cv.notify_one();
    }

    std::string reload_details_stat() {
        return _reloading.load() ? "Reloading config" : "Reload finished or not started";
    }

private:
    void _reload_worker_func() {
        while (true) {
            std::unique_lock<std::mutex> l(_reload_mt);
            // Wait until stopped or safe to reload (State == Reloading && JobCount == 0)
            _reload_cv.wait(l, [this]() { return _stop_thread; });

            if (_stop_thread) {
                break;
            }

            LOG(INFO) << "Starting configuration reload sequence...";

            l.unlock();

            doris::Status status;
            try {
                status = load_config();
                if (!status) {
                    LOG(ERROR) << "Failed to load config!";
                    throw std::runtime_error(status.to_string().c_str());
                }
                status = reload_cache_in_config();
                if (!status) {
                    LOG(ERROR) << "Failed to reload file cache!";
                    throw std::runtime_error(status.to_string().c_str());
                }
            } catch (const std::exception& e) {
                _reloading.store(false);
                LOG(ERROR) << "Exception during reload: " << e.what();
            }
            _reloading.store(false);
        }
    }

    std::string _doris_home;
    std::mutex _reload_mt;
    std::condition_variable _reload_cv;
    std::thread _reload_thread;
    bool _stop_thread {false};
    std::atomic<bool> _reloading {false};
};

class JobManager {
public:
    JobManager(std::shared_ptr<BenchEnvManager> env_mgr)
            : _next_job_id(0),
              _env_mgr(std::move(env_mgr)),
              _job_executor_pool(std::thread::hardware_concurrency()) {
        LOG(INFO) << "Initialized JobManager with " << std::thread::hardware_concurrency()
                  << " executor threads";
    }

    ~JobManager() {
        try {
            stop();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error stopping JobManager: " << e.what();
        }
    }

    // Submit a new job
    std::string submit_job(const JobConfig& config) {
        try {
            std::string job_id = generate_job_id();
            JobConfig job_config = config;
            if (job_config.job_type == JobType::PEER_CACHE_SEED &&
                job_config.source_job_id.empty()) {
                job_config.source_job_id = job_id;
            }

            {
                std::lock_guard<std::mutex> lock(_mutex);
                _jobs[job_id] = std::make_shared<Job>(job_id, job_config);
            }

            LOG(INFO) << "Submitting job " << job_id << " with config: " << job_config.to_string();

            // Execute the job asynchronously
            _job_executor_pool.enqueue(
                    [this, job_id]() { execute_job_with_status_updates(job_id); });

            return job_id;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error submitting job: " << e.what();
            throw std::runtime_error("Failed to submit job: " + std::string(e.what()));
        }
    }

    // Get job status
    const Job& get_job_status(const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            return *(it->second);
        }
        throw std::runtime_error("Job not found: " + job_id);
    }

    std::shared_ptr<Job> get_job_ptr(const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            return it->second;
        }
        return nullptr;
    }

    // List all jobs
    std::vector<std::shared_ptr<Job>> list_jobs() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::shared_ptr<Job>> job_list;
        job_list.reserve(_jobs.size());
        for (const auto& pair : _jobs) {
            job_list.push_back(pair.second);
        }
        return job_list;
    }

    void start() { LOG(INFO) << "JobManager started"; }

    void stop() {
        LOG(INFO) << "Stopping JobManager and waiting for all jobs to complete";
        _job_executor_pool.stop_and_wait();
        LOG(INFO) << "JobManager stopped";
    }

    // Record file information.
    // owner_job_id identifies the submitted Job in _jobs; source_id identifies the key namespace
    // stored in S3FileRecords (defaults to owner_job_id when empty).
    void record_file_info(const std::string& key, size_t data_size, const std::string& owner_job_id,
                          const std::string& source_id = "") {
        const std::string& effective_source_id = source_id.empty() ? owner_job_id : source_id;
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(owner_job_id);
        if (it != _jobs.end()) {
            ::FileInfo file_info = {
                    .filename = key, .data_size = data_size, .job_id = effective_source_id};
            it->second->file_records.push_back(file_info);
            s3_file_records.add_file_info(effective_source_id, file_info);
        } else {
            LOG(ERROR) << "Job ID not found when recording file info: " << owner_job_id;
        }
    }

    // Cancel job (not implemented yet)
    bool cancel_job(const std::string& job_id) {
        LOG(WARNING) << "Job cancellation not implemented yet: " << job_id;
        return false;
    }

private:
    // Generate a unique job ID
    std::string generate_job_id() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::string job_id =
                "job_" + std::to_string(std::time(nullptr)) + "_" + std::to_string(_next_job_id++);
        return job_id;
    }

    // Execute job with status updates
    void execute_job_with_status_updates(const std::string& job_id) {
        std::shared_ptr<Job> job_ptr;

        // Get job pointer and update status to RUNNING
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = _jobs.find(job_id);
            if (it == _jobs.end()) {
                LOG(ERROR) << "Job not found for execution: " << job_id;
                return;
            }
            job_ptr = it->second;
            job_ptr->status = JobStatus::RUNNING;
            job_ptr->start_time = std::chrono::system_clock::now();
        }

        LOG(INFO) << "Starting execution of job " << job_id;

        try {
            // Execute job
            execute_job(job_id);

            // Update status to COMPLETED
            {
                std::lock_guard<std::mutex> lock(_mutex);
                job_ptr->status = JobStatus::COMPLETED;
                job_ptr->end_time = std::chrono::system_clock::now();
            }

            LOG(INFO) << "Job " << job_id << " completed successfully";
        } catch (const std::exception& e) {
            // Update status to FAILED
            {
                std::lock_guard<std::mutex> lock(_mutex);
                job_ptr->status = JobStatus::FAILED;
                job_ptr->error_message = e.what();
                job_ptr->end_time = std::chrono::system_clock::now();
            }

            LOG(ERROR) << "Job " << job_id << " failed: " << e.what();
        }
    }

    // Core logic for executing a job
    void execute_job(const std::string& job_id) {
        std::shared_ptr<Job> job_ptr = get_job_ptr(job_id);
        if (!job_ptr) {
            throw std::runtime_error("Job not found");
        }

        Job& job = *job_ptr;
        JobConfig& config = job.config;
        LOG(INFO) << "Executing job " << job_id << " with config: " << config.to_string();

        if (config.job_type == JobType::FILE_CACHE_GET_OR_SET) {
            execute_file_cache_get_or_set(job);
            return;
        }
        if (config.job_type == JobType::PEER_CACHE_SEED) {
            execute_peer_cache_seed(job);
            return;
        }
        if (config.job_type == JobType::PEER_READ) {
            execute_peer_read(job);
            return;
        }

        // Generate multiple keys
        std::vector<std::string> keys;
        keys.reserve(config.num_files);

        std::string rewrite_job_id = job_id;
        // If it's a read-only job, find the previously written files
        if (config.read_iops > 0 && config.write_iops == 0) {
            std::string old_job_id =
                    s3_file_records.find_job_id_by_prefix(get_prefix() + config.file_prefix);
            if (old_job_id.empty()) {
                throw std::runtime_error(
                        "Can't find previously job uploaded files. Please make sure read "
                        "files exist in obj or It is also possible that you have restarted "
                        "the file_cache_microbench program, job_id = " +
                        job_id);
            }
            rewrite_job_id = old_job_id;
        }

        // Generate file keys
        for (int i = 0; i < config.num_files; ++i) {
            keys.push_back(get_prefix() + config.file_prefix + "/" + rewrite_job_id + "_" +
                           std::to_string(i));
        }

        // Execute write tasks
        if (config.write_iops > 0) {
            execute_write_tasks(keys, job, config);
        }

        // Execute read tasks
        if (config.read_iops > 0) {
            execute_read_tasks(keys, job, config);
        }

        LOG(INFO) << "Job " << job_id << " execution completed";
    }

    static doris::io::FileCacheType parse_file_cache_type(const std::string& cache_type) {
        if (cache_type == "TTL") {
            return doris::io::FileCacheType::TTL;
        }
        if (cache_type == "INDEX") {
            return doris::io::FileCacheType::INDEX;
        }
        if (cache_type == "DISPOSABLE") {
            return doris::io::FileCacheType::DISPOSABLE;
        }
        return doris::io::FileCacheType::NORMAL;
    }

    static size_t pick_range_value(std::mt19937_64& gen, int64_t left, int64_t right) {
        if (left + 1 == right) {
            return static_cast<size_t>(left);
        }
        std::uniform_int_distribution<int64_t> dis(left, right - 1);
        return static_cast<size_t>(dis(gen));
    }

    std::map<std::string, std::string> object_storage_properties(const JobConfig& config) const {
        return {{"provider", "AWS"},
                {"AWS_ENDPOINT", doris::config::test_s3_endpoint},
                {"AWS_REGION", doris::config::test_s3_region},
                {"AWS_ACCESS_KEY", doris::config::test_s3_ak},
                {"AWS_SECRET_KEY", doris::config::test_s3_sk},
                {"AWS_MAX_CONNECTIONS", std::to_string(std::max(256, config.num_threads * 4))},
                {"AWS_REQUEST_TIMEOUT_MS", "60000"},
                {"AWS_CONNECTION_TIMEOUT_MS", "3000"}};
    }

    // Execute write tasks
    void execute_write_tasks(const std::vector<std::string>& keys, Job& job,
                             const JobConfig& config) {
        doris::S3URI uri("s3://" + doris::config::test_s3_bucket);
        auto parsed_uri = uri.parse();
        if (!parsed_uri.ok()) {
            throw std::runtime_error(parsed_uri.to_string());
        }
        auto parsed = doris::parse_be_obj_storage_config(object_storage_properties(config),
                                                         uri.get_bucket());
        if (!parsed.has_value()) {
            throw std::runtime_error(parsed.error().to_string());
        }
        auto resource = std::move(parsed).value();
        auto client = std::make_shared<doris::io::ObjStorageClientHolder>(
                std::move(resource.client),
                doris::ObjStorageRateLimiterManager::policy_for(resource.is_internal_bucket),
                resource.bucket);
        doris::Status init_status = client->init();
        if (!init_status.ok()) {
            throw std::runtime_error("Failed to initialize S3 client: " + init_status.to_string());
        }

        std::atomic<int> completed_writes(0);
        std::vector<std::future<void>> write_futures;
        write_futures.reserve(keys.size());
        BenchThreadPool write_pool(config.num_threads);

        // Start write tasks
        doris::MonotonicStopWatch write_stopwatch;
        write_stopwatch.start();
        for (int i = 0; i < keys.size(); ++i) {
            const auto& key = keys[i];
            write_futures.push_back(write_pool.enqueue([&, key]() {
                try {
                    DataGenerator data_generator(config.size_bytes_perfile);
                    doris::io::FileWriterOptions options;
                    if (config.cache_type == "TTL") {
                        options.file_cache_expiration_time = config.expiration;
                    }
                    options.write_file_cache = config.write_file_cache;
                    auto writer = std::make_unique<MicrobenchS3FileWriter>(
                            client, doris::config::test_s3_bucket, key, &options,
                            job.write_limiter);
                    doris::Defer defer {[&]() {
                        if (auto status = writer->close(); !status.ok()) {
                            LOG(ERROR) << "close file writer failed" << status.to_string();
                        }
                    }};

                    std::vector<doris::Slice> slices;
                    slices.reserve(4);
                    std::vector<std::string> slice_buffers;
                    slice_buffers.reserve(4);
                    size_t accumulated_size = 0;

                    // Stream data writing
                    while (data_generator.has_more()) {
                        doris::Slice chunk = data_generator.next_chunk(key);
                        slice_buffers.emplace_back(chunk.data, chunk.size);
                        const std::string& stored_chunk = slice_buffers.back();
                        slices.emplace_back(stored_chunk.data(), stored_chunk.size());
                        accumulated_size += stored_chunk.size();

                        if (accumulated_size >= config.write_batch_size ||
                            !data_generator.has_more()) {
                            doris::Status status = writer->appendv(slices.data(), slices.size(),
                                                                   job.write_latency);
                            if (!status.ok()) {
                                throw std::runtime_error("Write error for key " + key + ": " +
                                                         status.to_string());
                            }
                            slices.clear();
                            slice_buffers.clear();
                            accumulated_size = 0;
                        }
                    }
                    if (job.completion_tracker) {
                        job.completion_tracker->mark_completed(key);
                    }

                    // Record successful file information
                    size_t data_size = config.size_bytes_perfile;
                    record_file_info(key, data_size, job.job_id);
                    completed_writes++;
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Write task failed for segment " << key << ": " << e.what();
                }
            }));
        }

        // Wait for all write tasks to complete
        for (auto& future : write_futures) {
            future.get();
        }
        write_stopwatch.stop();

        // Convert write time from nanoseconds to seconds and format as string
        double total_write_time_seconds =
                write_stopwatch.elapsed_time() / 1e9; // nanoseconds to seconds
        job.stats.total_write_time =
                std::to_string(total_write_time_seconds) + " seconds"; // Save as string
        LOG(INFO) << "Total write time: " << job.stats.total_write_time << " seconds";
    }

    void execute_file_cache_get_or_set(Job& job) {
        const JobConfig& config = job.config;
        LOG(INFO) << "Executing file_cache_get_or_set job " << job.job_id
                  << " with config: " << config.to_string();

        std::vector<std::string> keys;
        keys.reserve(config.num_files);
        for (int i = 0; i < config.num_files; ++i) {
            keys.push_back(get_prefix() + config.file_prefix + "/" + job.job_id + "_" +
                           std::to_string(i));
        }

        BenchThreadPool pool(config.num_threads);
        std::vector<std::future<void>> futures;
        futures.reserve(keys.size());
        std::mutex stats_mutex;
        std::mutex error_mutex;
        doris::io::ReadStatistics total_stats;
        std::atomic<int64_t> total_ops {0};
        std::atomic<int64_t> total_blocks {0};
        std::atomic<bool> has_error {false};
        std::string first_error;

        doris::MonotonicStopWatch stopwatch;
        stopwatch.start();
        for (const auto& key : keys) {
            futures.push_back(pool.enqueue([&, key]() {
                try {
                    doris::io::ReadStatistics local_stats;
                    int64_t local_ops = 0;
                    int64_t local_blocks = 0;
                    doris::io::FileCacheType cache_type = parse_file_cache_type(config.cache_type);

                    auto hash = doris::io::BlockFileCache::hash(key);
                    doris::io::BlockFileCache* cache =
                            doris::io::FileCacheFactory::instance()->get_by_path(hash);
                    if (cache == nullptr) {
                        throw std::runtime_error("No file cache instance found for key " + key);
                    }

                    std::mt19937_64 gen(std::hash<std::string> {}(key));
                    for (int64_t i = 0; i < config.repeat; ++i) {
                        size_t offset = pick_range_value(gen, config.get_or_set_offset_left,
                                                         config.get_or_set_offset_right);
                        size_t size = pick_range_value(gen, config.get_or_set_size_left,
                                                       config.get_or_set_size_right);
                        if (offset >= static_cast<size_t>(config.size_bytes_perfile)) {
                            offset = static_cast<size_t>(config.size_bytes_perfile - 1);
                        }
                        if (offset + size > static_cast<size_t>(config.size_bytes_perfile)) {
                            size = static_cast<size_t>(config.size_bytes_perfile) - offset;
                        }

                        doris::MonotonicStopWatch single_sw;
                        single_sw.start();
                        doris::io::CacheContext cache_context;
                        cache_context.stats = &local_stats;
                        cache_context.cache_type = cache_type;
                        cache_context.expiration_time = config.expiration;
                        auto holder = cache->get_or_set(hash, offset, size, cache_context);
                        single_sw.stop();
                        local_stats.cache_get_or_set_timer += single_sw.elapsed_time();
                        local_blocks += holder.file_blocks.size();
                        ++local_ops;
                        microbench_get_or_set_latency << (single_sw.elapsed_time() / 1000);
                    }

                    {
                        std::lock_guard<std::mutex> lock(stats_mutex);
                        total_stats.cache_get_or_set_timer += local_stats.cache_get_or_set_timer;
                        total_stats.lock_wait_timer += local_stats.lock_wait_timer;
                        total_stats.get_timer += local_stats.get_timer;
                        total_stats.set_timer += local_stats.set_timer;
                    }
                    total_ops.fetch_add(local_ops, std::memory_order_relaxed);
                    total_blocks.fetch_add(local_blocks, std::memory_order_relaxed);
                } catch (const std::exception& e) {
                    has_error.store(true, std::memory_order_relaxed);
                    std::lock_guard<std::mutex> lock(error_mutex);
                    if (first_error.empty()) {
                        first_error = e.what();
                    }
                }
            }));
        }

        for (auto& future : futures) {
            future.get();
        }
        stopwatch.stop();
        if (has_error.load(std::memory_order_relaxed)) {
            throw std::runtime_error(first_error.empty() ? "file_cache_get_or_set task failed"
                                                         : first_error);
        }

        const double elapsed_sec = stopwatch.elapsed_time() / 1e9;
        job.stats.total_read_time = std::to_string(elapsed_sec) + " seconds";
        job.stats.cache_get_or_set_timer = total_stats.cache_get_or_set_timer;
        job.stats.lock_wait_timer = total_stats.lock_wait_timer;
        job.stats.get_timer = total_stats.get_timer;
        job.stats.set_timer = total_stats.set_timer;
        job.stats.get_or_set_ops = total_ops.load(std::memory_order_relaxed);
        job.stats.get_or_set_blocks = total_blocks.load(std::memory_order_relaxed);
        job.stats.get_or_set_qps =
                elapsed_sec > 0 ? static_cast<double>(job.stats.get_or_set_ops) / elapsed_sec : 0;
        job.stats.avg_get_or_set_us =
                job.stats.get_or_set_ops > 0
                        ? static_cast<double>(job.stats.cache_get_or_set_timer) /
                                  job.stats.get_or_set_ops / 1000
                        : 0;

        LOG(INFO) << "Completed file_cache_get_or_set job " << job.job_id
                  << ", ops=" << job.stats.get_or_set_ops
                  << ", blocks=" << job.stats.get_or_set_blocks << ", elapsed_sec=" << elapsed_sec
                  << ", qps=" << job.stats.get_or_set_qps;
    }

public:
    static void apply_peer_candidates(int64_t tablet_id,
                                      const std::vector<PeerEndpointConfig>& peers) {
        if (!doris::config::is_cloud_mode()) {
            throw std::runtime_error(
                    "Cloud mode is required for peer candidates (set cloud_unique_id in be.conf)");
        }
        doris::TabletPeerCandidates tpc;
        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        tpc.candidates.reserve(peers.size());
        for (const auto& ep : peers) {
            doris::PeerCandidate cand;
            cand.host = ep.host;
            cand.brpc_port = ep.port;
            cand.compute_group_id = ep.compute_group_id;
            cand.last_access_time_ms = now_ms;
            cand.consecutive_rpc_failures = 0;
            tpc.candidates.emplace_back(std::move(cand));
        }
        doris::ExecEnv::GetInstance()
                ->storage_engine()
                .to_cloud()
                .cloud_warm_up_manager()
                .set_tablet_peer_candidates(tablet_id, std::move(tpc));
    }

private:
    static bool has_inflight_async_write_for_key(doris::io::BlockFileCache* cache,
                                                 const doris::io::UInt128Wrapper& hash,
                                                 size_t file_size) {
        auto* inflight_index = cache->inflight_write_buffer_index();
        if (inflight_index != nullptr && inflight_index->count() > 0 && file_size > 0) {
            const size_t block_size = std::max<size_t>(
                    1, static_cast<size_t>(doris::config::file_cache_each_block_size));
            for (size_t off = 0; off < file_size; off += block_size) {
                if (inflight_index->lookup(hash, off) != nullptr) {
                    return true;
                }
            }
        }
        return false;
    }

    static void clear_local_cache_for_key(const std::string& key, size_t file_size,
                                          bool wait_for_cold = true) {
        auto hash = doris::io::BlockFileCache::hash(doris::io::Path(key).filename().native());
        auto* cache = doris::io::FileCacheFactory::instance()->get_by_path(hash);
        if (cache == nullptr) {
            return;
        }
        cache->remove_if_cached(hash);
        if (!wait_for_cold) {
            return;
        }

        constexpr auto kColdClearTimeout = std::chrono::seconds(30);
        const auto deadline = std::chrono::steady_clock::now() + kColdClearTimeout;
        auto* awm = cache->async_write_manager();
        auto* inflight_index = cache->inflight_write_buffer_index();

        auto has_pending_async_state = [&]() -> bool {
            if (has_inflight_async_write_for_key(cache, hash, file_size)) {
                return true;
            }
            if ((inflight_index == nullptr ||
                 !doris::config::enable_async_file_cache_write_inflight_write_buffer_index) &&
                awm != nullptr && awm->pending_count() > 0) {
                return true;
            }
            return false;
        };

        while (true) {
            if (!has_pending_async_state()) {
                // Re-run remove_if_cached after async write tasks finalize (and once any pinned
                // deleting block drops its external reference) so resident/deleting blocks exit.
                if (!cache->dump_structure(hash).empty()) {
                    cache->remove_if_cached(hash);
                }
                if (cache->dump_structure(hash).empty() && !has_pending_async_state()) {
                    return;
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(
                        "Timed out waiting for local cache blocks and async inflight writes to "
                        "drain for key " +
                        key);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void execute_peer_cache_seed(Job& job) {
        const JobConfig& config = job.config;
        const std::string seed_id =
                config.source_job_id.empty() ? job.job_id : config.source_job_id;
        LOG(INFO) << "Executing peer_cache_seed job " << job.job_id << ", seed_id=" << seed_id
                  << ", config: " << config.to_string();

        std::vector<std::string> keys;
        keys.reserve(config.num_files);
        for (int i = 0; i < config.num_files; ++i) {
            keys.push_back(get_prefix() + config.file_prefix + "/" + seed_id + "_" +
                           std::to_string(i));
        }

        const doris::io::FileCacheType cache_type = parse_file_cache_type(config.cache_type);
        const size_t file_size = static_cast<size_t>(config.size_bytes_perfile);
        const size_t step_size =
                std::max<size_t>(1, static_cast<size_t>(doris::config::file_cache_each_block_size));

        BenchThreadPool pool(config.num_threads);
        std::vector<std::future<void>> futures;
        futures.reserve(keys.size());
        std::mutex error_mutex;
        std::atomic<bool> has_error {false};
        std::string first_error;
        std::atomic<int64_t> total_bytes_written {0};
        std::atomic<int64_t> total_blocks_seeded {0};
        std::atomic<int64_t> total_files_seeded {0};

        doris::MonotonicStopWatch stopwatch;
        stopwatch.start();

        for (const auto& key : keys) {
            futures.push_back(pool.enqueue([&, key]() {
                if (has_error.load(std::memory_order_relaxed)) {
                    return;
                }
                try {
                    auto filename = doris::io::Path(key).filename().native();
                    auto hash = doris::io::BlockFileCache::hash(filename);
                    auto* cache = doris::io::FileCacheFactory::instance()->get_by_path(hash);
                    if (cache == nullptr) {
                        throw std::runtime_error("No file cache instance found for key " + key);
                    }
                    if (config.overwrite_existing) {
                        clear_local_cache_for_key(key, file_size, /*wait_for_cold=*/true);
                    }

                    doris::io::ReadStatistics local_stats;
                    std::vector<char> block_buf;
                    size_t offset = 0;
                    int64_t local_blocks = 0;
                    int64_t local_bytes = 0;

                    while (offset < file_size) {
                        size_t req_size = std::min(step_size, file_size - offset);
                        doris::io::CacheContext ctx;
                        ctx.stats = &local_stats;
                        ctx.cache_type = cache_type;
                        ctx.expiration_time = config.expiration;
                        ctx.tablet_id = config.tablet_id;

                        doris::MonotonicStopWatch block_sw;
                        block_sw.start();
                        constexpr auto kSeedBlockTimeout = std::chrono::seconds(10);
                        const auto deadline = std::chrono::steady_clock::now() + kSeedBlockTimeout;
                        bool range_seeded = false;
                        while (!range_seeded) {
                            bool need_retry = false;
                            bool saw_deleting = false;
                            std::string last_reason;
                            {
                                auto holder = cache->get_or_set(hash, offset, req_size, ctx);
                                if (holder.file_blocks.empty()) {
                                    throw std::runtime_error(
                                            "FileCache::get_or_set returned no blocks for key " +
                                            key + ", offset=" + std::to_string(offset));
                                }
                                int64_t attempt_blocks = 0;
                                for (auto& fb : holder.file_blocks) {
                                    auto st_state = fb->state();
                                    if (st_state == doris::io::FileBlock::State::SKIP_CACHE) {
                                        throw std::runtime_error(
                                                "FileBlock entered SKIP_CACHE while seeding key " +
                                                key +
                                                " (check file_cache_path total_size / "
                                                "query_limit / disk space)");
                                    }
                                    if (cache->is_block_deleting(fb)) {
                                        need_retry = true;
                                        saw_deleting = true;
                                        last_reason = "block is marked deleting";
                                        break;
                                    }
                                    if (st_state == doris::io::FileBlock::State::EMPTY) {
                                        if (fb->get_or_set_downloader() ==
                                            doris::io::FileBlock::get_caller_id()) {
                                            size_t fb_offset = fb->offset();
                                            size_t fb_size = fb->range().size();
                                            if (block_buf.size() < fb_size) {
                                                block_buf.resize(fb_size);
                                            }
                                            fill_deterministic_data(key, file_size, fb_offset,
                                                                    block_buf.data(), fb_size);
                                            auto st = fb->append(
                                                    doris::Slice(block_buf.data(), fb_size));
                                            if (!st.ok()) {
                                                throw std::runtime_error(
                                                        "FileBlock::append failed for key " + key +
                                                        ": " + st.to_string());
                                            }
                                            st = fb->finalize();
                                            if (!st.ok()) {
                                                throw std::runtime_error(
                                                        "FileBlock::finalize failed for key " +
                                                        key + ": " + st.to_string());
                                            }
                                            local_bytes += static_cast<int64_t>(fb_size);
                                        } else {
                                            st_state = fb->wait();
                                        }
                                    } else if (st_state ==
                                               doris::io::FileBlock::State::DOWNLOADING) {
                                        st_state = fb->wait();
                                    }
                                    if (cache->is_block_deleting(fb)) {
                                        need_retry = true;
                                        saw_deleting = true;
                                        last_reason = "block became deleting during seed";
                                        break;
                                    }
                                    st_state = fb->state();
                                    if (st_state == doris::io::FileBlock::State::DOWNLOADING ||
                                        st_state == doris::io::FileBlock::State::EMPTY) {
                                        need_retry = true;
                                        last_reason =
                                                "block state after wait is " +
                                                doris::io::FileBlock::state_to_string(st_state);
                                        break;
                                    }
                                    if (st_state != doris::io::FileBlock::State::DOWNLOADED) {
                                        throw std::runtime_error(
                                                "Peer seed block is not DOWNLOADED for key " + key +
                                                ", offset=" + std::to_string(fb->offset()) +
                                                ", state=" +
                                                doris::io::FileBlock::state_to_string(st_state));
                                    }
                                    ++attempt_blocks;
                                }
                                if (!need_retry) {
                                    local_blocks += attempt_blocks;
                                    range_seeded = true;
                                }
                            }
                            if (need_retry) {
                                if (saw_deleting && config.overwrite_existing) {
                                    cache->remove_if_cached(hash);
                                }
                                if (std::chrono::steady_clock::now() >= deadline) {
                                    throw std::runtime_error("Timed out seeding block for key " +
                                                             key +
                                                             ", offset=" + std::to_string(offset) +
                                                             ": " + last_reason);
                                }
                                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                            }
                        }
                        block_sw.stop();
                        microbench_peer_seed_latency << (block_sw.elapsed_time() / 1000);
                        offset += req_size;
                    }

                    record_file_info(key, file_size, job.job_id, seed_id);
                    total_bytes_written.fetch_add(local_bytes, std::memory_order_relaxed);
                    total_blocks_seeded.fetch_add(local_blocks, std::memory_order_relaxed);
                    total_files_seeded.fetch_add(1, std::memory_order_relaxed);
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    if (!has_error.exchange(true)) {
                        first_error = e.what();
                    }
                    LOG(ERROR) << "peer_cache_seed failed for key " << key << ": " << e.what();
                }
            }));
        }

        for (auto& f : futures) {
            f.get();
        }
        stopwatch.stop();

        if (has_error.load()) {
            throw std::runtime_error(first_error);
        }

        double elapsed_sec = stopwatch.elapsed_time() / 1e9;
        job.stats.total_write_time = std::to_string(elapsed_sec) + " seconds";
        job.stats.bytes_write_into_cache = total_bytes_written.load();
        job.stats.peer_seed_files = total_files_seeded.load();
        job.stats.peer_seed_blocks = total_blocks_seeded.load();
        job.stats.peer_seed_throughput_mb_s =
                elapsed_sec > 0 ? (static_cast<double>(job.stats.bytes_write_into_cache) /
                                   (1024.0 * 1024.0)) /
                                          elapsed_sec
                                : 0;

        LOG(INFO) << "Completed peer_cache_seed job " << job.job_id
                  << ", files=" << job.stats.peer_seed_files
                  << ", blocks=" << job.stats.peer_seed_blocks
                  << ", bytes=" << job.stats.bytes_write_into_cache
                  << ", elapsed_sec=" << elapsed_sec
                  << ", throughput_mb_s=" << job.stats.peer_seed_throughput_mb_s;
    }

    void execute_peer_read(Job& job) {
        const JobConfig& config = job.config;
        LOG(INFO) << "Executing peer_read job " << job.job_id
                  << " with config: " << config.to_string();

        if (!config.direct_peer_rpc) {
            if (!doris::config::is_cloud_mode()) {
                throw std::runtime_error(
                        "peer_read requires cloud mode (set cloud_unique_id in be.conf)");
            }
            if (!config.bypass_peer_read && !doris::config::enable_cache_read_from_peer) {
                throw std::runtime_error(
                        "config::enable_cache_read_from_peer is false; enable it via be.conf or "
                        "/update_config before running peer_read");
            }
        }

        if (!config.peer_endpoints.empty() && !config.direct_peer_rpc) {
            apply_peer_candidates(config.tablet_id, config.peer_endpoints);
        }

        std::vector<PeerEndpointConfig> direct_endpoints = config.peer_endpoints;
        if (config.direct_peer_rpc && direct_endpoints.empty()) {
            if (doris::config::is_cloud_mode()) {
                auto cands = doris::ExecEnv::GetInstance()
                                     ->storage_engine()
                                     .to_cloud()
                                     .cloud_warm_up_manager()
                                     .get_peer_candidates(config.tablet_id);
                for (const auto& c : cands) {
                    direct_endpoints.push_back({c.host, c.brpc_port, c.compute_group_id});
                }
            }
            if (direct_endpoints.empty()) {
                throw std::runtime_error(
                        "direct_peer_rpc requires peer_endpoints or pre-configured "
                        "/set_peer_candidates");
            }
        }

        std::string resolved_source_job_id = config.source_job_id;
        if (resolved_source_job_id.empty()) {
            resolved_source_job_id =
                    s3_file_records.find_job_id_by_prefix(get_prefix() + config.file_prefix + "/");
            if (resolved_source_job_id.empty()) {
                throw std::runtime_error(
                        "source_job_id is required when no local file records match file_prefix=" +
                        config.file_prefix);
            }
            job.set_resolved_source_job_id(resolved_source_job_id);
        }

        std::vector<std::string> keys;
        keys.reserve(config.num_files);
        for (int i = 0; i < config.num_files; ++i) {
            keys.push_back(get_prefix() + config.file_prefix + "/" + resolved_source_job_id + "_" +
                           std::to_string(i));
        }

        const size_t file_size_for_clear = static_cast<size_t>(config.size_bytes_perfile);
        if (!config.direct_peer_rpc && config.clear_local_cache_before_read) {
            for (const auto& key : keys) {
                clear_local_cache_for_key(key, file_size_for_clear, /*wait_for_cold=*/true);
            }
        }

        const size_t file_size = static_cast<size_t>(config.size_bytes_perfile);
        const int64_t offset_left = config.read_offset_left;
        const int64_t offset_right = std::min(config.read_offset_right, config.size_bytes_perfile);
        const int64_t length_left = config.read_length_left;
        const int64_t length_right =
                std::min(config.read_length_right, config.size_bytes_perfile + 1);
        if (offset_left >= offset_right) {
            throw std::runtime_error(
                    "read_offset_left must be less than min(read_offset_right, "
                    "size_bytes_perfile)");
        }
        if (length_left >= length_right) {
            throw std::runtime_error(
                    "read_length_left must be less than min(read_length_right, "
                    "size_bytes_perfile + 1)");
        }

        const auto properties = object_storage_properties(config);
        const std::string bucket = doris::config::test_s3_bucket.empty()
                                           ? "microbench"
                                           : doris::config::test_s3_bucket;
        const std::string obj_path_prefix = "s3://" + bucket + "/";

        std::mutex stats_mutex;
        std::mutex error_mutex;
        std::atomic<bool> has_error {false};
        std::string first_error;
        doris::io::FileCacheStatistics merged_stats;
        struct WeightedLatencySample {
            double latency_us = 0.0;
            double weight = 1.0;
        };
        std::atomic<int64_t> total_ops {0};
        std::atomic<int64_t> total_bytes_read {0};
        std::atomic<int64_t> total_wire_bytes_read {0};
        std::atomic<int64_t> total_read_ns {0};
        constexpr size_t kMaxLatencySamples = 2000000;
        std::vector<WeightedLatencySample> all_latencies_us;
        double global_max_read_us = 0.0;

        auto init_io_ctx = [&](doris::io::IOContext& io_ctx,
                               doris::io::FileCacheStatistics* thread_stats) {
            io_ctx.file_cache_stats = thread_stats;
            io_ctx.bypass_peer_read = config.bypass_peer_read;
            if (config.cache_type == "DISPOSABLE") {
                io_ctx.is_disposable = true;
            } else if (config.cache_type == "TTL") {
                io_ctx.expiration_time = config.expiration;
            } else if (config.cache_type == "INDEX") {
                io_ctx.is_index_data = true;
            }
            if (config.cache_write_mode == "SYNC_WRITE") {
                io_ctx.cache_write_mode_override = doris::io::CacheWriteMode::SYNC_WRITE;
            } else if (config.cache_write_mode == "ASYNC_WRITE") {
                io_ctx.cache_write_mode_override = doris::io::CacheWriteMode::ASYNC_WRITE;
            }
        };

        auto merge_stats = [&](const doris::io::FileCacheStatistics& s) {
            std::lock_guard<std::mutex> lock(stats_mutex);
            merged_stats.num_local_io_total += s.num_local_io_total;
            merged_stats.num_remote_io_total += s.num_remote_io_total;
            merged_stats.num_peer_io_total += s.num_peer_io_total;
            merged_stats.inverted_index_num_remote_io_total += s.inverted_index_num_remote_io_total;
            merged_stats.local_io_timer += s.local_io_timer;
            merged_stats.bytes_read_from_local += s.bytes_read_from_local;
            merged_stats.bytes_read_from_remote += s.bytes_read_from_remote;
            merged_stats.bytes_read_from_peer += s.bytes_read_from_peer;
            merged_stats.num_same_cg_peer_io_total += s.num_same_cg_peer_io_total;
            merged_stats.bytes_read_from_same_cg_peer += s.bytes_read_from_same_cg_peer;
            merged_stats.same_cg_peer_io_timer += s.same_cg_peer_io_timer;
            merged_stats.num_cross_cg_peer_io_total += s.num_cross_cg_peer_io_total;
            merged_stats.bytes_read_from_cross_cg_peer += s.bytes_read_from_cross_cg_peer;
            merged_stats.cross_cg_peer_io_timer += s.cross_cg_peer_io_timer;
            merged_stats.remote_io_timer += s.remote_io_timer;
            merged_stats.peer_io_timer += s.peer_io_timer;
            merged_stats.write_cache_io_timer += s.write_cache_io_timer;
            merged_stats.bytes_write_into_cache += s.bytes_write_into_cache;
            merged_stats.num_skip_cache_io_total += s.num_skip_cache_io_total;
            merged_stats.read_cache_file_directly_timer += s.read_cache_file_directly_timer;
            merged_stats.cache_get_or_set_timer += s.cache_get_or_set_timer;
            merged_stats.lock_wait_timer += s.lock_wait_timer;
            merged_stats.get_timer += s.get_timer;
            merged_stats.set_timer += s.set_timer;
        };

        auto open_cached_reader = [&](const std::string& key) -> doris::io::FileReaderSPtr {
            doris::io::FileReaderOptions reader_opts;
            reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
            reader_opts.is_doris_table = true;
            reader_opts.tablet_id = config.tablet_id;

            doris::io::Path full_path(obj_path_prefix + key);
            if (!config.use_s3_fallback) {
                auto raw_reader =
                        std::make_shared<PeerOnlyFallbackFileReader>(full_path, file_size);
                return std::make_shared<doris::io::CachedRemoteFileReader>(std::move(raw_reader),
                                                                           reader_opts);
            }

            doris::io::FileDescription fd;
            fd.path = full_path;
            fd.file_size = static_cast<int64_t>(file_size);
            doris::io::FileSystemProperties fs_props;
            fs_props.system_type = doris::TFileType::FILE_S3;
            fs_props.properties = properties;

            auto status_or_reader =
                    doris::FileFactory::create_file_reader(fs_props, fd, reader_opts, nullptr);
            if (!status_or_reader.has_value()) {
                throw std::runtime_error("Failed to create reader for key " + key + ": " +
                                         status_or_reader.error().to_string());
            }
            return status_or_reader.value();
        };

        BenchThreadPool read_pool(config.num_threads);
        std::vector<std::future<void>> futures;
        doris::MonotonicStopWatch stopwatch;
        stopwatch.start();

        if (config.direct_peer_rpc) {
            const int64_t total_target_ops =
                    static_cast<int64_t>(keys.size()) * static_cast<int64_t>(config.repeat);
            const int worker_count =
                    static_cast<int>(std::min<int64_t>(config.num_threads, total_target_ops));
            const size_t block_size = std::max<size_t>(
                    1, static_cast<size_t>(doris::config::file_cache_each_block_size));
            const size_t worker_sample_cap =
                    std::max<size_t>(1, kMaxLatencySamples / std::max<int>(1, worker_count));
            futures.reserve(worker_count);

            for (int w = 0; w < worker_count; ++w) {
                futures.push_back(read_pool.enqueue([&, w, worker_sample_cap]() {
                    try {
                        doris::io::FileCacheStatistics local_stats;
                        doris::io::IOContext io_ctx;
                        init_io_ctx(io_ctx, &local_stats);
                        std::string buffer;
                        buffer.resize(static_cast<size_t>(length_right - 1));
                        std::mt19937_64 gen(std::hash<int> {}(w + 1) ^ 0x9e3779b97f4a7c15ULL);
                        std::mt19937_64 sample_gen(std::hash<int> {}(w + 1) ^
                                                   0x517cc1b727220a95ULL);

                        int64_t worker_ops = 0;
                        int64_t worker_bytes = 0;
                        int64_t worker_wire_bytes = 0;
                        int64_t worker_ns = 0;
                        double worker_max_us = 0.0;
                        std::vector<double> worker_lat_us;
                        worker_lat_us.reserve(std::min<size_t>(
                                std::min<size_t>(65536, worker_sample_cap),
                                static_cast<size_t>(total_target_ops / worker_count + 1)));

                        for (int64_t op = w; op < total_target_ops; op += worker_count) {
                            if (has_error.load(std::memory_order_relaxed)) {
                                break;
                            }
                            const std::string& key = keys[static_cast<size_t>(op % keys.size())];
                            const auto& ep = direct_endpoints[static_cast<size_t>(
                                    op % direct_endpoints.size())];

                            size_t read_offset = pick_range_value(gen, offset_left, offset_right);
                            size_t read_length = pick_range_value(gen, length_left, length_right);
                            if (read_offset + read_length > file_size) {
                                read_length = file_size - read_offset;
                            }
                            if (read_length == 0) {
                                continue;
                            }

                            doris::io::PeerFileCacheReader peer_reader(
                                    doris::io::Path(obj_path_prefix + key), true, ep.host, ep.port);

                            size_t done_bytes = 0;
                            doris::MonotonicStopWatch op_sw;
                            op_sw.start();
                            while (done_bytes < read_length) {
                                if (job.read_limiter) {
                                    job.read_limiter->add(1);
                                }
                                size_t chunk_len =
                                        std::min(read_length - done_bytes,
                                                 static_cast<size_t>(config.read_chunk_size));
                                size_t chunk_start = read_offset + done_bytes;
                                size_t chunk_end = chunk_start + chunk_len;

                                size_t aligned_start = (chunk_start / block_size) * block_size;
                                size_t aligned_end = std::min(
                                        file_size,
                                        ((chunk_end + block_size - 1) / block_size) * block_size);
                                std::vector<doris::io::FileBlock::Range> ranges;
                                for (size_t cur = aligned_start; cur < aligned_end;) {
                                    size_t seg_end = std::min(aligned_end, cur + block_size);
                                    ranges.emplace_back(cur, seg_end - 1);
                                    cur = seg_end;
                                }
                                size_t expected_fetch_bytes = aligned_end - aligned_start;

                                doris::io::PeerFetchResult peer_res;
                                doris::MonotonicStopWatch rpc_sw;
                                rpc_sw.start();
                                auto st =
                                        peer_reader.fetch_blocks(ranges, &peer_res, file_size,
                                                                 &io_ctx, false, config.tablet_id);
                                rpc_sw.stop();
                                if (!st.ok()) {
                                    throw std::runtime_error("Direct peer fetch failed for key " +
                                                             key + ": " + st.to_string());
                                }
                                if (peer_res.bytes_read != expected_fetch_bytes) {
                                    throw std::runtime_error(
                                            "Incomplete direct peer fetch: expected " +
                                            std::to_string(expected_fetch_bytes) + ", got " +
                                            std::to_string(peer_res.bytes_read));
                                }

                                for (const auto& chunk : peer_res.chunks) {
                                    size_t c_begin = chunk.block_offset;
                                    size_t c_end = c_begin + chunk.payload.size();
                                    if (c_begin < aligned_start || c_end > aligned_end) {
                                        throw std::runtime_error(
                                                "Peer fetch chunk out of bounds for key " + key);
                                    }
                                    size_t copy_begin = std::max(c_begin, chunk_start);
                                    size_t copy_end = std::min(c_end, chunk_end);
                                    if (copy_begin < copy_end) {
                                        size_t copy_len = copy_end - copy_begin;
                                        size_t src_off = copy_begin - c_begin;
                                        size_t dst_off = done_bytes + (copy_begin - chunk_start);
                                        chunk.payload.copy_to(buffer.data() + dst_off, copy_len,
                                                              src_off);
                                    }
                                }

                                local_stats.num_peer_io_total += 1;
                                local_stats.bytes_read_from_peer += static_cast<int64_t>(chunk_len);
                                worker_wire_bytes += static_cast<int64_t>(peer_res.bytes_read);
                                local_stats.peer_io_timer += rpc_sw.elapsed_time();
                                done_bytes += chunk_len;
                            }
                            op_sw.stop();

                            if (config.verify_data &&
                                !DataVerifier::verify_data(key, file_size, read_offset, buffer,
                                                           read_length)) {
                                throw std::runtime_error("Data verification failed for key: " +
                                                         key);
                            }

                            int64_t elapsed_ns = op_sw.elapsed_time();
                            microbench_peer_read_latency << (elapsed_ns / 1000);
                            if (job.read_latency) {
                                *job.read_latency << (elapsed_ns / 1000);
                            }
                            const double lat_us = static_cast<double>(elapsed_ns) / 1000.0;
                            worker_max_us = std::max(worker_max_us, lat_us);
                            if (worker_lat_us.size() < worker_sample_cap) {
                                worker_lat_us.push_back(lat_us);
                            } else {
                                size_t slot = std::uniform_int_distribution<size_t>(
                                        0, static_cast<size_t>(worker_ops))(sample_gen);
                                if (slot < worker_sample_cap) {
                                    worker_lat_us[slot] = lat_us;
                                }
                            }
                            ++worker_ops;
                            worker_bytes += static_cast<int64_t>(read_length);
                            worker_ns += elapsed_ns;
                        }

                        merge_stats(local_stats);
                        if (worker_ops > 0) {
                            const double sample_weight = static_cast<double>(worker_ops) /
                                                         static_cast<double>(worker_lat_us.size());
                            std::lock_guard<std::mutex> lock(stats_mutex);
                            global_max_read_us = std::max(global_max_read_us, worker_max_us);
                            all_latencies_us.reserve(all_latencies_us.size() +
                                                     worker_lat_us.size());
                            for (double lat : worker_lat_us) {
                                all_latencies_us.push_back({lat, sample_weight});
                            }
                        }
                        total_ops.fetch_add(worker_ops, std::memory_order_relaxed);
                        total_bytes_read.fetch_add(worker_bytes, std::memory_order_relaxed);
                        total_wire_bytes_read.fetch_add(worker_wire_bytes,
                                                        std::memory_order_relaxed);
                        total_read_ns.fetch_add(worker_ns, std::memory_order_relaxed);
                    } catch (const std::exception& e) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (!has_error.exchange(true)) {
                            first_error = e.what();
                        }
                        LOG(ERROR) << "peer_read (direct_peer_rpc) worker failed: " << e.what();
                    }
                }));
            }
        } else {
            const size_t worker_sample_cap =
                    std::max<size_t>(1, kMaxLatencySamples / std::max<size_t>(1, keys.size()));
            futures.reserve(keys.size());
            for (size_t idx = 0; idx < keys.size(); ++idx) {
                const std::string key = keys[idx];
                futures.push_back(read_pool.enqueue([&, key, idx, worker_sample_cap]() {
                    try {
                        doris::io::FileCacheStatistics local_stats;
                        doris::io::IOContext io_ctx;
                        init_io_ctx(io_ctx, &local_stats);
                        std::string buffer;
                        buffer.resize(static_cast<size_t>(length_right - 1));
                        std::mt19937_64 gen(std::hash<std::string> {}(key) ^
                                            (static_cast<uint64_t>(idx) + 1));
                        std::mt19937_64 sample_gen(
                                std::hash<std::string> {}(key) ^
                                ((static_cast<uint64_t>(idx) + 1) * 0x9e3779b97f4a7c15ULL));

                        std::unique_ptr<MicrobenchFileReader> persistent_reader;
                        doris::Defer close_persistent {[&]() {
                            if (persistent_reader) {
                                if (auto st = persistent_reader->close(); !st.ok()) {
                                    LOG(ERROR) << "close peer reader failed: " << st.to_string();
                                }
                            }
                        }};

                        if (!config.clear_local_cache_each_repeat) {
                            persistent_reader = std::make_unique<MicrobenchFileReader>(
                                    open_cached_reader(key), job.read_limiter);
                        }

                        int64_t worker_ops = 0;
                        int64_t worker_bytes = 0;
                        int64_t worker_ns = 0;
                        double worker_max_us = 0.0;
                        std::vector<double> worker_lat_us;
                        worker_lat_us.reserve(
                                std::min<size_t>(std::min<size_t>(65536, worker_sample_cap),
                                                 static_cast<size_t>(config.repeat)));

                        for (int j = 0; j < config.repeat; ++j) {
                            if (has_error.load(std::memory_order_relaxed)) {
                                break;
                            }
                            std::unique_ptr<MicrobenchFileReader> repeat_reader;
                            MicrobenchFileReader* active_reader = persistent_reader.get();
                            doris::Defer close_repeat {[&]() {
                                if (repeat_reader) {
                                    if (auto st = repeat_reader->close(); !st.ok()) {
                                        LOG(ERROR) << "close repeat peer reader failed: "
                                                   << st.to_string();
                                    }
                                }
                            }};
                            if (config.clear_local_cache_each_repeat) {
                                clear_local_cache_for_key(key, file_size, /*wait_for_cold=*/true);
                                repeat_reader = std::make_unique<MicrobenchFileReader>(
                                        open_cached_reader(key), job.read_limiter);
                                active_reader = repeat_reader.get();
                            }

                            size_t read_offset = pick_range_value(gen, offset_left, offset_right);
                            size_t read_length = pick_range_value(gen, length_left, length_right);
                            if (read_offset + read_length > file_size) {
                                read_length = file_size - read_offset;
                            }
                            if (read_length == 0) {
                                continue;
                            }

                            size_t done_bytes = 0;
                            doris::MonotonicStopWatch op_sw;
                            op_sw.start();
                            while (done_bytes < read_length) {
                                size_t chunk_len =
                                        std::min(read_length - done_bytes,
                                                 static_cast<size_t>(config.read_chunk_size));
                                doris::Slice read_slice(buffer.data() + done_bytes, chunk_len);
                                size_t bytes_read = 0;
                                doris::Status read_status = active_reader->read_at(
                                        read_offset + done_bytes, read_slice, &bytes_read, &io_ctx,
                                        job.read_latency);
                                if (!read_status.ok()) {
                                    throw std::runtime_error("Peer read error for key " + key +
                                                             ": " + read_status.to_string());
                                }
                                if (bytes_read != chunk_len) {
                                    throw std::runtime_error("Incomplete peer read: expected " +
                                                             std::to_string(chunk_len) +
                                                             " bytes, got " +
                                                             std::to_string(bytes_read));
                                }
                                done_bytes += bytes_read;
                            }
                            op_sw.stop();

                            if (config.verify_data &&
                                !DataVerifier::verify_data(key, file_size, read_offset, buffer,
                                                           read_length)) {
                                throw std::runtime_error("Data verification failed for key: " +
                                                         key);
                            }

                            int64_t elapsed_ns = op_sw.elapsed_time();
                            microbench_peer_read_latency << (elapsed_ns / 1000);
                            const double lat_us = static_cast<double>(elapsed_ns) / 1000.0;
                            worker_max_us = std::max(worker_max_us, lat_us);
                            if (worker_lat_us.size() < worker_sample_cap) {
                                worker_lat_us.push_back(lat_us);
                            } else {
                                size_t slot = std::uniform_int_distribution<size_t>(
                                        0, static_cast<size_t>(worker_ops))(sample_gen);
                                if (slot < worker_sample_cap) {
                                    worker_lat_us[slot] = lat_us;
                                }
                            }
                            ++worker_ops;
                            worker_bytes += static_cast<int64_t>(read_length);
                            worker_ns += elapsed_ns;
                        }

                        merge_stats(local_stats);
                        if (worker_ops > 0) {
                            const double sample_weight = static_cast<double>(worker_ops) /
                                                         static_cast<double>(worker_lat_us.size());
                            std::lock_guard<std::mutex> lock(stats_mutex);
                            global_max_read_us = std::max(global_max_read_us, worker_max_us);
                            all_latencies_us.reserve(all_latencies_us.size() +
                                                     worker_lat_us.size());
                            for (double lat : worker_lat_us) {
                                all_latencies_us.push_back({lat, sample_weight});
                            }
                        }
                        total_ops.fetch_add(worker_ops, std::memory_order_relaxed);
                        total_bytes_read.fetch_add(worker_bytes, std::memory_order_relaxed);
                        total_read_ns.fetch_add(worker_ns, std::memory_order_relaxed);
                    } catch (const std::exception& e) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (!has_error.exchange(true)) {
                            first_error = e.what();
                        }
                        LOG(ERROR) << "peer_read failed for key " << key << ": " << e.what();
                    }
                }));
            }
        }

        for (auto& f : futures) {
            f.get();
        }
        stopwatch.stop();

        if (has_error.load()) {
            throw std::runtime_error(first_error);
        }

        double elapsed_sec = stopwatch.elapsed_time() / 1e9;
        job.stats.total_read_time = std::to_string(elapsed_sec) + " seconds";
        job.stats.num_local_io_total = merged_stats.num_local_io_total;
        job.stats.num_remote_io_total = merged_stats.num_remote_io_total;
        job.stats.num_peer_io_total = merged_stats.num_peer_io_total;
        job.stats.num_inverted_index_remote_io_total =
                merged_stats.inverted_index_num_remote_io_total;
        job.stats.local_io_timer = merged_stats.local_io_timer;
        job.stats.bytes_read_from_local = merged_stats.bytes_read_from_local;
        job.stats.bytes_read_from_remote = merged_stats.bytes_read_from_remote;
        job.stats.bytes_read_from_peer = merged_stats.bytes_read_from_peer;
        job.stats.peer_wire_bytes_read = config.direct_peer_rpc
                                                 ? total_wire_bytes_read.load()
                                                 : (merged_stats.bytes_read_from_same_cg_peer +
                                                    merged_stats.bytes_read_from_cross_cg_peer);
        job.stats.remote_io_timer = merged_stats.remote_io_timer;
        job.stats.peer_io_timer = merged_stats.peer_io_timer;
        job.stats.write_cache_io_timer = merged_stats.write_cache_io_timer;
        job.stats.bytes_write_into_cache = merged_stats.bytes_write_into_cache;
        job.stats.num_skip_cache_io_total = merged_stats.num_skip_cache_io_total;
        job.stats.read_cache_file_directly_timer = merged_stats.read_cache_file_directly_timer;
        job.stats.cache_get_or_set_timer = merged_stats.cache_get_or_set_timer;
        job.stats.lock_wait_timer = merged_stats.lock_wait_timer;
        job.stats.get_timer = merged_stats.get_timer;
        job.stats.set_timer = merged_stats.set_timer;
        job.stats.peer_read_ops = total_ops.load();
        job.stats.peer_total_bytes_read = total_bytes_read.load();
        job.stats.peer_read_qps =
                elapsed_sec > 0 ? static_cast<double>(job.stats.peer_read_ops) / elapsed_sec : 0;
        job.stats.peer_rpc_qps =
                elapsed_sec > 0 ? static_cast<double>(job.stats.num_peer_io_total) / elapsed_sec
                                : 0;
        job.stats.peer_throughput_mb_s =
                elapsed_sec > 0 ? (static_cast<double>(job.stats.peer_total_bytes_read) /
                                   (1024.0 * 1024.0)) /
                                          elapsed_sec
                                : 0;
        job.stats.peer_wire_throughput_mb_s =
                elapsed_sec > 0 ? (static_cast<double>(job.stats.peer_wire_bytes_read) /
                                   (1024.0 * 1024.0)) /
                                          elapsed_sec
                                : 0;
        job.stats.peer_avg_read_us =
                job.stats.peer_read_ops > 0 ? (static_cast<double>(total_read_ns.load()) / 1000.0) /
                                                      static_cast<double>(job.stats.peer_read_ops)
                                            : 0;
        if (!all_latencies_us.empty()) {
            std::sort(all_latencies_us.begin(), all_latencies_us.end(),
                      [](const WeightedLatencySample& a, const WeightedLatencySample& b) {
                          return a.latency_us < b.latency_us;
                      });
            double total_weight = 0.0;
            for (const auto& sample : all_latencies_us) {
                total_weight += sample.weight;
            }
            auto weighted_pct = [&](double q) -> double {
                const double target = q * total_weight;
                double accum = 0.0;
                for (const auto& sample : all_latencies_us) {
                    accum += sample.weight;
                    if (accum >= target) {
                        return sample.latency_us;
                    }
                }
                return all_latencies_us.back().latency_us;
            };
            job.stats.peer_p50_read_us = weighted_pct(0.50);
            job.stats.peer_p95_read_us = weighted_pct(0.95);
            job.stats.peer_p99_read_us = weighted_pct(0.99);
            job.stats.peer_max_read_us = global_max_read_us;
        }

        LOG(INFO) << "Completed peer_read job " << job.job_id << ", ops=" << job.stats.peer_read_ops
                  << ", total_bytes=" << job.stats.peer_total_bytes_read
                  << ", bytes_from_peer=" << job.stats.bytes_read_from_peer
                  << ", peer_wire_bytes=" << job.stats.peer_wire_bytes_read
                  << ", bytes_from_local=" << job.stats.bytes_read_from_local
                  << ", bytes_from_remote=" << job.stats.bytes_read_from_remote
                  << ", elapsed_sec=" << elapsed_sec
                  << ", throughput_mb_s=" << job.stats.peer_throughput_mb_s
                  << ", peer_wire_throughput_mb_s=" << job.stats.peer_wire_throughput_mb_s;
    }

    // Execute read tasks
    void execute_read_tasks(const std::vector<std::string>& keys, Job& job, JobConfig& config) {
        LOG(INFO) << "Starting read tasks for job " << job.job_id << ", num_keys=" << keys.size()
                  << ", read_iops=" << config.read_iops;
        auto start_time = std::chrono::steady_clock::now();

        int64_t exist_job_perfile_size = s3_file_records.get_exist_job_perfile_size_by_prefix(
                get_prefix() + config.file_prefix);
        std::vector<std::future<void>> read_futures;
        doris::io::IOContext io_ctx;
        doris::io::FileCacheStatistics total_stats;
        io_ctx.file_cache_stats = &total_stats;
        if (config.cache_type == "DISPOSABLE") {
            io_ctx.is_disposable = true;
        } else if (config.cache_type == "TTL") {
            io_ctx.expiration_time = config.expiration;
        } else if (config.cache_type == "INDEX") {
            io_ctx.is_index_data = true;
        } else { // default NORMAL
            // do nothing
        }
        BenchThreadPool read_pool(config.num_threads);
        std::atomic<int> completed_reads(0);
        doris::MonotonicStopWatch read_stopwatch; // Add read task timer

        // Create S3 client configuration
        const auto properties = object_storage_properties(config);
        std::vector<std::string> read_files;
        if (exist_job_perfile_size != -1) {
            // read exist files
            s3_file_records.get_exist_job_files_by_prefix(get_prefix() + config.file_prefix,
                                                          read_files, config.num_files);
        }

        if (read_files.empty()) {
            // not read exist files
            read_files = keys;
        }
        LOG(INFO) << "job_id = " << job.job_id << " read_files size = " << read_files.size();

        read_stopwatch.start();
        std::vector<std::string> read_buffers(read_files.size());

        for (auto& buffer : read_buffers) {
            buffer.resize(config.read_length_right);
        }

        for (int i = 0; i < read_files.size(); ++i) {
            const auto& key = read_files[i];
            read_futures.push_back(read_pool.enqueue([&, &buffer = read_buffers[i], key]() {
                try {
                    if (job.completion_tracker) {
                        job.completion_tracker->wait_for_completion(
                                key); // Wait for file completion
                    }
                    doris::io::FileReaderOptions reader_opts;
                    reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
                    reader_opts.is_doris_table = true;
                    reader_opts.tablet_id = 1; // microbench placeholder

                    doris::io::FileDescription fd;
                    std::string obj_path = "s3://" + doris::config::test_s3_bucket + "/";
                    fd.path = doris::io::Path(obj_path + key);
                    fd.file_size = exist_job_perfile_size != -1 ? exist_job_perfile_size
                                                                : config.size_bytes_perfile;
                    doris::io::FileSystemProperties fs_props;
                    fs_props.system_type = doris::TFileType::FILE_S3;

                    fs_props.properties = properties;

                    int read_retry_count = 0;
                    const int max_read_retries = 50;
                    while (read_retry_count < max_read_retries) {
                        auto status_or_reader = doris::FileFactory::create_file_reader(
                                fs_props, fd, reader_opts, nullptr);
                        if (!status_or_reader.has_value()) {
                            if (++read_retry_count >= max_read_retries) {
                                LOG(ERROR) << "Failed to create reader for key " << key
                                           << status_or_reader.error();
                            }
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            continue;
                        }

                        auto reader = std::make_unique<MicrobenchFileReader>(
                                status_or_reader.value(), job.read_limiter);

                        doris::Defer defer {[&]() {
                            if (auto status = reader->close(); !status.ok()) {
                                LOG(ERROR) << "close file reader failed" << status.to_string();
                            }
                        }};

                        for (int j = 0; j < config.repeat; j++) {
                            size_t read_offset = 0;
                            size_t read_length = 0;

                            bool use_random = true;
                            if (config.read_offset_left + 1 == config.read_offset_right) {
                                use_random = false;
                            }
                            if (exist_job_perfile_size != -1) {
                                // read exist files
                                config.read_offset_right =
                                        std::min(config.read_offset_right, exist_job_perfile_size);
                                config.read_length_right =
                                        std::min(config.read_length_right, exist_job_perfile_size);

                                if (use_random) {
                                    std::random_device rd;
                                    std::mt19937 gen(rd());
                                    // Generate random read_offset between read_offset_left and read_offset_right - 1
                                    std::uniform_int_distribution<size_t> dis_offset(
                                            config.read_offset_left, config.read_offset_right - 1);
                                    read_offset = dis_offset(gen); // Generate random read_offset
                                    std::uniform_int_distribution<size_t> dis_length(
                                            config.read_length_left, config.read_length_right - 1);
                                    read_length = dis_length(gen); // Generate random read_length
                                    if (read_offset + read_length > exist_job_perfile_size) {
                                        read_length = exist_job_perfile_size - read_offset;
                                    }
                                } else { // not random
                                    read_offset = config.read_offset_left;
                                    read_length = config.read_length_left;
                                }
                            } else {
                                // new files
                                read_offset = config.read_offset_left;
                                read_length = config.read_length_left;
                                if (read_length == -1 ||
                                    read_offset + read_length > config.size_bytes_perfile) {
                                    read_length = config.size_bytes_perfile - read_offset;
                                }
                            }
                            LOG(INFO) << "read_offset=" << read_offset
                                      << " read_length=" << read_length;
                            CHECK(read_offset >= 0)
                                    << "Calculated read_offset is negative: " << read_offset;
                            CHECK(read_length >= 0)
                                    << "Calculated read_length is negative: " << read_length;

                            size_t total_bytes_read = 0;
                            while (total_bytes_read < read_length) {
                                size_t bytes_to_read = std::min(
                                        read_length - total_bytes_read,
                                        static_cast<size_t>(4 * 1024 * 1024)); // 4MB chunks

                                doris::Slice read_slice(buffer.data() + total_bytes_read,
                                                        bytes_to_read);
                                size_t bytes_read = 0;

                                doris::Status read_status =
                                        reader->read_at(read_offset + total_bytes_read, read_slice,
                                                        &bytes_read, &io_ctx, job.read_latency);

                                if (!read_status.ok()) {
                                    throw std::runtime_error("Read error: " +
                                                             read_status.to_string());
                                }

                                if (bytes_read != bytes_to_read) {
                                    throw std::runtime_error("Incomplete read: expected " +
                                                             std::to_string(bytes_to_read) +
                                                             " bytes, got " +
                                                             std::to_string(bytes_read));
                                }

                                total_bytes_read += bytes_read;
                            }

                            size_t file_size = config.size_bytes_perfile;
                            if (exist_job_perfile_size != -1) {
                                file_size = exist_job_perfile_size;
                            }

                            // Verify read data
                            if (!DataVerifier::verify_data(key, file_size, read_offset, buffer,
                                                           read_length)) {
                                throw std::runtime_error("Data verification failed for key: " +
                                                         key);
                            }

                            LOG(INFO)
                                    << "read_offset=" << read_offset
                                    << " read_length=" << read_length << " file_size=" << file_size;

                            completed_reads++;
                        }
                        break;
                    }
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Read task failed for key " << key << ": " << e.what();
                }
            }));
        }

        // Wait for all read tasks to complete
        for (auto& future : read_futures) {
            future.get();
        }
        read_stopwatch.stop(); // Stop timer

        // Convert read time from nanoseconds to seconds and format as string
        double total_read_time_seconds =
                read_stopwatch.elapsed_time() / 1e9; // nanoseconds to seconds
        job.stats.total_read_time =
                std::to_string(total_read_time_seconds) + " seconds"; // Save as string
        LOG(INFO) << "Total read time: " << job.stats.total_read_time << " seconds";

        // Update job statistics
        job.stats.num_local_io_total = total_stats.num_local_io_total;
        job.stats.num_remote_io_total = total_stats.num_remote_io_total;
        job.stats.num_peer_io_total = total_stats.num_peer_io_total;
        job.stats.num_inverted_index_remote_io_total =
                total_stats.inverted_index_num_remote_io_total;
        job.stats.local_io_timer = total_stats.local_io_timer;
        job.stats.bytes_read_from_local = total_stats.bytes_read_from_local;
        job.stats.bytes_read_from_remote = total_stats.bytes_read_from_remote;
        job.stats.bytes_read_from_peer = total_stats.bytes_read_from_peer;
        job.stats.remote_io_timer = total_stats.remote_io_timer;
        job.stats.peer_io_timer = total_stats.peer_io_timer;
        job.stats.write_cache_io_timer = total_stats.write_cache_io_timer;
        job.stats.bytes_write_into_cache = total_stats.bytes_write_into_cache;
        job.stats.num_skip_cache_io_total = total_stats.num_skip_cache_io_total;
        job.stats.read_cache_file_directly_timer = total_stats.read_cache_file_directly_timer;
        job.stats.cache_get_or_set_timer = total_stats.cache_get_or_set_timer;
        job.stats.lock_wait_timer = total_stats.lock_wait_timer;
        job.stats.get_timer = total_stats.lock_wait_timer;
        job.stats.set_timer = total_stats.lock_wait_timer;

        auto end_time = std::chrono::steady_clock::now();
        auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Completed read tasks for job " << job.job_id
                  << ", duration=" << duration.count() << "ms";
    }

    std::mutex _mutex;
    std::atomic<int> _next_job_id;
    std::map<std::string, std::shared_ptr<Job>> _jobs;
    std::shared_ptr<BenchEnvManager> _env_mgr;
    BenchThreadPool _job_executor_pool;
};

class MicrobenchServiceImpl : public microbench::MicrobenchService {
public:
    MicrobenchServiceImpl(std::string_view doris_home_path)
            : _bench_env_mgr(std::make_shared<BenchEnvManager>(doris_home_path)) {}

    ~MicrobenchServiceImpl() override {
        if (_job_manager) {
            _job_manager->stop();
        }
        if (_bench_env_mgr) {
            _bench_env_mgr->stop_reload_worker();
        }
    }

    doris::Status init_microbench_service() {
        auto status = _bench_env_mgr->load_config();
        if (!status) {
            return status;
        }

        status = _bench_env_mgr->load_bench_exec_env();
        if (!status) {
            return status;
        }
        _job_manager = _bench_env_mgr->create_job_manager_from_current_config();
        _bench_env_mgr->start_reload_worker();

        return Status::OK();
    }

    void show_reload_status(google::protobuf::RpcController* cntl_base,
                            const microbench::HttpRequest* request,
                            microbench::HttpResponse* response,
                            google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartObject();
        writer.Key("status");
        writer.String("OK");
        writer.Key("ReloadStat");

        writer.String(_bench_env_mgr->reload_details_stat().c_str());

        writer.EndObject();

        cntl->http_response().set_content_type("application/json");
        cntl->response_attachment().append(buffer.GetString());
    }

    void reload_config(google::protobuf::RpcController* cntl_base,
                       const microbench::HttpRequest* request, microbench::HttpResponse* response,
                       google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Hot reload config of microbench service";

        try {
            LOG(INFO) << "Request reload. May be execute after";
            _bench_env_mgr->reload_request();

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            writer.StartObject();
            writer.Key("status");
            writer.String("OK");
            writer.Key("message");
            writer.String("Reload requested");
            writer.EndObject();
            cntl->http_response().set_content_type("application/json");
            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error reloading config job: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Submit a job
     * 
     * Receive JSON-formatted job configuration, create and submit the job
     * Return a JSON response containing the job ID
     */
    void submit_job(google::protobuf::RpcController* cntl_base,
                    const microbench::HttpRequest* request, microbench::HttpResponse* response,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Received submit job request";

        try {
            // Parse request body JSON
            std::string job_config = cntl->request_attachment().to_string();
            JobConfig config = JobConfig::from_json(job_config);

            LOG(INFO) << "Parsed JobConfig: " << config.to_string();

            std::string job_id = _job_manager->submit_job(config);
            LOG(INFO) << "Job submitted successfully with ID: " << job_id;

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Return job_id
            rapidjson::Document response_doc;
            response_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = response_doc.GetAllocator();
            response_doc.AddMember("job_id", rapidjson::Value(job_id.c_str(), allocator),
                                   allocator);
            response_doc.AddMember("status", "success", allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            response_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error submitting job: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Get job status
     * 
     * Return detailed job status information based on job ID
     * Optional parameter 'files' is used to limit the number of file records returned
     */
    void get_job_status(google::protobuf::RpcController* cntl_base,
                        const microbench::HttpRequest* request, microbench::HttpResponse* response,
                        google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        std::string job_id = cntl->http_request().unresolved_path();
        const std::string* files_value = cntl->http_request().uri().GetQuery("files");
        size_t max_files = 1000; // Set maximum file record limit

        if (files_value != nullptr) {
            try {
                max_files = std::stoi(*files_value);
            } catch (const std::exception& e) {
                LOG(WARNING) << "Invalid files parameter: " << *files_value
                             << ", using default, error: " << e.what();
            }
        }

        LOG(INFO) << "Received get_job_status request for job " << job_id
                  << ", max_files=" << max_files;

        try {
            const Job& job = _job_manager->get_job_status(job_id);

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build JSON response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            d.AddMember("job_id", rapidjson::Value(job.job_id.c_str(), allocator), allocator);
            if (job.config.job_type != JobType::FILE_CACHE) {
                auto job_type = job_type_to_string(job.config.job_type);
                d.AddMember("job_type", rapidjson::Value(job_type.c_str(), allocator), allocator);
            }
            d.AddMember("status",
                        rapidjson::Value(get_status_string(job.status).c_str(), allocator),
                        allocator);

            // Add time information
            add_time_info(d, allocator, job);

            // Add error information (if any)
            if (!job.error_message.empty()) {
                d.AddMember("error_message", rapidjson::Value(job.error_message.c_str(), allocator),
                            allocator);
            }

            // Add configuration information
            if (job.config.job_type == JobType::FILE_CACHE_GET_OR_SET) {
                add_get_or_set_config_info(d, allocator, job.config);
            } else if (job.config.job_type == JobType::PEER_CACHE_SEED) {
                add_peer_seed_config_info(d, allocator, job.config);
            } else if (job.config.job_type == JobType::PEER_READ) {
                add_peer_read_config_info(d, allocator, job);
            } else {
                add_config_info(d, allocator, job.config);
            }

            // Add statistics information
            if (job.config.job_type == JobType::FILE_CACHE_GET_OR_SET) {
                add_get_or_set_stats_info(d, allocator, job.stats);
            } else if (job.config.job_type == JobType::PEER_CACHE_SEED) {
                add_peer_seed_stats_info(d, allocator, job.stats);
            } else if (job.config.job_type == JobType::PEER_READ) {
                add_peer_read_stats_info(d, allocator, job.stats);
            } else {
                add_stats_info(d, allocator, job.stats);
            }

            // Add file records (if requested)
            if (files_value) {
                add_file_records(d, allocator, job.file_records, max_files);
            }

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error getting job status: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", "Job not found", allocator);
            error_doc.AddMember("exception", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * List all jobs
     * 
     * Return a list of basic information for all jobs
     */
    void list_jobs(google::protobuf::RpcController* cntl_base,
                   const microbench::HttpRequest* request, microbench::HttpResponse* response,
                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Received list_jobs request";

        try {
            std::vector<std::shared_ptr<Job>> jobs = _job_manager->list_jobs();

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build JSON response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            rapidjson::Value jobs_array(rapidjson::kArrayType);
            for (const auto& job : jobs) {
                rapidjson::Value job_obj(rapidjson::kObjectType);
                job_obj.AddMember("job_id", rapidjson::Value(job->job_id.c_str(), allocator),
                                  allocator);
                job_obj.AddMember(
                        "status",
                        rapidjson::Value(get_status_string(job->status).c_str(), allocator),
                        allocator);

                // Add creation time
                auto create_time_t = std::chrono::system_clock::to_time_t(job->create_time);
                std::string create_time_str = std::ctime(&create_time_t);
                if (!create_time_str.empty() && create_time_str.back() == '\n') {
                    create_time_str.pop_back(); // Remove trailing newline character
                }
                job_obj.AddMember("create_time",
                                  rapidjson::Value(create_time_str.c_str(), allocator), allocator);

                // Add file prefix
                job_obj.AddMember("file_prefix",
                                  rapidjson::Value(job->config.file_prefix.c_str(), allocator),
                                  allocator);
                if (job->config.job_type != JobType::FILE_CACHE) {
                    auto job_type = job_type_to_string(job->config.job_type);
                    job_obj.AddMember("job_type", rapidjson::Value(job_type.c_str(), allocator),
                                      allocator);
                }

                jobs_array.PushBack(job_obj, allocator);
            }

            d.AddMember("jobs", jobs_array, allocator);
            d.AddMember("total", rapidjson::Value(static_cast<int>(jobs.size())), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error listing jobs: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Cancel a job
     * 
     * Attempt to cancel the specified job (currently not implemented)
     */
    void cancel_job(google::protobuf::RpcController* cntl_base,
                    const microbench::HttpRequest* request, microbench::HttpResponse* response,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        std::string job_id = cntl->http_request().unresolved_path();
        LOG(INFO) << "Received cancel_job request for job " << job_id;

        // Set response headers
        cntl->http_response().set_content_type("application/json");
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_IMPLEMENTED);

        // Build response
        rapidjson::Document d;
        d.SetObject();
        rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
        d.AddMember("status", "error", allocator);
        d.AddMember("message", "Job cancellation not implemented", allocator);

        // Serialize to string
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.Accept(writer);

        cntl->response_attachment().append(buffer.GetString());
    }

    /**
     * Get help information
     * 
     * Return usage instructions for the tool
     */
    void get_help(google::protobuf::RpcController* cntl_base,
                  const microbench::HttpRequest* request, microbench::HttpResponse* response,
                  google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Received get_help request";

        // Get usage help information
        std::string help_info = get_usage("Doris Microbench Tool");

        // Return help information
        cntl->response_attachment().append(help_info);
    }

    /**
     * Clear file cache
     * 
     * Clear file cache for the specified path or all caches
     */
    void file_cache_clear(google::protobuf::RpcController* cntl_base,
                          const microbench::HttpRequest* request,
                          microbench::HttpResponse* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        const std::string* sync_str = cntl->http_request().uri().GetQuery("sync");
        const std::string* segment_path = cntl->http_request().uri().GetQuery("segment_path");

        LOG(INFO) << "Received file_cache_clear request, sync=" << (sync_str ? *sync_str : "")
                  << ", segment_path=" << (segment_path ? *segment_path : "");

        try {
            bool sync = sync_str ? (doris::to_lower(*sync_str) == "true") : false;

            if (segment_path == nullptr) {
                // Clear all caches
                FileCacheFactory::instance()->clear_file_caches(sync);
                LOG(INFO) << "Cleared all file caches, sync=" << sync;
            } else {
                // Clear cache for specific path
                doris::io::UInt128Wrapper hash = doris::io::BlockFileCache::hash(*segment_path);
                doris::io::BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(hash);
                if (cache) {
                    cache->remove_if_cached(hash);
                    LOG(INFO) << "Cleared cache for path: " << *segment_path;
                } else {
                    LOG(WARNING) << "No cache found for path: " << *segment_path;
                }
            }

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build success response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
            d.AddMember("status", "OK", allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error clearing file cache: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Reset file cache
     * 
     * Reset file cache for the specified path or all caches
     */
    void file_cache_reset(google::protobuf::RpcController* cntl_base,
                          const microbench::HttpRequest* request,
                          microbench::HttpResponse* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received file_cache_reset request";

        try {
            const std::string* capacity_str = cntl->http_request().uri().GetQuery("capacity");
            int64_t new_capacity = 0;
            new_capacity = std::stoll(*capacity_str);
            if (new_capacity <= 0) {
                LOG(ERROR) << "Invalid capacity: " << (capacity_str ? *capacity_str : "null");
                throw std::runtime_error("Invalid capacity");
            }
            const std::string* path_str = cntl->http_request().uri().GetQuery("path");
            if (path_str == nullptr) {
                LOG(ERROR) << "Path is empty";
                throw std::runtime_error("Path is empty");
            }
            std::string path = *path_str;
            auto ret = FileCacheFactory::instance()->reset_capacity(path, new_capacity);
            LOG(INFO) << "Reset capacity for path: " << path << ", new capacity: " << new_capacity
                      << ", result: " << ret;

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build success response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
            d.AddMember("status", "OK", allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error resetting file cache: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Release file cache
     * 
     * Release file cache for the specified path or all caches
     */
    void file_cache_release(google::protobuf::RpcController* cntl_base,
                            const microbench::HttpRequest* request,
                            microbench::HttpResponse* response,
                            google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received file_cache_release request";

        try {
            const std::string* base_path_str = cntl->http_request().uri().GetQuery("base_path");
            size_t released = 0;
            if (base_path_str == nullptr) {
                released = FileCacheFactory::instance()->try_release();
            } else {
                released = FileCacheFactory::instance()->try_release(*base_path_str);
            }
            LOG(INFO) << "Released file caches: " << released
                      << " path: " << (base_path_str ? *base_path_str : "null");

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build success response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
            d.AddMember("status", "OK", allocator);
            d.AddMember("released_elements", released, allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error releasing file cache: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Update configuration
     */
    void update_config(google::protobuf::RpcController* cntl_base,
                       const microbench::HttpRequest* request, microbench::HttpResponse* response,
                       google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received update_config request";

        try {
            bool need_persist = false;
            const std::string* persist_str = cntl->http_request().uri().GetQuery("persist");
            if (persist_str && *persist_str == "true") {
                need_persist = true;
            }
            cntl->http_request().uri().RemoveQuery("persist");
            std::string key;
            std::string value;
            for (brpc::URI::QueryIterator it = cntl->http_request().uri().QueryBegin();
                 it != cntl->http_request().uri().QueryEnd(); ++it) {
                key = it->first;
                value = it->second;
                auto s = doris::config::set_config(key, value, need_persist);
                if (s.ok()) {
                    LOG(INFO) << "set_config " << key << "=" << value
                              << " success. persist: " << need_persist;
                } else {
                    LOG(WARNING) << "set_config " << key << "=" << value << " failed";
                }
                // just support update one config
                break;
            }

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build success response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
            d.AddMember("status", "OK", allocator);
            d.AddMember(rapidjson::Value(key.c_str(), allocator),
                        rapidjson::Value(value.c_str(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error updating config: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    /**
     * Show configuration
     */
    void show_config(google::protobuf::RpcController* cntl_base,
                     const microbench::HttpRequest* request, microbench::HttpResponse* response,
                     google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received show_config request";

        try {
            std::vector<std::vector<std::string>> config_info = doris::config::get_config_info();

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            writer.StartObject();

            writer.Key("status");
            writer.String("OK");

            writer.Key("config");

            writer.StartArray();

            const std::string* conf_item_str = cntl->http_request().uri().GetQuery("conf_item");
            std::string conf_item = conf_item_str ? *conf_item_str : "";

            for (const auto& _config : config_info) {
                if (!conf_item.empty()) {
                    if (_config[0] == conf_item) {
                        writer.StartArray();
                        for (const std::string& config_filed : _config) {
                            writer.String(config_filed.c_str());
                        }
                        writer.EndArray();
                        break;
                    }
                } else {
                    writer.StartArray();
                    for (const std::string& config_filed : _config) {
                        writer.String(config_filed.c_str());
                    }
                    writer.EndArray();
                }
            }

            writer.EndArray();

            writer.EndObject();

            cntl->http_response().set_content_type("application/json");
            cntl->response_attachment().append(buffer.GetString());

        } catch (const std::exception& e) {
            LOG(ERROR) << "Error showing config: " << e.what();

            // Set error status code and response
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            cntl->http_response().set_content_type("application/json");

            // Build error response
            rapidjson::Document error_doc;
            error_doc.SetObject();
            rapidjson::Document::AllocatorType& allocator = error_doc.GetAllocator();
            error_doc.AddMember("status", "error", allocator);
            error_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);

            // Serialize to string
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            error_doc.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        }
    }

    void set_peer_candidates(google::protobuf::RpcController* cntl_base,
                             const microbench::HttpRequest* request,
                             microbench::HttpResponse* response,
                             google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->http_response().set_content_type("application/json");

        try {
            std::string body = cntl->request_attachment().to_string();
            rapidjson::Document d;
            d.Parse(body.c_str());
            if (d.HasParseError() || !d.IsObject()) {
                throw std::runtime_error("Invalid JSON body for set_peer_candidates");
            }
            int64_t tablet_id = 1;
            if (d.HasMember("tablet_id") && d["tablet_id"].IsInt64()) {
                tablet_id = d["tablet_id"].GetInt64();
            }
            if (tablet_id <= 0) {
                throw std::runtime_error("tablet_id must be positive");
            }
            std::vector<PeerEndpointConfig> peers;
            if (d.HasMember("peers")) {
                peers = JobConfig::parse_endpoints_from_json(d["peers"]);
            } else if (d.HasMember("peer_endpoints")) {
                peers = JobConfig::parse_endpoints_from_json(d["peer_endpoints"]);
            } else {
                throw std::runtime_error(
                        "Missing required array field 'peers' or 'peer_endpoints'");
            }

            JobManager::apply_peer_candidates(tablet_id, peers);

            rapidjson::Document resp_doc;
            resp_doc.SetObject();
            auto& allocator = resp_doc.GetAllocator();
            resp_doc.AddMember("status", "OK", allocator);
            resp_doc.AddMember("tablet_id", tablet_id, allocator);
            resp_doc.AddMember("peer_count", static_cast<uint64_t>(peers.size()), allocator);

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            resp_doc.Accept(writer);
            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error in set_peer_candidates: " << e.what();
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            rapidjson::Document err_doc;
            err_doc.SetObject();
            auto& allocator = err_doc.GetAllocator();
            err_doc.AddMember("status", "error", allocator);
            err_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            err_doc.Accept(writer);
            cntl->response_attachment().append(buffer.GetString());
        }
    }

    void get_peer_candidates(google::protobuf::RpcController* cntl_base,
                             const microbench::HttpRequest* request,
                             microbench::HttpResponse* response,
                             google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->http_response().set_content_type("application/json");

        try {
            if (!doris::config::is_cloud_mode()) {
                throw std::runtime_error(
                        "Cloud mode is required for peer candidates (set cloud_unique_id in "
                        "be.conf)");
            }
            auto& mgr = doris::ExecEnv::GetInstance()
                                ->storage_engine()
                                .to_cloud()
                                .cloud_warm_up_manager();

            rapidjson::Document resp_doc;
            resp_doc.SetObject();
            auto& allocator = resp_doc.GetAllocator();
            resp_doc.AddMember("status", "OK", allocator);

            auto encode_tpc = [&](int64_t tid, const doris::TabletPeerCandidates& tpc) {
                rapidjson::Value obj(rapidjson::kObjectType);
                obj.AddMember("tablet_id", tid, allocator);
                obj.AddMember(
                        "last_successful_compute_group_id",
                        rapidjson::Value(tpc.last_successful_compute_group_id.c_str(), allocator),
                        allocator);
                obj.AddMember("consecutive_all_miss", tpc.consecutive_all_miss, allocator);
                obj.AddMember("cooldown_until_ms", tpc.cooldown_until_ms, allocator);
                rapidjson::Value arr(rapidjson::kArrayType);
                for (const auto& c : tpc.candidates) {
                    rapidjson::Value item(rapidjson::kObjectType);
                    item.AddMember("host", rapidjson::Value(c.host.c_str(), allocator), allocator);
                    item.AddMember("port", c.brpc_port, allocator);
                    item.AddMember("compute_group_id",
                                   rapidjson::Value(c.compute_group_id.c_str(), allocator),
                                   allocator);
                    item.AddMember("consecutive_rpc_failures", c.consecutive_rpc_failures,
                                   allocator);
                    arr.PushBack(item, allocator);
                }
                obj.AddMember("peers", arr, allocator);
                return obj;
            };

            const std::string* tablet_id_param = cntl->http_request().uri().GetQuery("tablet_id");
            rapidjson::Value tablets_arr(rapidjson::kArrayType);
            if (tablet_id_param != nullptr) {
                int64_t tid = std::stoll(*tablet_id_param);
                auto info = mgr.get_tablet_peer_info(tid);
                if (info.has_value()) {
                    tablets_arr.PushBack(encode_tpc(tid, *info), allocator);
                }
            } else {
                auto all_info = mgr.get_all_peer_info(1000);
                for (const auto& [tid, tpc] : all_info) {
                    tablets_arr.PushBack(encode_tpc(tid, tpc), allocator);
                }
            }
            resp_doc.AddMember("tablets", tablets_arr, allocator);

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            resp_doc.Accept(writer);
            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error in get_peer_candidates: " << e.what();
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            rapidjson::Document err_doc;
            err_doc.SetObject();
            auto& allocator = err_doc.GetAllocator();
            err_doc.AddMember("status", "error", allocator);
            err_doc.AddMember("message", rapidjson::Value(e.what(), allocator), allocator);
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            err_doc.Accept(writer);
            cntl->response_attachment().append(buffer.GetString());
        }
    }

private:
    // Get string representation of job status
    std::string get_status_string(JobStatus status) {
        switch (status) {
        case JobStatus::PENDING:
            return "PENDING";
        case JobStatus::RUNNING:
            return "RUNNING";
        case JobStatus::COMPLETED:
            return "COMPLETED";
        case JobStatus::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
        }
    }

    // Add time information to JSON response
    void add_time_info(rapidjson::Document& doc, rapidjson::Document::AllocatorType& allocator,
                       const Job& job) {
        // Add creation time
        auto create_time_t = std::chrono::system_clock::to_time_t(job.create_time);
        std::string create_time_str = std::ctime(&create_time_t);
        if (!create_time_str.empty() && create_time_str.back() == '\n') {
            create_time_str.pop_back(); // Remove trailing newline character
        }
        doc.AddMember("create_time", rapidjson::Value(create_time_str.c_str(), allocator),
                      allocator);

        // Add start time (if available)
        if (job.status != JobStatus::PENDING) {
            auto start_time_t = std::chrono::system_clock::to_time_t(job.start_time);
            std::string start_time_str = std::ctime(&start_time_t);
            if (!start_time_str.empty() && start_time_str.back() == '\n') {
                start_time_str.pop_back();
            }
            doc.AddMember("start_time", rapidjson::Value(start_time_str.c_str(), allocator),
                          allocator);
        }

        // Add end time (if available)
        if (job.status == JobStatus::COMPLETED || job.status == JobStatus::FAILED) {
            auto end_time_t = std::chrono::system_clock::to_time_t(job.end_time);
            std::string end_time_str = std::ctime(&end_time_t);
            if (!end_time_str.empty() && end_time_str.back() == '\n') {
                end_time_str.pop_back();
            }
            doc.AddMember("end_time", rapidjson::Value(end_time_str.c_str(), allocator), allocator);

            // Calculate duration of the run
            auto duration =
                    std::chrono::duration_cast<std::chrono::seconds>(job.end_time - job.start_time)
                            .count();
            doc.AddMember("duration_seconds", duration, allocator);
        }
    }

    // Add configuration information to JSON response
    void add_config_info(rapidjson::Document& doc, rapidjson::Document::AllocatorType& allocator,
                         const JobConfig& config) {
        rapidjson::Value config_obj(rapidjson::kObjectType);

        config_obj.AddMember("size_bytes_perfile", config.size_bytes_perfile, allocator);
        config_obj.AddMember("write_iops", config.write_iops, allocator);
        config_obj.AddMember("read_iops", config.read_iops, allocator);
        config_obj.AddMember("num_threads", config.num_threads, allocator);
        config_obj.AddMember("num_files", config.num_files, allocator);
        config_obj.AddMember("file_prefix", rapidjson::Value(config.file_prefix.c_str(), allocator),
                             allocator);
        config_obj.AddMember("cache_type", rapidjson::Value(config.cache_type.c_str(), allocator),
                             allocator);
        config_obj.AddMember("expiration", config.expiration, allocator);
        config_obj.AddMember("repeat", config.repeat, allocator);
        config_obj.AddMember("write_batch_size", config.write_batch_size, allocator);
        config_obj.AddMember("write_file_cache", config.write_file_cache, allocator);
        config_obj.AddMember("bvar_enable", config.bvar_enable, allocator);

        // Add read offset (if applicable)
        if (config.read_iops > 0) {
            rapidjson::Value read_offset_array(rapidjson::kArrayType);
            read_offset_array.PushBack(config.read_offset_left, allocator);
            read_offset_array.PushBack(config.read_offset_right, allocator);
            config_obj.AddMember("read_offset", read_offset_array, allocator);

            rapidjson::Value read_length_array(rapidjson::kArrayType);
            read_length_array.PushBack(config.read_length_left, allocator);
            read_length_array.PushBack(config.read_length_right, allocator);
            config_obj.AddMember("read_length", read_length_array, allocator);
        }

        doc.AddMember("config", config_obj, allocator);
    }

    void add_get_or_set_config_info(rapidjson::Document& doc,
                                    rapidjson::Document::AllocatorType& allocator,
                                    const JobConfig& config) {
        rapidjson::Value config_obj(rapidjson::kObjectType);
        config_obj.AddMember("num_threads", config.num_threads, allocator);
        config_obj.AddMember("num_files", config.num_files, allocator);
        config_obj.AddMember("file_prefix", rapidjson::Value(config.file_prefix.c_str(), allocator),
                             allocator);
        config_obj.AddMember("size_bytes_perfile", config.size_bytes_perfile, allocator);
        config_obj.AddMember("repeat", config.repeat, allocator);
        config_obj.AddMember("cache_type", rapidjson::Value(config.cache_type.c_str(), allocator),
                             allocator);
        config_obj.AddMember("expiration", config.expiration, allocator);
        config_obj.AddMember("bvar_enable", config.bvar_enable, allocator);

        rapidjson::Value offset_array(rapidjson::kArrayType);
        offset_array.PushBack(config.get_or_set_offset_left, allocator);
        offset_array.PushBack(config.get_or_set_offset_right, allocator);
        config_obj.AddMember("get_or_set_offset", offset_array, allocator);

        rapidjson::Value size_array(rapidjson::kArrayType);
        size_array.PushBack(config.get_or_set_size_left, allocator);
        size_array.PushBack(config.get_or_set_size_right, allocator);
        config_obj.AddMember("get_or_set_size", size_array, allocator);

        doc.AddMember("config", config_obj, allocator);
    }

    // Add statistics information to JSON response
    void add_stats_info(rapidjson::Document& doc, rapidjson::Document::AllocatorType& allocator,
                        const Job::Statistics& stats) {
        rapidjson::Value stats_obj(rapidjson::kObjectType);

        stats_obj.AddMember("total_write_time",
                            rapidjson::Value(stats.total_write_time.c_str(), allocator), allocator);
        stats_obj.AddMember("total_read_time",
                            rapidjson::Value(stats.total_read_time.c_str(), allocator), allocator);

        // struct FileCacheStatistics
        stats_obj.AddMember("num_local_io_total", static_cast<uint64_t>(stats.num_local_io_total),
                            allocator);
        stats_obj.AddMember("num_remote_io_total", static_cast<uint64_t>(stats.num_remote_io_total),
                            allocator);
        stats_obj.AddMember("num_inverted_index_remote_io_total",
                            static_cast<uint64_t>(stats.num_inverted_index_remote_io_total),
                            allocator);
        stats_obj.AddMember("local_io_timer", static_cast<uint64_t>(stats.local_io_timer),
                            allocator);
        stats_obj.AddMember("bytes_read_from_local",
                            static_cast<uint64_t>(stats.bytes_read_from_local), allocator);
        stats_obj.AddMember("bytes_read_from_remote",
                            static_cast<uint64_t>(stats.bytes_read_from_remote), allocator);
        stats_obj.AddMember("remote_io_timer", static_cast<uint64_t>(stats.remote_io_timer),
                            allocator);
        if (stats.num_peer_io_total > 0 || stats.bytes_read_from_peer > 0 ||
            stats.peer_io_timer > 0) {
            stats_obj.AddMember("num_peer_io_total", static_cast<uint64_t>(stats.num_peer_io_total),
                                allocator);
            stats_obj.AddMember("bytes_read_from_peer",
                                static_cast<uint64_t>(stats.bytes_read_from_peer), allocator);
            stats_obj.AddMember("peer_io_timer", static_cast<uint64_t>(stats.peer_io_timer),
                                allocator);
        }
        stats_obj.AddMember("write_cache_io_timer",
                            static_cast<uint64_t>(stats.write_cache_io_timer), allocator);
        stats_obj.AddMember("bytes_write_into_cache",
                            static_cast<uint64_t>(stats.bytes_write_into_cache), allocator);
        stats_obj.AddMember("num_skip_cache_io_total",
                            static_cast<uint64_t>(stats.num_skip_cache_io_total), allocator);
        stats_obj.AddMember("read_cache_file_directly_timer",
                            static_cast<uint64_t>(stats.read_cache_file_directly_timer), allocator);
        stats_obj.AddMember("cache_get_or_set_timer",
                            static_cast<uint64_t>(stats.cache_get_or_set_timer), allocator);
        stats_obj.AddMember("lock_wait_timer", static_cast<uint64_t>(stats.lock_wait_timer),
                            allocator);
        stats_obj.AddMember("get_timer", static_cast<uint64_t>(stats.get_timer), allocator);
        stats_obj.AddMember("set_timer", static_cast<uint64_t>(stats.set_timer), allocator);

        doc.AddMember("statistics", stats_obj, allocator);
    }

    void add_get_or_set_stats_info(rapidjson::Document& doc,
                                   rapidjson::Document::AllocatorType& allocator,
                                   const Job::Statistics& stats) {
        rapidjson::Value stats_obj(rapidjson::kObjectType);
        stats_obj.AddMember("total_time",
                            rapidjson::Value(stats.total_read_time.c_str(), allocator), allocator);
        stats_obj.AddMember("get_or_set_ops", static_cast<uint64_t>(stats.get_or_set_ops),
                            allocator);
        stats_obj.AddMember("get_or_set_blocks", static_cast<uint64_t>(stats.get_or_set_blocks),
                            allocator);
        stats_obj.AddMember("get_or_set_qps", stats.get_or_set_qps, allocator);
        stats_obj.AddMember("avg_get_or_set_us", stats.avg_get_or_set_us, allocator);
        stats_obj.AddMember("cache_get_or_set_timer",
                            static_cast<uint64_t>(stats.cache_get_or_set_timer), allocator);
        stats_obj.AddMember("lock_wait_timer", static_cast<uint64_t>(stats.lock_wait_timer),
                            allocator);
        stats_obj.AddMember("get_timer", static_cast<uint64_t>(stats.get_timer), allocator);
        stats_obj.AddMember("set_timer", static_cast<uint64_t>(stats.set_timer), allocator);

        doc.AddMember("statistics", stats_obj, allocator);
    }

    void add_peer_seed_config_info(rapidjson::Document& doc,
                                   rapidjson::Document::AllocatorType& allocator,
                                   const JobConfig& config) {
        rapidjson::Value config_obj(rapidjson::kObjectType);
        config_obj.AddMember("num_threads", config.num_threads, allocator);
        config_obj.AddMember("num_files", config.num_files, allocator);
        config_obj.AddMember("file_prefix", rapidjson::Value(config.file_prefix.c_str(), allocator),
                             allocator);
        config_obj.AddMember("source_job_id",
                             rapidjson::Value(config.source_job_id.c_str(), allocator), allocator);
        config_obj.AddMember("size_bytes_perfile", config.size_bytes_perfile, allocator);
        config_obj.AddMember("tablet_id", config.tablet_id, allocator);
        config_obj.AddMember("cache_type", rapidjson::Value(config.cache_type.c_str(), allocator),
                             allocator);
        config_obj.AddMember("expiration", config.expiration, allocator);
        config_obj.AddMember("overwrite_existing", config.overwrite_existing, allocator);
        doc.AddMember("config", config_obj, allocator);
    }

    void add_peer_seed_stats_info(rapidjson::Document& doc,
                                  rapidjson::Document::AllocatorType& allocator,
                                  const Job::Statistics& stats) {
        rapidjson::Value stats_obj(rapidjson::kObjectType);
        stats_obj.AddMember("total_write_time",
                            rapidjson::Value(stats.total_write_time.c_str(), allocator), allocator);
        stats_obj.AddMember("seeded_files", static_cast<uint64_t>(stats.peer_seed_files),
                            allocator);
        stats_obj.AddMember("seeded_blocks", static_cast<uint64_t>(stats.peer_seed_blocks),
                            allocator);
        stats_obj.AddMember("bytes_write_into_cache",
                            static_cast<uint64_t>(stats.bytes_write_into_cache), allocator);
        stats_obj.AddMember("throughput_mb_s", stats.peer_seed_throughput_mb_s, allocator);
        doc.AddMember("statistics", stats_obj, allocator);
    }

    void add_peer_read_config_info(rapidjson::Document& doc,
                                   rapidjson::Document::AllocatorType& allocator, const Job& job) {
        const JobConfig& config = job.config;
        const std::string source_job_id = job.effective_source_job_id();
        rapidjson::Value config_obj(rapidjson::kObjectType);
        config_obj.AddMember("num_threads", config.num_threads, allocator);
        config_obj.AddMember("num_files", config.num_files, allocator);
        config_obj.AddMember("file_prefix", rapidjson::Value(config.file_prefix.c_str(), allocator),
                             allocator);
        config_obj.AddMember("source_job_id", rapidjson::Value(source_job_id.c_str(), allocator),
                             allocator);
        config_obj.AddMember("size_bytes_perfile", config.size_bytes_perfile, allocator);
        config_obj.AddMember("repeat", config.repeat, allocator);
        config_obj.AddMember("read_iops", config.read_iops, allocator);
        config_obj.AddMember("read_chunk_size", config.read_chunk_size, allocator);
        config_obj.AddMember("tablet_id", config.tablet_id, allocator);
        config_obj.AddMember("cache_type", rapidjson::Value(config.cache_type.c_str(), allocator),
                             allocator);
        config_obj.AddMember("expiration", config.expiration, allocator);
        config_obj.AddMember("use_s3_fallback", config.use_s3_fallback, allocator);
        config_obj.AddMember("bypass_peer_read", config.bypass_peer_read, allocator);
        config_obj.AddMember("direct_peer_rpc", config.direct_peer_rpc, allocator);
        config_obj.AddMember("cache_write_mode",
                             rapidjson::Value(config.cache_write_mode.c_str(), allocator),
                             allocator);
        config_obj.AddMember("clear_local_cache_before_read", config.clear_local_cache_before_read,
                             allocator);
        config_obj.AddMember("clear_local_cache_each_repeat", config.clear_local_cache_each_repeat,
                             allocator);
        config_obj.AddMember("verify_data", config.verify_data, allocator);
        config_obj.AddMember("bvar_enable", config.bvar_enable, allocator);

        rapidjson::Value read_offset_array(rapidjson::kArrayType);
        read_offset_array.PushBack(config.read_offset_left, allocator);
        read_offset_array.PushBack(config.read_offset_right, allocator);
        config_obj.AddMember("read_offset", read_offset_array, allocator);

        rapidjson::Value read_length_array(rapidjson::kArrayType);
        read_length_array.PushBack(config.read_length_left, allocator);
        read_length_array.PushBack(config.read_length_right, allocator);
        config_obj.AddMember("read_length", read_length_array, allocator);

        rapidjson::Value peers_array(rapidjson::kArrayType);
        for (const auto& ep : config.peer_endpoints) {
            rapidjson::Value ep_obj(rapidjson::kObjectType);
            ep_obj.AddMember("host", rapidjson::Value(ep.host.c_str(), allocator), allocator);
            ep_obj.AddMember("port", ep.port, allocator);
            ep_obj.AddMember("compute_group_id",
                             rapidjson::Value(ep.compute_group_id.c_str(), allocator), allocator);
            peers_array.PushBack(ep_obj, allocator);
        }
        config_obj.AddMember("peer_endpoints", peers_array, allocator);

        doc.AddMember("config", config_obj, allocator);
    }

    void add_peer_read_stats_info(rapidjson::Document& doc,
                                  rapidjson::Document::AllocatorType& allocator,
                                  const Job::Statistics& stats) {
        rapidjson::Value stats_obj(rapidjson::kObjectType);
        stats_obj.AddMember("total_read_time",
                            rapidjson::Value(stats.total_read_time.c_str(), allocator), allocator);
        stats_obj.AddMember("read_ops", static_cast<uint64_t>(stats.peer_read_ops), allocator);
        stats_obj.AddMember("total_bytes_read", static_cast<uint64_t>(stats.peer_total_bytes_read),
                            allocator);
        stats_obj.AddMember("read_qps", stats.peer_read_qps, allocator);
        stats_obj.AddMember("peer_rpc_qps", stats.peer_rpc_qps, allocator);
        stats_obj.AddMember("throughput_mb_s", stats.peer_throughput_mb_s, allocator);
        stats_obj.AddMember("peer_wire_throughput_mb_s", stats.peer_wire_throughput_mb_s,
                            allocator);
        stats_obj.AddMember("avg_read_us", stats.peer_avg_read_us, allocator);
        stats_obj.AddMember("p50_read_us", stats.peer_p50_read_us, allocator);
        stats_obj.AddMember("p95_read_us", stats.peer_p95_read_us, allocator);
        stats_obj.AddMember("p99_read_us", stats.peer_p99_read_us, allocator);
        stats_obj.AddMember("max_read_us", stats.peer_max_read_us, allocator);
        stats_obj.AddMember("num_peer_io_total", static_cast<uint64_t>(stats.num_peer_io_total),
                            allocator);
        stats_obj.AddMember("bytes_read_from_peer",
                            static_cast<uint64_t>(stats.bytes_read_from_peer), allocator);
        stats_obj.AddMember("peer_wire_bytes_read",
                            static_cast<uint64_t>(stats.peer_wire_bytes_read), allocator);
        stats_obj.AddMember("peer_io_timer", static_cast<uint64_t>(stats.peer_io_timer), allocator);
        stats_obj.AddMember("num_local_io_total", static_cast<uint64_t>(stats.num_local_io_total),
                            allocator);
        stats_obj.AddMember("bytes_read_from_local",
                            static_cast<uint64_t>(stats.bytes_read_from_local), allocator);
        stats_obj.AddMember("local_io_timer", static_cast<uint64_t>(stats.local_io_timer),
                            allocator);
        stats_obj.AddMember("num_remote_io_total", static_cast<uint64_t>(stats.num_remote_io_total),
                            allocator);
        stats_obj.AddMember("bytes_read_from_remote",
                            static_cast<uint64_t>(stats.bytes_read_from_remote), allocator);
        stats_obj.AddMember("remote_io_timer", static_cast<uint64_t>(stats.remote_io_timer),
                            allocator);
        stats_obj.AddMember("write_cache_io_timer",
                            static_cast<uint64_t>(stats.write_cache_io_timer), allocator);
        stats_obj.AddMember("bytes_write_into_cache",
                            static_cast<uint64_t>(stats.bytes_write_into_cache), allocator);
        stats_obj.AddMember("num_skip_cache_io_total",
                            static_cast<uint64_t>(stats.num_skip_cache_io_total), allocator);
        stats_obj.AddMember("cache_get_or_set_timer",
                            static_cast<uint64_t>(stats.cache_get_or_set_timer), allocator);
        stats_obj.AddMember("lock_wait_timer", static_cast<uint64_t>(stats.lock_wait_timer),
                            allocator);
        stats_obj.AddMember("get_timer", static_cast<uint64_t>(stats.get_timer), allocator);
        stats_obj.AddMember("set_timer", static_cast<uint64_t>(stats.set_timer), allocator);
        doc.AddMember("statistics", stats_obj, allocator);
    }

    // Add file records to JSON response
    void add_file_records(rapidjson::Document& doc, rapidjson::Document::AllocatorType& allocator,
                          const std::vector<::FileInfo>& file_records, size_t max_files) {
        rapidjson::Value files_array(rapidjson::kArrayType);
        size_t count = 0;

        for (const auto& file_info : file_records) {
            if (count >= max_files) {
                break; // Stop adding if max limit is reached
            }
            rapidjson::Value file_obj(rapidjson::kObjectType);
            file_obj.AddMember("filename", rapidjson::Value(file_info.filename.c_str(), allocator),
                               allocator);
            file_obj.AddMember("data_size", static_cast<uint64_t>(file_info.data_size), allocator);
            file_obj.AddMember("job_id", rapidjson::Value(file_info.job_id.c_str(), allocator),
                               allocator);
            files_array.PushBack(file_obj, allocator);
            count++;
        }

        doc.AddMember("file_records", files_array, allocator);
        doc.AddMember("file_records_count", static_cast<uint64_t>(count), allocator);
        doc.AddMember("file_records_total", static_cast<uint64_t>(file_records.size()), allocator);
    }

    std::shared_ptr<BenchEnvManager> _bench_env_mgr;
    std::unique_ptr<JobManager> _job_manager;
};
} // namespace microbenchService

// HTTP server handling
class HttpServer {
public:
    HttpServer() = default;

    void start(std::string_view doris_home_path) {
        _server = new brpc::Server();
        microbenchService::MicrobenchServiceImpl http_svc(doris_home_path);
        auto status = http_svc.init_microbench_service();
        if (!status) {
            LOG(ERROR) << status.to_string();
            return;
        }

        LOG(INFO) << "Starting HTTP server on port " << FLAGS_port;

        if (_server->AddService(&http_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Failed to add http service";
            return;
        }

        brpc::ServerOptions options;
        if (_server->Start(FLAGS_port, &options) != 0) {
            LOG(ERROR) << "Failed to start HttpServer";
            return;
        }

        LOG(INFO) << "HTTP server started successfully";

        if (FLAGS_brpc_port > 0) {
            if (!doris::config::is_cloud_mode()) {
                LOG(ERROR) << "Cannot start peer BRPC server on port " << FLAGS_brpc_port
                           << " because cloud_unique_id is not set in be.conf";
                _server->Stop(0);
                _server->Join();
                _server->ClearServices();
                return;
            }
            _peer_brpc_server = new brpc::Server();
            auto* exec_env = doris::ExecEnv::GetInstance();
            if (_peer_brpc_server->AddService(
                        new doris::CloudInternalServiceImpl(exec_env->storage_engine().to_cloud(),
                                                            exec_env),
                        brpc::SERVER_OWNS_SERVICE) != 0) {
                LOG(ERROR) << "Failed to add CloudInternalServiceImpl to peer BRPC server";
                _server->Stop(0);
                _server->Join();
                _server->ClearServices();
                return;
            }
            brpc::ServerOptions brpc_options;
            if (doris::config::brpc_num_threads != -1) {
                brpc_options.num_threads = doris::config::brpc_num_threads;
            }
            brpc_options.idle_timeout_sec = doris::config::brpc_idle_timeout_sec;
            brpc_options.has_builtin_services = doris::config::enable_brpc_builtin_services;
            if (_peer_brpc_server->Start(FLAGS_brpc_port, &brpc_options) != 0) {
                LOG(ERROR) << "Failed to start peer BRPC server on port " << FLAGS_brpc_port;
                _server->Stop(0);
                _server->Join();
                _server->ClearServices();
                return;
            }
            LOG(INFO) << "Peer BRPC server (CloudInternalServiceImpl) started on port "
                      << FLAGS_brpc_port;
        }

        _server->RunUntilAskedToQuit(); // Wait for signals
        if (_peer_brpc_server) {
            _peer_brpc_server->Stop(0);
            _peer_brpc_server->Join();
            _peer_brpc_server->ClearServices();
            LOG(INFO) << "Peer BRPC server stopped";
        }
        _server->ClearServices();

        LOG(INFO) << "HTTP server stopped";
    }

    ~HttpServer() {
        if (_peer_brpc_server) {
            LOG(INFO) << "Cleaning up peer BRPC server in destructor";
            delete _peer_brpc_server;
        }
        if (_server) {
            LOG(INFO) << "Cleaning up HTTP server in destructor";
            delete _server;
        }
    }

private:
    brpc::Server* _server {nullptr};
    brpc::Server* _peer_brpc_server {nullptr};
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_minloglevel = google::GLOG_INFO;
    FLAGS_log_dir = "./logs";
    FLAGS_logbufsecs = 0; // Disable buffering, write immediately
    std::filesystem::path log_dir(FLAGS_log_dir);
    if (!std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
        LOG(INFO) << "Log directory created successfully: " << log_dir.string();
    } else {
        LOG(INFO) << "Log directory already exists: " << log_dir.string();
    }
    google::InitGoogleLogging(argv[0]);

    std::string doris_home = getenv("DORIS_HOME");
    if (doris_home.empty()) {
        LOG(ERROR) << "DORIS_HOME environment variable not set";
    }

    LOG(INFO) << "env=" << doris_home;
    std::thread periodiccally_log_thread;
    std::mutex periodiccally_log_thread_lock;
    std::condition_variable periodiccally_log_thread_cv;
    std::atomic_bool periodiccally_log_thread_run = true;
    auto periodiccally_log = [&]() {
        while (periodiccally_log_thread_run) {
            std::unique_lock<std::mutex> lck {periodiccally_log_thread_lock};
            periodiccally_log_thread_cv.wait_for(lck, std::chrono::milliseconds(5000));
            LOG(INFO) << "Periodically log for file cache microbench";
        }
    };
    periodiccally_log_thread = std::thread {periodiccally_log};

    HttpServer http_server;
    http_server.start(doris_home);

    if (periodiccally_log_thread.joinable()) {
        {
            std::unique_lock<std::mutex> lck {periodiccally_log_thread_lock};
            periodiccally_log_thread_run = false;
            // immediately notify the log thread to quickly exit in case it block the
            // whole procedure
            periodiccally_log_thread_cv.notify_all();
        }
        periodiccally_log_thread.join();
    }
    LOG(INFO) << "Program exiting normally";
    return 0;
}
#endif
