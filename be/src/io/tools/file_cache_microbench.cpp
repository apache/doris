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
#if defined(BE_TEST) && defined(BUILD_FILE_CACHE_MICROBENCH_TOOL)
#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/server.h>
#include <brpc/uri.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <glog/logging.h>

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
#include <vector>

#include "build/proto/microbench.pb.h"
#include "common/config.h"
#include "common/status.h"
#include "gflags/gflags.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow"
#endif

#define private public
#include "runtime/exec_env.h"
#undef private

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

bvar::LatencyRecorder microbench_write_latency("file_cache_microbench_append");
bvar::LatencyRecorder microbench_read_latency("file_cache_microbench_read_at");

const std::string HIDDEN_PREFIX = "test_file_cache_microbench/";
const char PAD_CHAR = 'x';
const size_t BUFFER_SIZE = 1024 * 1024;
// Just 10^9.
static constexpr auto NS = 1000000000UL;

DEFINE_int32(port, 8888, "Http Port of this server");

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

// Define a struct to store file information
struct FileInfo {
    std::string filename; // File name
    size_t data_size;     // Data size
    std::string job_id;   // Associated job ID
};

class S3FileRecords {
public:
    void add_file_info(const std::string& job_id, const FileInfo& file_info) {
        std::lock_guard<std::mutex> lock(mutex_);
        records_[job_id].emplace_back(file_info);
    }

    int64_t get_exist_job_perfile_size_by_prefix(const std::string& file_prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& pair : records_) {
            const std::vector<FileInfo>& file_infos = pair.second;
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
            const std::vector<FileInfo>& file_infos = pair.second;
            for (const auto& file_info : file_infos) {
                if (file_info.filename.compare(0, file_prefix.length(), file_prefix) == 0) {
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
            const std::vector<FileInfo>& file_infos = pair.second;
            for (const auto& file_info : file_infos) {
                if (file_info.filename.compare(0, file_prefix.length(), file_prefix) == 0) {
                    return pair.first;
                }
            }
        }
        return "";
    }

private:
    std::mutex mutex_;
    std::map<std::string, std::vector<FileInfo>> records_;
};

// Create a global S3FileRecords instance
S3FileRecords s3_file_records;

class MicrobenchS3FileWriter {
public:
    MicrobenchS3FileWriter(std::shared_ptr<doris::io::ObjClientHolder> client,
                           const std::string& bucket, const std::string& key,
                           const doris::io::FileWriterOptions* options,
                           std::shared_ptr<doris::S3RateLimiterHolder> rate_limiter)
            : _writer(client, bucket, key, options), _rate_limiter(rate_limiter) {}

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
    doris::io::S3FileWriter _writer;
    std::shared_ptr<doris::S3RateLimiterHolder> _rate_limiter;
};

class MicrobenchFileReader {
public:
    MicrobenchFileReader(std::shared_ptr<doris::io::FileReader> base_reader,
                         std::shared_ptr<doris::S3RateLimiterHolder> rate_limiter)
            : _base_reader(std::move(base_reader)), _rate_limiter(rate_limiter) {}

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
        SCOPED_BVAR_LATENCY(microbench_write_latency);
        return _base_reader->read_at(offset, result, bytes_read, io_ctx);
    }

    size_t size() const { return _base_reader->size(); }

    doris::Status close() { return _base_reader->close(); }

private:
    std::shared_ptr<doris::io::FileReader> _base_reader;
    std::shared_ptr<doris::S3RateLimiterHolder> _rate_limiter;
};

class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop(false) {
        try {
            for (size_t i = 0; i < num_threads; ++i) {
                workers.emplace_back([this] {
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

    ~ThreadPool() {
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
        )" + progname + R"( --port=<port_number>
        
    API Endpoints:
      POST /submit_job
        Submit a job with the following JSON body:
        {
          "size_bytes_perfile": <size>,        // Number of bytes to write per segment file
          "write_iops": <limit>,               // IOPS limit for writing per segment files
          "read_iops": <limit>,                // IOPS limit for reading per segment files
          "num_threads": <count>,              // Number of threads in the thread pool, default 200
          "num_files": <count>,                // Number of segments to write/read
          "file_prefix": "<prefix>",           // Prefix for segment files, Notice: this tools hide prefix(test_file_cache_microbench/) before file_prefix
          "write_batch_size": <size>,          // Size of data to write in each write operation
          "cache_type": <type>,                // Write or Read data enter file cache queue type, support NORMAL | TTL | INDEX | DISPOSABLE, default NORMAL
          "expiration": <timestamp>,           // File cache ttl expire time, value is a unix timestamp
          "repeat": <count>,                   // Read repeat times, default 1
          "read_offset": [<left>, <right>],    // Range for reading (left inclusive, right exclusive)
          "read_length": [<left>, <right>]     // Range for reading length (left inclusive, right exclusive)
        }

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
struct JobConfig {
    // Default value initialization
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
    bool write_file_cache = true;
    bool bvar_enable = false;

    // Parse configuration from JSON
    static JobConfig from_json(const std::string& json_str) {
        JobConfig config;
        rapidjson::Document d;
        d.Parse(json_str.c_str());

        if (d.HasParseError()) {
            throw std::runtime_error("JSON parse error json args=" + json_str);
        }

        // Basic validation
        validate(d);

        // Use helper functions to parse each field
        parse_basic_fields(d, config);
        parse_cache_settings(d, config);
        parse_read_settings(d, config);

        // Additional validation
        validate_config(config);

        return config;
    }

private:
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
            config.repeat = d["repeat"].GetInt64();
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

public:
    std::string to_string() const {
        return fmt::format(
                "size_bytes_perfile: {}, write_iops: {}, read_iops: {}, num_threads: {}, "
                "num_files: {}, file_prefix: {}, write_file_cache: {}, write_batch_size: {}, "
                "repeat: {}, expiration: {}, cache_type: {}, read_offset: [{}, {}), "
                "read_length: [{}, {})",
                size_bytes_perfile, write_iops, read_iops, num_threads, num_files,
                HIDDEN_PREFIX + file_prefix, write_file_cache, write_batch_size, repeat, expiration,
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
    } stats;

    // Record associated file information for the job
    std::vector<FileInfo> file_records;

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

        if (config.read_iops > 0 && config.bvar_enable) {
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

// Job manager
class JobManager {
public:
    JobManager() : _next_job_id(0), _job_executor_pool(std::thread::hardware_concurrency()) {
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

            {
                std::lock_guard<std::mutex> lock(_mutex);
                _jobs[job_id] = std::make_shared<Job>(job_id, config);
            }

            LOG(INFO) << "Submitting job " << job_id << " with config: " << config.to_string();

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

    // Record file information
    void record_file_info(const std::string& key, size_t data_size, const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            FileInfo file_info = {key, data_size, job_id};
            it->second->file_records.push_back(file_info);
            s3_file_records.add_file_info(job_id, file_info);
        } else {
            LOG(ERROR) << "Job ID not found when recording file info: " << job_id;
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

        // Generate multiple keys
        std::vector<std::string> keys;
        keys.reserve(config.num_files);

        std::string rewrite_job_id = job_id;
        // If it's a read-only job, find the previously written files
        if (config.read_iops > 0 && config.write_iops == 0) {
            std::string old_job_id =
                    s3_file_records.find_job_id_by_prefix(HIDDEN_PREFIX + config.file_prefix);
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
            keys.push_back(HIDDEN_PREFIX + config.file_prefix + "/" + rewrite_job_id + "_" +
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

private:
    doris::S3ClientConf create_s3_client_conf(const JobConfig& config) {
        doris::S3ClientConf s3_conf;
        s3_conf.max_connections = std::max(256, config.num_threads * 4);
        s3_conf.request_timeout_ms = 60000;
        s3_conf.connect_timeout_ms = 3000;
        s3_conf.ak = doris::config::test_s3_ak;
        s3_conf.sk = doris::config::test_s3_sk;
        s3_conf.region = doris::config::test_s3_region;
        s3_conf.endpoint = doris::config::test_s3_endpoint;
        return s3_conf;
    }

    // Execute write tasks
    void execute_write_tasks(const std::vector<std::string>& keys, Job& job,
                             const JobConfig& config) {
        // Create S3 client configuration
        doris::S3ClientConf s3_conf = create_s3_client_conf(config);

        // Initialize S3 client
        auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
        doris::Status init_status = client->init();
        if (!init_status.ok()) {
            throw std::runtime_error("Failed to initialize S3 client: " + init_status.to_string());
        }

        std::atomic<int> completed_writes(0);
        std::vector<std::future<void>> write_futures;
        write_futures.reserve(keys.size());
        ThreadPool write_pool(config.num_threads);

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
                        options.file_cache_expiration = config.expiration;
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
                    size_t accumulated_size = 0;

                    // Stream data writing
                    while (data_generator.has_more()) {
                        doris::Slice chunk = data_generator.next_chunk(key);
                        slices.push_back(chunk);
                        accumulated_size += chunk.size;

                        if (accumulated_size >= config.write_batch_size ||
                            !data_generator.has_more()) {
                            doris::Status status = writer->appendv(slices.data(), slices.size(),
                                                                   job.write_latency);
                            if (!status.ok()) {
                                throw std::runtime_error("Write error for key " + key + ": " +
                                                         status.to_string());
                            }
                            slices.clear();
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

    // Execute read tasks
    void execute_read_tasks(const std::vector<std::string>& keys, Job& job, JobConfig& config) {
        LOG(INFO) << "Starting read tasks for job " << job.job_id << ", num_keys=" << keys.size()
                  << ", read_iops=" << config.read_iops;
        auto start_time = std::chrono::steady_clock::now();

        int64_t exist_job_perfile_size = s3_file_records.get_exist_job_perfile_size_by_prefix(
                HIDDEN_PREFIX + config.file_prefix);
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
        ThreadPool read_pool(config.num_threads);
        std::atomic<int> completed_reads(0);
        doris::MonotonicStopWatch read_stopwatch; // Add read task timer

        // Create S3 client configuration
        doris::S3ClientConf s3_conf = create_s3_client_conf(config);
        std::vector<std::string> read_files;
        if (exist_job_perfile_size != -1) {
            // read exist files
            s3_file_records.get_exist_job_files_by_prefix(HIDDEN_PREFIX + config.file_prefix,
                                                          read_files, config.num_files);
        }

        if (read_files.empty()) {
            // not read exist files
            read_files = keys;
        }
        LOG(INFO) << "job_id = " << job.job_id << " read_files size = " << read_files.size();

        read_stopwatch.start();
        for (int i = 0; i < read_files.size(); ++i) {
            const auto& key = read_files[i];
            read_futures.push_back(read_pool.enqueue([&, key]() {
                try {
                    if (job.completion_tracker) {
                        job.completion_tracker->wait_for_completion(
                                key); // Wait for file completion
                    }
                    doris::io::FileReaderOptions reader_opts;
                    reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
                    reader_opts.is_doris_table = true;

                    doris::io::FileDescription fd;
                    std::string obj_path = "s3://" + doris::config::test_s3_bucket + "/";
                    fd.path = doris::io::Path(obj_path + key);
                    fd.file_size = exist_job_perfile_size != -1 ? exist_job_perfile_size
                                                                : config.size_bytes_perfile;
                    doris::io::FileSystemProperties fs_props;
                    fs_props.system_type = doris::TFileType::FILE_S3;

                    std::map<std::string, std::string> props;
                    props["AWS_ACCESS_KEY"] = s3_conf.ak;
                    props["AWS_SECRET_KEY"] = s3_conf.sk;
                    props["AWS_ENDPOINT"] = s3_conf.endpoint;
                    props["AWS_REGION"] = s3_conf.region;
                    props["AWS_MAX_CONNECTIONS"] = std::to_string(s3_conf.max_connections);
                    props["AWS_REQUEST_TIMEOUT_MS"] = std::to_string(s3_conf.request_timeout_ms);
                    props["AWS_CONNECT_TIMEOUT_MS"] = std::to_string(s3_conf.connect_timeout_ms);
                    props["use_path_style"] = s3_conf.use_virtual_addressing ? "false" : "true";

                    fs_props.properties = std::move(props);

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

                        for (int i = 0; i < config.repeat; i++) {
                            auto reader = std::make_unique<MicrobenchFileReader>(
                                    status_or_reader.value(), job.read_limiter);
                            doris::Defer defer {[&]() {
                                if (auto status = reader->close(); !status.ok()) {
                                    LOG(ERROR) << "close file reader failed" << status.to_string();
                                }
                            }};

                            size_t read_offset = 0;
                            size_t read_length = 0;

                            bool use_random = true;
                            if (config.read_offset_left + 1 == config.read_offset_right) {
                                use_random = false;
                            }
                            if (exist_job_perfile_size != -1) {
                                // read exist files
                                if (config.read_offset_right > exist_job_perfile_size) {
                                    config.read_offset_right = exist_job_perfile_size;
                                }
                                if (config.read_length_right > exist_job_perfile_size) {
                                    config.read_length_right = exist_job_perfile_size;
                                }

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

                            std::string read_buffer;
                            read_buffer.resize(read_length);

                            size_t total_bytes_read = 0;
                            while (total_bytes_read < read_length) {
                                size_t bytes_to_read = std::min(
                                        read_length - total_bytes_read,
                                        static_cast<size_t>(4 * 1024 * 1024)); // 4MB chunks

                                doris::Slice read_slice(read_buffer.data() + total_bytes_read,
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
                            if (!DataVerifier::verify_data(key, file_size, read_offset, read_buffer,
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
        job.stats.num_inverted_index_remote_io_total =
                total_stats.num_inverted_index_remote_io_total;
        job.stats.local_io_timer = total_stats.local_io_timer;
        job.stats.bytes_read_from_local = total_stats.bytes_read_from_local;
        job.stats.bytes_read_from_remote = total_stats.bytes_read_from_remote;
        job.stats.remote_io_timer = total_stats.remote_io_timer;
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
    ThreadPool _job_executor_pool;
};

namespace microbenchService {

class MicrobenchServiceImpl : public microbench::MicrobenchService {
public:
    MicrobenchServiceImpl(JobManager& job_manager) : _job_manager(job_manager) {}
    virtual ~MicrobenchServiceImpl() {}

    /**
     * Submit a job
     * 
     * Receive JSON-formatted job configuration, create and submit the job
     * Return a JSON response containing the job ID
     */
    void submit_job(google::protobuf::RpcController* cntl_base,
                    const microbench::HttpRequest* request, microbench::HttpResponse* response,
                    google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Received submit job request";

        try {
            // Parse request body JSON
            std::string job_config = cntl->request_attachment().to_string();
            JobConfig config = JobConfig::from_json(job_config);

            LOG(INFO) << "Parsed JobConfig: " << config.to_string();

            std::string job_id = _job_manager.submit_job(config);
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
                        google::protobuf::Closure* done) {
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
            const Job& job = _job_manager.get_job_status(job_id);

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build JSON response
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            d.AddMember("job_id", rapidjson::Value(job.job_id.c_str(), allocator), allocator);
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
            add_config_info(d, allocator, job.config);

            // Add statistics information
            add_stats_info(d, allocator, job.stats);

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
                   google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        LOG(INFO) << "Received list_jobs request";

        try {
            std::vector<std::shared_ptr<Job>> jobs = _job_manager.list_jobs();

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
                    google::protobuf::Closure* done) {
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
                  google::protobuf::Closure* done) {
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
                          microbench::HttpResponse* response, google::protobuf::Closure* done) {
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
                          microbench::HttpResponse* response, google::protobuf::Closure* done) {
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
                            microbench::HttpResponse* response, google::protobuf::Closure* done) {
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
                       google::protobuf::Closure* done) {
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
            std::string key = "";
            std::string value = "";
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
                     google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received show_config request";

        try {
            std::vector<std::vector<std::string>> config_info = doris::config::get_config_info();
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            // Write config array
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

            // Set response headers
            cntl->http_response().set_content_type("application/json");

            // Build success response
            d.AddMember("status", "OK", allocator);
            d.AddMember("config", rapidjson::Value(buffer.GetString(), allocator), allocator);

            buffer.Clear();
            d.Accept(writer);

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

    // Add file records to JSON response
    void add_file_records(rapidjson::Document& doc, rapidjson::Document::AllocatorType& allocator,
                          const std::vector<FileInfo>& file_records, size_t max_files) {
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

    JobManager& _job_manager;
};
} // namespace microbenchService

// HTTP server handling
class HttpServer {
public:
    HttpServer(JobManager& job_manager) : _job_manager(job_manager), _server(nullptr) {}

    void start() {
        _server = new brpc::Server();
        microbenchService::MicrobenchServiceImpl http_svc(_job_manager);

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
        _server->RunUntilAskedToQuit(); // Wait for signals
        _server->ClearServices();

        LOG(INFO) << "HTTP server stopped";
    }

    ~HttpServer() {
        if (_server) {
            LOG(INFO) << "Cleaning up HTTP server in destructor";
            delete _server;
        }
    }

private:
    JobManager& _job_manager;
    brpc::Server* _server;
};

void init_exec_env() {
    auto* exec_env = doris::ExecEnv::GetInstance();
    static_cast<void>(doris::ThreadPoolBuilder("MicrobenchS3FileUploadThreadPool")
                              .set_min_threads(256)
                              .set_max_threads(512)
                              .build(&(exec_env->_s3_file_upload_thread_pool)));
    exec_env->_file_cache_factory = new FileCacheFactory();
    std::vector<doris::CachePath> cache_paths;
    exec_env->init_file_cache_factory(cache_paths);
    exec_env->_file_cache_open_fd_cache = std::make_unique<doris::io::FDCache>();
}

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

    if (-1 == setenv("DORIS_HOME", ".", 0)) {
        LOG(WARNING) << "set DORIS_HOME error";
    }
    const char* doris_home = getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        LOG(INFO) << "DORIS_HOME environment variable not set";
    }
    LOG(INFO) << "env=" << doris_home;
    std::string conffile = std::string(doris_home) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        LOG(ERROR) << "Error reading config file";
        return -1;
    }
    std::string custom_conffile = doris::config::custom_config_dir + "/be_custom.conf";
    if (!doris::config::init(custom_conffile.c_str(), true, false, false)) {
        LOG(ERROR) << "Error reading custom config file";
        return -1;
    }

    LOG(INFO) << "Obj config. ak=" << doris::config::test_s3_ak
              << " sk=" << doris::config::test_s3_sk << " region=" << doris::config::test_s3_region
              << " endpoint=" << doris::config::test_s3_endpoint
              << " bucket=" << doris::config::test_s3_bucket;
    LOG(INFO) << "File cache config. enable_file_cache=" << doris::config::enable_file_cache
              << " file_cache_path=" << doris::config::file_cache_path
              << " file_cache_each_block_size=" << doris::config::file_cache_each_block_size
              << " clear_file_cache=" << doris::config::clear_file_cache
              << " enable_file_cache_query_limit=" << doris::config::enable_file_cache_query_limit
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
              << " enable_flush_file_cache_async=" << doris::config::enable_flush_file_cache_async;

    init_exec_env();
    JobManager job_manager;

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

    try {
        HttpServer http_server(job_manager);
        http_server.start();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error in HTTP server: " << e.what();
    }

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