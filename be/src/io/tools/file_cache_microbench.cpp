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
#include <filesystem> // 添加这个头文件
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
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/exec_env.h"
#include "util/bvar_helper.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"

const std::string HIDDEN_PREFIX = "test_file_cache_microbench/";
// Just 10^9.
static constexpr auto NS = 1000000000UL;

DEFINE_int32(port, 8888, "Http Port of this server");

// 添加一个数据生成器类
class DataGenerator {
public:
    DataGenerator(size_t total_size, size_t buffer_size = 1024 * 1024) // 默认1MB缓冲区
            : _total_size(total_size), _generated_size(0), _buffer_size(buffer_size) {
        _buffer.resize(_buffer_size, 'x');
    }

    // 生成特定大小的数据，作为静态函数
    static std::string generate_fixed_size_data(size_t size) {
        return std::string(size, 'x'); // 生成指定大小的 'x' 字符串
    }

    // 获取下一块数据
    doris::Slice next_chunk() {
        if (_generated_size >= _total_size) {
            return doris::Slice(); // 返回空slice表示结束
        }

        size_t remaining = _total_size - _generated_size;
        size_t chunk_size = std::min(remaining, _buffer_size);
        _generated_size += chunk_size;

        return doris::Slice(_buffer.data(), chunk_size);
    }

    // 重置生成器
    void reset() { _generated_size = 0; }

    // 检查是否还有更多数据
    bool has_more() const { return _generated_size < _total_size; }

    // 获取总大小
    size_t total_size() const { return _total_size; }

private:
    const size_t _total_size;
    size_t _generated_size;
    const size_t _buffer_size;
    std::string _buffer;
};

// 定义一个结构体来存储文件信息
struct FileInfo {
    std::string filename; // 文件名
    size_t data_size;     // 数据大小
    std::string job_id;   // 关联的作业ID
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

    std::map<std::string, std::vector<FileInfo>> get_all_records() {
        std::lock_guard<std::mutex> lock(mutex_);
        return records_;
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

// 创建一个全局的 S3FileRecords 实例
S3FileRecords s3_file_records;

class MircobenchS3FileWriter {
public:
    MircobenchS3FileWriter(std::shared_ptr<doris::io::ObjClientHolder> client,
                           const std::string& bucket, const std::string& key,
                           const doris::io::FileWriterOptions* options,
                           std::shared_ptr<doris::S3RateLimiterHolder> rate_limiter)
            : _writer(client, bucket, key, options), _rate_limiter(rate_limiter) {}

    doris::Status appendv(const doris::Slice* slices, size_t slices_size,
                          const std::shared_ptr<bvar::LatencyRecorder>& write_bvar) {
        if (_rate_limiter) {
            _rate_limiter->add(1);
        }
        using namespace doris;
        SCOPED_BVAR_LATENCY(*write_bvar)
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
            _rate_limiter->add(1); // 消耗一个令牌
        }
        using namespace doris;
        SCOPED_BVAR_LATENCY(*read_bvar)
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
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back([this] {
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
            });
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
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers) {
            worker.join();
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
        _cv.notify_all(); // 通知所有等待的线程
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

    Notes:
      - Ensure that the S3 configuration is set correctly in the environment.
      - The program will create and read files in the specified S3 bucket.
      - Monitor the logs for detailed execution information and errors.
    )";
    return usage;
}

// Job配置结构
struct JobConfig {
    int64_t size_bytes_perfile;
    int32_t write_iops = 0;
    int32_t read_iops = 0;
    int32_t num_threads;
    int32_t num_files;
    std::string file_prefix;
    std::string cache_type;
    int64_t expiration;
    int32_t repeat = 1;
    int64_t write_batch_size;
    int64_t read_offset_left;
    int64_t read_offset_right;
    int64_t read_length_left;
    int64_t read_length_right;
    bool write_file_cache = true;

    // 从JSON解析配置
    static JobConfig from_json(const std::string& json_str) {
        JobConfig config;
        // 使用rapidjson解析
        rapidjson::Document d;
        d.Parse(json_str.c_str());

        if (d.HasParseError()) {
            throw std::runtime_error("JSON parse error json args=" + json_str);
        }
        validate(d);
        if (d.HasMember("write_file_cache") && d["write_file_cache"].GetBool() == false) {
            config.write_file_cache = false;
        }
        config.num_files = d["num_files"].GetInt();
        if (config.num_files == 0) {
            config.num_files = 1;
        }
        config.size_bytes_perfile = d["size_bytes_perfile"].GetInt64();
        config.write_iops = d["write_iops"].GetInt();
        config.read_iops = d["read_iops"].GetInt();
        config.num_threads = d["num_threads"].GetInt();
        if (config.num_threads == 0) {
            config.num_threads = 200;
        }
        config.file_prefix = d["file_prefix"].GetString();

        if (!d.HasMember("cache_type")) {
            config.cache_type = "NORMAL";
        } else {
            config.cache_type = d["cache_type"].IsString() ? d["cache_type"].GetString() : "NORMAL";
        }

        if (config.cache_type == "TTL") {
            if (!d.HasMember("expiration")) {
                throw std::runtime_error("expiration is required when cache type eq TTL");
            }
            config.expiration = d["expiration"].GetInt64();
            if (config.expiration <= 0) {
                throw std::runtime_error("expiration <= 0 when cache type eq TTL");
            }
        }

        config.repeat = d["repeat"].GetInt64();

        config.write_batch_size = d["write_batch_size"].GetInt64();
        if (config.write_batch_size == 0) {
            config.write_batch_size = doris::config::s3_write_buffer_size;
        }

        if (config.read_iops > 0) {
            // such as [0, 100)
            const rapidjson::Value& read_offset_array = d["read_offset"];
            if (!read_offset_array.IsArray() || read_offset_array.Size() != 2) {
                throw std::runtime_error("Invalid read_offset format, expected array of size 2");
            }
            config.read_offset_left = read_offset_array[0].GetInt64();
            config.read_offset_right = read_offset_array[1].GetInt64();
            if (config.read_offset_left >= config.read_offset_right) {
                throw std::runtime_error("read_offset_left must be less than read_offset_right");
            }

            // such as [100, 500) or [-200, -10)
            const rapidjson::Value& read_length_array = d["read_length"];
            if (!read_length_array.IsArray() || read_length_array.Size() != 2) {
                throw std::runtime_error("Invalid read_length format, expected array of size 2");
            }
            config.read_length_left = read_length_array[0].GetInt64();
            config.read_length_right = read_length_array[1].GetInt64();
            if (config.read_length_left >= config.read_length_right) {
                throw std::runtime_error("read_length_left must be less than read_length_right");
            }
        }

        // 添加更多参数验证
        if (config.num_threads > 10000) {
            throw std::runtime_error("num_threads cannot exceed 10000");
        }
        if (config.size_bytes_perfile <= 0) {
            throw std::runtime_error("size_bytes_perfile must be positive");
        }

        return config;
    }

    static void validate(const rapidjson::Document& json_data) {
        if (!json_data.HasMember("file_prefix") || !json_data["file_prefix"].IsString() ||
            strlen(json_data["file_prefix"].GetString()) == 0) {
            throw std::runtime_error("file_prefix is required and cannot be empty");
        }
    }

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

// Job状态
enum class JobStatus { PENDING, RUNNING, COMPLETED, FAILED };

// Job结构
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

    // Job执行结果统计
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

    // 记录与作业相关的文件信息
    std::vector<FileInfo> file_records;

    // 添加 completion_tracker
    std::shared_ptr<FileCompletionTracker> completion_tracker;

    std::shared_ptr<bvar::LatencyRecorder> write_latency;
    std::shared_ptr<bvar::Adder<int64_t>> write_rate_limit_s;
    std::shared_ptr<bvar::LatencyRecorder> read_latency;
    std::shared_ptr<bvar::Adder<int64_t>> read_rate_limit_s;

    // 默认构造函数
    Job()
            : job_id(""),
              config(),
              status(JobStatus::PENDING),
              create_time(std::chrono::system_clock::now()) {
        init_latency_recorders("");
        completion_tracker = std::make_shared<FileCompletionTracker>();
    }

    // 带参数的构造函数
    Job(const std::string& id, const JobConfig& cfg)
            : job_id(id),
              config(cfg),
              status(JobStatus::PENDING),
              create_time(std::chrono::system_clock::now()) {
        init_latency_recorders(id);
        if (config.write_iops && config.read_iops) {
            completion_tracker = std::make_shared<FileCompletionTracker>();
        } else {
            completion_tracker = nullptr;
        }
        init_limiters(cfg);
    }

private:
    void init_latency_recorders(const std::string& id) {
        if (config.write_iops > 0) {
            write_latency =
                    std::make_shared<bvar::LatencyRecorder>("file_cache_microbench_append_" + id);
            write_rate_limit_s = std::make_shared<bvar::Adder<int64_t>>(
                    "file_cache_microbench_append_rate_limit_ns_" + id);
        }
        if (config.read_iops > 0) {
            read_latency =
                    std::make_shared<bvar::LatencyRecorder>("file_cache_microbench_read_at_" + id);
            read_rate_limit_s = std::make_shared<bvar::Adder<int64_t>>(
                    "file_cache_microbench_read_rate_limit_ns_" + id);
        }
    }

    void init_limiters(const JobConfig& cfg) {
        if (cfg.write_iops > 0) {
            write_limiter = std::make_shared<doris::S3RateLimiterHolder>(
                    doris::S3RateLimitType::PUT,
                    cfg.write_iops, // max_speed (IOPS)
                    cfg.write_iops, // max_burst
                    0,              // no limit
                    [this](int64_t wait_time_ns) {
                        if (wait_time_ns > 0) {
                            *write_rate_limit_s << wait_time_ns / NS;
                        }
                    });
        }

        if (cfg.read_iops > 0) {
            read_limiter = std::make_shared<doris::S3RateLimiterHolder>(
                    doris::S3RateLimitType::GET,
                    cfg.read_iops, // max_speed (IOPS)
                    cfg.read_iops, // max_burst
                    0,             // no limit
                    [this](int64_t wait_time_ns) {
                        if (wait_time_ns > 0) {
                            *read_rate_limit_s << wait_time_ns / NS;
                        }
                    });
        }
    }
};

// Job管理器
class JobManager {
public:
    JobManager() : _next_job_id(0), _job_executor_pool(std::thread::hardware_concurrency()) {}

    ~JobManager() {
        try {
            stop();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error stopping JobManager: " << e.what();
        }
    }

    std::string submit_job(const JobConfig& config) {
        try {
            std::lock_guard<std::mutex> lock(_mutex);
            std::string job_id = "job_" + std::to_string(std::time(nullptr)) + "_" +
                                 std::to_string(_next_job_id++);
            _jobs[job_id] = std::make_shared<Job>(job_id, config);

            _job_executor_pool.enqueue([this, job_id]() {
                try {
                    {
                        std::lock_guard<std::mutex> lock(_mutex);
                        _jobs[job_id]->status = JobStatus::RUNNING;
                        _jobs[job_id]->start_time = std::chrono::system_clock::now();
                    }

                    execute_job(job_id);

                    {
                        std::lock_guard<std::mutex> lock(_mutex);
                        _jobs[job_id]->status = JobStatus::COMPLETED;
                        _jobs[job_id]->end_time = std::chrono::system_clock::now();
                    }
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _jobs[job_id]->status = JobStatus::FAILED;
                    _jobs[job_id]->error_message = e.what();
                    _jobs[job_id]->end_time = std::chrono::system_clock::now();
                    LOG(ERROR) << "Job " << job_id << " failed: " << e.what();
                }
            });

            return job_id;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error submitting job: " << e.what();
            // 返回错误信息
            return R"({"error": ")" + std::string(e.what()) + R"("})";
        }
    }

    const Job& get_job_status(const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            return *(it->second);
        }
        throw std::runtime_error("Job not found");
    }

    std::vector<std::shared_ptr<Job>> list_jobs() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::shared_ptr<Job>> job_list;
        for (const auto& pair : _jobs) {
            job_list.push_back(pair.second);
        }
        return job_list;
    }

    void start() {
        // 不再需要启动worker线程
    }

    void stop() {
        // 等待所有作业完成
        _job_executor_pool.~ThreadPool();
    }

    void record_file_info(const std::string& key, size_t data_size, const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            FileInfo file_info = {key, data_size, job_id};
            it->second->file_records.push_back(file_info);
            s3_file_records.add_file_info(job_id, file_info);
        } else {
            LOG(ERROR) << "Job ID not found: " << job_id;
        }
    }

    void execute_job(const std::string& job_id) {
        try {
            std::shared_ptr<Job> job_ptr = _jobs.at(job_id);
            if (!job_ptr) {
                throw std::runtime_error("Job not found");
            }
            Job& job = *job_ptr;
            JobConfig config = job.config;
            LOG(INFO) << "begin to Run " << job_id << " job config: " << config.to_string();

            // 生成多个key
            std::vector<std::string> keys;
            keys.reserve(config.num_files);

            std::string rewrite_job_id = job_id;
            // Job Read the previously job uploaded files
            if (config.read_iops != 0 && config.write_iops == 0) {
                // 当 read_iops != 0 && write_iops == 0 时，表示读取之前写入的文件
                std::string old_job_id =
                        s3_file_records.find_job_id_by_prefix(HIDDEN_PREFIX + config.file_prefix);
                if (old_job_id == "") {
                    std::string err_msg =
                            "Can't find previously job uploaded files, Please make sure read "
                            "files exist in obj or It is also possible that you have restarted "
                            "the file_cache_microbench program, job_id = " +
                            job_id;
                    LOG(WARNING) << err_msg;
                    throw std::runtime_error(err_msg);
                }
                rewrite_job_id = old_job_id;
            }

            // 继续生成 keys
            for (int i = 0; i < config.num_files; ++i) {
                keys.push_back(HIDDEN_PREFIX + config.file_prefix + "/" + rewrite_job_id + "_" +
                               std::to_string(i));
            }

            if (config.write_iops) {
                // 执行写操作
                execute_write_tasks(keys, job, config);
            }

            if (config.read_iops) {
                // 执行读操作
                execute_read_tasks(keys, job, config);
            }
            LOG(INFO) << "finish to Run " << job_id;
        } catch (const std::out_of_range& e) {
            LOG(ERROR) << "Job not found: " << job_id << ": " << e.what();
            throw;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error executing job " << job_id << ": " << e.what();
            throw;
        }
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

    void execute_write_tasks(const std::vector<std::string>& keys, Job& job,
                             const JobConfig& config) {
        // 创建 S3 客户端配置
        doris::S3ClientConf s3_conf = create_s3_client_conf(config);

        // 初始化 S3 客户端
        auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
        doris::Status init_status = client->init();
        if (!init_status.ok()) {
            throw std::runtime_error("Failed to initialize S3 client: " + init_status.to_string());
        }

        std::atomic<int> completed_writes(0);
        std::vector<std::future<void>> write_futures;
        write_futures.reserve(keys.size());
        ThreadPool write_pool(config.num_threads);

        // 启动写入任务
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
                    auto writer = std::make_unique<MircobenchS3FileWriter>(
                            client, doris::config::test_s3_bucket, key, &options,
                            job.write_limiter);

                    std::vector<doris::Slice> slices;
                    slices.reserve(4);
                    size_t accumulated_size = 0;

                    // 流式写入数据
                    while (data_generator.has_more()) {
                        doris::Slice chunk = data_generator.next_chunk();
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

                    doris::Status status = writer->close();
                    if (!status.ok()) {
                        throw std::runtime_error("Close error for key " + key + ": " +
                                                 status.to_string());
                    }
                    if (job.completion_tracker) {
                        job.completion_tracker->mark_completed(key);
                    }
                    completed_writes++;
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Write task failed for segment " << key << ": " << e.what();
                }
            }));
        }

        // 等待所有写入任务完成
        for (auto& future : write_futures) {
            future.get();
        }
        write_stopwatch.stop(); // 停止计时

        // 将写入时间从纳秒转换为秒并格式化为字符串
        double total_write_time_seconds = write_stopwatch.elapsed_time() / 1e9; // 纳秒转秒
        job.stats.total_write_time =
                std::to_string(total_write_time_seconds) + " seconds"; // 保存为字符串
        LOG(INFO) << "Total write time: " << job.stats.total_write_time << " seconds";

        // 记录写入的文件信息
        for (const auto& key : keys) {
            size_t data_size = config.size_bytes_perfile;
            record_file_info(key, data_size, job.job_id);
        }
    }

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
        doris::MonotonicStopWatch read_stopwatch; // 添加读取任务计时器

        // 创建 S3 客户端配置
        doris::S3ClientConf s3_conf = create_s3_client_conf(config);

        read_stopwatch.start();
        for (int i = 0; i < keys.size(); ++i) {
            const auto& key = keys[i];
            read_futures.push_back(read_pool.enqueue([&, key]() {
                try {
                    if (job.completion_tracker) {
                        job.completion_tracker->wait_for_completion(key); // 等待文件完成
                    }
                    doris::io::FileReaderOptions reader_opts;
                    reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
                    reader_opts.is_doris_table = true;

                    doris::io::FileDescription fd;
                    std::string obj_path = "s3://" + doris::config::test_s3_bucket + "/";
                    fd.path = doris::io::Path(obj_path + key);
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

                        auto reader = std::make_unique<MicrobenchFileReader>(
                                status_or_reader.value(), job.read_limiter);

                        for (int i = 0; i < config.repeat; i++) {
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
                                    // 在 read_offset_left 和 read_offset_right 之间生成随机 read_offset
                                    std::uniform_int_distribution<size_t> dis_offset(
                                            config.read_offset_left, config.read_offset_right - 1);
                                    read_offset = dis_offset(gen); // 生成随机的 read_offset
                                    std::uniform_int_distribution<size_t> dis_length(
                                            config.read_length_left, config.read_length_right - 1);
                                    read_length = dis_length(gen); // 生成随机的 read_length
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

                            const size_t block_size = 512 * 1024;
                            size_t num_blocks = (read_length + block_size - 1) / block_size;

                            bool read_success = true;
                            for (size_t j = 0; j < num_blocks && read_success; ++j) {
                                size_t block_offset = j * block_size;
                                int64_t current_block_size =
                                        std::min(block_size, read_length - block_offset);
                                doris::Slice read_slice(read_buffer.data() + block_offset,
                                                        current_block_size);
                                size_t bytes_read = 0;

                                doris::Status read_status =
                                        reader->read_at(read_offset + block_offset, read_slice,
                                                        &bytes_read, &io_ctx, job.read_latency);
                                if (!read_status.ok()) {
                                    read_success = false;
                                    if (++read_retry_count >= max_read_retries) {
                                        throw std::runtime_error("Read error for segment " + key +
                                                                 ": " + read_status.to_string());
                                    }
                                    std::this_thread::sleep_for(std::chrono::seconds(1));
                                    break;
                                }

                                if (bytes_read != current_block_size) {
                                    read_success = false;
                                    if (++read_retry_count >= max_read_retries) {
                                        throw std::runtime_error(
                                                "Size mismatch for key " + key + ": expected " +
                                                std::to_string(current_block_size) + ", got " +
                                                std::to_string(bytes_read));
                                    }
                                    std::this_thread::sleep_for(std::chrono::seconds(1));
                                    break;
                                }
                            }

                            if (read_success) {
                                std::string expected_data =
                                        DataGenerator::generate_fixed_size_data(read_length);

                                // 校验读取的数据是否与生成的数据匹配
                                if (memcmp(read_buffer.data(), expected_data.data(),
                                           std::min(static_cast<size_t>(read_length),
                                                    expected_data.size())) != 0) {
                                    if (++read_retry_count >= max_read_retries) {
                                        throw std::runtime_error(
                                                "Data verification failed for key " + key);
                                    }
                                    std::this_thread::sleep_for(std::chrono::seconds(1));
                                    continue;
                                }
                            }
                        }
                        break;
                        completed_reads++;
                    }
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Read task failed for segment " << key << ": " << e.what();
                }
            }));
        }

        // 等待所有读取任务完成
        for (auto& future : read_futures) {
            future.get();
        }
        read_stopwatch.stop(); // 停止计时

        // 将读取时间从纳秒转换为秒并格式化为字符串
        double total_read_time_seconds = read_stopwatch.elapsed_time() / 1e9; // 纳秒转秒
        job.stats.total_read_time =
                std::to_string(total_read_time_seconds) + " seconds"; // 保存为字符串
        LOG(INFO) << "Total read time: " << job.stats.total_read_time << " seconds";

        // 更新作业统计信息
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
    ThreadPool _job_executor_pool; // 专门用于执行作业的线程池
    std::shared_ptr<FileCompletionTracker> completion_tracker; // 共享的完成跟踪器
};

namespace microbenchService {

class MicrobenchServiceImpl : public microbench::MicrobenchService {
public:
    MicrobenchServiceImpl(JobManager& job_manager) : _job_manager(job_manager) {}
    virtual ~MicrobenchServiceImpl() {}
    void submit_job(google::protobuf::RpcController* cntl_base,
                    const microbench::HttpRequest* request, microbench::HttpResponse* response,
                    google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received submit job request";

        // 解析请求体JSON
        std::string job_config = cntl->request_attachment().to_string();
        try {
            JobConfig config = JobConfig::from_json(job_config);

            LOG(INFO) << "JobConfig: " << config.to_string();

            std::string job_id = _job_manager.submit_job(config);

            LOG(INFO) << "Job submitted successfully with ID: " << job_id;

            // 返回job_id
            cntl->response_attachment().append(R"({"job_id": ")" + job_id + R"("})");
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error submitting job: " << e.what();
            cntl->http_response().set_status_code(400);
            cntl->response_attachment().append(R"({"error": ")" + std::string(e.what()) + R"("})");
        }
    }

    void get_job_status(google::protobuf::RpcController* cntl_base,
                        const microbench::HttpRequest* request, microbench::HttpResponse* response,
                        google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        std::string job_id = cntl->http_request().unresolved_path();
        const std::string* files_value = cntl->http_request().uri().GetQuery("files");
        size_t max_files = 1000; // 设置最大文件记录数
        if (files_value != NULL) {
            max_files = std::stoi(*files_value);
            LOG(INFO) << "file values = " << max_files;
        }

        try {
            const Job& job = _job_manager.get_job_status(job_id);

            // 构建JSON响应
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            d.AddMember("job_id", rapidjson::Value(job.job_id.c_str(), allocator), allocator);
            d.AddMember("status",
                        rapidjson::Value(get_status_string(job.status).c_str(), allocator),
                        allocator);

            if (!job.error_message.empty()) {
                d.AddMember("error_message", rapidjson::Value(job.error_message.c_str(), allocator),
                            allocator);
            }

            // 添加统计信息
            rapidjson::Value stats(rapidjson::kObjectType);
            stats.AddMember("total_write_time",
                            rapidjson::Value(job.stats.total_write_time.c_str(), allocator),
                            allocator);
            stats.AddMember("total_read_time",
                            rapidjson::Value(job.stats.total_read_time.c_str(), allocator),
                            allocator);

            // struct FileCacheStatistics
            stats.AddMember("num_local_io_total",
                            static_cast<uint64_t>(job.stats.num_local_io_total), allocator);
            stats.AddMember("num_remote_io_total",
                            static_cast<uint64_t>(job.stats.num_remote_io_total), allocator);
            stats.AddMember("num_inverted_index_remote_io_total",
                            static_cast<uint64_t>(job.stats.num_inverted_index_remote_io_total),
                            allocator);
            stats.AddMember("local_io_timer", static_cast<uint64_t>(job.stats.local_io_timer),
                            allocator);
            stats.AddMember("bytes_read_from_local",
                            static_cast<uint64_t>(job.stats.bytes_read_from_local), allocator);
            stats.AddMember("bytes_read_from_remote",
                            static_cast<uint64_t>(job.stats.bytes_read_from_remote), allocator);
            stats.AddMember("remote_io_timer", static_cast<uint64_t>(job.stats.remote_io_timer),
                            allocator);
            stats.AddMember("write_cache_io_timer",
                            static_cast<uint64_t>(job.stats.write_cache_io_timer), allocator);
            stats.AddMember("bytes_write_into_cache",
                            static_cast<uint64_t>(job.stats.bytes_write_into_cache), allocator);
            stats.AddMember("num_skip_cache_io_total",
                            static_cast<uint64_t>(job.stats.num_skip_cache_io_total), allocator);
            stats.AddMember("read_cache_file_directly_timer",
                            static_cast<uint64_t>(job.stats.read_cache_file_directly_timer),
                            allocator);
            stats.AddMember("cache_get_or_set_timer",
                            static_cast<uint64_t>(job.stats.cache_get_or_set_timer), allocator);
            stats.AddMember("lock_wait_timer", static_cast<uint64_t>(job.stats.lock_wait_timer),
                            allocator);
            stats.AddMember("get_timer", static_cast<uint64_t>(job.stats.get_timer), allocator);
            stats.AddMember("set_timer", static_cast<uint64_t>(job.stats.set_timer), allocator);

            d.AddMember("statistics", stats, allocator);

            if (files_value) {
                rapidjson::Value files_array(rapidjson::kArrayType);
                size_t count = 0;

                for (const auto& file_info : job.file_records) {
                    if (count >= max_files) {
                        break; // 超过最大限制，停止添加
                    }
                    rapidjson::Value file_obj(rapidjson::kObjectType);
                    file_obj.AddMember("filename",
                                       rapidjson::Value(file_info.filename.c_str(), allocator),
                                       allocator);
                    file_obj.AddMember("data_size", static_cast<uint64_t>(file_info.data_size),
                                       allocator);
                    file_obj.AddMember("job_id",
                                       rapidjson::Value(file_info.job_id.c_str(), allocator),
                                       allocator);
                    files_array.PushBack(file_obj, allocator);
                    count++;
                }
                d.AddMember("file_records", files_array, allocator);
            }

            // 序列化为字符串
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            cntl->http_response().set_status_code(404);
            std::string error_message = R"({"error": "Job not found", "exception": ")" +
                                        std::string(e.what()) + R"("})";
            cntl->response_attachment().append(error_message);
        }
    }

    void list_jobs(google::protobuf::RpcController* cntl_base,
                   const microbench::HttpRequest* request, microbench::HttpResponse* response,
                   google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        std::vector<std::shared_ptr<Job>> jobs = _job_manager.list_jobs();

        // 构建JSON响应
        rapidjson::Document d;
        d.SetObject();
        rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

        rapidjson::Value jobs_array(rapidjson::kArrayType);
        for (const auto& job : jobs) {
            rapidjson::Value job_obj(rapidjson::kObjectType);
            job_obj.AddMember("job_id", rapidjson::Value(job->job_id.c_str(), allocator),
                              allocator);
            job_obj.AddMember("status",
                              rapidjson::Value(get_status_string(job->status).c_str(), allocator),
                              allocator);
            jobs_array.PushBack(job_obj, allocator);
        }

        d.AddMember("jobs", jobs_array, allocator);

        // 序列化为字符串
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.Accept(writer);

        cntl->response_attachment().append(buffer.GetString());
    }

    void cancel_job(google::protobuf::RpcController* cntl_base,
                    const microbench::HttpRequest* request, microbench::HttpResponse* response,
                    google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // TODO: 实现取消作业的功能
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->http_response().set_status_code(501); // Not Implemented
        cntl->response_attachment().append(R"({"error": "Not implemented"})");
    }

    void get_help(google::protobuf::RpcController* cntl_base,
                  const microbench::HttpRequest* request, microbench::HttpResponse* response,
                  google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "Received get help request";

        // 获取使用帮助信息
        std::string help_info = get_usage("Doris Microbench Tool");

        // 返回帮助信息
        cntl->response_attachment().append(help_info);
    }

private:
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

    JobManager& _job_manager;
};
} // namespace microbenchService

// HTTP服务器处理
class HttpServer {
public:
    HttpServer(JobManager& job_manager) : _job_manager(job_manager) {}

    void start() {
        brpc::Server server;
        microbenchService::MicrobenchServiceImpl http_svc(_job_manager);

        LOG(INFO) << "Starting HTTP server on port " << FLAGS_port;

        if (server.AddService(&http_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Failed to add http service";
            return;
        }

        brpc::ServerOptions options;
        if (server.Start(FLAGS_port, &options) != 0) {
            LOG(ERROR) << "Failed to start HttpServer";
            return;
        }

        LOG(INFO) << "HTTP server started successfully";
        server.RunUntilAskedToQuit();
    }

private:
    JobManager& _job_manager;
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_minloglevel = google::GLOG_INFO;
    FLAGS_log_dir = "./logs";
    FLAGS_logbufsecs = 0; // 禁用缓冲，立即写入
    std::filesystem::path log_dir(FLAGS_log_dir);
    if (!std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
        LOG(INFO) << "日志目录创建成功: " << log_dir.string();
    } else {
        LOG(INFO) << "日志目录已存在: " << log_dir.string();
    }
    google::InitGoogleLogging(argv[0]);

    if (-1 == setenv("DORIS_HOME", ".", 0)) {
        LOG(WARNING) << "set DORIS_HOME error";
    }
    const char* doris_home = getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        LOG(INFO) << "DORIS_HOME 环境变量未设置";
    }
    LOG(INFO) << "env=" << doris_home;
    std::string conffile = std::string(doris_home) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        LOG(ERROR) << "读取配置文件错误";
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

    doris::ExecEnv::GetInstance()->init_file_cache_microbench_env();
    JobManager job_manager;
    HttpServer http_server(job_manager);
    http_server.start();

    return 0;
}
