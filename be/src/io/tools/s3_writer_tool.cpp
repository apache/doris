#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/server.h>
#include <brpc/uri.h>
#include <bvar/bvar.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <glog/logging.h>
#include <filesystem> // 添加这个头文件

#include "build/proto/mircobench.pb.h"
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

bvar::LatencyRecorder write_latency("s3_tool_write");
bvar::LatencyRecorder read_latency("s3_tool_read");

// 添加一个全局互斥锁用于保护控制台输出
// static std::mutex console_mutex;


// 添加gflags定义
DEFINE_int32(port, 8888, "Http Port of this server");
DEFINE_int64(data_size_bytes, 1024 * 1024 * 1024, "Number of bytes to write per object key");
DEFINE_int32(obj_key_write_iops, 100, "IOPS limit for writing each object key");
DEFINE_int32(obj_key_read_iops, 100, "IOPS limit for reading each object key");
DEFINE_int32(num_threads, 32, "Number of concurrent threads");
DEFINE_int32(num_keys, 10, "Number of object keys to write/read");
DEFINE_string(key_prefix, "dx_micro_bench/test_multi/key_", "Prefix for object keys");
DEFINE_int64(write_batch_size, 1024 * 1024, "Size of data to write in each write operation");
DEFINE_int64(read_offset, 0, "Start offset for range read");
DEFINE_int64(read_length, -1, "Length to read for range read, -1 means read to end");

// IOPS 统计器
class IopsStats {
public:
    IopsStats() : _start_time(std::chrono::steady_clock::now()), _last_update_time(_start_time) {}

    void record_operation() {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        _op_times.push_back(now);
        LOG(INFO) << "Recorded operation at time: " << now.time_since_epoch().count();

        // 只保留最近1秒内的操作记录
        auto one_second_ago = now - std::chrono::seconds(1);
        while (!_op_times.empty() && _op_times.front() < one_second_ago) {
            _op_times.pop_front();
        }

        // 计算当前IOPS（最近1秒内的操作数）
        _current_iops = _op_times.size();

        // 更新峰值IOPS
        if (_current_iops > _peak_iops) {
            _peak_iops = _current_iops;
        }

        // 每秒更新一次显示
        if (now - _last_update_time >= std::chrono::seconds(1)) {
            _last_update_time = now;
        }
    }

    double get_current_iops() const {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        // 如果最后一次操作距离现在超过1秒，返回0
        if (_op_times.empty() || (now - _op_times.back() > std::chrono::seconds(1))) {
            return 0.0;
        }
        return _current_iops;
    }

    double get_peak_iops() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _peak_iops;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(_mutex);
        _start_time = std::chrono::steady_clock::now();
        _last_update_time = _start_time;
        _op_times.clear();
        _current_iops = 0;
        _peak_iops = 0;
    }

private:
    mutable std::mutex _mutex;
    std::chrono::steady_clock::time_point _start_time;
    std::chrono::steady_clock::time_point _last_update_time;
    std::deque<std::chrono::steady_clock::time_point> _op_times; // 记录每个操作的时间点
    double _current_iops = 0;
    double _peak_iops = 0;
};

// IOPS Rate Limiter implementation
class IopsRateLimiter {
public:
    IopsRateLimiter(int iops_limit)
            : _iops_limit(iops_limit), _tokens(0), _last_update(std::chrono::steady_clock::now()) {}

    void acquire() {
        if (_iops_limit <= 0) return;

        std::unique_lock<std::mutex> lock(_mutex);
        LOG(INFO) << "Acquiring tokens, current tokens: " << _tokens;

        while (true) {
            // 更新令牌桶
            auto now = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _last_update)
                                    .count();
            double new_tokens = (duration / 1e9) * _iops_limit;
            _tokens = std::min(_iops_limit * 1.0, _tokens + new_tokens);
            _last_update = now;

            if (_tokens >= 1.0) {
                _tokens -= 1.0;
                break;
            }

            // 计算需要等待的时间
            double tokens_needed = 1.0 - _tokens;
            int64_t wait_ns = static_cast<int64_t>((tokens_needed / _iops_limit) * 1e9);
            auto wait_time = now + std::chrono::nanoseconds(wait_ns);

            _cv.wait_until(lock, wait_time);
        }
    }

    void set_iops_limit(int iops_limit) {
        std::lock_guard<std::mutex> lock(_mutex);
        _iops_limit = iops_limit;
        _cv.notify_all(); // 通知所有等待的线程重新检查限制
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv; // 添加条件变量
    std::atomic<int> _iops_limit;
    double _tokens;
    std::chrono::steady_clock::time_point _last_update;
};

// IOPS-controlled S3 file writer
class IopsControlledS3FileWriter {
public:
    IopsControlledS3FileWriter(std::shared_ptr<doris::io::ObjClientHolder> client,
                               const std::string& bucket, const std::string& key,
                               const doris::io::FileWriterOptions* options,
                               std::shared_ptr<IopsRateLimiter> rate_limiter,
                               std::shared_ptr<IopsStats> stats)
            : _writer(client, bucket, key, options), _rate_limiter(rate_limiter), _stats(stats) {}

    doris::Status appendv(const doris::Slice* slices, size_t slices_size) {
        _rate_limiter->acquire();
        _stats->record_operation();
        using namespace doris;
        SCOPED_BVAR_LATENCY(write_latency)
        return _writer.appendv(slices, slices_size);
    }

    doris::Status close() { return _writer.close(); }

private:
    doris::io::S3FileWriter _writer;
    std::shared_ptr<IopsRateLimiter> _rate_limiter;
    std::shared_ptr<IopsStats> _stats;
};

// IOPS-controlled file reader
class IopsControlledFileReader {
public:
    IopsControlledFileReader(std::shared_ptr<doris::io::FileReader> base_reader,
                             std::shared_ptr<IopsRateLimiter> rate_limiter,
                             std::shared_ptr<IopsStats> stats)
            : _base_reader(std::move(base_reader)), _rate_limiter(rate_limiter), _stats(stats) {}

    doris::Status read_at(size_t offset, const doris::Slice& result, size_t* bytes_read,
                          const doris::io::IOContext* io_ctx) {
        _rate_limiter->acquire();
        _stats->record_operation();
        using namespace doris;
        SCOPED_BVAR_LATENCY(read_latency)
        return _base_reader->read_at(offset, result, bytes_read, io_ctx);
    }

    size_t size() const { return _base_reader->size(); }

    doris::Status close() { return _base_reader->close(); }

private:
    std::shared_ptr<doris::io::FileReader> _base_reader;
    std::shared_ptr<IopsRateLimiter> _rate_limiter;
    std::shared_ptr<IopsStats> _stats;
};

// 显示实时IOPS统计信息的线程函数
void display_iops_stats(const std::string& operation, const std::shared_ptr<IopsStats>& stats,
                        std::atomic<bool>& should_stop, std::atomic<int>& completed_ops,
                        int total_ops) {
    while (!should_stop) {
        double current_iops = stats->get_current_iops();
        double peak_iops = stats->get_peak_iops();
        int completed = completed_ops.load();

        {
            // std::lock_guard<std::mutex> lock(console_mutex);
            LOG(INFO) << "\r" << operation << " - 当前IOPS: " << std::fixed << std::setprecision(2)
                      << current_iops << ", 峰值IOPS: " << peak_iops << ", 进度: " << completed
                      << "/" << total_ops << ", 当前完成数: " << completed_ops.load();
        }

        // 如果所有操作都完成了，且IOPS降为0，就退出显示
        if (completed >= total_ops && current_iops == 0) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 打印最终换行
    // if (completed_ops.load() >= total_ops) {
        // LOG(INFO) << "";
        // std::lock_guard<std::mutex> lock(console_mutex);
        // std::cout << std::endl;
    // }
}

// 线程池实现
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

// 添加一个文件完成状态跟踪器
class FileCompletionTracker {
public:
    void mark_completed(const std::string& key) {
        std::lock_guard<std::mutex> lock(_mutex);
        _completed_files.insert(key);
    }

    bool is_completed(const std::string& key) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _completed_files.find(key) != _completed_files.end();
    }

private:
    std::mutex _mutex;
    std::unordered_set<std::string> _completed_files;
};


std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris microbench tool for testing file cache in cloud.\n";
    ss << "\nFlags:\n";
    ss << "  --data_size_bytes: Number of bytes to write per object key\n";
    ss << "  --obj_key_write_iops: IOPS limit for writing each object key\n";
    ss << "  --obj_key_read_iops: IOPS limit for reading each object key\n";
    ss << "  --num_threads: Thread number of thread pool\n";
    ss << "  --num_keys: Number of object keys to write/read\n";
    ss << "  --key_prefix: Prefix for object keys\n";
    ss << "  --write_batch_size: Size of data to write in each write operation\n";
    ss << "  --read_offset: Start offset for range read\n";
    ss << "  --read_length: Length to read for range read, -1 means read to end\n";
    ss << "\nExample:\n";
    ss << progname << " --data_size_bytes=1048576 --obj_key_write_iops=100 "
       << "--obj_key_read_iops=100 --num_threads=4 --num_keys=1 "
       << "--key_prefix=dx_micro_bench/test_multi/key_ --write_batch_size=2097152 "
          "--read_offset=1048576 --read_length=2097152\n";
    return ss.str();
}

// Job配置结构
struct JobConfig {
    int64_t data_size_bytes;
    int32_t write_iops;
    int32_t read_iops;
    int32_t num_threads;
    int32_t num_keys;
    std::string key_prefix;
    int64_t write_batch_size;
    int64_t read_offset;
    int64_t read_length;

    // 从JSON解析配置
    static JobConfig from_json(const std::string& json_str) {
        JobConfig config;
        // 使用rapidjson解析
        rapidjson::Document d;
        d.Parse(json_str.c_str());

        config.data_size_bytes = d["data_size_bytes"].GetInt64();
        config.write_iops = d["write_iops"].GetInt();
        config.read_iops = d["read_iops"].GetInt();
        config.num_threads = d["num_threads"].GetInt();
        config.num_keys = d["num_keys"].GetInt();
        config.key_prefix = d["key_prefix"].GetString();
        config.write_batch_size = d["write_batch_size"].GetInt64();
        config.read_offset = d["read_offset"].GetInt64();
        config.read_length = d["read_length"].GetInt64();

        return config;
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << "data_size_bytes: " << data_size_bytes
           << ", write_iops: " << write_iops
           << ", read_iops: " << read_iops
           << ", num_threads: " << num_threads
           << ", num_keys: " << num_keys
           << ", key_prefix: " << key_prefix
           << ", write_batch_size: " << write_batch_size
           << ", read_offset: " << read_offset
           << ", read_length: " << read_length;
        return ss.str();
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

    // Job执行结果统计
    struct Statistics {
        double peak_write_iops;
        double peak_read_iops;
        int64_t cache_hits;
        int64_t cache_misses;
        int64_t bytes_read_local;
        int64_t bytes_read_remote;
    } stats;
};

// Job管理器
class JobManager {
public:
    JobManager() : _next_job_id(0), _job_executor_pool(4) {} // 创建4个线程的执行池

    std::string submit_job(const JobConfig& config) {
        std::lock_guard<std::mutex> lock(_mutex);
        std::string job_id = "job_" + std::to_string(_next_job_id++);
        Job job;
        job.job_id = job_id;
        job.config = config;
        job.status = JobStatus::PENDING;
        job.create_time = std::chrono::system_clock::now();
        _jobs[job_id] = job;
        
        // 将作业提交到执行线程池
        _job_executor_pool.enqueue([this, job_id]() {
            try {
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _jobs[job_id].status = JobStatus::RUNNING;
                    _jobs[job_id].start_time = std::chrono::system_clock::now();
                }

                execute_job(job_id);

                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _jobs[job_id].status = JobStatus::COMPLETED;
                    _jobs[job_id].end_time = std::chrono::system_clock::now();
                }
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(_mutex);
                _jobs[job_id].status = JobStatus::FAILED;
                _jobs[job_id].error_message = e.what();
                _jobs[job_id].end_time = std::chrono::system_clock::now();
                LOG(ERROR) << "Job " << job_id << " failed: " << e.what();
            }
        });

        return job_id;
    }

    Job get_job_status(const std::string& job_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _jobs.find(job_id);
        if (it != _jobs.end()) {
            return it->second;
        }
        throw std::runtime_error("Job not found");
    }

    std::vector<Job> list_jobs() {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<Job> result;
        for (const auto& pair : _jobs) {
            result.push_back(pair.second);
        }
        return result;
    }

    void start() {
        // 不再需要启动worker线程
    }

    void stop() {
        // 等待所有作业完成
        _job_executor_pool.~ThreadPool();
    }

private:
    void execute_job(const std::string& job_id);  // 实现保持不变

    std::mutex _mutex;
    std::atomic<int> _next_job_id;
    std::map<std::string, Job> _jobs;
    ThreadPool _job_executor_pool;  // 专门用于执行作业的线程池
};

namespace microbenchService {

class MicrobenchServiceImpl : public mircobench::MicrobenchService {
public:
    MicrobenchServiceImpl(JobManager& job_manager) : _job_manager(job_manager) {}
    virtual ~MicrobenchServiceImpl() {}
    void submit_job(google::protobuf::RpcController* cntl_base, const mircobench::HttpRequest* request, 
                   mircobench::HttpResponse* response, google::protobuf::Closure* done) {
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
            cntl->response_attachment().append("{\"job_id\": \"" + job_id + "\"}");
        } catch (const std::exception& e) {
            LOG(ERROR) << "Error submitting job: " << e.what();
            cntl->http_response().set_status_code(400);
            cntl->response_attachment().append("{\"error\": \"" + std::string(e.what()) + "\"}");
        }
    }

    void get_job_status(google::protobuf::RpcController* cntl_base, const mircobench::HttpRequest* request,
                       mircobench::HttpResponse* response, google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);                 
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        std::string job_id = cntl->http_request().unresolved_path();

        try {
            Job job = _job_manager.get_job_status(job_id);
            
            // 构建JSON响应
            rapidjson::Document d;
            d.SetObject();
            rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

            d.AddMember("job_id", rapidjson::Value(job.job_id.c_str(), allocator), allocator);
            d.AddMember("status", rapidjson::Value(get_status_string(job.status).c_str(), allocator), allocator);
            
            if (!job.error_message.empty()) {
                d.AddMember("error_message", rapidjson::Value(job.error_message.c_str(), allocator), allocator);
            }

            // 添加统计信息
            rapidjson::Value stats(rapidjson::kObjectType);
            stats.AddMember("peak_write_iops", job.stats.peak_write_iops, allocator);
            stats.AddMember("peak_read_iops", job.stats.peak_read_iops, allocator);
            stats.AddMember("cache_hits", job.stats.cache_hits, allocator);
            stats.AddMember("cache_misses", job.stats.cache_misses, allocator);
            stats.AddMember("bytes_read_local", job.stats.bytes_read_local, allocator);
            stats.AddMember("bytes_read_remote", job.stats.bytes_read_remote, allocator);
            
            d.AddMember("statistics", stats, allocator);

            // 序列化为字符串
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);

            cntl->response_attachment().append(buffer.GetString());
        } catch (const std::exception& e) {
            cntl->http_response().set_status_code(404);
            std::string error_message = "{\"error\": \"Job not found\", \"exception\": \"" +
                                      std::string(e.what()) + "\"}";
            cntl->response_attachment().append(error_message);
        }
    }

    void list_jobs(google::protobuf::RpcController* cntl_base, const mircobench::HttpRequest* request,
                  mircobench::HttpResponse* response, google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        
        std::vector<Job> jobs = _job_manager.list_jobs();
        
        // 构建JSON响应
        rapidjson::Document d;
        d.SetObject();
        rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
        
        rapidjson::Value jobs_array(rapidjson::kArrayType);
        for (const auto& job : jobs) {
            rapidjson::Value job_obj(rapidjson::kObjectType);
            job_obj.AddMember("job_id", rapidjson::Value(job.job_id.c_str(), allocator), allocator);
            job_obj.AddMember("status", rapidjson::Value(get_status_string(job.status).c_str(), allocator), allocator);
            jobs_array.PushBack(job_obj, allocator);
        }
        
        d.AddMember("jobs", jobs_array, allocator);
        
        // 序列化为字符串
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.Accept(writer);
        
        cntl->response_attachment().append(buffer.GetString());
    }

    void cancel_job(google::protobuf::RpcController* cntl_base, const mircobench::HttpRequest* request,
                   mircobench::HttpResponse* response, google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // TODO: 实现取消作业的功能
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->http_response().set_status_code(501); // Not Implemented
        cntl->response_attachment().append("{\"error\": \"Not implemented\"}");
    }

private:
    std::string get_status_string(JobStatus status) {
        switch (status) {
            case JobStatus::PENDING: return "PENDING";
            case JobStatus::RUNNING: return "RUNNING";
            case JobStatus::COMPLETED: return "COMPLETED";
            case JobStatus::FAILED: return "FAILED";
            default: return "UNKNOWN";
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

        // 添加HTTP服务
        if (server.AddService(&http_svc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Failed to add http service";
            return;
        }

        // 启动服务器
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
    std::filesystem::path log_dir(FLAGS_log_dir);
    if (!std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
        LOG(INFO) << "日志目录创建成功: " << log_dir.string();
    } else {
        LOG(INFO) << "日志目录已存在: " << log_dir.string();
    }
    google::InitGoogleLogging(argv[0]);

    const char* doris_home = getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        LOG(INFO) << "DORIS_HOME 环境变量未设置";
        doris_home = ".";
    }
    LOG(INFO) << "env=" << doris_home;
    std::string conffile = std::string(doris_home) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        LOG(ERROR) << "读取配置文件错误";
        return -1;
    }

    doris::ExecEnv::GetInstance()->init_file_cache_microbench_env();
    JobManager job_manager;
    HttpServer http_server(job_manager);
    http_server.start();

    return 0;
}

void JobManager::execute_job(const std::string& job_id) {
    Job& job = _jobs[job_id];
    const JobConfig& config = job.config;

    // 生成多个key
    std::vector<std::string> keys;
    keys.reserve(config.num_keys);
    for (int i = 0; i < config.num_keys; ++i) {
        keys.push_back(config.key_prefix + std::to_string(i));
    }

    // 创建速率限制器和统计器
    std::vector<std::shared_ptr<IopsRateLimiter>> write_limiters;
    std::vector<std::shared_ptr<IopsRateLimiter>> read_limiters;
    write_limiters.reserve(config.num_keys);
    read_limiters.reserve(config.num_keys);
    for (int i = 0; i < config.num_keys; ++i) {
        write_limiters.push_back(std::make_shared<IopsRateLimiter>(config.write_iops));
        read_limiters.push_back(std::make_shared<IopsRateLimiter>(config.read_iops));
    }
    auto write_stats = std::make_shared<IopsStats>();
    auto read_stats = std::make_shared<IopsStats>();

    // 创建线程池
    ThreadPool pool(config.num_threads);

    // 生成测试数据
    std::string data;
    data.reserve(config.data_size_bytes);
    data.assign(config.data_size_bytes, 'x');

    // 初始化S3客户端
    doris::S3ClientConf s3_conf;
    s3_conf.max_connections = std::max(256, config.num_threads * 4);
    s3_conf.request_timeout_ms = 60000;
    s3_conf.connect_timeout_ms = 3000;

    auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
    doris::Status init_status = client->init();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize S3 client: " + init_status.to_string());
    }

    auto completion_tracker = std::make_shared<FileCompletionTracker>();
    std::atomic<int> completed_writes(0);
    std::atomic<int> completed_reads(0);
    std::vector<std::future<void>> futures;

    LOG(INFO) << "xxxxx JobConfig: " << config.to_string();
    // 启动写入任务
    for (int i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        futures.push_back(pool.enqueue([&, key, i]() {
            doris::io::FileWriterOptions options;
            options.write_file_cache = true;
            auto writer = std::make_unique<IopsControlledS3FileWriter>(
                    client, "gavin-test-hk-1308700295", key, &options, write_limiters[i],
                    write_stats);

            const size_t block_size = 1 * 1024 * 1024;
            size_t num_blocks = (config.data_size_bytes + block_size - 1) / block_size;
            std::vector<doris::Slice> slices;
            slices.reserve(4);
            size_t accumulated_size = 0;

            for (size_t j = 0; j < num_blocks; ++j) {
                size_t offset = j * block_size;
                size_t current_block_size = std::min(block_size, config.data_size_bytes - offset);
                slices.emplace_back(data.data() + offset, current_block_size);
                accumulated_size += current_block_size;

                if (accumulated_size >= config.write_batch_size || j == num_blocks - 1) {
                    doris::Status status = writer->appendv(slices.data(), slices.size());
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
                throw std::runtime_error("Close error for key " + key + ": " + status.to_string());
            }

            completion_tracker->mark_completed(key);
            completed_writes++;
        }));
    }

    // 启动读取任务
    doris::io::IOContext io_ctx;
    doris::io::FileCacheStatistics total_stats;
    io_ctx.file_cache_stats = &total_stats;

    for (int i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        futures.push_back(pool.enqueue([&, key, i]() {
            int retry_count = 0;
            const int max_retries = 300;
            while (!completion_tracker->is_completed(key)) {
                if (retry_count++ >= max_retries) {
                    throw std::runtime_error("Timeout waiting for file completion: " + key);
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            doris::io::FileReaderOptions reader_opts;
            reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
            reader_opts.is_doris_table = true;

            doris::io::FileDescription fd;
            fd.path = doris::io::Path("s3://gavin-test-hk-1308700295/" + key);
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
                auto status_or_reader =
                        doris::FileFactory::create_file_reader(fs_props, fd, reader_opts, nullptr);
                if (!status_or_reader.has_value()) {
                    if (++read_retry_count >= max_read_retries) {
                        // throw std::runtime_error("Failed to create reader for key " + key + ": " +
                        //                        status_or_reader.error());
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                auto reader = std::make_unique<IopsControlledFileReader>(
                        status_or_reader.value(), read_limiters[i], read_stats);

                size_t read_offset = config.read_offset;
                size_t read_length = config.read_length;
                if (read_length == -1 || read_offset + read_length > config.data_size_bytes) {
                    read_length = config.data_size_bytes - read_offset;
                }

                std::string read_buffer;
                read_buffer.resize(read_length);

                const size_t block_size = 512 * 1024;
                size_t num_blocks = (read_length + block_size - 1) / block_size;

                bool read_success = true;
                for (size_t j = 0; j < num_blocks && read_success; ++j) {
                    size_t block_offset = j * block_size;
                    size_t current_block_size = std::min(block_size, read_length - block_offset);
                    doris::Slice read_slice(read_buffer.data() + block_offset, current_block_size);
                    size_t bytes_read = 0;

                    doris::Status read_status = reader->read_at(read_offset + block_offset,
                                                                read_slice, &bytes_read, &io_ctx);
                    if (!read_status.ok()) {
                        read_success = false;
                        if (++read_retry_count >= max_read_retries) {
                            throw std::runtime_error("Read error for key " + key + ": " +
                                                     read_status.to_string());
                        }
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        break;
                    }

                    if (bytes_read != current_block_size) {
                        read_success = false;
                        if (++read_retry_count >= max_read_retries) {
                            throw std::runtime_error("Size mismatch for key " + key +
                                                     ": expected " +
                                                     std::to_string(current_block_size) + ", got " +
                                                     std::to_string(bytes_read));
                        }
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        break;
                    }
                }

                if (read_success) {
                    if (memcmp(read_buffer.data(), data.data() + read_offset, read_length) != 0) {
                        if (++read_retry_count >= max_read_retries) {
                            throw std::runtime_error("Data verification failed for key " + key);
                        }
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }
                    break;
                }
            }
            completed_reads++;
        }));
    }

    // 等待所有操作完成
    try {
        for (auto& future : futures) {
            future.get();
        }
    } catch (const std::exception& e) {
        job.error_message = e.what();
        throw;
    }

    // 更新作业统计信息
    job.stats.peak_write_iops = write_stats->get_peak_iops();
    job.stats.peak_read_iops = read_stats->get_peak_iops();
    job.stats.cache_hits = total_stats.num_local_io_total;
    job.stats.cache_misses = total_stats.num_remote_io_total;
    job.stats.bytes_read_local = total_stats.bytes_read_from_local;
    job.stats.bytes_read_remote = total_stats.bytes_read_from_remote;
}