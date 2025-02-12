#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <iomanip>
#include <queue>
#include <future>
#include <unordered_set>
#include <brpc/server.h>
#include <bvar/bvar.h>
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/bvar_helper.h"
#include "common/status.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/exec_env.h"
#include "gflags/gflags.h"

bvar::LatencyRecorder write_latency("s3_tool_write");
bvar::LatencyRecorder read_latency("s3_tool_read");

// 添加一个全局互斥锁用于保护控制台输出
static std::mutex console_mutex;

// IOPS 统计器
class IopsStats {
public:
    IopsStats() : _start_time(std::chrono::steady_clock::now()), _last_update_time(_start_time) {}

    void record_operation() {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        _op_times.push_back(now);
        
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
    std::deque<std::chrono::steady_clock::time_point> _op_times;  // 记录每个操作的时间点
    double _current_iops = 0;
    double _peak_iops = 0;
};

// IOPS Rate Limiter implementation
class IopsRateLimiter {
public:
    IopsRateLimiter(int iops_limit) 
        : _iops_limit(iops_limit), 
          _tokens(0),
          _last_update(std::chrono::steady_clock::now()) {}

    void acquire() {
        if (_iops_limit <= 0) return;

        std::unique_lock<std::mutex> lock(_mutex);
        
        while (true) {
            // 更新令牌桶
            auto now = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _last_update).count();
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
        _cv.notify_all();  // 通知所有等待的线程重新检查限制
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;  // 添加条件变量
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
            : _writer(client, bucket, key, options),
              _rate_limiter(rate_limiter),
              _stats(stats) {}

    doris::Status appendv(const doris::Slice* slices, size_t slices_size) {
        _rate_limiter->acquire();
        _stats->record_operation();
        using namespace doris;
        SCOPED_BVAR_LATENCY(write_latency)
        return _writer.appendv(slices, slices_size);
    }

    doris::Status close() {
        return _writer.close();
    }

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
            : _base_reader(std::move(base_reader)), 
              _rate_limiter(rate_limiter),
              _stats(stats) {}

    doris::Status read_at(size_t offset, const doris::Slice& result, size_t* bytes_read,
                         const doris::io::IOContext* io_ctx) {
        _rate_limiter->acquire();
        _stats->record_operation();
        using namespace doris;
        SCOPED_BVAR_LATENCY(read_latency)
        return _base_reader->read_at(offset, result, bytes_read, io_ctx);
    }

    size_t size() const {
        return _base_reader->size();
    }

    doris::Status close() {
        return _base_reader->close();
    }

private:
    std::shared_ptr<doris::io::FileReader> _base_reader;
    std::shared_ptr<IopsRateLimiter> _rate_limiter;
    std::shared_ptr<IopsStats> _stats;
};

// 显示实时IOPS统计信息的线程函数
void display_iops_stats(const std::string& operation,
                       const std::shared_ptr<IopsStats>& stats,
                       std::atomic<bool>& should_stop,
                       std::atomic<int>& completed_ops,
                       int total_ops) {
    while (!should_stop) {
        double current_iops = stats->get_current_iops();
        double peak_iops = stats->get_peak_iops();
        int completed = completed_ops.load();
        
        {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cout << "\r" << operation << " - 当前IOPS: " << std::fixed << std::setprecision(2)
                     << current_iops << ", 峰值IOPS: " << peak_iops 
                     << ", 进度: " << completed << "/" << total_ops 
                     << ", 当前完成数: " << completed_ops.load() << std::flush;  // 添加更多信息
        }

        // 如果所有操作都完成了，且IOPS降为0，就退出显示
        if (completed >= total_ops && current_iops == 0) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 打印最终换行
    if (completed_ops.load() >= total_ops) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << std::endl;
    }
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
                        condition.wait(lock, [this] {
                            return stop || !tasks.empty();
                        });
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

    template<class F>
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

// 添加gflags定义
DEFINE_int64(data_size_bytes, 1024*1024*1024, "Number of bytes to write per object key");
DEFINE_int32(obj_key_write_iops, 100, "IOPS limit for writing each object key");
DEFINE_int32(obj_key_read_iops, 100, "IOPS limit for reading each object key");
DEFINE_int32(num_threads, 32, "Number of concurrent threads");
DEFINE_int32(num_keys, 10, "Number of object keys to write/read");
DEFINE_string(key_prefix, "dx_micro_bench/test_multi/key_", "Prefix for object keys");
DEFINE_int64(write_batch_size, 1024*1024, "Size of data to write in each write operation");
DEFINE_int64(read_offset, 0, "Start offset for range read");
DEFINE_int64(read_length, -1, "Length to read for range read, -1 means read to end");

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
       << "--key_prefix=dx_micro_bench/test_multi/key_ --write_batch_size=2097152 --read_offset=1048576 --read_length=2097152\n";
    return ss.str();
}

int main(int argc, char* argv[]) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    // 打印所有配置参数
    std::cout << "\n配置参数:" << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "数据大小 (data_size_bytes): " << FLAGS_data_size_bytes << " bytes" 
              << " (" << FLAGS_data_size_bytes/1024.0/1024.0 << " MB)" << std::endl;
    std::cout << "写入IOPS限制 (obj_key_write_iops): " << FLAGS_obj_key_write_iops << std::endl;
    std::cout << "读取IOPS限制 (obj_key_read_iops): " << FLAGS_obj_key_read_iops << std::endl;
    std::cout << "线程数 (num_threads): " << FLAGS_num_threads << std::endl;
    std::cout << "对象数量 (num_keys): " << FLAGS_num_keys << std::endl;
    std::cout << "对象前缀 (key_prefix): " << FLAGS_key_prefix << std::endl;
    std::cout << "写入批次大小 (write_batch_size): " << FLAGS_write_batch_size << " bytes"
              << " (" << FLAGS_write_batch_size/1024.0/1024.0 << " MB)" << std::endl;
    std::cout << "读取偏移量 (read_offset): " << FLAGS_read_offset << " bytes"
              << " (" << FLAGS_read_offset/1024.0/1024.0 << " MB)" << std::endl;
    std::cout << "读取长度 (read_length): " << (FLAGS_read_length == -1 ? "到文件末尾" : 
              std::to_string(FLAGS_read_length) + " bytes (" + 
              std::to_string(FLAGS_read_length/1024.0/1024.0) + " MB)") << std::endl;
    std::cout << "----------------------------------------\n" << std::endl;

    brpc::StartDummyServerAt(32888/*port*/);
    const char* doris_home = getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        std::cout << "DORIS_HOME 环境变量未设置" << std::endl;
        doris_home = ".";
    }
    std::cout << "env=" << doris_home << std::endl;
    std::string conffile = std::string(doris_home) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        fprintf(stderr, "读取配置文件错误\n");
        return -1;
    }

    std::string bucket = "gavin-test-hk-1308700295";
    std::string key_prefix = FLAGS_key_prefix;
    size_t data_size = FLAGS_data_size_bytes;
    int write_iops = FLAGS_obj_key_write_iops;
    int read_iops = FLAGS_obj_key_read_iops;
    int num_threads = FLAGS_num_threads;
    int num_keys = FLAGS_num_keys;

    // 生成多个key
    std::vector<std::string> keys;
    keys.reserve(num_keys);
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back(key_prefix + std::to_string(i));
    }

    // 创建速率限制器和统计器
    std::vector<std::shared_ptr<IopsRateLimiter>> write_limiters;
    std::vector<std::shared_ptr<IopsRateLimiter>> read_limiters;
    write_limiters.reserve(num_keys);
    read_limiters.reserve(num_keys);
    for (int i = 0; i < num_keys; ++i) {
        write_limiters.push_back(std::make_shared<IopsRateLimiter>(write_iops));
        read_limiters.push_back(std::make_shared<IopsRateLimiter>(read_iops));
    }
    auto write_stats = std::make_shared<IopsStats>();
    auto read_stats = std::make_shared<IopsStats>();

    // 创建线程池
    ThreadPool pool(num_threads);

    // 生成测试数据
    std::string data;
    data.reserve(data_size);
    data.assign(data_size, 'x');  // 使用字符'x'填充整个字符串
    
    std::cout << "生成测试数据大小: " << data.size() << " 字节" << std::endl;
    std::cout << "每个key的写入IOPS限制: " << write_iops << std::endl;
    std::cout << "每个key的读取IOPS限制: " << read_iops << std::endl;
    std::cout << "总写入IOPS限制: " << write_iops * num_keys << std::endl;
    std::cout << "总读取IOPS限制: " << read_iops * num_keys << std::endl;
    std::cout << "线程数: " << num_threads << std::endl;
    std::cout << "Obj Key数量: " << num_keys << std::endl;
    std::cout << "Obj Key前缀: " << key_prefix << std::endl;

    // 初始化S3客户端
    doris::S3ClientConf s3_conf;
    s3_conf.max_connections = std::max(256, num_threads * 4);  // 增加连接数
    s3_conf.request_timeout_ms = 60000;  // 增加超时时间
    s3_conf.connect_timeout_ms = 3000;

    auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
    doris::Status init_status = client->init();
    if (!init_status.ok()) {
        std::cerr << "初始化ObjClientHolder错误: " << init_status.to_string() << std::endl;
        return -1;
    }

    doris::ExecEnv::GetInstance()->init_file_cache_microbench_env();

    auto completion_tracker = std::make_shared<FileCompletionTracker>();
    std::atomic<bool> should_stop_write_display(false);
    std::atomic<bool> should_stop_read_display(false);
    std::atomic<int> completed_writes(0);  // 添加写完成计数器
    std::atomic<int> completed_reads(0);   // 添加读完成计数器
    
    std::thread write_display_thread(display_iops_stats, "写入", write_stats, 
                                   std::ref(should_stop_write_display),
                                   std::ref(completed_writes), num_keys);
    std::thread read_display_thread(display_iops_stats, "读取", read_stats, 
                                  std::ref(should_stop_read_display),
                                  std::ref(completed_reads), num_keys);

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::future<void>> futures;

    // 启动写入任务
    for (int i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        futures.push_back(pool.enqueue([&, key, i]() -> void {
            doris::io::FileWriterOptions options;
            options.write_file_cache = true;
            auto writer = std::make_unique<IopsControlledS3FileWriter>(
                    client, bucket, key, &options, write_limiters[i], write_stats);

            const size_t block_size = 1 * 1024 * 1024;
            size_t num_blocks = (data_size + block_size - 1) / block_size;

            // 写入数据块
            std::vector<doris::Slice> slices;
            slices.reserve(4);
            size_t accumulated_size = 0;
            size_t write_batch_size = FLAGS_write_batch_size;

            for (size_t j = 0; j < num_blocks; ++j) {
                size_t offset = j * block_size;
                size_t current_block_size = std::min(block_size, data_size - offset);
                slices.emplace_back(data.data() + offset, current_block_size);
                accumulated_size += current_block_size;

                if (accumulated_size >= write_batch_size || j == num_blocks - 1) {
                    doris::Status status = writer->appendv(slices.data(), slices.size());
                    if (!status.ok()) {
                        std::cerr << "写入S3错误 (key=" << key << "): " << status.to_string() << std::endl;
                        return;
                    }
                    slices.clear();
                    accumulated_size = 0;
                }
            }

            doris::Status status = writer->close();
            if (!status.ok()) {
                std::cerr << "关闭S3FileWriter错误 (key=" << key << "): " << status.to_string() << std::endl;
                return;
            }

            // 标记文件写入完成
            completion_tracker->mark_completed(key);
            completed_writes++;  // 增加完成计数
        }));
    }

    // 启动读取任务
    doris::io::IOContext io_ctx;
    doris::io::FileCacheStatistics total_stats;
    io_ctx.file_cache_stats = &total_stats;

    for (int i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        futures.push_back(pool.enqueue([&, key, i]() -> void {
            // 等待文件写入完成，最多重试30次，每次等待1秒
            int retry_count = 0;
            const int max_retries = 300;
            while (!completion_tracker->is_completed(key)) {
                if (retry_count++ >= max_retries) {
                    std::cerr << "等待文件写入超时 (key=" << key << ")" << std::endl;
                    return;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            // 添加日志
            std::cout << "开始读取文件: " << key << std::endl;

            doris::io::FileReaderOptions reader_opts;
            reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
            reader_opts.is_doris_table = true;

            doris::io::FileDescription fd;
            fd.path = doris::io::Path("s3://" + bucket + "/" + key);
            doris::io::FileSystemProperties fs_props;
            fs_props.system_type = doris::TFileType::FILE_S3;

            std::map<std::string, std::string> props;
            props["AWS_ACCESS_KEY"] = s3_conf.ak;
            props["AWS_SECRET_KEY"] = s3_conf.sk;
            props["AWS_ENDPOINT"] = s3_conf.endpoint;
            props["AWS_REGION"] = s3_conf.region;
            if (!s3_conf.token.empty()) {
                props["AWS_TOKEN"] = s3_conf.token;
            }
            props["AWS_MAX_CONNECTIONS"] = std::to_string(s3_conf.max_connections);
            props["AWS_REQUEST_TIMEOUT_MS"] = std::to_string(s3_conf.request_timeout_ms);
            props["AWS_CONNECT_TIMEOUT_MS"] = std::to_string(s3_conf.connect_timeout_ms);
            props["use_path_style"] = s3_conf.use_virtual_addressing ? "false" : "true";

            fs_props.properties = std::move(props);

            // 重试读取文件，最多重试5次
            int read_retry_count = 0;
            const int max_read_retries = 50;
            while (read_retry_count < max_read_retries) {
                auto status_or_reader = doris::FileFactory::create_file_reader(fs_props, fd, reader_opts, nullptr);
                if (!status_or_reader.has_value()) {
                    if (++read_retry_count >= max_read_retries) {
                        std::cerr << "创建文件读取器失败 (key=" << key << "): " << status_or_reader.error() << std::endl;
                        return;
                    }
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                auto reader = std::make_unique<IopsControlledFileReader>(
                        status_or_reader.value(), read_limiters[i], read_stats);

                // 计算实际读取范围
                size_t read_offset = FLAGS_read_offset;
                size_t read_length = FLAGS_read_length;
                if (read_length == -1 || read_offset + read_length > data_size) {
                    read_length = data_size - read_offset;
                }

                // 分配只读取指定range的buffer
                std::string read_buffer;
                read_buffer.resize(read_length);

                const size_t block_size = 512 * 1024;
                size_t num_blocks = (read_length + block_size - 1) / block_size;

                for (size_t j = 0; j < num_blocks; ++j) {
                    size_t block_offset = j * block_size;
                    size_t current_block_size = std::min(block_size, read_length - block_offset);
                    doris::Slice read_slice(read_buffer.data() + block_offset, current_block_size);
                    size_t bytes_read = 0;
                    
                    doris::Status read_status = reader->read_at(read_offset + block_offset, read_slice, &bytes_read, &io_ctx);
                    if (!read_status.ok()) {
                        std::cerr << "读取S3错误 (key=" << key << "): " << read_status.to_string() << std::endl;
                        return;
                    }

                    if (bytes_read != current_block_size) {
                        std::cerr << "读取大小不匹配 (key=" << key << "): 期望 " << current_block_size 
                                 << ", 实际 " << bytes_read << std::endl;
                        return;
                    }
                }

                // 验证数据：只比较指定range的数据
                if (memcmp(read_buffer.data(), data.data() + read_offset, read_length) != 0) {
                    std::cerr << "数据验证失败! (key=" << key << ", range: offset=" << read_offset 
                             << ", length=" << read_length << ")" << std::endl;
                    
                    // 输出前100个字节的比较结果以帮助调试
                    for (size_t i = 0; i < std::min(read_length, size_t(100)); ++i) {
                        if (read_buffer[i] != data[read_offset + i]) {
                            std::cerr << "First mismatch at offset " << i << ": expected " 
                                     << (int)data[read_offset + i] << ", got " << (int)read_buffer[i] << std::endl;
                            break;
                        }
                    }
                    return;
                }
                break;
            }

            // 添加完成日志
            std::cout << "完成读取文件: " << key << std::endl;
            completed_reads++;  // 增加完成计数
        }));
    }

    // 等待所有操作完成
    for (auto& future : futures) {
        future.get();
    }

    // 停止监控显示
    should_stop_write_display = true;
    should_stop_read_display = true;
    write_display_thread.join();
    read_display_thread.join();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n所有操作完成，总耗时: " << duration.count() << "ms" << std::endl;
    // const size_t block_size = 10 * 1024 * 1024;
    // double total_blocks = (double)(data_size + block_size - 1) / block_size * num_keys;
    // std::cout << "平均IOPS: " << std::fixed << std::setprecision(2)
    //          << total_blocks / (duration.count() / 1000.0) << std::endl;
    std::cout << "缓存统计:" << std::endl;
    std::cout << "- 缓存命中: " << total_stats.num_local_io_total << std::endl;
    std::cout << "- 缓存未命中: " << total_stats.num_remote_io_total << std::endl;
    std::cout << "- 从缓存读取字节数: " << total_stats.bytes_read_from_local << std::endl;
    std::cout << "- 从远程读取字节数: " << total_stats.bytes_read_from_remote << std::endl;

    while (1) {
        sleep(3);
    }
    return 0;
}