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

#include "common/status.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/exec_env.h"

// IOPS 统计器
class IopsStats {
public:
    IopsStats() : _start_time(std::chrono::steady_clock::now()), _op_count(0) {}

    void record_operation() {
        ++_op_count;
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - _start_time).count();
        if (duration >= 1) {
            // 每秒更新一次统计
            std::lock_guard<std::mutex> lock(_mutex);
            _current_iops = _op_count / duration;
            if (_current_iops > _peak_iops) {
                _peak_iops = _current_iops;
            }
        }
    }

    double get_current_iops() const {
        return _current_iops;
    }

    double get_peak_iops() const {
        return _peak_iops;
    }

    void reset() {
        _start_time = std::chrono::steady_clock::now();
        _op_count = 0;
        _current_iops = 0;
        _peak_iops = 0;
    }

private:
    std::chrono::steady_clock::time_point _start_time;
    std::atomic<uint64_t> _op_count;
    std::mutex _mutex;
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
        
        // 更新令牌桶
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _last_update).count();
        double new_tokens = (duration / 1e9) * _iops_limit;  // 计算这段时间内应该添加的令牌数
        _tokens = std::min(_iops_limit * 1.0, _tokens + new_tokens);  // 令牌数不超过最大IOPS
        _last_update = now;

        // 如果没有可用的令牌，等待
        while (_tokens < 1.0) {
            // 计算需要等待的时间
            double tokens_needed = 1.0 - _tokens;
            int64_t wait_ns = static_cast<int64_t>((tokens_needed / _iops_limit) * 1e9);
            
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::nanoseconds(wait_ns));
            lock.lock();

            // 重新计算可用令牌
            now = std::chrono::steady_clock::now();
            duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _last_update).count();
            new_tokens = (duration / 1e9) * _iops_limit;
            _tokens = std::min(_iops_limit * 1.0, _tokens + new_tokens);
            _last_update = now;
        }

        // 消耗一个令牌
        _tokens -= 1.0;
    }

    void set_iops_limit(int iops_limit) {
        std::lock_guard<std::mutex> lock(_mutex);
        _iops_limit = iops_limit;
    }

private:
    std::mutex _mutex;
    std::atomic<int> _iops_limit;
    double _tokens;  // 当前可用的令牌数
    std::chrono::steady_clock::time_point _last_update;  // 上次更新令牌的时间
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
                       std::atomic<bool>& should_stop) {
    while (!should_stop) {
        std::cout << "\r" << operation << " - 当前IOPS: " << std::fixed << std::setprecision(2)
                 << stats->get_current_iops() << ", 峰值IOPS: " << stats->get_peak_iops() << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << std::endl;
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

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "用法: s3_iops_writer_tool <bucket> <key> <data_size_bytes> <write_iops> <read_iops> <num_threads>" << std::endl;
        return 1;
    }

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
    std::string key = "dx_micro_bench/test28/bbbbb";
    size_t data_size = std::stoull(argv[1]);
    int write_iops = std::stoi(argv[2]);
    int read_iops = std::stoi(argv[3]);
    int num_threads = std::stoi(argv[4]);

    // 创建速率限制器和统计器
    auto write_limiter = std::make_shared<IopsRateLimiter>(write_iops);
    auto read_limiter = std::make_shared<IopsRateLimiter>(read_iops);
    auto write_stats = std::make_shared<IopsStats>();
    auto read_stats = std::make_shared<IopsStats>();

    // 创建线程池
    ThreadPool write_pool(num_threads);
    ThreadPool read_pool(num_threads);

    // 生成测试数据
    std::string data;
    data.reserve(data_size);
    data.assign(data_size, 'x');  // 使用字符'x'填充整个字符串
    
    std::cout << "生成测试数据大小: " << data.size() << " 字节" << std::endl;
    std::cout << "写入IOPS限制: " << write_iops << std::endl;
    std::cout << "读取IOPS限制: " << read_iops << std::endl;
    std::cout << "线程数: " << num_threads << std::endl;

    // 初始化S3客户端
    doris::S3ClientConf s3_conf;
    s3_conf.max_connections = std::max(256, num_threads * 2);  // 确保有足够的连接数


    auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
    doris::Status init_status = client->init();
    if (!init_status.ok()) {
        std::cerr << "初始化ObjClientHolder错误: " << init_status.to_string() << std::endl;
        return 1;
    }

    doris::ExecEnv::GetInstance()->init_file_cache_microbench_env();

    // 写入数据到S3（多线程分块写入）
    {
        doris::io::FileWriterOptions options;
        options.write_file_cache = true;
        auto writer = std::make_unique<IopsControlledS3FileWriter>(
                client, bucket, key, &options, write_limiter, write_stats);

        // 设置块大小为1MB
        const size_t block_size = 1 * 1024 * 1024;
        size_t num_blocks = (data_size + block_size - 1) / block_size;
        std::atomic<size_t> completed_blocks(0);
        std::mutex write_mutex;  // 用于同步写入操作

        std::atomic<bool> should_stop_display(false);
        std::thread display_thread(display_iops_stats, "写入", write_stats, std::ref(should_stop_display));

        auto start_time = std::chrono::steady_clock::now();
        std::vector<std::future<void>> futures;

        // 将数据分成多个块并分配给线程池
        for (size_t i = 0; i < num_blocks; ++i) {
            size_t offset = i * block_size;
            size_t current_block_size = std::min(block_size, data_size - offset);

            futures.push_back(write_pool.enqueue([&, offset, current_block_size]() {
                doris::Slice slice(data.data() + offset, current_block_size);
                
                std::unique_lock<std::mutex> lock(write_mutex);
                doris::Status status = writer->appendv(&slice, 1);
                lock.unlock();

                if (!status.ok()) {
                    std::cerr << "写入S3错误: " << status.to_string() << std::endl;
                    should_stop_display = true;
                    return;
                }
                ++completed_blocks;
            }));
        }

        // 等待所有写入操作完成
        for (auto& future : futures) {
            future.get();
        }

        doris::Status status = writer->close();
        if (!status.ok()) {
            std::cerr << "关闭S3FileWriter错误: " << status.to_string() << std::endl;
            should_stop_display = true;
            display_thread.join();
            return 1;
        }

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        should_stop_display = true;
        display_thread.join();
        
        std::cout << "\n写入完成，耗时: " << duration.count() << "ms" << std::endl;
        std::cout << "平均写入IOPS: " << std::fixed << std::setprecision(2)
                 << (double)num_blocks / (duration.count() / 1000.0) << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 读取并验证数据（多线程分块读取）
    {
        doris::io::FileReaderOptions reader_opts;
        reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
        reader_opts.is_doris_table = true;

        doris::io::FileDescription fd;
        std::cout << "读取路径 = s3://" + bucket + "/" + key << std::endl;
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
        if (s3_conf.max_connections > 0) {
            props["AWS_MAX_CONNECTIONS"] = std::to_string(s3_conf.max_connections);
        }
        if (s3_conf.request_timeout_ms > 0) {
            props["AWS_REQUEST_TIMEOUT_MS"] = std::to_string(s3_conf.request_timeout_ms);
        }
        if (s3_conf.connect_timeout_ms > 0) {
            props["AWS_CONNECT_TIMEOUT_MS"] = std::to_string(s3_conf.connect_timeout_ms);
        }
        props["use_path_style"] = s3_conf.use_virtual_addressing ? "false" : "true";

        fs_props.properties = std::move(props);

        auto status_or_reader = DORIS_TRY(
                doris::FileFactory::create_file_reader(fs_props, fd, reader_opts, nullptr));
        auto reader = std::make_unique<IopsControlledFileReader>(
                status_or_reader, read_limiter, read_stats);

        // 设置读取块大小为1MB
        const size_t block_size = 1 * 1024 * 1024;
        size_t num_blocks = (data_size + block_size - 1) / block_size;
        std::string read_buffer;
        read_buffer.resize(data_size);
        std::atomic<size_t> completed_blocks(0);

        doris::io::IOContext io_ctx;
        doris::io::FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;

        std::atomic<bool> should_stop_display(false);
        std::thread display_thread(display_iops_stats, "读取", read_stats, std::ref(should_stop_display));

        auto start_time = std::chrono::steady_clock::now();
        std::vector<std::future<void>> futures;

        // 将读取操作分配给线程池
        for (size_t i = 0; i < num_blocks; ++i) {
            size_t offset = i * block_size;
            size_t current_block_size = std::min(block_size, data_size - offset);

            futures.push_back(read_pool.enqueue([&, offset, current_block_size]() {
                doris::Slice read_slice(read_buffer.data() + offset, current_block_size);
                size_t bytes_read = 0;
                
                doris::Status read_status = reader->read_at(offset, read_slice, &bytes_read, &io_ctx);

                if (!read_status.ok()) {
                    std::cerr << "读取S3错误: " << read_status.to_string() << std::endl;
                    should_stop_display = true;
                    return;
                }

                if (bytes_read != current_block_size) {
                    std::cerr << "读取大小不匹配: 期望 " << current_block_size 
                             << ", 实际 " << bytes_read << std::endl;
                    should_stop_display = true;
                    return;
                }

                ++completed_blocks;
            }));
        }

        // 等待所有读取操作完成
        for (auto& future : futures) {
            future.get();
        }

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        should_stop_display = true;
        display_thread.join();

        if (completed_blocks != num_blocks || read_buffer != data) {
            std::cerr << "数据验证失败!" << std::endl;
            std::cerr << "完成的块数: " << completed_blocks << ", 总块数: " << num_blocks << std::endl;
            return 1;
        }

        std::cout << "\n读取完成，耗时: " << duration.count() << "ms" << std::endl;
        std::cout << "平均读取IOPS: " << std::fixed << std::setprecision(2)
                 << (double)num_blocks / (duration.count() / 1000.0) << std::endl;
        std::cout << "缓存统计:" << std::endl;
        std::cout << "- 缓存命中: " << stats.num_local_io_total << std::endl;
        std::cout << "- 缓存未命中: " << stats.num_remote_io_total << std::endl;
        std::cout << "- 从缓存读取字节数: " << stats.bytes_read_from_local << std::endl;
        std::cout << "- 从远程读取字节数: " << stats.bytes_read_from_remote << std::endl;
    }

    return 0;
}