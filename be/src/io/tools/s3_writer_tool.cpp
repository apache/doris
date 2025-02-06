#include <iostream>
#include <memory>
#include <string>
#include "io/fs/s3_file_writer.h"
#include "io/fs/s3_file_system.h"  // 添加此行以包含 S3FileSystem
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "common/status.h"
#include "runtime/exec_env.h"

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: s3_writer_tool <bucket> <key> <data_size_bytes>" << std::endl;
        return 1;
    }
    const char* doris_home = getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        std::cout << "DORIS_HOME environment variable is not set" << std::endl;
        doris_home = ".";
    }
    std::cout << "env=" << doris_home << std::endl;
    std::string conffile = std::string(doris_home) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    std::string bucket = "gavin-test-hk-1308700295";
    // prefix, in key
    std::string key = "/dx_micro_bench/test25/aaaaaa";
    size_t data_size = std::stoull(argv[3]);  // 数据大小（字节）

    // 生成随机数据
    std::string data;
    data.reserve(data_size);
    const char charset[] = "0123456789"
                          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                          "abcdefghijklmnopqrstuvwxyz";
    const size_t charset_size = sizeof(charset) - 1;

    srand(time(nullptr));  // 初始化随机数种子
    for (size_t i = 0; i < data_size; ++i) {
        data += charset[rand() % charset_size];
    }

    std::cout << "Generated random data of size: " << data.size() << " bytes" << std::endl;

    // 创建 S3ClientConf 对象（根据需要设置配置）
    doris::S3ClientConf s3_conf;
    // set s3_conf.max_connections, ignore scanner_scheduler nullptr, core dump
    s3_conf.max_connections = 256;
    // 设置 s3_conf 的属性，例如 ak、sk、region 等


    // 创建 ObjClientHolder 实例
    auto client = std::make_shared<doris::io::ObjClientHolder>(s3_conf);
    doris::Status init_status = client->init();
    if (!init_status.ok()) {
        std::cerr << "Error initializing ObjClientHolder: " << init_status.to_string() << std::endl;
        return 1;
    }

    doris::ExecEnv::GetInstance()->init_file_cache_microbench_env();

    // 写入数据到 S3
    {
        doris::io::FileWriterOptions options;
        auto writer = std::make_unique<doris::io::S3FileWriter>(client, bucket, key, &options);

        doris::Slice slice(data.data(), data.size());
        doris::Status status = writer->appendv(&slice, 1);

        if (!status.ok()) {
            std::cerr << "Error writing to S3: " << status.to_string() << std::endl;
            return 1;
        }

        status = writer->close();
        if (!status.ok()) {
            std::cerr << "Error closing S3FileWriter: " << status.to_string() << std::endl;
            return 1;
        }

        std::cout << "Data written to S3 successfully." << std::endl;
    }

    // 使用 CachedRemoteFileReader 读取数据进行验证
    {
        // 创建读取选项
        doris::io::FileReaderOptions reader_opts;
        reader_opts.cache_type = doris::io::FileCachePolicy::FILE_BLOCK_CACHE;
        reader_opts.is_doris_table = false;  // 非 Doris 表数据
        reader_opts.cache_base_path = "/mnt/disk2/dengxin/file_cache";

        // 创建基础的 S3FileReader
        doris::io::FileDescription fd;
        fd.path = doris::io::Path("s3://" + bucket + key);
        doris::io::FileSystemProperties fs_props;
        fs_props.system_type = doris::TFileType::FILE_S3;
        
        // 手动构建 properties map
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

        auto s3_reader = DORIS_TRY(doris::FileFactory::create_file_reader(fs_props, fd, reader_opts, nullptr));
        if (!s3_reader) {
            std::cerr << "Failed to create S3FileReader" << std::endl;
            return 1;
        }

        // 创建 CachedRemoteFileReader
        auto cached_reader = std::make_shared<doris::io::CachedRemoteFileReader>(s3_reader, reader_opts);

        // 准备读取缓冲区
        std::string read_buffer;
        read_buffer.resize(data.size());
        doris::Slice read_slice(read_buffer.data(), read_buffer.size());
        
        // 设置 IO 上下文
        doris::io::IOContext io_ctx;
        doris::io::FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;

        // 读取数据
        size_t bytes_read = 0;
        doris::Status read_status = cached_reader->read_at(0, read_slice, &bytes_read, &io_ctx);
        
        if (!read_status.ok()) {
            std::cerr << "Error reading from S3: " << read_status.to_string() << std::endl;
            return 1;
        }

        // 验证数据
        if (bytes_read != data.size() || read_buffer != data) {
            std::cerr << "Data verification failed!" << std::endl;
            std::cerr << "Original size: " << data.size() << ", Read size: " << bytes_read << std::endl;
            return 1;
        }

        std::cout << "Data verification successful!" << std::endl;
        std::cout << "Cache statistics:" << std::endl;
        std::cout << "- Cache hits: " << stats.num_local_io_total << std::endl;
        std::cout << "- Cache misses: " << stats.num_remote_io_total << std::endl;
        std::cout << "- Bytes from cache: " << stats.bytes_read_from_local << std::endl;
        std::cout << "- Bytes from remote: " << stats.bytes_read_from_remote << std::endl;
    }

    return 0;
}