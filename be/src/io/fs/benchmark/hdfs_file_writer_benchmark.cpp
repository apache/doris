#include <bthread/countdown_event.h>
#include <cpp/sync_point.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gflags/gflags.h>

#include <thread>

#include "common/logging.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "runtime/exec_env.h"
#include "util/jvm_metrics.h"
#include "util/threadpool.h"

using namespace doris;

DEFINE_string(fs_name, "", "hdfs fs name");
DEFINE_string(path_prefix, "/hdfs_file_writer_bench", "hdfs path prefix");
DEFINE_int32(num_files, 32, "number of files to write");
DEFINE_uint64(file_size, 10 * 1024 * 1024 /* 10MB */, "size of each file");
DEFINE_int32(parallel, 16, "number of parallel writers");
DEFINE_int32(flush_delay, 0, "delay in ms when flushing data to HDFS");

namespace doris::io {
extern bvar::Adder<uint64_t> hdfs_write_acquire_memory_failed;
extern bvar::Adder<uint64_t> hdfs_write_acquire_memory_reach_max_retry;
} // namespace doris::io

void run_benchmark(io::HdfsFileSystem& fs, int num_files, size_t file_size, int parallel,
                   int flush_delay_ms) {
    std::cout << "Running benchmark with " << num_files << " files, each " << file_size
              << " bytes, " << parallel << " parallel writers, flush delay " << flush_delay_ms
              << std::endl;

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "HdfsFileWriter::hdfsCloseFile",
            [flush_delay_ms](auto&&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(flush_delay_ms));
            },
            &guards.emplace_back());
    sp->set_call_back(
            "HdfsFileWriter::hdfsHFlush",
            [flush_delay_ms](auto&&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(flush_delay_ms));
            },
            &guards.emplace_back());
    sp->enable_processing();

    std::atomic_bool success {true};
    std::string data(512 * 1024, 'a'); // 512KB

    std::unique_ptr<ThreadPool> workers;
    auto st = ThreadPoolBuilder("MemTableFlushThreadPool")
                      .set_min_threads(parallel)
                      .set_max_threads(parallel)
                      .build(&workers);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create thread pool: " << st;
        return;
    }

    bthread::CountdownEvent count_down(num_files);

    std::vector<std::unique_ptr<io::FileWriter>> writers;
    writers.resize(num_files);

    for (int i = 0; i < num_files; ++i) {
        st = workers->submit_func([&, i] {
            Defer defer([&] { count_down.signal(); });

            auto& writer = writers[i];
            auto st = fs.create_file(std::to_string(i), &writer);
            if (!st.ok()) {
                success = false;
                LOG(WARNING) << "Failed to create file: " << i;
                return;
            }

            while (writer->bytes_appended() < file_size) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                st = writer->append({data.data(), data.size()});
                if (!st.ok()) {
                    success = false;
                    LOG(WARNING) << "Failed to append data: " << st;
                    return;
                }
            }

            st = writer->close(true);
            if (!st.ok()) {
                success = false;
                LOG(WARNING) << "Failed to close file: " << st;
                return;
            }
        });

        if (!st.ok()) {
            success = false;
            LOG(WARNING) << "Failed to submit task: " << st << ", i=" << i;
            return;
        }
    }

    count_down.wait();

    LOG(INFO) << "All files written, success=" << success << ", hdfs_write_acquire_memory_failed="
              << io::hdfs_write_acquire_memory_failed.get_value()
              << ", hdfs_write_acquire_memory_reach_max_retry="
              << io::hdfs_write_acquire_memory_reach_max_retry.get_value();
}

int main(int argc, char** argv) {
    // init config.
    // the config in be_custom.conf will overwrite the config in be.conf
    // Must init custom config after init config, separately.
    // Because the path of custom config file is defined in be.conf
    auto conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!config::init(conffile.c_str(), true, true, true)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    // ATTN: Callers that want to override default gflags variables should do so before calling this method
    google::ParseCommandLineFlags(&argc, &argv, true);
    // ATTN: MUST init before LOG
    init_glog("hdfs_file_writer_benchmark");

    auto fs = io::HdfsFileSystem::create(
            {
                    {"hdfs_name", FLAGS_fs_name},
            },
            FLAGS_fs_name, io::FileSystem::TMP_FS_ID, nullptr, FLAGS_path_prefix);

    if (!fs) {
        std::cerr << "Failed to create HDFS file system: " << fs.error() << std::endl;
        return -1;
    }

    run_benchmark(**fs, FLAGS_num_files, FLAGS_file_size, FLAGS_parallel, FLAGS_flush_delay);
    std::cout << "Benchmark finished" << std::endl;

    auto st = fs.value()->delete_directory("");
    if (!st.ok()) {
        std::cerr << "Failed to delete directory: " << st << std::endl;
        return -1;
    }

    return 0;
}
