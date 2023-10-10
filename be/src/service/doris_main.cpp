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

#include <butil/macros.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#include <libgen.h>
#include <setjmp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cstring>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/jni-util.h"

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include <curl/curl.h>
#include <thrift/TOutput.h>

#include "agent/heartbeat_server.h"
#include "common/config.h"
#include "common/daemon.h"
#include "common/logging.h"
#include "common/phdr_cache.h"
#include "common/resource_tls.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "io/fs/s3_file_write_bufferpool.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/brpc_service.h"
#include "service/http_service.h"
#include "util/debug_util.h"
#include "util/telemetry/telemetry.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"

namespace doris {
class TMasterInfo;
} // namespace doris

static void help(const char*);

extern "C" {
void __lsan_do_leak_check();
}

namespace doris {
extern bool k_doris_exit;

static void thrift_output(const char* x) {
    LOG(WARNING) << "thrift internal message: " << x;
}

} // namespace doris

// These code is referenced from clickhouse
// It is used to check the SIMD instructions
enum class InstructionFail {
    NONE = 0,
    SSE3 = 1,
    SSSE3 = 2,
    SSE4_1 = 3,
    SSE4_2 = 4,
    POPCNT = 5,
    AVX = 6,
    AVX2 = 7,
    AVX512 = 8,
    ARM_NEON = 9
};

auto instruction_fail_to_string(InstructionFail fail) {
    switch (fail) {
#define ret(x) return std::make_tuple(STDERR_FILENO, x, ARRAY_SIZE(x) - 1)
    case InstructionFail::NONE:
        ret("NONE");
    case InstructionFail::SSE3:
        ret("SSE3");
    case InstructionFail::SSSE3:
        ret("SSSE3");
    case InstructionFail::SSE4_1:
        ret("SSE4.1");
    case InstructionFail::SSE4_2:
        ret("SSE4.2");
    case InstructionFail::POPCNT:
        ret("POPCNT");
    case InstructionFail::AVX:
        ret("AVX");
    case InstructionFail::AVX2:
        ret("AVX2");
    case InstructionFail::AVX512:
        ret("AVX512");
    case InstructionFail::ARM_NEON:
        ret("ARM_NEON");
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

sigjmp_buf jmpbuf;

void sig_ill_check_handler(int, siginfo_t*, void*) {
    siglongjmp(jmpbuf, 1);
}

/// Check if necessary SSE extensions are available by trying to execute some sse instructions.
/// If instruction is unavailable, SIGILL will be sent by kernel.
void check_required_instructions_impl(volatile InstructionFail& fail) {
#if defined(__SSE3__)
    fail = InstructionFail::SSE3;
    __asm__ volatile("addsubpd %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSSE3__)
    fail = InstructionFail::SSSE3;
    __asm__ volatile("pabsw %%xmm0, %%xmm0" : : : "xmm0");

#endif

#if defined(__SSE4_1__)
    fail = InstructionFail::SSE4_1;
    __asm__ volatile("pmaxud %%xmm0, %%xmm0" : : : "xmm0");
#endif

#if defined(__SSE4_2__)
    fail = InstructionFail::SSE4_2;
    __asm__ volatile("pcmpgtq %%xmm0, %%xmm0" : : : "xmm0");
#endif

    /// Defined by -msse4.2
#if defined(__POPCNT__)
    fail = InstructionFail::POPCNT;
    {
        uint64_t a = 0;
        uint64_t b = 0;
        __asm__ volatile("popcnt %1, %0" : "=r"(a) : "r"(b) :);
    }
#endif

#if defined(__AVX__)
    fail = InstructionFail::AVX;
    __asm__ volatile("vaddpd %%ymm0, %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX2__)
    fail = InstructionFail::AVX2;
    __asm__ volatile("vpabsw %%ymm0, %%ymm0" : : : "ymm0");
#endif

#if defined(__AVX512__)
    fail = InstructionFail::AVX512;
    __asm__ volatile("vpabsw %%zmm0, %%zmm0" : : : "zmm0");
#endif

#if defined(__ARM_NEON__)
    fail = InstructionFail::ARM_NEON;
#ifndef __APPLE__
    __asm__ volatile("vadd.i32  q8, q8, q8" : : : "q8");
#endif
#endif

    fail = InstructionFail::NONE;
}

bool write_retry(int fd, const char* data, size_t size) {
    if (!size) size = strlen(data);

    while (size != 0) {
        ssize_t res = ::write(fd, data, size);

        if ((-1 == res || 0 == res) && errno != EINTR) return false;

        if (res > 0) {
            data += res;
            size -= res;
        }
    }

    return true;
}

/// Macros to avoid using strlen(), since it may fail if SSE is not supported.
#define WRITE_ERROR(data)                                                      \
    do {                                                                       \
        static_assert(__builtin_constant_p(data));                             \
        if (!write_retry(STDERR_FILENO, data, ARRAY_SIZE(data) - 1)) _Exit(1); \
    } while (false)

/// Check SSE and others instructions availability. Calls exit on fail.
/// This function must be called as early as possible, even before main, because static initializers may use unavailable instructions.
void check_required_instructions() {
    struct sigaction sa {};
    struct sigaction sa_old {};
    sa.sa_sigaction = sig_ill_check_handler;
    sa.sa_flags = SA_SIGINFO;
    auto signal = SIGILL;
    if (sigemptyset(&sa.sa_mask) != 0 || sigaddset(&sa.sa_mask, signal) != 0 ||
        sigaction(signal, &sa, &sa_old) != 0) {
        /// You may wonder about strlen.
        /// Typical implementation of strlen is using SSE4.2 or AVX2.
        /// But this is not the case because it's compiler builtin and is executed at compile time.

        WRITE_ERROR("Can not set signal handler\n");
        _Exit(1);
    }

    volatile InstructionFail fail = InstructionFail::NONE;

    if (sigsetjmp(jmpbuf, 1)) {
        WRITE_ERROR("Instruction check fail. The CPU does not support ");
        if (!std::apply(write_retry, instruction_fail_to_string(fail))) _Exit(1);
        WRITE_ERROR(" instruction set.\n");
        WRITE_ERROR(
                "For example, if your CPU does not support AVX2, you need to rebuild the Doris BE "
                "with: USE_AVX2=0 sh build.sh --be");
        _Exit(1);
    }

    check_required_instructions_impl(fail);

    if (sigaction(signal, &sa_old, nullptr)) {
        WRITE_ERROR("Can not set signal handler\n");
        _Exit(1);
    }
}

struct Checker {
    Checker() { check_required_instructions(); }
} checker
#ifndef __APPLE__
        __attribute__((init_priority(101))) /// Run before other static initializers.
#endif
        ;

int main(int argc, char** argv) {
    doris::signal::InstallFailureSignalHandler();

    // check if print version or help
    if (argc > 1) {
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-v") == 0) {
            puts(doris::get_build_version(false).c_str());
            exit(0);
        } else if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(basename(argv[0]));
            exit(0);
        }
    }

    if (getenv("DORIS_HOME") == nullptr) {
        fprintf(stderr, "you need set DORIS_HOME environment variable.\n");
        exit(-1);
    }
    if (getenv("PID_DIR") == nullptr) {
        fprintf(stderr, "you need set PID_DIR environment variable.\n");
        exit(-1);
    }

    using doris::Status;
    using std::string;

    // open pid file, obtain file lock and save pid
    string pid_file = string(getenv("PID_DIR")) + "/be.pid";
    int fd = open(pid_file.c_str(), O_RDWR | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    if (fd < 0) {
        fprintf(stderr, "fail to create pid file.");
        exit(-1);
    }

    string pid = std::to_string((long)getpid());
    pid += "\n";
    size_t length = write(fd, pid.c_str(), pid.size());
    if (length != pid.size()) {
        fprintf(stderr, "fail to save pid into pid file.");
        exit(-1);
    }

    // descriptor will be leaked when failing to close fd
    if (::close(fd) < 0) {
        fprintf(stderr, "failed to close fd of pidfile.");
        exit(-1);
    }

    // init config.
    // the config in be_custom.conf will overwrite the config in be.conf
    // Must init custom config after init config, separately.
    // Because the path of custom config file is defined in be.conf
    string conffile = string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), true, true, true)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    string custom_conffile = doris::config::custom_config_dir + "/be_custom.conf";
    if (!doris::config::init(custom_conffile.c_str(), true, false, false)) {
        fprintf(stderr, "error read custom config file. \n");
        return -1;
    }

    if (doris::config::enable_fuzzy_mode) {
        LOG(INFO) << "enable_fuzzy_mode is true, set fuzzy configs";
        doris::config::set_fuzzy_configs();
    }

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
    // Change the total TCMalloc thread cache size if necessary.
    const size_t kDefaultTotalThreadCacheBytes = 1024 * 1024 * 1024;
    if (!MallocExtension::instance()->SetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                                         kDefaultTotalThreadCacheBytes)) {
        fprintf(stderr, "Failed to change TCMalloc total thread cache size.\n");
        return -1;
    }
#endif

    std::vector<doris::StorePath> paths;
    auto olap_res = doris::parse_conf_store_paths(doris::config::storage_root_path, &paths);
    if (!olap_res) {
        LOG(FATAL) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }
    std::set<std::string> broken_paths;
    doris::parse_conf_broken_store_paths(doris::config::broken_storage_path, &broken_paths);

    auto it = paths.begin();
    for (; it != paths.end();) {
        if (broken_paths.count(it->path) > 0) {
            if (doris::config::ignore_broken_disk) {
                LOG(WARNING) << "ignore broken disk, path = " << it->path;
                it = paths.erase(it);
            } else {
                LOG(FATAL) << "a broken disk is found " << it->path;
                exit(-1);
            }
        } else if (!doris::check_datapath_rw(it->path)) {
            if (doris::config::ignore_broken_disk) {
                LOG(WARNING) << "read write test file failed, path=" << it->path;
                it = paths.erase(it);
            } else {
                LOG(FATAL) << "read write test file failed, path=" << it->path;
                exit(-1);
            }
        } else {
            ++it;
        }
    }

    if (paths.empty()) {
        LOG(FATAL) << "All disks are broken, exit.";
        exit(-1);
    }

    // initialize libcurl here to avoid concurrent initialization
    auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_ret != 0) {
        LOG(FATAL) << "fail to initialize libcurl, curl_ret=" << curl_ret;
        exit(-1);
    }
    // add logger for thrift internal
    apache::thrift::GlobalOutput.setOutputFunction(doris::thrift_output);

    Status status = Status::OK();
    if (doris::config::enable_java_support) {
        // Init jni
        status = doris::JniUtil::Init();
        if (!status.ok()) {
            LOG(WARNING) << "Failed to initialize JNI: " << status;
            exit(1);
        }
    }

    if (doris::config::enable_file_cache) {
        doris::io::IFileCache::init();
        std::unordered_set<std::string> cache_path_set;
        std::vector<doris::CachePath> cache_paths;
        olap_res = doris::parse_conf_cache_paths(doris::config::file_cache_path, cache_paths);
        if (!olap_res) {
            LOG(FATAL) << "parse config file cache path failed, path="
                       << doris::config::file_cache_path;
            exit(-1);
        }

        std::unique_ptr<doris::ThreadPool> file_cache_init_pool;
        doris::ThreadPoolBuilder("FileCacheInitThreadPool")
                .set_min_threads(cache_paths.size())
                .set_max_threads(cache_paths.size())
                .build(&file_cache_init_pool);

        std::list<doris::Status> cache_status;
        for (auto& cache_path : cache_paths) {
            if (cache_path_set.find(cache_path.path) != cache_path_set.end()) {
                LOG(WARNING) << fmt::format("cache path {} is duplicate", cache_path.path);
                continue;
            }

            RETURN_IF_ERROR(file_cache_init_pool->submit_func(
                    std::bind(&doris::io::FileCacheFactory::create_file_cache,
                              &(doris::io::FileCacheFactory::instance()), cache_path.path,
                              cache_path.init_settings(), &(cache_status.emplace_back()))));

            cache_path_set.emplace(cache_path.path);
        }

        file_cache_init_pool->wait();
        for (const auto& status : cache_status) {
            if (!status.ok()) {
                LOG(FATAL) << "failed to init file cache, err: " << status;
                exit(-1);
            }
        }
    }

    // PHDR speed up exception handling, but exceptions from dynamically loaded libraries (dlopen)
    // will work only after additional call of this function.
    // rewrites dl_iterate_phdr will cause Jemalloc to fail to run after enable profile. see #
    // updatePHDRCache();

    // Load file cache before starting up daemon threads to make sure StorageEngine is read.
    doris::Daemon daemon;
    daemon.init(argc, argv, paths);
    daemon.start();

    doris::ResourceTls::init();
    if (!doris::BackendOptions::init()) {
        exit(-1);
    }

    // init exec env
    auto exec_env = doris::ExecEnv::GetInstance();
    doris::ExecEnv::init(exec_env, paths);
    doris::TabletSchemaCache::create_global_schema_cache();

    // init s3 write buffer pool
    doris::io::S3FileBufferPool* s3_buffer_pool = doris::io::S3FileBufferPool::GetInstance();
    s3_buffer_pool->init(doris::config::s3_write_buffer_whole_size,
                         doris::config::s3_write_buffer_size,
                         exec_env->buffered_reader_prefetch_thread_pool());

    // init and open storage engine
    doris::EngineOptions options;
    options.store_paths = paths;
    options.broken_paths = broken_paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    doris::StorageEngine* engine = nullptr;
    auto st = doris::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st;
        exit(-1);
    }
    exec_env->set_storage_engine(engine);
    engine->set_heartbeat_flags(exec_env->heartbeat_flags());

    // start all background threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(engine->start_bg_threads());

    doris::telemetry::init_tracer();

    // begin to start services
    doris::ThriftRpcHelper::setup(exec_env);
    // 1. thrift server with be_port
    doris::ThriftServer* be_server = nullptr;
    EXIT_IF_ERROR(
            doris::BackendService::create_service(exec_env, doris::config::be_port, &be_server));
    status = be_server->start();
    if (!status.ok()) {
        LOG(FATAL) << fmt::format(
                "Doris be server did not start correctly, maybe be_port {} is conflect, "
                "aborting...",
                doris::config::be_port);
    }

    // 2. bprc service
    doris::BRpcService brpc_service(exec_env);
    status = brpc_service.start(doris::config::brpc_port, doris::config::brpc_num_threads);
    if (!status.ok()) {
        LOG(FATAL) << fmt::format(
                "Doris brpc service server did not start correctly, maybe brpc_port {} is "
                "conflect, aborting...",
                doris::config::brpc_port);
    }

    // 3. http service
    doris::HttpService http_service(exec_env, doris::config::webserver_port,
                                    doris::config::webserver_num_workers);
    status = http_service.start();
    if (!status.ok()) {
        LOG(FATAL) << fmt::format(
                "Doris http service server did not start correctly, maybe webserver_port {} is "
                "conflect, aborting...",
                doris::config::webserver_port);
    }

    // 4. heart beat server
    doris::TMasterInfo* master_info = exec_env->master_info();
    doris::ThriftServer* heartbeat_thrift_server;
    doris::Status heartbeat_status = doris::create_heartbeat_server(
            exec_env, doris::config::heartbeat_service_port, &heartbeat_thrift_server,
            doris::config::heartbeat_service_thread_count, master_info);

    if (!heartbeat_status.ok()) {
        LOG(FATAL) << fmt::format("Heartbeat services did not start correctly, {}, exiting",
                                  heartbeat_status.to_string());
    }

    status = heartbeat_thrift_server->start();
    if (!status.ok()) {
        LOG(FATAL) << fmt::format("Heartbeat server did not start correctly, {}, exiting",
                                  status.to_string());
    }

    while (!doris::k_doris_exit) {
#if defined(LEAK_SANITIZER)
        __lsan_do_leak_check();
#endif
        sleep(10);
    }

    doris::TabletSchemaCache::stop_and_join();
    http_service.stop();
    brpc_service.join();
    daemon.stop();
    heartbeat_thrift_server->stop();
    heartbeat_thrift_server->join();
    be_server->stop();
    be_server->join();
    engine->stop();

    delete be_server;
    be_server = nullptr;

    delete heartbeat_thrift_server;
    heartbeat_thrift_server = nullptr;

    doris::ExecEnv::destroy(exec_env);
    delete engine;
    engine = nullptr;

    return 0;
}

static void help(const char* progname) {
    printf("%s is the Doris backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}
