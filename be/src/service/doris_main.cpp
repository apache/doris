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

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <butil/macros.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <fmt/core.h>
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#endif
#include <libgen.h>
#include <setjmp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "cloud/cloud_backend_service.h"
#include "cloud/config.h"
#include "common/phdr_cache.h"
#include "common/stack_trace.h"
#if defined(__ELF__) && !defined(__FreeBSD__)
#include "common/symbol_index.h"
#endif
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/tablet/tablet_schema_cache.h"
#include "storage/utils.h"
#include "util/concurrency_stats.h"
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
#include "common/signal_handler.h"
#include "common/status.h"
#include "io/cache/block_file_cache_factory.h"
#include "runtime/exec_env.h"
#include "runtime/user_function_cache.h"
#include "service/arrow_flight/flight_sql_service.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/http_service.h"
#include "service/server/be_server_starter_factory.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "udf/python/python_env.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"

namespace doris {} // namespace doris

static void help(const char*);

extern "C" {
void __lsan_do_leak_check();
int __llvm_profile_write_file();
}

namespace doris {

void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        k_doris_exit = true;
    }
}

int install_signal(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        char buf[64];
        LOG(ERROR) << "install signal failed, signo=" << signo << ", errno=" << errno
                   << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
}

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

    LOG(ERROR) << "Unrecognized instruction fail value." << std::endl;
    exit(-1);
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

// A startup failure that happens after ExecEnv::init() has run must terminate the
// process the same way normal shutdown does (see the _exit(0) at the end of main):
// in the default mode we _exit() immediately, skipping global destructors and the
// LeakSanitizer atexit check. Init-time singletons (e.g. the internal workload
// group's task scheduler) intentionally live for the whole process lifetime, so
// running the leak check on this abnormal-exit path reports them as false-positive
// leaks. enable_graceful_exit_check is honored so memleak-check mode still runs LSAN.
[[noreturn]] static void exit_on_startup_failure() {
    google::FlushLogFiles(google::GLOG_INFO);
    if (!doris::config::enable_graceful_exit_check) {
        _exit(1);
    }
    exit(1);
}

int main(int argc, char** argv) {
    doris::signal::InstallFailureSignalHandler();
    // create StackTraceCache Instance, at the beginning, other static destructors may use.
    StackTrace::createCache();
    // extern doris::ErrorCode::ErrorCodeInitializer error_code_init;
    // Some developers will modify status.h and we use a very ticky logic to init error_states
    // and it maybe not inited. So add a check here.
    doris::ErrorCode::error_code_init.check_init();
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

    SCOPED_INIT_THREAD_CONTEXT();

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

    // ATTN: Callers that want to override default gflags variables should do so before calling this method
    google::ParseCommandLineFlags(&argc, &argv, true);
    // ATTN: MUST init before LOG
    doris::init_glog("be");

    LOG(INFO) << doris::get_version_string(false);

    doris::init_thrift_logging();

    if (doris::config::enable_fuzzy_mode) {
        Status status = doris::config::set_fuzzy_configs();
        if (!status.ok()) {
            LOG(WARNING) << "Failed to initialize fuzzy config: " << status;
            exit(1);
        }
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
        LOG(ERROR) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }

    std::vector<doris::StorePath> spill_paths;
    if (doris::config::spill_storage_root_path.empty()) {
        doris::config::spill_storage_root_path = doris::config::storage_root_path;
    }
    olap_res = doris::parse_conf_store_paths(doris::config::spill_storage_root_path, &spill_paths);
    if (!olap_res) {
        LOG(ERROR) << "parse config spill storage path failed, path="
                   << doris::config::spill_storage_root_path;
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

    // initialize libcurl here to avoid concurrent initialization
    auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_ret != 0) {
        LOG(ERROR) << "fail to initialize libcurl, curl_ret=" << curl_ret;
        exit(-1);
    }
    // add logger for thrift internal
    apache::thrift::GlobalOutput.setOutputFunction(doris::thrift_output);

    Status status = Status::OK();
    if (doris::config::enable_java_support) {
        // Init jni
        status = doris::Jni::Util::Init();
        if (!status.ok()) {
            LOG(WARNING) << "Failed to initialize JNI: " << status;
            exit(1);
        } else {
            LOG(INFO) << "Doris backend JNI is initialized.";
        }
    }

    if (doris::config::enable_python_udf_support) {
        if (std::string python_udf_root_path =
                    fmt::format("{}/lib/udf/python", std::getenv("DORIS_HOME"));
            !std::filesystem::exists(python_udf_root_path)) {
            std::filesystem::create_directories(python_udf_root_path);
        }

        // Normalize and trim all Python-related config parameters
        std::string python_env_mode =
                std::string(doris::trim(doris::to_lower(doris::config::python_env_mode)));
        std::string python_conda_root_path =
                std::string(doris::trim(doris::config::python_conda_root_path));
        std::string python_venv_root_path =
                std::string(doris::trim(doris::config::python_venv_root_path));
        std::string python_venv_interpreter_paths =
                std::string(doris::trim(doris::config::python_venv_interpreter_paths));

        if (python_env_mode == "conda") {
            if (python_conda_root_path.empty()) {
                LOG(ERROR)
                        << "Python conda root path is empty, please set `python_conda_root_path` "
                           "or set `enable_python_udf_support` to `false`";
                exit(1);
            }
            LOG(INFO) << "Doris backend python version manager is initialized. Python conda "
                         "root path: "
                      << python_conda_root_path;
            status = doris::PythonVersionManager::instance().init(doris::PythonEnvType::CONDA,
                                                                  python_conda_root_path, "");
        } else if (python_env_mode == "venv") {
            if (python_venv_root_path.empty()) {
                LOG(ERROR)
                        << "Python venv root path is empty, please set `python_venv_root_path` or "
                           "set `enable_python_udf_support` to `false`";
                exit(1);
            }
            if (python_venv_interpreter_paths.empty()) {
                LOG(ERROR)
                        << "Python interpreter paths is empty, please set "
                           "`python_venv_interpreter_paths` or set `enable_python_udf_support` to "
                           "`false`";
                exit(1);
            }
            LOG(INFO) << "Doris backend python version manager is initialized. Python venv "
                         "root path: "
                      << python_venv_root_path
                      << ", python interpreter paths: " << python_venv_interpreter_paths;
            status = doris::PythonVersionManager::instance().init(doris::PythonEnvType::VENV,
                                                                  python_venv_root_path,
                                                                  python_venv_interpreter_paths);
        } else {
            status = Status::InvalidArgument(
                    "Python env mode is invalid, should be `conda` or `venv`. If you don't want to "
                    "enable the Python UDF function, please set `enable_python_udf_support` to "
                    "`false`");
        }

        if (!status.ok()) {
            LOG(ERROR) << "Failed to initialize python version manager: " << status;
            exit(1);
        }
        LOG(INFO) << doris::PythonVersionManager::instance().to_string();
    }

    // Doris own signal handler must be register after jvm is init.
    // Or our own sig-handler for SIGINT & SIGTERM will not be chained ...
    // https://www.oracle.com/java/technologies/javase/signals.html
    doris::init_signals();
    // ATTN: MUST init before `ExecEnv`, `StorageEngine` and other daemon services
    //
    //       Daemon ───┬──► StorageEngine ──► ExecEnv ──► Disk/Mem/CpuInfo
    //                 │
    //                 │
    // BackendService ─┘
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();

    LOG(INFO) << doris::CpuInfo::debug_string();
    LOG(INFO) << doris::DiskInfo::debug_string();
    LOG(INFO) << doris::MemInfo::debug_string();

    // Doris-patched GNU libunwind reads PHDR metadata from our lock-free snapshot instead of
    // entering glibc dl_iterate_phdr while jemalloc profiling or signal-context unwinding may
    // already be involved in loader-lock-sensitive code. Configure libunwind before daemon threads
    // start so all later heap-profile and stack-trace unwinds use the same lock-safe policy.
    configureLibunwindPHDRCache();
    updatePHDRCache();
    LOG(INFO) << "PHDR cache enabled: " << hasPHDRCache();
#if defined(__ELF__) && !defined(__FreeBSD__)
    auto symbol_index = doris::SymbolIndex::instance();
    LOG(INFO) << "SymbolIndex preloaded: objects=" << symbol_index->objects().size()
              << " symbols=" << symbol_index->symbols().size();
#endif
    if (!doris::BackendOptions::init()) {
        exit(-1);
    }

    // init exec env
    auto* exec_env(doris::ExecEnv::GetInstance());
    status = doris::ExecEnv::init(doris::ExecEnv::GetInstance(), paths, spill_paths, broken_paths);
    if (status != Status::OK()) {
        std::cerr << "failed to init doris storage engine, res=" << status;
        exit_on_startup_failure();
    }

    // Start concurrency stats manager
    doris::ConcurrencyStatsManager::instance().start();

    // begin to start services
    doris::ThriftRpcHelper::setup(exec_env);
    // 1. thrift server with be_port
    std::shared_ptr<doris::BaseBackendService> service;
    std::function<void(Status&, std::string_view)> stop_work_if_error = [&](Status& status,
                                                                            std::string_view msg) {
        if (!status.ok()) {
            std::cerr << msg << '\n';
            service->stop_works();
            exit_on_startup_failure();
        }
    };

    if (doris::config::is_cloud_mode()) {
        service = std::make_shared<doris::CloudBackendService>(
                exec_env->storage_engine().to_cloud(), exec_env);
    } else {
        service = std::make_shared<doris::BackendService>(exec_env->storage_engine().to_local(),
                                                          exec_env);
    }

    std::unique_ptr<doris::server::IServerStarter> backend_thrift_starter;
    EXIT_IF_ERROR(doris::server::create_backend_thrift_starter(exec_env, doris::config::be_port,
                                                               service, &backend_thrift_starter));
    status = backend_thrift_starter->start();
    stop_work_if_error(status, "Doris BE server did not start correctly, exiting");

    // 2. brpc service
    std::unique_ptr<doris::server::IServerStarter> brpc_starter;
    EXIT_IF_ERROR(doris::server::create_brpc_starter(
            exec_env, doris::config::brpc_port, doris::config::brpc_num_threads, &brpc_starter));
    status = brpc_starter->start();
    stop_work_if_error(status, "BRPC service did not start correctly, exiting");

    // 3. http service
    std::unique_ptr<doris::server::IServerStarter> http_starter;
    EXIT_IF_ERROR(doris::server::create_http_starter(exec_env, doris::config::webserver_port,
                                                     doris::config::webserver_num_workers,
                                                     &http_starter));
    status = http_starter->start();
    stop_work_if_error(status, "Doris Be http service did not start correctly, exiting");

    // 4. heart beat server
    doris::ClusterInfo* cluster_info = exec_env->cluster_info();
    std::unique_ptr<doris::server::IServerStarter> heartbeat_thrift_starter;
    status = doris::server::create_heartbeat_thrift_starter(
            exec_env, doris::config::heartbeat_service_port,
            doris::config::heartbeat_service_thread_count, cluster_info, &heartbeat_thrift_starter);
    stop_work_if_error(status, "Heartbeat services did not start correctly, exiting");

    status = heartbeat_thrift_starter->start();
    stop_work_if_error(status, "Doris BE HeartBeat Service did not start correctly, exiting: " +
                                       status.to_string());

    // 5. arrow flight service
    std::unique_ptr<doris::server::IServerStarter> flight_starter;
    EXIT_IF_ERROR(doris::server::create_flight_starter(doris::config::arrow_flight_sql_port,
                                                       &flight_starter));
    status = flight_starter->start();
    stop_work_if_error(
            status, "Arrow Flight Service did not start correctly, exiting, " + status.to_string());

    // 6. start daemon thread to do clean or gc jobs
    doris::Daemon daemon;
    daemon.start();

    exec_env->storage_engine().notify_listeners();

    doris::k_is_server_ready = true;

    while (!doris::k_doris_exit) {
#if defined(LEAK_SANITIZER)
        __lsan_do_leak_check();
#endif
        sleep(3);
    }
    doris::k_is_server_ready = false;
    LOG(INFO) << "Doris main exiting.";
#if defined(LLVM_PROFILE)
    __llvm_profile_write_file();
    LOG(INFO) << "Flush profile file.";
#endif
    // For graceful shutdown, need to wait for all running queries to stop
    // Phase A: wait for FE Master to learn we are shutting down (via heartbeat),
    // then sleep an extra buffer so the OP_HEARTBEAT EditLog reaches all Followers.
    exec_env->wait_for_all_fe_known();
    // Phase B: wait for in-flight queries to finish.
    exec_env->wait_for_all_tasks_done();

    if (!doris::config::enable_graceful_exit_check) {
        // If not in memleak check mode, no need to wait all objects de-constructed normally, just exit.
        // It will make sure that graceful shutdown can be done definitely.
        LOG(INFO) << "Doris main exited.";
        google::FlushLogFiles(google::GLOG_INFO);
        _exit(0); // Do not call exit(0), it will wait for all objects de-constructed normally
        return 0;
    }
    daemon.stop();
    flight_starter->stop();
    flight_starter->join();
    LOG(INFO) << "Flight server stopped.";
    heartbeat_thrift_starter->stop();
    heartbeat_thrift_starter->join();
    LOG(INFO) << "Heartbeat server stopped";
    // TODO(zhiqiang): http_service
    http_starter->stop();
    http_starter->join();
    LOG(INFO) << "Http service stopped";
    backend_thrift_starter->stop();
    backend_thrift_starter->join();
    LOG(INFO) << "Be server stopped";
    brpc_starter->stop();
    brpc_starter->join();
    LOG(INFO) << "Brpc service stopped";
    service.reset();
    LOG(INFO) << "Backend Service stopped";
    exec_env->destroy();
    LOG(INFO) << "All service stopped, doris main exited.";
    return 0;
}

static void help(const char* progname) {
    printf("%s is the Doris backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}
