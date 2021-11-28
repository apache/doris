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

#include <gperftools/malloc_extension.h>
#include <sys/file.h>
#include <unistd.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include <curl/curl.h>
#include <gperftools/profiler.h>
#include <thrift/TOutput.h>

#include "agent/heartbeat_server.h"
#include "agent/status.h"
#include "agent/topic_subscriber.h"
#include "common/config.h"
#include "common/daemon.h"
#include "common/logging.h"
#include "common/resource_tls.h"
#include "common/status.h"
#include "env/env.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/minidump.h"
#include "runtime/tcmalloc_hook.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/brpc_service.h"
#include "service/http_service.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/logging.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"

static void help(const char*);

#include <dlfcn.h>

extern "C" {
void __lsan_do_leak_check();
}

namespace doris {
extern bool k_doris_exit;

static void thrift_output(const char* x) {
    LOG(WARNING) << "thrift internal message: " << x;
}

} // namespace doris

int main(int argc, char** argv) {
    init_hook();
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

    using doris::Status;
    using std::string;

    // open pid file, obtain file lock and save pid
    string pid_file = string(getenv("PID_DIR")) + "/be.pid";
    int fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
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

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    // Aggressive decommit is required so that unused pages in the TCMalloc page heap are
    // not backed by physical pages and do not contribute towards memory consumption.
    MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit", 1);
    // Change the total TCMalloc thread cache size if necessary.
    if (!MallocExtension::instance()->SetNumericProperty(
                "tcmalloc.max_total_thread_cache_bytes",
                doris::config::tc_max_total_thread_cache_bytes)) {
        fprintf(stderr, "Failed to change TCMalloc total thread cache size.\n");
        return -1;
    }
#endif

    if (!doris::Env::init()) {
        LOG(FATAL) << "init env failed.";
        exit(-1);
    }

    std::vector<doris::StorePath> paths;
    auto olap_res = doris::parse_conf_store_paths(doris::config::storage_root_path, &paths);
    if (olap_res != doris::OLAP_SUCCESS) {
        LOG(FATAL) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }
    auto it = paths.begin();
    for (; it != paths.end();) {
        if (!doris::check_datapath_rw(it->path)) {
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

    doris::Daemon daemon;
    daemon.init(argc, argv, paths);
    daemon.start();

    doris::ResourceTls::init();
    if (!doris::BackendOptions::init()) {
        exit(-1);
    }

    // init and open storage engine
    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    doris::StorageEngine* engine = nullptr;
    auto st = doris::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st.get_error_msg();
        exit(-1);
    }

    // init exec env
    auto exec_env = doris::ExecEnv::GetInstance();
    doris::ExecEnv::init(exec_env, paths);
    exec_env->set_storage_engine(engine);
    engine->set_heartbeat_flags(exec_env->heartbeat_flags());

    // start all backgroud threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(engine->start_bg_threads());

    // begin to start services
    doris::ThriftRpcHelper::setup(exec_env);
    // 1. thrift server with be_port
    doris::ThriftServer* be_server = nullptr;
    EXIT_IF_ERROR(
            doris::BackendService::create_service(exec_env, doris::config::be_port, &be_server));
    Status status = be_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris Be server did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    // 2. bprc service
    doris::BRpcService brpc_service(exec_env);
    status = brpc_service.start(doris::config::brpc_port);
    if (!status.ok()) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    // 3. http service
    doris::HttpService http_service(exec_env, doris::config::webserver_port,
                                    doris::config::webserver_num_workers);
    status = http_service.start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris Be http service did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    // 4. heart beat server
    doris::TMasterInfo* master_info = exec_env->master_info();
    doris::ThriftServer* heartbeat_thrift_server;
    doris::AgentStatus heartbeat_status = doris::create_heartbeat_server(
            exec_env, doris::config::heartbeat_service_port, &heartbeat_thrift_server,
            doris::config::heartbeat_service_thread_count, master_info);

    if (doris::AgentStatus::DORIS_SUCCESS != heartbeat_status) {
        LOG(ERROR) << "Heartbeat services did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    status = heartbeat_thrift_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris BE HeartBeat Service did not start correctly, exiting: "
                   << status.get_error_msg();
        doris::shutdown_logging();
        exit(1);
    }

    // 5. init minidump
    doris::Minidump minidump;
    status = minidump.init();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to initialize minidump: " << status.get_error_msg();
        doris::shutdown_logging();
        exit(1);
    }

    while (!doris::k_doris_exit) {
#if defined(LEAK_SANITIZER)
        __lsan_do_leak_check();
#endif

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
        doris::MemInfo::refresh_current_mem();
#endif
        doris::ExecEnv::GetInstance()->query_mem_tracker_registry()->DeregisterQueryMemTracker();
        sleep(10);
    }

    http_service.stop();
    brpc_service.join();
    daemon.stop();
    heartbeat_thrift_server->stop();
    heartbeat_thrift_server->join();
    be_server->stop();
    be_server->join();
    engine->stop();
    minidump.stop();

    delete be_server;
    be_server = nullptr;
    delete engine;
    engine = nullptr;
    delete heartbeat_thrift_server;
    heartbeat_thrift_server = nullptr;
    doris::ExecEnv::destroy(exec_env);
    return 0;
}

static void help(const char* progname) {
    printf("%s is the Doris backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}
