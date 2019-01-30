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

#include <sys/file.h>
#include <unistd.h>

#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <gperftools/malloc_extension.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include <curl/curl.h>

#include "common/logging.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/status.h"
#include "codegen/llvm_codegen.h"
#include "runtime/exec_env.h"
#include "util/logging.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/thrift_server.h"
#include "util/debug_util.h"
#include "agent/heartbeat_server.h"
#include "agent/status.h"
#include "agent/topic_subscriber.h"
#include "util/doris_metrics.h"
#include "olap/options.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/brpc_service.h"
#include "service/http_service.h"
#include <gperftools/profiler.h>
#include "common/resource_tls.h"
#include "exec/schema_scanner/frontend_helper.h"

static void help(const char*);

#include <dlfcn.h>

extern "C" { void __lsan_do_leak_check(); }

namespace doris {
extern bool k_doris_exit;
}

int main(int argc, char** argv) {
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

    string conffile = string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    MallocExtension::instance()->SetNumericProperty(
        "tcmalloc.aggressive_memory_decommit", 21474836480);
#endif

    std::vector<doris::StorePath> paths;
    auto olap_res = doris::parse_conf_store_paths(doris::config::storage_root_path, &paths);
    if (olap_res != doris::OLAP_SUCCESS) {
        LOG(FATAL) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }

    doris::LlvmCodeGen::initialize_llvm();

    // initilize libcurl here to avoid concurrent initialization
    auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_ret != 0) {
        LOG(FATAL) << "fail to initialize libcurl, curl_ret=" << curl_ret;
        exit(-1);
    }

    doris::init_daemon(argc, argv, paths);

    doris::ResourceTls::init();
    if (!doris::BackendOptions::init()) {
        exit(-1);
    }

    // options
    doris::EngineOptions options;
    options.store_paths = paths;
    doris::OLAPEngine* engine = nullptr;
    auto st = doris::OLAPEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open OLAPEngine, res=" << st.get_error_msg();
        exit(-1);
    }

    // start backend service for the coordinator on be_port
    auto exec_env = doris::ExecEnv::GetInstance();
    doris::ExecEnv::init(exec_env, paths);
    exec_env->set_olap_engine(engine);

    doris::FrontendHelper::setup(exec_env);
    doris::ThriftServer* be_server = nullptr;

    EXIT_IF_ERROR(doris::BackendService::create_service(
            exec_env,
            doris::config::be_port,
            &be_server));
    Status status = be_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris Be server did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    doris::BRpcService brpc_service(exec_env);
    status = brpc_service.start(doris::config::brpc_port);
    if (!status.ok()) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    doris::HttpService http_service(
        exec_env, doris::config::webserver_port, doris::config::webserver_num_workers);
    status = http_service.start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris Be http service did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    doris::TMasterInfo* master_info = exec_env->master_info();
    // start heart beat server
    doris::ThriftServer* heartbeat_thrift_server;
    doris::AgentStatus heartbeat_status = doris::create_heartbeat_server(
            exec_env,
            doris::config::heartbeat_service_port,
            &heartbeat_thrift_server,
            doris::config::heartbeat_service_thread_count,
            master_info);

    if (doris::AgentStatus::DORIS_SUCCESS != heartbeat_status) {
        LOG(ERROR) << "Heartbeat services did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    status = heartbeat_thrift_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Doris BE HeartBeat Service did not start correctly, exiting";
        doris::shutdown_logging();
        exit(1);
    }

    while (!doris::k_doris_exit) {
#if defined(LEAK_SANITIZER)
        __lsan_do_leak_check();
#endif
        sleep(10);
    }
    heartbeat_thrift_server->stop();
    heartbeat_thrift_server->join();
    be_server->stop();
    be_server->join();

    delete be_server;
    return 0;
}

static void help(const char* progname) {
    printf("%s is the Doris backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}

