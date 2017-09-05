// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include "util/palo_metrics.h"
#include "olap/olap_main.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include <gperftools/profiler.h>
#include "common/resource_tls.h"
#include "exec/schema_scanner/frontend_helper.h"

#include "rpc/reactor_factory.h"

static void help(const char*);

#include <dlfcn.h>

int main(int argc, char** argv) {
    // check if print version or help
    if (argc > 1) {
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-v") == 0) {
            puts(palo::get_build_version(false).c_str());
            exit(0);
        } else if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(basename(argv[0]));
            exit(0);
        }
    }

    if (getenv("PALO_HOME") == nullptr) {
        fprintf(stderr, "you need set PALO_HOME environment variable.\n");
        exit(-1);
    }

    using palo::Status;
    using std::string;

    // open pid file, obtain file lock and save pid
    string pid_file = string(getenv("PID_DIR")) + "/be.pid";
    int fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
        fprintf(stderr, "fail to create pid file.");
        exit(-1);
    }

    int lock_res = flock(fd, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        fprintf(stderr, "fail to lock pid file, maybe another process is locking it.");
        exit(-1);
    }

    string pid = std::to_string((long)getpid());
    pid += "\n";
    size_t length = write(fd, pid.c_str(), pid.size());
    if (length != pid.size()) {
        fprintf(stderr, "fail to save pid into pid file.");
        exit(-1);
    }

    string conffile = string(getenv("PALO_HOME")) + "/conf/be.conf"; 
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

    palo::LlvmCodeGen::initialize_llvm();
    palo::init_daemon(argc, argv);

    palo::ResourceTls::init();
    palo::BackendOptions::init();

    // initialize storage
    if (0 != palo::olap_main(argc, argv)) {
        LOG(ERROR) << "olap start error!";
        palo::shutdown_logging();
        exit(1);
    }

    // start backend service for the coordinator on be_port
    palo::ExecEnv exec_env;
    palo::FrontendHelper::setup(&exec_env);
    palo::ThriftServer* be_server = nullptr;

    EXIT_IF_ERROR(palo::BackendService::create_service(
            &exec_env, 
            palo::config::be_port, 
            &be_server));
    Status status = be_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "Palo Be server did not start correctly, exiting";
        palo::shutdown_logging();
        exit(1);
    }

    status = palo::BackendService::create_rpc_service(&exec_env);
    if (!status.ok()) {
        LOG(ERROR) << "Palo Be services did not start correctly, exiting";
        palo::shutdown_logging();
        exit(1);
    }

    status = exec_env.start_services();
    if (!status.ok()) {
        LOG(ERROR) << "Palo Be services did not start correctly, exiting";
        palo::shutdown_logging();
        exit(1);
    }

    palo::TMasterInfo* master_info = exec_env.master_info();
    // start heart beat server
    palo::ThriftServer* heartbeat_thrift_server;
    palo::AgentStatus heartbeat_status = palo::create_heartbeat_server(
            &exec_env,
            palo::config::heartbeat_service_port,
            &heartbeat_thrift_server,
            palo::config::heartbeat_service_thread_count,
            master_info);

    if (palo::AgentStatus::PALO_SUCCESS != heartbeat_status) {
        LOG(ERROR) << "Heartbeat services did not start correctly, exiting";
        palo::shutdown_logging();
        exit(1);
    }
    heartbeat_thrift_server->start();

    // this blocks until the beeswax and hs2 servers terminate
    palo::PaloMetrics::palo_be_ready()->update(true);
    LOG(INFO) << "Palo has started.";

    //be_server->join();

    palo::ReactorFactory::join();

    delete be_server;
    return 0;
}

static void help(const char* progname) {
    printf("%s is the Palo backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}

