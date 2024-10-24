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

#include <brpc/server.h>
#include <fcntl.h> // ::open
#include <gen_cpp/cloud_version.h>
#include <unistd.h> // ::lockf

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "common/arg_parser.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_server.h"
#include "meta-service/txn_kv.h"
#include "recycler/recycler.h"

using namespace doris::cloud;

/**
 * Generates a pidfile with given process name
 *
 * @return an fd holder with auto-storage-lifecycle
 */
std::shared_ptr<int> gen_pidfile(const std::string& process_name) {
    std::cerr << "process working directory: " << std::filesystem::current_path() << std::endl;
    std::string pid_path = "./bin/" + process_name + ".pid";
    int fd = ::open(pid_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    // clang-format off
    std::shared_ptr<int> holder(&fd, // Just pretend to need an address of int
            [fd, pid_path](...) {    // The real fd is captured
                if (fd <= 0) { return; }
                [[maybe_unused]] auto x = ::lockf(fd, F_UNLCK, 0);
                ::close(fd);
                // FIXME: removing the pidfile may result in missing pidfile
                //        after launching the process...
                // std::error_code ec; std::filesystem::remove(pid_path, ec);
            });
    // clang-format on
    if (::lockf(fd, F_TLOCK, 0) != 0) {
        std::cerr << "failed to lock pidfile=" << pid_path << " fd=" << fd << std::endl;
        return nullptr;
    }
    std::fstream pidfile(pid_path, std::ios::out);
    if (!pidfile.is_open()) {
        std::cerr << "failed to open pid file " << pid_path << std::endl;
        return nullptr;
    }
    pidfile << getpid() << std::endl;
    pidfile.close();
    std::cout << "pid=" << getpid() << " written to file=" << pid_path << std::endl;
    return holder;
}

/**
 * Prepares extra conf files
 */
std::string prepare_extra_conf_file() {
    // If the target file is not empty and `config::fdb_cluster` is empty, use the exists file.
    if (config::fdb_cluster.empty()) {
        try {
            if (std::filesystem::exists(config::fdb_cluster_file_path) &&
                std::filesystem::file_size(config::fdb_cluster_file_path) > 0) {
                return "";
            }
        } catch (std::filesystem::filesystem_error& e) {
            return fmt::format("prepare_extra_conf_file: {}", e.what());
        }

        return "Please specify the fdb_cluster in doris_cloud.conf";
    }

    std::fstream fdb_cluster_file(config::fdb_cluster_file_path, std::ios::out);
    fdb_cluster_file << "# DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING!\n"
                     << "# This file is auto-generated with doris_cloud.conf:fdb_cluster.\n"
                     << "# It is not to be edited by hand.\n"
                     << config::fdb_cluster;
    fdb_cluster_file.close();
    return "";
}

// TODO(gavin): support daemon mode
// must be called before pidfile generation and any network resource
// initialization, <https://man7.org/linux/man-pages/man3/daemon.3.html>
// void daemonize(1, 1); // Maybe nohup will do?

// Arguments
// clang-format off
constexpr static const char* ARG_META_SERVICE = "meta-service";
constexpr static const char* ARG_RECYCLER     = "recycler";
constexpr static const char* ARG_HELP         = "help";
constexpr static const char* ARG_VERSION      = "version";
constexpr static const char* ARG_CONF         = "conf";
ArgParser args(
  {
    ArgParser::new_arg<bool>(ARG_META_SERVICE, false, "run as meta-service"),
    ArgParser::new_arg<bool>(ARG_RECYCLER, false, "run as recycler")    ,
    ArgParser::new_arg<bool>(ARG_HELP, false, "print help msg")     ,
    ArgParser::new_arg<bool>(ARG_VERSION, false, "print version info") ,
    ArgParser::new_arg<std::string>(ARG_CONF, "./conf/doris_cloud.conf", "path to conf file")  ,
  }
);
// clang-format on

static void help() {
    args.print();
}

static std::string build_info() {
    std::stringstream ss;
#if defined(NDEBUG)
    ss << "version:{" DORIS_CLOUD_BUILD_VERSION "-release}"
#else
    ss << "version:{" DORIS_CLOUD_BUILD_VERSION "-debug}"
#endif
       << " code_version:{commit=" DORIS_CLOUD_BUILD_HASH " time=" DORIS_CLOUD_BUILD_VERSION_TIME
          "}"
       << " build_info:{initiator=" DORIS_CLOUD_BUILD_INITIATOR " build_at=" DORIS_CLOUD_BUILD_TIME
          " build_on=" DORIS_CLOUD_BUILD_OS_VERSION "}\n";
    return ss.str();
}

// TODO(gavin): add doris cloud role to the metrics name
bvar::Status<uint64_t> doris_cloud_version_metrics("doris_cloud_version", [] {
    std::stringstream ss;
    ss << DORIS_CLOUD_BUILD_VERSION_MAJOR << 0 << DORIS_CLOUD_BUILD_VERSION_MINOR << 0
       << DORIS_CLOUD_BUILD_VERSION_PATCH;
    return std::strtoul(ss.str().c_str(), nullptr, 10);
}());

namespace brpc {
DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);
} // namespace brpc

int main(int argc, char** argv) {
    if (argc > 1) {
        if (auto ret = args.parse(argc - 1, argv + 1); !ret.empty()) {
            std::cerr << "parse arguments error: " << ret << std::endl;
            help();
            return -1;
        }
    }

    if (args.get<bool>(ARG_HELP)) {
        help();
        return 0;
    }

    if (args.get<bool>(ARG_VERSION)) {
        std::cout << build_info();
        return 0;
    }

    // There may be more roles to play in the future, if there are multi roles specified,
    // use meta_service as the process name
    std::string process_name = args.get<bool>(ARG_META_SERVICE) ? "meta_service"
                               : args.get<bool>(ARG_RECYCLER)   ? "recycler"
                                                                : "meta_service";

    using namespace std::chrono;

    auto start = steady_clock::now();
    auto end = start;

    auto pid_file_fd_holder = gen_pidfile("doris_cloud");
    if (pid_file_fd_holder == nullptr) {
        return -1;
    }

    auto conf_file = args.get<std::string>(ARG_CONF);
    if (!config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (auto ret = prepare_extra_conf_file(); !ret.empty()) {
        std::cerr << "failed to prepare extra conf file, err=" << ret << std::endl;
        return -1;
    }

    if (!init_glog(process_name.data())) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    // We can invoke glog from now on
    std::string msg;
    LOG(INFO) << "try to start doris_cloud";
    LOG(INFO) << build_info();
    std::cout << build_info() << std::endl;

    if (!args.get<bool>(ARG_META_SERVICE) && !args.get<bool>(ARG_RECYCLER)) {
        std::get<0>(args.args()[ARG_META_SERVICE]) = true;
        std::get<0>(args.args()[ARG_RECYCLER]) = true;
        LOG(INFO) << "meta_service and recycler are both not specified, "
                     "run doris_cloud as meta_service and recycler by default";
        std::cout << "run doris_cloud as meta_service and recycler by default" << std::endl;
    }

    brpc::Server server;
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = config::brpc_socket_max_unwritten_bytes;

    std::shared_ptr<TxnKv> txn_kv;
    if (config::use_mem_kv) {
        // MUST NOT be used in production environment
        std::cerr << "use volatile mem kv, please make sure it is not a production environment"
                  << std::endl;
        txn_kv = std::make_shared<MemTxnKv>();
    } else {
        txn_kv = std::make_shared<FdbTxnKv>();
    }
    if (txn_kv == nullptr) {
        LOG(WARNING) << "failed to create txn kv, invalid txnkv type";
        return 1;
    }
    LOG(INFO) << "begin to init txn kv";
    auto start_init_kv = steady_clock::now();
    int ret = txn_kv->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }
    end = steady_clock::now();
    LOG(INFO) << "successfully init txn kv, elapsed milliseconds: "
              << duration_cast<milliseconds>(end - start_init_kv).count();

    if (init_global_encryption_key_info_map(txn_kv.get()) != 0) {
        LOG(WARNING) << "failed to init global encryption key map";
        return -1;
    }

    std::unique_ptr<MetaServer> meta_server; // meta-service
    std::unique_ptr<Recycler> recycler;
    std::thread periodiccally_log_thread;
    std::mutex periodiccally_log_thread_lock;
    std::condition_variable periodiccally_log_thread_cv;
    std::atomic_bool periodiccally_log_thread_run = true;

    if (args.get<bool>(ARG_META_SERVICE)) {
        meta_server = std::make_unique<MetaServer>(txn_kv);
        int ret = meta_server->start(&server);
        if (ret != 0) {
            msg = "failed to start meta server";
            LOG(ERROR) << msg;
            std::cerr << msg << std::endl;
            return ret;
        }
        msg = "meta-service started";
        LOG(INFO) << msg;
        std::cout << msg << std::endl;
    }
    if (args.get<bool>(ARG_RECYCLER)) {
        recycler = std::make_unique<Recycler>(txn_kv);
        int ret = recycler->start(&server);
        if (ret != 0) {
            msg = "failed to start recycler";
            LOG(ERROR) << msg;
            std::cerr << msg << std::endl;
            return ret;
        }
        msg = "recycler started";
        LOG(INFO) << msg;
        std::cout << msg << std::endl;
        auto periodiccally_log = [&]() {
            while (periodiccally_log_thread_run) {
                std::unique_lock<std::mutex> lck {periodiccally_log_thread_lock};
                periodiccally_log_thread_cv.wait_for(lck,
                                                     milliseconds(config::periodically_log_ms));
                LOG(INFO) << "Periodically log for recycler";
            }
        };
        periodiccally_log_thread = std::thread {periodiccally_log};
    }
    // start service
    brpc::ServerOptions options;
    if (config::brpc_idle_timeout_sec != -1) {
        options.idle_timeout_sec = config::brpc_idle_timeout_sec;
    }
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    int port = config::brpc_listen_port;
    if (server.Start(port, &options) != 0) {
        char buf[64];
        LOG(WARNING) << "failed to start brpc, errno=" << errno
                     << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << port;
        return -1;
    }
    end = steady_clock::now();
    msg = "successfully started brpc listening on port=" + std::to_string(port) +
          " time_elapsed_ms=" + std::to_string(duration_cast<milliseconds>(end - start).count());
    LOG(INFO) << msg;
    std::cout << msg << std::endl;

    server.RunUntilAskedToQuit(); // Wait for signals
    server.ClearServices();
    if (meta_server) {
        meta_server->stop();
    }
    if (recycler) {
        recycler->stop();
    }

    if (periodiccally_log_thread.joinable()) {
        {
            std::unique_lock<std::mutex> lck {periodiccally_log_thread_lock};
            periodiccally_log_thread_run = false;
            // immediately notify the log thread to quickly exit in case it block the
            // whole procedure
            periodiccally_log_thread_cv.notify_all();
        }
        periodiccally_log_thread.join();
    }

    return 0;
}
