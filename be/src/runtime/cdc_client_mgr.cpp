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

#include "runtime/cdc_client_mgr.h"

#include <brpc/closure_guard.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <gen_cpp/internal_service.pb.h>
#include <google/protobuf/stubs/callback.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#ifndef __APPLE__
#include <sys/prctl.h>
#endif

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "http/http_client.h"

namespace doris {

namespace {
// Handle SIGCHLD signal to prevent zombie processes
void handle_sigchld(int sig_no) {
    int status = 0;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    }
}

// Check CDC client health
#ifndef BE_TEST
Status check_cdc_client_health(int retry_times, int sleep_time, std::string& health_response) {
    const std::string cdc_health_url =
            "http://127.0.0.1:" + std::to_string(doris::config::cdc_client_port) +
            "/actuator/health";

    auto health_request = [cdc_health_url, &health_response](HttpClient* client) {
        RETURN_IF_ERROR(client->init(cdc_health_url));
        client->set_timeout_ms(5000);
        RETURN_IF_ERROR(client->execute(&health_response));
        return Status::OK();
    };

    Status status = HttpClient::execute_with_retry(retry_times, sleep_time, health_request);

    if (!status.ok()) {
        return Status::InternalError("CDC client health check failed");
    }

    bool is_up = health_response.find("UP") != std::string::npos;

    if (!is_up) {
        return Status::InternalError(fmt::format("CDC client unhealthy: {}", health_response));
    }

    return Status::OK();
}
#endif

} // anonymous namespace

CdcClientMgr::CdcClientMgr() = default;

CdcClientMgr::~CdcClientMgr() {
    stop();
}

void CdcClientMgr::stop() {
    pid_t pid = _child_pid.load();
    if (pid > 0) {
        // Check if process is still alive
        if (kill(pid, 0) == 0) {
            LOG(INFO) << "Stopping CDC client process, pid=" << pid;
            // Send SIGTERM for graceful shutdown
            kill(pid, SIGTERM);
            // Wait a short time for graceful shutdown
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            // Force kill if still alive
            if (kill(pid, 0) == 0) {
                LOG(INFO) << "Force killing CDC client process, pid=" << pid;
                kill(pid, SIGKILL);
                int status = 0;
                waitpid(pid, &status, 0);
            }
        }
        _child_pid.store(0);
    }

    LOG(INFO) << "CdcClientMgr is stopped";
}

Status CdcClientMgr::start_cdc_client(PRequestCdcClientResult* result) {
    std::lock_guard<std::mutex> lock(_start_mutex);

    Status st = Status::OK();
    pid_t exist_pid = _child_pid.load();
    if (exist_pid > 0) {
#ifdef BE_TEST
        // In test mode, directly return OK if PID exists
        LOG(INFO) << "cdc client already started (BE_TEST mode), pid=" << exist_pid;
        return Status::OK();
#else
        // Check if process is still alive
        if (kill(exist_pid, 0) == 0) {
            // Process exists, verify it's actually our CDC client by health check
            std::string check_response;
            auto check_st = check_cdc_client_health(3, 1, check_response);
            if (check_st.ok()) {
                // Process exists and responding, CDC client is running
                return Status::OK();
            } else {
                // Process exists but CDC client not responding
                // Either it's a different process (PID reused) or CDC client is unhealthy
                st = Status::InternalError(fmt::format("CDC client {} unresponsive", exist_pid));
                st.to_protobuf(result->mutable_status());
                return st;
            }
        } else {
            LOG(INFO) << "CDC client is dead, pid=" << exist_pid;
            // Process is dead, reset PID and continue to start
            _child_pid.store(0);
        }
#endif
    } else {
        LOG(INFO) << "CDC client has never been started";
    }

    const char* doris_home = getenv("DORIS_HOME");
    const char* log_dir = getenv("LOG_DIR");
    const std::string cdc_jar_path = std::string(doris_home) + "/lib/cdc_client/cdc-client.jar";
    const std::string cdc_jar_port =
            "--server.port=" + std::to_string(doris::config::cdc_client_port);
    const std::string backend_http_port =
            "--backend.http.port=" + std::to_string(config::webserver_port);
    const std::string java_opts = "-Dlog.path=" + std::string(log_dir);

    // check cdc jar exists
    struct stat buffer;
    if (stat(cdc_jar_path.c_str(), &buffer) != 0) {
        st = Status::InternalError("Can not find cdc-client.jar.");
        st.to_protobuf(result->mutable_status());
        return st;
    }

    // Ready to start cdc client
    LOG(INFO) << "Ready to start cdc client";
    const auto* java_home = getenv("JAVA_HOME");
    if (!java_home) {
        st = Status::InternalError("Can not find JAVA_HOME");
        st.to_protobuf(result->mutable_status());
        return st;
    }
    std::string path(java_home);
    std::string java_bin = path + "/bin/java";
    // Capture signal to prevent child process from becoming a zombie process
    struct sigaction act;
    act.sa_flags = 0;
    act.sa_handler = handle_sigchld;
    sigaction(SIGCHLD, &act, NULL);
    LOG(INFO) << "Start to fork cdc client process with " << path;
#ifdef BE_TEST
    _child_pid.store(99999);
    st = Status::OK();
    return st;
#else
    pid_t pid = fork();
    if (pid < 0) {
        // Fork failed
        st = Status::InternalError("Fork cdc client failed.");
        st.to_protobuf(result->mutable_status());
        return st;
    } else if (pid == 0) {
        // Child process
        // When the parent process is killed, the child process also needs to exit
#ifndef __APPLE__
        prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
        // Redirect stdout and stderr to log out file
        std::string cdc_out_file = std::string(log_dir) + "/cdc-client.out";
        int out_fd = open(cdc_out_file.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
        if (out_fd < 0) {
            perror("open cdc-client.out file failed");
            exit(1);
        }
        dup2(out_fd, STDOUT_FILENO);
        dup2(out_fd, STDERR_FILENO);
        close(out_fd);

        // java -jar -Dlog.path=xx cdc-client.jar --server.port=9096 --backend.http.port=8040
        execlp(java_bin.c_str(), "java", java_opts.c_str(), "-jar", cdc_jar_path.c_str(),
               cdc_jar_port.c_str(), backend_http_port.c_str(), (char*)NULL);
        // If execlp returns, it means it failed
        perror("Cdc client child process error");
        exit(1);
    } else {
        // Parent process: save PID and wait for startup
        _child_pid.store(pid);

        // Waiting for cdc to start, failed after more than 3 * 10 seconds
        std::string health_response;
        Status status = check_cdc_client_health(3, 10, health_response);
        if (!status.ok()) {
            // Reset PID if startup failed
            _child_pid.store(0);
            st = Status::InternalError("Start cdc client failed.");
            st.to_protobuf(result->mutable_status());
        } else {
            LOG(INFO) << "Start cdc client success, pid=" << pid
                      << ", status=" << status.to_string() << ", response=" << health_response;
        }
    }
#endif //BE_TEST
    return st;
}

void CdcClientMgr::request_cdc_client_impl(const PRequestCdcClientRequest* request,
                                           PRequestCdcClientResult* result,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);

    // Start CDC client if not started
    Status start_st = start_cdc_client(result);
    if (!start_st.ok()) {
        LOG(ERROR) << "Failed to start CDC client, status=" << start_st.to_string();
        start_st.to_protobuf(result->mutable_status());
        return;
    }

    std::string cdc_response;
    Status st = send_request_to_cdc_client(request->api(), request->params(), &cdc_response);
    result->set_response(cdc_response);
    st.to_protobuf(result->mutable_status());
}

Status CdcClientMgr::send_request_to_cdc_client(const std::string& api,
                                                const std::string& params_body,
                                                std::string* response) {
    std::string remote_url_prefix =
            fmt::format("http://127.0.0.1:{}{}", doris::config::cdc_client_port, api);

    auto cdc_request = [&remote_url_prefix, response, &params_body](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(doris::config::request_cdc_client_timeout_ms);
        if (!params_body.empty()) {
            client->set_payload(params_body);
        }
        client->set_content_type("application/json");
        client->set_method(POST);
        RETURN_IF_ERROR(client->execute(response));
        return Status::OK();
    };

    return HttpClient::execute_with_retry(3, 1, cdc_request);
}

} // namespace doris
