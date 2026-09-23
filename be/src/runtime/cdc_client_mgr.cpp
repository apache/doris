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
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "service/http/http_client.h"

namespace doris {

namespace {
// The pid of the cdc client this process forked, published for handle_sigchld(). A signal handler
// may only touch lock-free atomics, so the one pid it is allowed to reap lives here rather than
// behind CdcClientMgr's mutex. ExecEnv owns a single CdcClientMgr, so there is a single pid.
std::atomic<pid_t> g_cdc_child_pid {0};

// Reap the cdc client so it does not linger as a zombie.
//
// waitpid(-1) here would reap ANY child of this process, including the ones the embedded JVM forks
// for Runtime.exec(): the JVM's process-reaper thread would then find its own child already gone,
// and java.lang.ProcessHandleImpl turns that ECHILD into exit code 0 no matter what the child
// really returned. Java code inside BE that branches on an exit status would silently take the
// wrong branch - which is how frocksdbjni's `ldd /usr/bin/env | grep -q musl` probe answered "yes"
// on a glibc host and loaded the musl build of librocksdbjni.so. Wait for our own pid only.
void handle_sigchld(int sig_no) {
    const pid_t cdc_pid = g_cdc_child_pid.load(std::memory_order_relaxed);
    if (cdc_pid <= 0) {
        return;
    }
    // A handler must leave errno as it found it: it can interrupt a thread between a failing call
    // and its errno check.
    const int saved_errno = errno;
    int status = 0;
    pid_t wait_result;
    do {
        wait_result = waitpid(cdc_pid, &status, WNOHANG);
    } while (wait_result < 0 && errno == EINTR);
    if (wait_result == cdc_pid) {
        pid_t expected = cdc_pid;
        g_cdc_child_pid.compare_exchange_strong(expected, 0, std::memory_order_relaxed);
    }
    errno = saved_errno;
}

// Terminate and collect one child owned by this process. The SIGCHLD handler may have won the
// waitpid race already; ECHILD is therefore success, not a reason to send a signal to a reused pid.
void terminate_and_reap_child(pid_t pid) {
    if (pid <= 0) {
        return;
    }

    int status = 0;
    pid_t wait_result;
    do {
        wait_result = waitpid(pid, &status, WNOHANG);
    } while (wait_result < 0 && errno == EINTR);
    if (wait_result == pid || (wait_result < 0 && errno == ECHILD)) {
        return;
    }

    LOG(INFO) << "Stopping CDC client process, pid=" << pid;
    if (kill(pid, SIGTERM) != 0 && errno != ESRCH) {
        LOG(WARNING) << "Failed to terminate CDC client process, pid=" << pid
                     << ", error=" << strerror(errno);
    }
    for (int i = 0; i < 20; ++i) {
        do {
            wait_result = waitpid(pid, &status, WNOHANG);
        } while (wait_result < 0 && errno == EINTR);
        if (wait_result == pid || (wait_result < 0 && errno == ECHILD)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    LOG(INFO) << "Force killing CDC client process, pid=" << pid;
    if (kill(pid, SIGKILL) != 0 && errno != ESRCH) {
        LOG(WARNING) << "Failed to kill CDC client process, pid=" << pid
                     << ", error=" << strerror(errno);
    }
    do {
        wait_result = waitpid(pid, &status, 0);
    } while (wait_result < 0 && errno == EINTR);
}

#ifndef BE_TEST
std::string child_exit_description(int status) {
    if (WIFEXITED(status)) {
        return fmt::format("exit code {}", WEXITSTATUS(status));
    }
    if (WIFSIGNALED(status)) {
        return fmt::format("signal {}", WTERMSIG(status));
    }
    return fmt::format("wait status {}", status);
}

// Check CDC client health
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

void CdcClientMgr::_set_child_pid(pid_t pid) {
    _child_pid.store(pid);
    g_cdc_child_pid.store(pid, std::memory_order_relaxed);
}

#ifdef BE_TEST
void CdcClientMgr::invoke_sigchld_handler_for_test() {
    handle_sigchld(SIGCHLD);
}
#endif

void CdcClientMgr::stop() {
    std::lock_guard<std::mutex> lock(_start_mutex);
    pid_t pid = _child_pid.load();
    // Stop publishing before signalling: from here this thread owns the waitpid, and the handler must
    // not race it or keep a stale pid after teardown.
    _set_child_pid(0);
    terminate_and_reap_child(pid);

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
            _set_child_pid(0);
        }
#endif
    } else if (!_adopted_external.load()) {
        LOG(INFO) << "CDC client has never been started";
    }

#ifndef BE_TEST
    // Adopt an externally-managed cdc_client if the port already answers
    // healthy (e.g. one started manually for debug / hotfix).
    {
        std::string adopt_response;
        if (check_cdc_client_health(1, 0, adopt_response).ok()) {
            if (!_adopted_external.exchange(true)) {
                LOG(INFO) << "Adopting external cdc client on port "
                          << doris::config::cdc_client_port;
            }
            return Status::OK();
        }
    }
    _adopted_external.store(false);
#endif

    const char* doris_home = getenv("DORIS_HOME");
    const char* log_dir = getenv("LOG_DIR");
    const std::string cdc_jar_path = std::string(doris_home) + "/lib/cdc_client/cdc-client.jar";
    const std::string cdc_jar_port =
            "--server.port=" + std::to_string(doris::config::cdc_client_port);
    const std::string backend_http_port =
            "--backend.http.port=" + std::to_string(config::webserver_port);
    const std::string cluster_token = "--cluster.token=" + ExecEnv::GetInstance()->token();
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

    // Pre-build everything the child needs before fork(): heap allocation after
    // fork() in a multi-threaded process can deadlock on inherited libc locks.
    std::vector<std::string> argv_storage;
    argv_storage.emplace_back("java");
    const std::string user_java_opts = doris::config::cdc_client_java_opts;
    if (!user_java_opts.empty()) {
        std::istringstream iss(user_java_opts);
        argv_storage.insert(argv_storage.end(), std::istream_iterator<std::string>(iss),
                            std::istream_iterator<std::string>());
    }
    argv_storage.emplace_back(java_opts);
    // OOM safety net (last-wins, user opts cannot disable).
    argv_storage.emplace_back("-XX:+ExitOnOutOfMemoryError");
    // JDK17 opens for debezium ObjectSizeCalculator reflection.
    argv_storage.emplace_back("--add-opens=java.base/java.lang=ALL-UNNAMED");
    argv_storage.emplace_back("--add-opens=java.base/java.util=ALL-UNNAMED");
    argv_storage.emplace_back("--add-opens=java.base/java.math=ALL-UNNAMED");
    argv_storage.emplace_back("--add-opens=java.base/java.nio=ALL-UNNAMED");
    argv_storage.emplace_back("-jar");
    argv_storage.emplace_back(cdc_jar_path);
    argv_storage.emplace_back(cdc_jar_port);
    argv_storage.emplace_back(backend_http_port);
    argv_storage.emplace_back(cluster_token);

    std::vector<char*> argv;
    argv.reserve(argv_storage.size() + 1);
    for (auto& s : argv_storage) {
        argv.push_back(const_cast<char*>(s.c_str()));
    }
    argv.push_back(nullptr);

    const std::string cdc_out_file = std::string(log_dir) + "/cdc-client.out";

    struct sigaction act {};
    sigemptyset(&act.sa_mask);
    // SA_RESTART: the handler runs on whichever thread the kernel picks, and without it every
    // blocking call in BE becomes interruptible whenever the cdc client exits. SA_NOCLDSTOP: only
    // the child's exit is interesting, not its stops.
    act.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    act.sa_handler = handle_sigchld;
    sigaction(SIGCHLD, &act, nullptr);
    LOG(INFO) << "Start to fork cdc client process with " << path;
#ifdef BE_TEST
    _set_child_pid(99999);
    st = Status::OK();
    return st;
#else
    pid_t pid = fork();
    if (pid < 0) {
        st = Status::InternalError("Fork cdc client failed.");
        st.to_protobuf(result->mutable_status());
        return st;
    } else if (pid == 0) {
        // Child: async-signal-safe operations only until execv().
#ifndef __APPLE__
        prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
        int out_fd = open(cdc_out_file.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
        if (out_fd < 0) {
            perror("open cdc-client.out file failed");
            _exit(1);
        }
        dup2(out_fd, STDOUT_FILENO);
        dup2(out_fd, STDERR_FILENO);
        close(out_fd);
        execv(java_bin.c_str(), argv.data());
        perror("Cdc client child process error");
        _exit(1);
    } else {
        // Parent process: save PID and wait for startup
        _set_child_pid(pid);
        // A child that died between fork() returning and the store above raised a SIGCHLD the
        // handler saw with no pid to reap. Collect it here; a child still running just returns 0.
        int forked_status = 0;
        pid_t forked_wait_result;
        do {
            forked_wait_result = waitpid(pid, &forked_status, WNOHANG);
        } while (forked_wait_result < 0 && errno == EINTR);
        if (forked_wait_result == pid) {
            _set_child_pid(0);
            st = Status::InternalError(fmt::format("CDC client exited before startup with {}",
                                                   child_exit_description(forked_status)));
            st.to_protobuf(result->mutable_status());
            return st;
        }
        if (forked_wait_result < 0) {
            const int wait_errno = errno;
            _set_child_pid(0);
            st = Status::InternalError(
                    fmt::format("CDC client exited before startup or could not be waited for: {}",
                                strerror(wait_errno)));
            st.to_protobuf(result->mutable_status());
            return st;
        }

        // Waiting for cdc to start, failed after more than 3 * 10 seconds
        std::string health_response;
        Status status = check_cdc_client_health(3, 10, health_response);
        if (!status.ok()) {
            // A failed startup still owns a real child. Stop and reap it before forgetting the pid.
            _set_child_pid(0);
            terminate_and_reap_child(pid);
            st = Status::InternalError("Start cdc client failed.");
            st.to_protobuf(result->mutable_status());
        } else if (kill(pid, 0) != 0) {
            // Port healthy but our child has exited: an external process is
            // answering. Treat as adoption instead of masking dead PID as success.
            _set_child_pid(0);
            if (!_adopted_external.exchange(true)) {
                LOG(INFO) << "Forked cdc client " << pid << " exited but port "
                          << doris::config::cdc_client_port
                          << " is healthy, adopting external instance";
            }
        } else {
            _adopted_external.store(false);
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
