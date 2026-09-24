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
#include <cstdint>
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
// The identity of the cdc client this process forked, published for handle_sigchld(). A signal
// handler may only touch lock-free atomics, so the pid and its generation live in one 64-bit word
// rather than behind CdcClientMgr's mutex. The generation prevents a delayed handler from clearing
// ownership after the kernel has already reused the same numeric pid for a replacement child.
// ExecEnv owns a single CdcClientMgr, so there is a single published identity.
static_assert(sizeof(pid_t) <= sizeof(uint32_t));
static_assert(std::atomic<uint64_t>::is_always_lock_free);
static_assert(std::atomic<uint32_t>::is_always_lock_free);
std::atomic<uint64_t> g_cdc_child_identity {0};
std::atomic<uint32_t> g_cdc_child_generation {0};
// The identity whose OS process operations are currently owned by exactly one actor. A handler or
// normal thread must claim the published identity here BEFORE waitpid/kill and keep the claim until
// its last syscall. While claimed, the original child is either running or remains a zombie, so its
// numeric pid cannot be reused for another same-parent child.
std::atomic<uint64_t> g_cdc_child_operation {0};

#ifdef BE_TEST
static_assert(std::atomic<bool>::is_always_lock_free);
std::atomic<bool> g_pause_cdc_sigchld_handler {false};
std::atomic<bool> g_cdc_sigchld_handler_paused {false};
#endif

pid_t child_pid(uint64_t identity) {
    return static_cast<pid_t>(static_cast<uint32_t>(identity));
}

uint64_t new_child_identity(pid_t pid) {
    const uint64_t generation = g_cdc_child_generation.fetch_add(1, std::memory_order_relaxed) + 1;
    return (generation << 32) | static_cast<uint32_t>(pid);
}

// Signal-safe, non-blocking acquisition. Revalidate after publishing the claim: a normal thread may
// have revoked the identity between the first load and this CAS.
bool try_claim_child_identity(uint64_t identity) {
    if (identity == 0) {
        return false;
    }
    uint64_t unclaimed = 0;
    if (!g_cdc_child_operation.compare_exchange_strong(unclaimed, identity)) {
        return false;
    }
    if (g_cdc_child_identity.load() == identity) {
        return true;
    }
    g_cdc_child_operation.store(0);
    return false;
}

// Normal threads may wait for a handler's short WNOHANG operation. Returning false means the exact
// generation was revoked; the caller must not operate on its numeric pid.
bool claim_child_identity(uint64_t identity) {
    while (g_cdc_child_identity.load() == identity) {
        if (try_claim_child_identity(identity)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void release_child_identity() {
    g_cdc_child_operation.store(0);
}

// Reap the cdc client so it does not linger as a zombie.
//
// waitpid(-1) here would reap ANY child of this process, including the ones the embedded JVM forks
// for Runtime.exec(): the JVM's process-reaper thread would then find its own child already gone,
// and java.lang.ProcessHandleImpl turns that ECHILD into exit code 0 no matter what the child
// really returned. Java code inside BE that branches on an exit status would silently take the
// wrong branch - which is how frocksdbjni's `ldd /usr/bin/env | grep -q musl` probe answered "yes"
// on a glibc host and loaded the musl build of librocksdbjni.so. Wait for our own pid only.
void handle_sigchld(int sig_no) {
    const int saved_errno = errno;
    const uint64_t cdc_identity = g_cdc_child_identity.load();
    const pid_t cdc_pid = child_pid(cdc_identity);
    // Never retain a raw pid without the operation claim. If another actor owns the identity, that
    // actor also owns reaping it; returning is safe even when this signal was for the CDC child.
    if (cdc_pid <= 0 || !try_claim_child_identity(cdc_identity)) {
        errno = saved_errno;
        return;
    }
#ifdef BE_TEST
    if (g_pause_cdc_sigchld_handler.load()) {
        g_cdc_sigchld_handler_paused.store(true);
        while (g_pause_cdc_sigchld_handler.load()) {
        }
        g_cdc_sigchld_handler_paused.store(false);
    }
#endif
    int status = 0;
    pid_t wait_result;
    do {
        wait_result = waitpid(cdc_pid, &status, WNOHANG);
    } while (wait_result < 0 && errno == EINTR);
    if (wait_result == cdc_pid || (wait_result < 0 && errno == ECHILD)) {
        uint64_t expected = cdc_identity;
        g_cdc_child_identity.compare_exchange_strong(expected, 0);
    }
    // No syscall may use cdc_pid after this release. If waitpid collected it, only now can the
    // kernel reuse the number, and every stale generation will fail claim_child_identity().
    release_child_identity();
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

// Every normal-thread wait/signal is fenced by the exact published generation. Clearing publication
// while the operation claim is held transfers exclusive responsibility from the signal handler to
// this thread; the claim is released only after the child is reaped.
bool terminate_owned_child(uint64_t identity) {
    if (!claim_child_identity(identity)) {
        return false;
    }
    uint64_t expected = identity;
    if (!g_cdc_child_identity.compare_exchange_strong(expected, 0)) {
        release_child_identity();
        return false;
    }
    terminate_and_reap_child(child_pid(identity));
    release_child_identity();
    return true;
}

enum class OwnedChildState {
    RUNNING,
    EXITED,
    NOT_OWNED,
    WAIT_ERROR,
};

// Observes an owned child without leaving a raw pid usable after the ownership claim. An exited child
// is reaped and unpublished before the claim is released; a running child remains published.
[[maybe_unused]] OwnedChildState inspect_owned_child(uint64_t identity, int* status,
                                                     int* wait_error) {
    if (!claim_child_identity(identity)) {
        return OwnedChildState::NOT_OWNED;
    }

    int local_status = 0;
    pid_t wait_result;
    do {
        wait_result = waitpid(child_pid(identity), &local_status, WNOHANG);
    } while (wait_result < 0 && errno == EINTR);
    const int local_wait_error = wait_result < 0 ? errno : 0;

    OwnedChildState state = OwnedChildState::RUNNING;
    if (wait_result == child_pid(identity) || (wait_result < 0 && local_wait_error == ECHILD)) {
        uint64_t expected = identity;
        g_cdc_child_identity.compare_exchange_strong(expected, 0);
        state = OwnedChildState::EXITED;
    } else if (wait_result < 0) {
        state = OwnedChildState::WAIT_ERROR;
    }
    release_child_identity();

    if (status != nullptr) {
        *status = local_status;
    }
    if (wait_error != nullptr) {
        *wait_error = local_wait_error;
    }
    return state;
}

#ifdef BE_TEST
// Simulates "old child reaped, numeric pid reused" without asking the kernel to cycle its PID
// allocator. This revokes one generation without touching the process, after which a test can publish
// the same number under a new generation and challenge cleanup with the stale identity.
bool revoke_owned_child_for_test(uint64_t identity) {
    if (!claim_child_identity(identity)) {
        return false;
    }
    uint64_t expected = identity;
    const bool revoked = g_cdc_child_identity.compare_exchange_strong(expected, 0);
    release_child_identity();
    return revoked;
}
#endif

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

uint64_t CdcClientMgr::_publish_child_pid(pid_t pid) {
    if (pid <= 0) {
        return 0;
    }
    const uint64_t identity = new_child_identity(pid);
    uint64_t empty = 0;
    if (!g_cdc_child_identity.compare_exchange_strong(empty, identity)) {
        return 0;
    }
    return identity;
}

uint64_t CdcClientMgr::_get_child_identity() const {
    return g_cdc_child_identity.load();
}

pid_t CdcClientMgr::_get_child_pid() const {
    return child_pid(_get_child_identity());
}

bool CdcClientMgr::_terminate_child_identity(uint64_t identity) {
    return terminate_owned_child(identity);
}

#ifdef BE_TEST
uint64_t CdcClientMgr::set_child_pid_for_test(pid_t pid) {
    uint64_t current = _get_child_identity();
    while (current != 0 && !revoke_owned_child_for_test(current)) {
        current = _get_child_identity();
    }
    return _publish_child_pid(pid);
}

bool CdcClientMgr::terminate_child_identity_for_test(uint64_t identity) {
    return _terminate_child_identity(identity);
}

void CdcClientMgr::invoke_sigchld_handler_for_test() {
    handle_sigchld(SIGCHLD);
}

void CdcClientMgr::pause_sigchld_handler_for_test(bool pause) {
    g_pause_cdc_sigchld_handler.store(pause);
}

bool CdcClientMgr::sigchld_handler_paused_for_test() {
    return g_cdc_sigchld_handler_paused.load();
}
#endif

void CdcClientMgr::stop() {
    std::lock_guard<std::mutex> lock(_start_mutex);
    // Claim the exact generation before touching the OS process. If the handler is operating it, wait;
    // if the handler already reaped it, reload rather than retaining its now-reusable numeric pid.
    while (true) {
        const uint64_t identity = _get_child_identity();
        if (identity == 0 || _terminate_child_identity(identity)) {
            break;
        }
    }

    LOG(INFO) << "CdcClientMgr is stopped";
}

Status CdcClientMgr::start_cdc_client(PRequestCdcClientResult* result) {
    std::lock_guard<std::mutex> lock(_start_mutex);

    Status st = Status::OK();
    const uint64_t existing_identity = _get_child_identity();
    const pid_t exist_pid = child_pid(existing_identity);
    if (exist_pid > 0) {
#ifdef BE_TEST
        // In test mode, directly return OK if PID exists
        LOG(INFO) << "cdc client already started (BE_TEST mode), pid=" << exist_pid;
        return Status::OK();
#else
        int existing_wait_error = 0;
        const OwnedChildState existing_state =
                inspect_owned_child(existing_identity, nullptr, &existing_wait_error);
        if (existing_state == OwnedChildState::RUNNING) {
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
        } else if (existing_state == OwnedChildState::WAIT_ERROR) {
            st = Status::InternalError(fmt::format("Could not inspect CDC client {}: {}", exist_pid,
                                                   strerror(existing_wait_error)));
            st.to_protobuf(result->mutable_status());
            return st;
        } else {
            LOG(INFO) << "CDC client is dead, pid=" << exist_pid;
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
    // Unit tests can construct several managers even though ExecEnv owns only one in production.
    // A concurrent test manager may win publication after our initial empty check; in that case all
    // managers observe the same process-wide test child and start is still successful.
    if (_publish_child_pid(99999) == 0 && _get_child_identity() == 0) {
        st = Status::InternalError("Failed to publish test CDC child identity");
        st.to_protobuf(result->mutable_status());
        return st;
    }
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
        // Parent process: publish a generation-qualified identity. The child is not visible to the
        // SIGCHLD handler before this succeeds, so a publication conflict still leaves this thread as
        // the only possible reaper of the just-forked pid.
        const uint64_t forked_identity = _publish_child_pid(pid);
        if (forked_identity == 0) {
            terminate_and_reap_child(pid);
            st = Status::InternalError("Another CDC child identity was published during startup");
            st.to_protobuf(result->mutable_status());
            return st;
        }
        // A child that died between fork() returning and the store above raised a SIGCHLD the
        // handler saw with no identity to reap. Inspect the exact generation here; a child still
        // running remains published, while an exited one is reaped before its pid can be reused.
        int forked_status = 0;
        int forked_wait_error = 0;
        const OwnedChildState initial_state =
                inspect_owned_child(forked_identity, &forked_status, &forked_wait_error);
        if (initial_state == OwnedChildState::EXITED ||
            initial_state == OwnedChildState::NOT_OWNED) {
            st = forked_wait_error == 0 && initial_state == OwnedChildState::EXITED
                         ? Status::InternalError(
                                   fmt::format("CDC client exited before startup with {}",
                                               child_exit_description(forked_status)))
                         : Status::InternalError("CDC client exited before startup");
            st.to_protobuf(result->mutable_status());
            return st;
        }
        if (initial_state == OwnedChildState::WAIT_ERROR) {
            _terminate_child_identity(forked_identity);
            st = Status::InternalError(
                    fmt::format("CDC client exited before startup or could not be waited for: {}",
                                strerror(forked_wait_error)));
            st.to_protobuf(result->mutable_status());
            return st;
        }

        // Waiting for cdc to start, failed after more than 3 * 10 seconds
        std::string health_response;
        Status status = check_cdc_client_health(3, 10, health_response);
        if (!status.ok()) {
            // Cleanup is conditional on the exact generation still being ours. If the handler already
            // reaped it, a same-parent child may now reuse the number and must not be touched.
            _terminate_child_identity(forked_identity);
            st = Status::InternalError("Start cdc client failed.");
            st.to_protobuf(result->mutable_status());
        } else {
            int final_wait_error = 0;
            const OwnedChildState final_state =
                    inspect_owned_child(forked_identity, nullptr, &final_wait_error);
            if (final_state == OwnedChildState::WAIT_ERROR) {
                _terminate_child_identity(forked_identity);
                st = Status::InternalError(
                        fmt::format("Could not inspect started CDC client {}: {}", pid,
                                    strerror(final_wait_error)));
                st.to_protobuf(result->mutable_status());
                return st;
            }
            if (final_state == OwnedChildState::RUNNING) {
                _adopted_external.store(false);
                LOG(INFO) << "Start cdc client success, pid=" << pid
                          << ", status=" << status.to_string() << ", response=" << health_response;
                return st;
            }
            // Port healthy but our child has exited: an external process is
            // answering. Treat as adoption instead of masking dead PID as success.
            if (!_adopted_external.exchange(true)) {
                LOG(INFO) << "Forked cdc client " << pid << " exited but port "
                          << doris::config::cdc_client_port
                          << " is healthy, adopting external instance";
            }
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
