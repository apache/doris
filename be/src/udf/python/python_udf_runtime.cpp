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

#include "udf/python/python_udf_runtime.h"

#include <butil/fd_utility.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <boost/process.hpp>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#ifdef BE_TEST
#include <atomic>
#endif

#include "common/logging.h"
#include "cpp/sync_point.h"
#include "runtime/thread_context.h"

namespace doris {

#ifdef BE_TEST
static constexpr std::chrono::milliseconds PROCESS_START_TIMEOUT {500};
static constexpr std::chrono::milliseconds PROCESS_TERMINATE_TIMEOUT {100};
#else
static constexpr std::chrono::milliseconds PROCESS_START_TIMEOUT {5000};
static constexpr std::chrono::milliseconds PROCESS_TERMINATE_TIMEOUT {1000};
#endif
static constexpr std::chrono::milliseconds BACKGROUND_REAP_INTERVAL {1000};

Status PythonUDFProcess::create(const PythonVersion& version, ProcessPtr* process) {
    std::vector<std::string> args = {"-u", get_fight_server_path(), get_base_unix_socket_path()};
    boost::process::environment env = boost::this_process::environment();
    boost::process::ipstream child_output;
    // Test synchronization precedes child creation so it cannot consume readiness timeout.
#ifdef BE_TEST
    TEST_SYNC_POINT_CALLBACK("PythonUDFProcess::create:before_process_start", &version);
#endif
    try {
        boost::process::child c(
                version.get_executable_path(), args, boost::process::std_out > child_output,
                boost::process::env = env,
                boost::process::on_exit([](int exit_code, const std::error_code& ec) {
                    if (ec) {
                        LOG(WARNING) << "Python UDF server exited with error: " << ec.message();
                    }
                }));

        // A child may start without creating its Flight socket; bound the readiness wait.
        pid_t child_pid = c.id();
        std::string expected_socket_path = get_unix_socket_file_path(child_pid);
        bool started_successfully = false;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

        while (std::chrono::steady_clock::now() - start < PROCESS_START_TIMEOUT) {
            struct stat buffer;
            if (stat(expected_socket_path.c_str(), &buffer) == 0) {
                started_successfully = true;
                break;
            }
            if (!c.running()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!started_successfully) {
            int exit_status = 0;
            if (c.running()) {
                // Bound termination as well: boost::process::wait() can block indefinitely.
                ::kill(child_pid, SIGTERM);
                auto wait_result =
                        wait_child_exit(child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
                if (wait_result == ChildExitWaitResult::TIMEOUT ||
                    wait_result == ChildExitWaitResult::ERROR) {
                    LOG(WARNING) << "Python server start timeout and terminate timeout exceeded,"
                                 << " sending SIGKILL to pid=" << child_pid;
                    ::kill(child_pid, SIGKILL);
                    wait_result =
                            wait_child_exit(child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
                    if (wait_result == ChildExitWaitResult::TIMEOUT ||
                        wait_result == ChildExitWaitResult::ERROR) [[unlikely]] {
                        // Retain waitpid ownership until the child exits after SIGKILL.
                        enqueue_child_for_reap(child_pid);
                        c.detach();
                        return Status::InternalError(
                                "Python server start failed: process did not exit after SIGKILL, "
                                "pid={}",
                                child_pid);
                    }
                }
            } else {
                wait_child_exit(child_pid, std::chrono::milliseconds(0), &exit_status);
            }
            c.detach();
            return Status::InternalError("Python server start failed: socket file not found at {}",
                                         expected_socket_path);
        }

        *process = std::make_shared<PythonUDFProcess>(std::move(c), std::move(child_output));
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to start Python UDF server: {}", e.what());
    }
    return Status::OK();
}

#ifdef BE_TEST
static std::atomic<int> FORCED_CHILD_EXIT_TIMEOUTS {0};

static bool consume_forced_child_exit_timeout() {
    int remaining = FORCED_CHILD_EXIT_TIMEOUTS.load(std::memory_order_relaxed);
    while (remaining > 0) {
        if (FORCED_CHILD_EXIT_TIMEOUTS.compare_exchange_weak(remaining, remaining - 1,
                                                             std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}
#endif

struct BackgroundChildReaper {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<pid_t> pids;
#ifdef BE_TEST
    std::deque<pid_t> reaped_pids;
#endif
    std::thread thread;
};

static BackgroundChildReaper& background_child_reaper() {
    static auto* reaper = new BackgroundChildReaper();
    return *reaper;
}

void PythonUDFProcess::enqueue_child_for_reap(pid_t pid) {
    if (pid <= 0) [[unlikely]] {
        return;
    }

    auto& reaper = background_child_reaper();
    {
        std::lock_guard<std::mutex> lock(reaper.mutex);
        if (std::find(reaper.pids.begin(), reaper.pids.end(), pid) != reaper.pids.end()) {
            return;
        }
        reaper.pids.push_back(pid);
        if (!reaper.thread.joinable()) {
            // This thread only owns pids that were already SIGKILLed but could not be reaped within
            // the bounded shutdown wait. Such processes may be stuck in uninterruptible I/O; if they
            // exit later and nobody calls waitpid(), they stay as zombies under BE. Keep reaping them
            // asynchronously so foreground shutdown remains bounded without dropping wait ownership.
            reaper.thread = std::thread([]() {
                SCOPED_INIT_THREAD_CONTEXT();
                std::deque<pid_t> pending_pids;
                while (true) {
                    auto& reaper_ref = background_child_reaper();
                    std::unique_lock<std::mutex> lock(reaper_ref.mutex);
                    if (pending_pids.empty()) {
                        reaper_ref.cv.wait(lock,
                                           [&reaper_ref]() { return !reaper_ref.pids.empty(); });
                    } else {
                        reaper_ref.cv.wait_for(lock, BACKGROUND_REAP_INTERVAL);
                    }
                    pending_pids.insert(pending_pids.end(), reaper_ref.pids.begin(),
                                        reaper_ref.pids.end());
                    reaper_ref.pids.clear();
                    std::deque<pid_t> pids;
                    pids.swap(pending_pids);
                    lock.unlock();

                    for (pid_t pending_pid : pids) {
                        int exit_status = 0;
                        auto wait_result = PythonUDFProcess::wait_child_exit(
                                pending_pid, std::chrono::milliseconds(0), &exit_status);
                        if (wait_result == ChildExitWaitResult::EXITED ||
                            wait_result == ChildExitWaitResult::ALREADY_REAPED) {
                            LOG(INFO) << "Background reaped Python process pid=" << pending_pid;
#ifdef BE_TEST
                            {
                                std::lock_guard<std::mutex> reaped_lock(reaper_ref.mutex);
                                reaper_ref.reaped_pids.push_back(pending_pid);
                            }
                            reaper_ref.cv.notify_all();
#endif
                        } else if (wait_result == ChildExitWaitResult::TIMEOUT) {
                            pending_pids.push_back(pending_pid);
                        } else {
                            LOG(WARNING) << "Background failed to reap Python process pid="
                                         << pending_pid;
                        }
                    }
                }
            });
        }
    }
    reaper.cv.notify_one();
}

#ifdef BE_TEST
bool PythonUDFProcess::wait_background_reaped_for_test(pid_t pid,
                                                       std::chrono::milliseconds timeout) {
    auto& reaper = background_child_reaper();
    std::unique_lock<std::mutex> lock(reaper.mutex);
    return reaper.cv.wait_for(lock, timeout, [&reaper, pid]() {
        return std::find(reaper.reaped_pids.begin(), reaper.reaped_pids.end(), pid) !=
               reaper.reaped_pids.end();
    });
}

void PythonUDFProcess::force_child_exit_timeouts_for_test(int count) {
    FORCED_CHILD_EXIT_TIMEOUTS.store(count, std::memory_order_relaxed);
}
#endif

PythonUDFProcess::ChildExitWaitResult PythonUDFProcess::wait_child_exit(
        pid_t pid, std::chrono::milliseconds timeout, int* exit_status) {
#ifdef BE_TEST
    if (consume_forced_child_exit_timeout()) {
        return ChildExitWaitResult::TIMEOUT;
    }
#endif
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        pid_t ret = waitpid(pid, exit_status, WNOHANG);
        if (ret == pid) {
            return ChildExitWaitResult::EXITED;
        }
        if (ret < 0) {
            if (errno == EINTR) {
                // retry if interrupted
                continue;
            }
            // Another owner may already have observed the child exit through boost::process.
            if (errno == ECHILD) {
                return ChildExitWaitResult::ALREADY_REAPED;
            }
            LOG(WARNING) << "Failed to wait Python process pid=" << pid << ": " << strerror(errno);
            return ChildExitWaitResult::ERROR;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return ChildExitWaitResult::TIMEOUT;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void PythonUDFProcess::remove_unix_socket() {
    if (_uri.empty() || _unix_socket_file_path.empty()) return;

    if (unlink(_unix_socket_file_path.c_str()) == 0) {
        LOG(INFO) << "Successfully removed unix socket: " << _unix_socket_file_path;
        return;
    }

    if (errno == ENOENT) {
        // File does not exist, this is fine, no need to warn
        LOG(INFO) << "Unix socket not found (already removed): " << _uri;
    } else {
        LOG(WARNING) << "Failed to remove unix socket " << _uri << ": " << std::strerror(errno)
                     << " (errno=" << errno << ")";
    }
}

void PythonUDFProcess::shutdown() {
    if (!_child.valid() || _is_shutdown) return;

    int exit_status = 0;
    bool exited = !_child.running();
    bool status_available = false;
    bool already_reaped = false;
    if (!exited) {
        ::kill(_child_pid, SIGTERM);
        auto wait_result = wait_child_exit(_child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
        exited = wait_result == ChildExitWaitResult::EXITED ||
                 wait_result == ChildExitWaitResult::ALREADY_REAPED;
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    } else {
        auto wait_result = wait_child_exit(_child_pid, std::chrono::milliseconds(0), &exit_status);
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    }

    if (!exited) {
        LOG(WARNING) << "Python process did not terminate gracefully, sending SIGKILL, pid="
                     << _child_pid;
        ::kill(_child_pid, SIGKILL);
        auto wait_result = wait_child_exit(_child_pid, PROCESS_TERMINATE_TIMEOUT, &exit_status);
        exited = wait_result == ChildExitWaitResult::EXITED ||
                 wait_result == ChildExitWaitResult::ALREADY_REAPED;
        status_available = wait_result == ChildExitWaitResult::EXITED;
        already_reaped = wait_result == ChildExitWaitResult::ALREADY_REAPED;
    }
    _child.detach();

    if (!exited) [[unlikely]] {
        LOG(WARNING) << "Python process did not exit after SIGKILL, enqueue background reap, pid="
                     << _child_pid;
        enqueue_child_for_reap(_child_pid);
    } else if (already_reaped) {
        LOG(INFO) << "Python process already reaped by another owner, pid=" << _child_pid;
    } else if (!status_available) {
        LOG(INFO) << "Python process exited but exit status is unavailable, pid=" << _child_pid;
    } else {
        if (WIFSIGNALED(exit_status)) {
            LOG(INFO) << "Python process was killed by signal " << WTERMSIG(exit_status);
        } else if (WIFEXITED(exit_status)) {
            LOG(INFO) << "Python process exited normally with code: " << WEXITSTATUS(exit_status);
        } else {
            LOG(INFO) << "Python process exited";
        }
    }

    _output_stream.close();
    remove_unix_socket();
    _is_shutdown = true;
}

std::string PythonUDFProcess::to_string() const {
    return fmt::format(
            "PythonUDFProcess(child_pid={}, uri={}, "
            "unix_socket_file_path={}, is_shutdown={})",
            _child_pid, _uri, _unix_socket_file_path, _is_shutdown);
}

} // namespace doris
