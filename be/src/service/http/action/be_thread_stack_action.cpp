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

#include "service/http/action/be_thread_stack_action.h"

#include <fmt/format.h>

#ifdef __linux__
#include <fcntl.h>
#include <poll.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#endif

#include "common/logging.h"
#include "common/stack_trace.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris {

namespace {

constexpr std::string_view HEADER_TEXT = "text/plain; charset=utf-8";

#ifdef __linux__

const int STACK_TRACE_SIGNAL = SIGRTMIN;
constexpr int DEFAULT_TIMEOUT_MS = 100;
constexpr int MAX_TIMEOUT_MS = 10000;
constexpr std::string_view DEFAULT_DWARF_MODE = "FAST";

struct ThreadInfo {
    pid_t tid = 0;
    std::string name;
};

std::once_flag g_install_signal_once;
std::mutex g_collect_mutex;
std::atomic<pid_t> g_server_pid {0};
std::atomic<int> g_sequence {0};
std::atomic<int> g_data_ready_sequence {0};
std::atomic<bool> g_signal_latch {false};
StackTrace g_signal_stack_trace {NoCapture {}};
int g_notification_pipe[2] = {-1, -1};

int rt_tgsigqueueinfo(pid_t tgid, pid_t tid, int sig, siginfo_t* info) {
    return static_cast<int>(syscall(__NR_rt_tgsigqueueinfo, tgid, tid, sig, info));
}

pid_t get_current_tid() {
    return static_cast<pid_t>(syscall(SYS_gettid));
}

void stack_trace_signal_handler(int /*sig*/, siginfo_t* info, void* context) {
    auto saved_errno = errno;

    if (info == nullptr || info->si_pid != g_server_pid.load(std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    const int notification_sequence = info->si_value.sival_int;
    if (notification_sequence != g_sequence.load(std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    bool expected = false;
    if (!g_signal_latch.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    const ucontext_t signal_context = *reinterpret_cast<ucontext_t*>(context);
    g_signal_stack_trace = StackTrace(signal_context);
    g_data_ready_sequence.store(notification_sequence, std::memory_order_release);

    if (g_notification_pipe[1] >= 0) {
        ssize_t res = write(g_notification_pipe[1], &notification_sequence,
                            sizeof(notification_sequence));
        (void)res;
    }

    g_signal_latch.store(false, std::memory_order_release);
    errno = saved_errno;
}

void install_signal_handler() {
    g_server_pid.store(getpid(), std::memory_order_release);
    if (pipe2(g_notification_pipe, O_CLOEXEC | O_NONBLOCK) != 0) {
        PLOG(FATAL) << "failed to create stack trace notification pipe";
    }

    struct sigaction action {};
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_SIGINFO | SA_RESTART;
    action.sa_sigaction = stack_trace_signal_handler;
    if (sigaction(STACK_TRACE_SIGNAL, &action, nullptr) != 0) {
        PLOG(FATAL) << "failed to install BE thread stack trace signal handler";
    }
}

bool parse_int_param(const HttpRequest* req, std::string_view key, int default_value, int min_value,
                     int max_value, int* value, std::string* error) {
    const std::string& raw_value = req->param(std::string(key));
    if (raw_value.empty()) {
        *value = default_value;
        return true;
    }

    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(raw_value.c_str(), &end, 10);
    if (errno != 0 || end == raw_value.c_str() || *end != '\0') {
        *error = fmt::format("invalid {}: {}", key, raw_value);
        return false;
    }
    if (parsed < min_value || parsed > max_value) {
        *error = fmt::format("invalid {}: {}, expected range [{}, {}]", key, raw_value, min_value,
                             max_value);
        return false;
    }
    *value = static_cast<int>(parsed);
    return true;
}

std::optional<pid_t> parse_tid_filter(const HttpRequest* req, std::string* error) {
    const std::string& raw_tid = req->param("tid");
    if (raw_tid.empty()) {
        return std::nullopt;
    }

    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(raw_tid.c_str(), &end, 10);
    if (errno != 0 || end == raw_tid.c_str() || *end != '\0' || parsed <= 0) {
        *error = fmt::format("invalid tid: {}", raw_tid);
        return std::nullopt;
    }
    return static_cast<pid_t>(parsed);
}

bool parse_dwarf_mode(const HttpRequest* req, std::string* mode, std::string* error) {
    *mode = req->param("dwarf_location_info_mode");
    if (mode->empty()) {
        *mode = req->param("mode");
    }
    if (mode->empty()) {
        *mode = std::string(DEFAULT_DWARF_MODE);
        return true;
    }

    std::string lower_mode = *mode;
    std::transform(lower_mode.begin(), lower_mode.end(), lower_mode.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lower_mode == "disabled" || lower_mode == "fast" || lower_mode == "full" ||
        lower_mode == "full_with_inline") {
        *mode = lower_mode;
        return true;
    }

    *error = fmt::format(
            "invalid dwarf_location_info_mode: {}, expected one of DISABLED, FAST, "
            "FULL, FULL_WITH_INLINE",
            *mode);
    return false;
}

std::string read_thread_name(pid_t tid) {
    std::ifstream comm(fmt::format("/proc/self/task/{}/comm", tid));
    if (!comm.is_open()) {
        return "?";
    }
    std::string name;
    std::getline(comm, name);
    if (name.empty()) {
        return "?";
    }
    return name;
}

std::vector<ThreadInfo> list_threads(std::optional<pid_t> tid_filter) {
    std::vector<ThreadInfo> threads;

    if (tid_filter.has_value()) {
        threads.push_back({*tid_filter, read_thread_name(*tid_filter)});
        return threads;
    }

    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task", ec)) {
        if (ec) {
            break;
        }
        const auto filename = entry.path().filename().string();
        char* end = nullptr;
        errno = 0;
        long tid = std::strtol(filename.c_str(), &end, 10);
        if (errno != 0 || end == filename.c_str() || *end != '\0' || tid <= 0) {
            continue;
        }
        threads.push_back({static_cast<pid_t>(tid), read_thread_name(static_cast<pid_t>(tid))});
    }

    std::sort(threads.begin(), threads.end(),
              [](const ThreadInfo& lhs, const ThreadInfo& rhs) { return lhs.tid < rhs.tid; });
    return threads;
}

bool parse_hex_u64(std::string_view value, uint64_t* result) {
    std::string copy(value);
    char* end = nullptr;
    errno = 0;
    unsigned long long parsed = std::strtoull(copy.c_str(), &end, 16);
    if (errno != 0 || end == copy.c_str()) {
        return false;
    }
    *result = static_cast<uint64_t>(parsed);
    return true;
}

bool is_signal_blocked(pid_t tid) {
    std::ifstream status(fmt::format("/proc/self/task/{}/status", tid));
    if (!status.is_open()) {
        return false;
    }

    std::string line;
    while (std::getline(status, line)) {
        constexpr std::string_view prefix = "SigBlk:";
        if (!line.starts_with(prefix)) {
            continue;
        }

        uint64_t blocked_mask = 0;
        if (!parse_hex_u64(std::string_view(line).substr(prefix.size()), &blocked_mask)) {
            return false;
        }
        if (STACK_TRACE_SIGNAL <= 0 || STACK_TRACE_SIGNAL > 64) {
            return false;
        }
        return (blocked_mask & (uint64_t {1} << (STACK_TRACE_SIGNAL - 1))) != 0;
    }
    return false;
}

bool wait_for_stack_trace(int sequence, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while (true) {
        int remaining_ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                    deadline - std::chrono::steady_clock::now())
                                                    .count());
        if (remaining_ms < 0) {
            return false;
        }

        pollfd poll_fd {g_notification_pipe[0], POLLIN, 0};
        int poll_res = poll(&poll_fd, 1, remaining_ms);
        if (poll_res < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (poll_res == 0) {
            return false;
        }

        while (true) {
            int notification_sequence = 0;
            ssize_t read_res = read(g_notification_pipe[0], &notification_sequence,
                                    sizeof(notification_sequence));
            if (read_res < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                return false;
            }
            if (read_res != sizeof(notification_sequence)) {
                return false;
            }
            if (notification_sequence == sequence &&
                g_data_ready_sequence.load(std::memory_order_acquire) == sequence) {
                return true;
            }
        }
    }
}

std::string symbolize_stack_trace(const StackTrace& stack_trace, const std::string& dwarf_mode) {
    StackTrace::FramePointers frame_pointers = stack_trace.getFramePointers();
    return StackTrace::toString(frame_pointers.data(), stack_trace.getOffset(),
                                stack_trace.getSize(), dwarf_mode);
}

std::string capture_current_thread_stack(const std::string& dwarf_mode) {
    return StackTrace().toString(-3, dwarf_mode);
}

enum class CaptureStatus {
    OK,
    CURRENT_THREAD,
    SIGNAL_BLOCKED,
    THREAD_EXITED,
    SIGNAL_ERROR,
    TIMEOUT,
};

struct CaptureResult {
    CaptureStatus status = CaptureStatus::TIMEOUT;
    std::string stack;
    std::string error;
};

CaptureResult capture_thread_stack(pid_t tid, const std::string& dwarf_mode, int timeout_ms) {
    if (tid == get_current_tid()) {
        return {CaptureStatus::CURRENT_THREAD, capture_current_thread_stack(dwarf_mode), ""};
    }

    if (is_signal_blocked(tid)) {
        return {CaptureStatus::SIGNAL_BLOCKED, "", ""};
    }

    int sequence = g_sequence.fetch_add(1, std::memory_order_acq_rel) + 1;
    siginfo_t signal_info {};
    signal_info.si_code = SI_QUEUE;
    signal_info.si_pid = g_server_pid.load(std::memory_order_acquire);
    signal_info.si_uid = getuid();
    signal_info.si_value.sival_int = sequence;

    if (rt_tgsigqueueinfo(g_server_pid.load(std::memory_order_acquire), tid, STACK_TRACE_SIGNAL,
                          &signal_info) != 0) {
        if (errno == ESRCH) {
            return {CaptureStatus::THREAD_EXITED, "", ""};
        }
        return {CaptureStatus::SIGNAL_ERROR, "", std::strerror(errno)};
    }

    if (!wait_for_stack_trace(sequence, timeout_ms)) {
        return {CaptureStatus::TIMEOUT, "", ""};
    }

    return {CaptureStatus::OK, symbolize_stack_trace(g_signal_stack_trace, dwarf_mode), ""};
}

std::string status_to_string(CaptureStatus status) {
    switch (status) {
    case CaptureStatus::OK:
        return "ok";
    case CaptureStatus::CURRENT_THREAD:
        return "ok_current_thread";
    case CaptureStatus::SIGNAL_BLOCKED:
        return "signal_blocked";
    case CaptureStatus::THREAD_EXITED:
        return "thread_exited";
    case CaptureStatus::SIGNAL_ERROR:
        return "signal_error";
    case CaptureStatus::TIMEOUT:
        return "timeout";
    }
    return "unknown";
}

void append_thread_result(std::stringstream& out, const ThreadInfo& thread,
                          const CaptureResult& result) {
    out << "----- thread " << thread.tid << " (" << thread.name
        << ") status=" << status_to_string(result.status);
    if (!result.error.empty()) {
        out << " error=\"" << result.error << "\"";
    }
    out << " -----\n";

    if (result.stack.empty()) {
        out << "<no stack captured>\n\n";
        return;
    }
    out << result.stack;
    if (!result.stack.ends_with('\n')) {
        out << '\n';
    }
    out << '\n';
}

#endif // __linux__

} // namespace

void BeThreadStackAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_TEXT.data());

#ifndef __linux__
    HttpChannel::send_reply(req, HttpStatus::NOT_IMPLEMENTED,
                            "BE thread stack trace is only supported on Linux.\n");
#else
    std::call_once(g_install_signal_once, install_signal_handler);

    int timeout_ms = DEFAULT_TIMEOUT_MS;
    std::string error;
    if (!parse_int_param(req, "timeout_ms", DEFAULT_TIMEOUT_MS, 1, MAX_TIMEOUT_MS, &timeout_ms,
                         &error)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    std::optional<pid_t> tid_filter = parse_tid_filter(req, &error);
    if (!error.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    std::string dwarf_mode;
    if (!parse_dwarf_mode(req, &dwarf_mode, &error)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    std::unique_lock<std::mutex> lock(g_collect_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        HttpChannel::send_reply(req, HttpStatus::CONFLICT,
                                "another BE thread stack trace request is running\n");
        return;
    }

    auto threads = list_threads(tid_filter);
    std::stringstream out;
    out << "BE thread stack traces\n";
    out << "pid: " << g_server_pid.load(std::memory_order_acquire) << '\n';
    out << "thread_count: " << threads.size() << '\n';
    out << "timeout_ms_per_thread: " << timeout_ms << '\n';
    out << "dwarf_location_info_mode: " << dwarf_mode << "\n\n";

    int captured = 0;
    int skipped = 0;
    int timed_out = 0;

    for (const auto& thread : threads) {
        CaptureResult result = capture_thread_stack(thread.tid, dwarf_mode, timeout_ms);
        switch (result.status) {
        case CaptureStatus::OK:
        case CaptureStatus::CURRENT_THREAD:
            ++captured;
            break;
        case CaptureStatus::TIMEOUT:
            ++timed_out;
            break;
        case CaptureStatus::SIGNAL_BLOCKED:
        case CaptureStatus::THREAD_EXITED:
        case CaptureStatus::SIGNAL_ERROR:
            ++skipped;
            break;
        }
        append_thread_result(out, thread, result);
    }

    out << "summary: captured=" << captured << " skipped=" << skipped << " timed_out=" << timed_out
        << '\n';
    HttpChannel::send_reply(req, HttpStatus::OK, out.str());
#endif
}

} // namespace doris
