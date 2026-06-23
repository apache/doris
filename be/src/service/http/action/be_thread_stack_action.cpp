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
#include <ucontext.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(USE_UNWIND) && USE_UNWIND && defined(__x86_64__)
#ifndef UNW_LOCAL_ONLY
#define UNW_LOCAL_ONLY
#endif
#include <libunwind.h>
#endif
#endif

#include "common/logging.h"
#include "common/phdr_cache.h"
#include "common/stack_trace.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris {

namespace {

constexpr std::string_view HEADER_TEXT = "text/plain; charset=utf-8";

#ifdef __linux__

constexpr int STACK_TRACE_SIGNAL_OFFSET = 6;
constexpr int DEFAULT_TIMEOUT_MS = 100;
constexpr int MAX_TIMEOUT_MS = 10000;
constexpr std::string_view DEFAULT_DWARF_MODE = "FAST";

struct ThreadInfo {
    pid_t tid = 0;
    std::string name;
};

enum class SignalContextUnwindStatus {
    NOT_ATTEMPTED,
    NO_CONTEXT,
    END_OF_STACK,
    INIT_ERROR,
    GET_IP_ERROR,
    STEP_ERROR,
    FRAME_LIMIT,
    UNSUPPORTED,
};

struct SignalContextCapture {
    StackTrace::FramePointers frame_pointers {};
    size_t size = 0;
    SignalContextUnwindStatus unwind_status = SignalContextUnwindStatus::NOT_ATTEMPTED;
    int unwind_error = 0;
};

struct ThreadSyscall {
    long number = -1;
    std::string name;
};

std::once_flag g_install_signal_once;
std::mutex g_collect_mutex;
std::atomic<pid_t> g_server_pid {0};
std::atomic<int> g_sequence {0};
// The signal handler cannot allocate per-request state safely, so it publishes into one
// process-wide slot. The latch protects that slot from nested or back-to-back signals while the
// HTTP worker is still copying the previous thread's captured PCs.
std::atomic<int> g_active_sequence {0};
std::atomic<int> g_data_ready_sequence {0};
std::atomic<bool> g_signal_latch {false};
SignalContextCapture g_signal_capture;
int g_notification_pipe[2] = {-1, -1};

int rt_tgsigqueueinfo(pid_t tgid, pid_t tid, int sig, siginfo_t* info) {
    return static_cast<int>(syscall(__NR_rt_tgsigqueueinfo, tgid, tid, sig, info));
}

int stack_trace_signal() {
    static const int signal = [] {
        const int candidate = SIGRTMIN + STACK_TRACE_SIGNAL_OFFSET;
        return candidate <= SIGRTMAX ? candidate : -1;
    }();
    return signal;
}

pid_t get_current_tid() {
    return static_cast<pid_t>(syscall(SYS_gettid));
}

void append_frame(SignalContextCapture* capture, uintptr_t pc) {
    if (pc == 0 || capture->size >= capture->frame_pointers.size()) {
        return;
    }
    capture->frame_pointers[capture->size++] = reinterpret_cast<void*>(pc);
}

void capture_signal_context_unwind(const ucontext_t* context, SignalContextCapture* capture) {
    *capture = SignalContextCapture {};
    if (context == nullptr) {
        capture->unwind_status = SignalContextUnwindStatus::NO_CONTEXT;
        return;
    }

#if defined(USE_UNWIND) && USE_UNWIND && defined(__x86_64__)
    unw_cursor_t cursor;
    auto* unwind_context = reinterpret_cast<unw_context_t*>(const_cast<ucontext_t*>(context));
    int rc = unw_init_local2(&cursor, unwind_context, UNW_INIT_SIGNAL_FRAME);
    if (rc < 0) {
        capture->unwind_status = SignalContextUnwindStatus::INIT_ERROR;
        capture->unwind_error = rc;
        return;
    }

    SignalContextUnwindStatus status = SignalContextUnwindStatus::END_OF_STACK;
    int unwind_error = 0;
    while (capture->size < capture->frame_pointers.size()) {
        unw_word_t ip = 0;
        rc = unw_get_reg(&cursor, UNW_REG_IP, &ip);
        if (rc < 0) {
            status = SignalContextUnwindStatus::GET_IP_ERROR;
            unwind_error = rc;
            break;
        }
        if (ip != 0) {
            append_frame(capture, static_cast<uintptr_t>(ip));
        }

        rc = unw_step(&cursor);
        if (rc > 0) {
            continue;
        }
        if (rc == 0) {
            status = SignalContextUnwindStatus::END_OF_STACK;
            break;
        }
        status = SignalContextUnwindStatus::STEP_ERROR;
        unwind_error = rc;
        break;
    }
    if (capture->size == capture->frame_pointers.size()) {
        status = SignalContextUnwindStatus::FRAME_LIMIT;
    }

    capture->unwind_status = status;
    capture->unwind_error = unwind_error;
#else
    capture->unwind_status = SignalContextUnwindStatus::UNSUPPORTED;
#endif
}

// SAFETY: this handler only runs libunwind against the kernel-provided signal context and writes
// raw PCs into a preallocated process-wide slot. It never symbolicates, logs, allocates strings, or
// opens /proc. This is deliberately different from the old coordinator fallback: the target thread
// leaves the handler as soon as PCs are copied, so it cannot hold a loader/unwinder lock while an
// HTTP worker tries to unwind it.
void stack_trace_signal_handler(int /*sig*/, siginfo_t* info, void* context) {
    auto saved_errno = errno;

    if (info == nullptr || info->si_pid != g_server_pid.load(std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    const int notification_sequence = info->si_value.sival_int;
    if (notification_sequence != g_active_sequence.load(std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    bool expected = false;
    if (!g_signal_latch.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        errno = saved_errno;
        return;
    }

    const auto* signal_context = reinterpret_cast<const ucontext_t*>(context);
    capture_signal_context_unwind(signal_context, &g_signal_capture);
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
    if (stack_trace_signal() <= 0) {
        LOG(FATAL) << "SIGRTMIN+" << STACK_TRACE_SIGNAL_OFFSET << " exceeds SIGRTMAX";
    }

#if defined(USE_UNWIND) && USE_UNWIND && defined(__x86_64__)
    updatePHDRCache();
    if (!hasPHDRCache()) {
        LOG(FATAL) << "BE thread stack trace requires lock-free PHDR cache";
    }
#endif

    g_server_pid.store(getpid(), std::memory_order_release);
    if (pipe2(g_notification_pipe, O_CLOEXEC | O_NONBLOCK) != 0) {
        PLOG(FATAL) << "failed to create stack trace notification pipe";
    }

    struct sigaction action {};
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_SIGINFO | SA_RESTART;
    action.sa_sigaction = stack_trace_signal_handler;
    if (sigaction(stack_trace_signal(), &action, nullptr) != 0) {
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

bool parse_bool_param(const HttpRequest* req, std::string_view key, bool default_value, bool* value,
                      std::string* error) {
    const std::string& raw_value = req->param(std::string(key));
    if (raw_value.empty()) {
        *value = default_value;
        return true;
    }

    std::string lower_value = raw_value;
    std::transform(lower_value.begin(), lower_value.end(), lower_value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lower_value == "true" || lower_value == "1") {
        *value = true;
        return true;
    }
    if (lower_value == "false" || lower_value == "0") {
        *value = false;
        return true;
    }

    *error = fmt::format("invalid {}: {}, expected one of true, false, 1, 0", key, raw_value);
    return false;
}

bool parse_thread_id_token(std::string_view token, std::string_view param_name, pid_t* tid,
                           std::string* error) {
    if (token.empty()) {
        *error = fmt::format("invalid {}: empty token", param_name);
        return false;
    }
    if (!std::all_of(token.begin(), token.end(),
                     [](unsigned char c) { return std::isdigit(c) != 0; })) {
        *error = fmt::format("invalid {}: {}", param_name, token);
        return false;
    }

    std::string token_copy(token);
    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(token_copy.c_str(), &end, 10);
    if (errno != 0 || end == token_copy.c_str() || *end != '\0' || parsed <= 0 ||
        parsed > std::numeric_limits<pid_t>::max()) {
        *error = fmt::format("invalid {}: {}", param_name, token);
        return false;
    }

    *tid = static_cast<pid_t>(parsed);
    return true;
}

std::optional<std::vector<pid_t>> parse_thread_id_filter(const HttpRequest* req,
                                                         std::string* error) {
    const std::string& legacy_tid = req->param("tid");
    const std::string& thread_id = req->param("thread_id");
    if (!legacy_tid.empty() && !thread_id.empty()) {
        *error = "tid and thread_id are mutually exclusive";
        return std::nullopt;
    }

    const bool use_thread_id = !thread_id.empty();
    const std::string& raw = use_thread_id ? thread_id : legacy_tid;
    if (raw.empty()) {
        return std::nullopt;
    }

    std::vector<pid_t> tids;
    const std::string_view param_name = use_thread_id ? "thread_id" : "tid";
    size_t token_begin = 0;
    while (token_begin <= raw.size()) {
        const size_t comma = raw.find(',', token_begin);
        const size_t token_end = comma == std::string::npos ? raw.size() : comma;
        pid_t tid = 0;
        if (!parse_thread_id_token(
                    std::string_view(raw).substr(token_begin, token_end - token_begin), param_name,
                    &tid, error)) {
            return std::nullopt;
        }
        tids.push_back(tid);
        if (comma == std::string::npos) {
            break;
        }
        token_begin = comma + 1;
    }

    return tids;
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

std::vector<ThreadInfo> list_threads(const std::optional<std::vector<pid_t>>& tid_filter) {
    std::vector<ThreadInfo> threads;

    if (tid_filter.has_value()) {
        for (const pid_t tid : *tid_filter) {
            threads.push_back({tid, read_thread_name(tid)});
        }
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

bool wait_for_signal_handler_idle(int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (g_signal_latch.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

bool finish_signal_capture_and_wait(int timeout_ms) {
    g_active_sequence.store(0, std::memory_order_release);
    return wait_for_signal_handler_idle(timeout_ms);
}

bool prepare_signal_capture(int timeout_ms, std::string* error) {
    // Refresh PHDR cache before sending signals. Doing this outside the handler can safely take the
    // original glibc loader lock and picks up dynamic libraries loaded outside Doris wrappers.
    g_active_sequence.store(0, std::memory_order_release);
    if (!wait_for_signal_handler_idle(timeout_ms)) {
        *error = "previous stack trace signal handler is still running";
        return false;
    }

#if defined(USE_UNWIND) && USE_UNWIND && defined(__x86_64__)
    updatePHDRCache();
    if (!hasPHDRCache()) {
        *error = "lock-free PHDR cache is unavailable";
        return false;
    }
#else
    *error = "signal-context libunwind is unsupported on this build";
    return false;
#endif

    return true;
}

bool is_signal_blocked(pid_t tid) {
    // If the target masks the diagnostic signal, the kernel will not run our handler for that TID.
    // Detecting it up front turns an otherwise guaranteed timeout into an explicit output status.
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
        const int signal = stack_trace_signal();
        if (signal <= 0 || signal > 64) {
            return false;
        }
        return (blocked_mask & (uint64_t {1} << (signal - 1))) != 0;
    }
    return false;
}

bool parse_long_token(std::string_view token, long* result) {
    if (token.empty()) {
        return false;
    }
    std::string copy(token);
    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(copy.c_str(), &end, 10);
    if (errno != 0 || end == copy.c_str() || *end != '\0') {
        return false;
    }
    *result = parsed;
    return true;
}

std::string syscall_name(long number) {
    switch (number) {
#ifdef SYS_read
    case SYS_read:
        return "read";
#endif
#ifdef SYS_pread64
    case SYS_pread64:
        return "pread64";
#endif
#ifdef SYS_recvfrom
    case SYS_recvfrom:
        return "recvfrom";
#endif
#ifdef SYS_recvmsg
    case SYS_recvmsg:
        return "recvmsg";
#endif
#ifdef SYS_accept
    case SYS_accept:
        return "accept";
#endif
#ifdef SYS_accept4
    case SYS_accept4:
        return "accept4";
#endif
#ifdef SYS_poll
    case SYS_poll:
        return "poll";
#endif
#ifdef SYS_ppoll
    case SYS_ppoll:
        return "ppoll";
#endif
#ifdef SYS_select
    case SYS_select:
        return "select";
#endif
#ifdef SYS_pselect6
    case SYS_pselect6:
        return "pselect6";
#endif
#ifdef SYS_epoll_wait
    case SYS_epoll_wait:
        return "epoll_wait";
#endif
#ifdef SYS_epoll_pwait
    case SYS_epoll_pwait:
        return "epoll_pwait";
#endif
#ifdef SYS_epoll_pwait2
    case SYS_epoll_pwait2:
        return "epoll_pwait2";
#endif
#ifdef SYS_futex
    case SYS_futex:
        return "futex";
#endif
#ifdef SYS_nanosleep
    case SYS_nanosleep:
        return "nanosleep";
#endif
#ifdef SYS_clock_nanosleep
    case SYS_clock_nanosleep:
        return "clock_nanosleep";
#endif
    default:
        return fmt::format("syscall_{}", number);
    }
}

bool is_interrupt_sensitive_syscall(long number) {
    // This list is only used by the explicit conservative mode. The default path still samples
    // these threads so operators do not lose most blocked-worker stacks during real incidents.
    switch (number) {
#ifdef SYS_read
    case SYS_read:
#endif
#ifdef SYS_pread64
    case SYS_pread64:
#endif
#ifdef SYS_recvfrom
    case SYS_recvfrom:
#endif
#ifdef SYS_recvmsg
    case SYS_recvmsg:
#endif
#ifdef SYS_accept
    case SYS_accept:
#endif
#ifdef SYS_accept4
    case SYS_accept4:
#endif
#ifdef SYS_poll
    case SYS_poll:
#endif
#ifdef SYS_ppoll
    case SYS_ppoll:
#endif
#ifdef SYS_select
    case SYS_select:
#endif
#ifdef SYS_pselect6
    case SYS_pselect6:
#endif
#ifdef SYS_epoll_wait
    case SYS_epoll_wait:
#endif
#ifdef SYS_epoll_pwait
    case SYS_epoll_pwait:
#endif
#ifdef SYS_epoll_pwait2
    case SYS_epoll_pwait2:
#endif
#ifdef SYS_futex
    case SYS_futex:
#endif
#ifdef SYS_nanosleep
    case SYS_nanosleep:
#endif
#ifdef SYS_clock_nanosleep
    case SYS_clock_nanosleep:
#endif
        return true;
    default:
        return false;
    }
}

std::optional<ThreadSyscall> current_interrupt_sensitive_syscall(pid_t tid) {
    std::ifstream syscall_file(fmt::format("/proc/self/task/{}/syscall", tid));
    if (!syscall_file.is_open()) {
        return std::nullopt;
    }

    std::string token;
    syscall_file >> token;
    if (token.empty() || token == "running") {
        return std::nullopt;
    }

    long number = -1;
    if (!parse_long_token(token, &number) || !is_interrupt_sensitive_syscall(number)) {
        return std::nullopt;
    }
    return ThreadSyscall {.number = number, .name = syscall_name(number)};
}

std::string signal_context_unwind_status_to_string(SignalContextUnwindStatus status) {
    switch (status) {
    case SignalContextUnwindStatus::NOT_ATTEMPTED:
        return "not_attempted";
    case SignalContextUnwindStatus::NO_CONTEXT:
        return "no_context";
    case SignalContextUnwindStatus::END_OF_STACK:
        return "end_of_stack";
    case SignalContextUnwindStatus::INIT_ERROR:
        return "init_error";
    case SignalContextUnwindStatus::GET_IP_ERROR:
        return "get_ip_error";
    case SignalContextUnwindStatus::STEP_ERROR:
        return "step_error";
    case SignalContextUnwindStatus::FRAME_LIMIT:
        return "frame_limit";
    case SignalContextUnwindStatus::UNSUPPORTED:
        return "unsupported";
    }
    return "unknown";
}

std::string describe_signal_context_capture(const SignalContextCapture& capture) {
    std::stringstream out;
    out << "capture_method=signal_context_libunwind";
    out << " frames=" << capture.size;
    out << " unwind_status=" << signal_context_unwind_status_to_string(capture.unwind_status);
    if (capture.unwind_error != 0) {
        out << " unwind_error=" << capture.unwind_error;
    }
    out << " phdr_cache=" << (hasPHDRCache() ? "true" : "false");
    return out.str();
}

bool wait_for_stack_trace(int sequence, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while (true) {
        if (g_data_ready_sequence.load(std::memory_order_acquire) == sequence) {
            return true;
        }

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

std::string symbolize_stack_trace(const SignalContextCapture& capture,
                                  const std::string& dwarf_mode) {
    StackTrace::FramePointers frame_pointers = capture.frame_pointers;
    return StackTrace::toString(frame_pointers.data(), 0, capture.size, dwarf_mode);
}

std::string capture_current_thread_stack(const std::string& dwarf_mode) {
    return StackTrace().toString(-3, dwarf_mode);
}

enum class CaptureStatus {
    OK,
    CURRENT_THREAD,
    SKIPPED_BLOCKING_SYSCALL,
    SIGNAL_BLOCKED,
    THREAD_EXITED,
    SIGNAL_ERROR,
    TIMEOUT,
};

struct CaptureResult {
    CaptureStatus status = CaptureStatus::TIMEOUT;
    std::string stack;
    std::string error;
    std::string diagnostic;
};

CaptureResult capture_thread_stack(pid_t tid, const std::string& dwarf_mode, int timeout_ms,
                                   bool skip_blocking_syscalls) {
    if (tid == get_current_tid()) {
        return {CaptureStatus::CURRENT_THREAD, capture_current_thread_stack(dwarf_mode), "",
                fmt::format("capture_method=current_thread_libunwind phdr_cache={}",
                            hasPHDRCache() ? "true" : "false")};
    }

    if (skip_blocking_syscalls) {
        if (auto syscall = current_interrupt_sensitive_syscall(tid)) {
            return {CaptureStatus::SKIPPED_BLOCKING_SYSCALL, "", "",
                    fmt::format("syscall={} syscall_number={}", syscall->name, syscall->number)};
        }
    }

    if (is_signal_blocked(tid)) {
        return {CaptureStatus::SIGNAL_BLOCKED, "", "", ""};
    }

    // The handler publishes through process-global state, not per-thread storage. Waiting here
    // keeps a still-running handler from causing this TID's signal to be dropped by the latch CAS.
    if (!wait_for_signal_handler_idle(timeout_ms)) {
        return {CaptureStatus::TIMEOUT, "", "", "previous_signal_handler_still_running"};
    }

    int sequence = g_sequence.fetch_add(1, std::memory_order_acq_rel) + 1;
    g_active_sequence.store(sequence, std::memory_order_release);
    siginfo_t signal_info {};
    signal_info.si_code = SI_QUEUE;
    signal_info.si_pid = g_server_pid.load(std::memory_order_acquire);
    signal_info.si_uid = getuid();
    signal_info.si_value.sival_int = sequence;

    if (rt_tgsigqueueinfo(g_server_pid.load(std::memory_order_acquire), tid, stack_trace_signal(),
                          &signal_info) != 0) {
        g_active_sequence.store(0, std::memory_order_release);
        if (errno == ESRCH) {
            return {CaptureStatus::THREAD_EXITED, "", "", ""};
        }
        return {CaptureStatus::SIGNAL_ERROR, "", std::strerror(errno), ""};
    }

    if (!wait_for_stack_trace(sequence, timeout_ms)) {
        const bool handler_idle = finish_signal_capture_and_wait(timeout_ms);
        return {CaptureStatus::TIMEOUT, "", "",
                handler_idle ? "" : "signal_handler_release_timeout"};
    }

    SignalContextCapture capture = g_signal_capture;
    if (!finish_signal_capture_and_wait(timeout_ms)) {
        // The handler no longer waits for coordinator unwinding; a non-idle latch now means the
        // signal handler itself is stuck, which is a diagnostic side effect we must surface.
        return {CaptureStatus::TIMEOUT, "", "", "signal_handler_release_timeout"};
    }
    if (capture.size == 0 || capture.unwind_status == SignalContextUnwindStatus::NO_CONTEXT ||
        capture.unwind_status == SignalContextUnwindStatus::INIT_ERROR ||
        capture.unwind_status == SignalContextUnwindStatus::GET_IP_ERROR ||
        capture.unwind_status == SignalContextUnwindStatus::UNSUPPORTED) {
        return {CaptureStatus::SIGNAL_ERROR, "", "", describe_signal_context_capture(capture)};
    }

    return {CaptureStatus::OK, symbolize_stack_trace(capture, dwarf_mode), "",
            describe_signal_context_capture(capture)};
}

std::string status_to_string(CaptureStatus status) {
    switch (status) {
    case CaptureStatus::OK:
        return "ok";
    case CaptureStatus::CURRENT_THREAD:
        return "ok_current_thread";
    case CaptureStatus::SKIPPED_BLOCKING_SYSCALL:
        return "skipped_blocking_syscall";
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
    if (!result.diagnostic.empty()) {
        out << ' ' << result.diagnostic;
    }
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

    bool skip_blocking_syscalls = false;
    if (!parse_bool_param(req, "skip_blocking_syscalls", false, &skip_blocking_syscalls, &error)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    std::optional<std::vector<pid_t>> tid_filter = parse_thread_id_filter(req, &error);
    if (!error.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    std::string dwarf_mode;
    if (!parse_dwarf_mode(req, &dwarf_mode, &error)) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error + "\n");
        return;
    }

    // The signal handler state is process-global and intentionally single-slot, so concurrent HTTP
    // requests would corrupt capture ownership rather than merely interleave response text.
    std::unique_lock<std::mutex> lock(g_collect_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        HttpChannel::send_reply(req, HttpStatus::CONFLICT,
                                "another BE thread stack trace request is running\n");
        return;
    }

    auto threads = list_threads(tid_filter);
    if (!prepare_signal_capture(timeout_ms, &error)) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error + "\n");
        return;
    }

    std::stringstream out;
    out << "BE thread stack traces\n";
    out << "pid: " << g_server_pid.load(std::memory_order_acquire) << '\n';
    out << "service_signal: " << stack_trace_signal() << '\n';
    out << "thread_count: " << threads.size() << '\n';
    out << "timeout_ms_per_thread: " << timeout_ms << '\n';
    out << "dwarf_location_info_mode: " << dwarf_mode << '\n';
    out << "skip_blocking_syscalls: " << (skip_blocking_syscalls ? "true" : "false") << '\n';
    out << "phdr_cache: " << (hasPHDRCache() ? "true" : "false") << '\n';
    out << "signal_handler_unwinder: signal_context_libunwind\n\n";

    int captured = 0;
    int skipped = 0;
    int timed_out = 0;
    int remote_signal_attempts = 0;

    for (const auto& thread : threads) {
        CaptureResult result =
                capture_thread_stack(thread.tid, dwarf_mode, timeout_ms, skip_blocking_syscalls);
        switch (result.status) {
        case CaptureStatus::OK:
            ++remote_signal_attempts;
            ++captured;
            break;
        case CaptureStatus::CURRENT_THREAD:
            ++captured;
            break;
        case CaptureStatus::TIMEOUT:
            ++remote_signal_attempts;
            ++timed_out;
            break;
        case CaptureStatus::SIGNAL_ERROR:
            ++remote_signal_attempts;
            ++skipped;
            break;
        case CaptureStatus::SKIPPED_BLOCKING_SYSCALL:
        case CaptureStatus::SIGNAL_BLOCKED:
        case CaptureStatus::THREAD_EXITED:
            ++skipped;
            break;
        }
        append_thread_result(out, thread, result);
    }

    out << "summary: captured=" << captured << " skipped=" << skipped << " timed_out=" << timed_out
        << " remote_signal_attempts=" << remote_signal_attempts << '\n';
    HttpChannel::send_reply(req, HttpStatus::OK, out.str());
#endif
}

} // namespace doris
