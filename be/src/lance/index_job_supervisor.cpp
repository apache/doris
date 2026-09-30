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

#include "lance/index_job_supervisor.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <thrift/TConfiguration.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fmt/format.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#ifdef __linux__
#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "common/config.h"
#include "common/logging.h"
#include "lance/index_worker.h"
#include "util/blocking_queue.hpp"

// Kernel 5.3/5.9 era syscall numbers and constants that the CI glibc 2.28
// headers predate. The numbers live in the arch-independent syscall table.
#if defined(__linux__)
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC 0x63677270
#endif
#ifndef SYS_pidfd_open
#define SYS_pidfd_open 434
#endif
#ifndef SYS_close_range
#define SYS_close_range 436
#endif
#endif

namespace doris::lance {
namespace {

#ifdef __linux__
// Kernel UAPI idtype for waitid() on a pidfd; glibc defines P_PIDFD only since
// 2.36, so spell the value with the correct enum type ourselves.
#ifdef P_PIDFD
constexpr idtype_t PIDFD_IDTYPE = P_PIDFD;
#else
constexpr idtype_t PIDFD_IDTYPE = static_cast<idtype_t>(3);
#endif
#endif

// ---------------------------------------------------------------------------
// Protocol bounds (cpp_interface_contract §2). These mirror the worker-side
// defaults in index_worker.cpp; both sides cap BEFORE any allocation.
// ---------------------------------------------------------------------------
constexpr uint32_t MAX_DISPATCH_FRAME_BYTES = 512 * 1024;
constexpr uint32_t MAX_HANDSHAKE_FRAME_BYTES = 4 * 1024;
constexpr uint32_t MAX_RESULT_FRAME_BYTES = 8 * 1024;
constexpr int THRIFT_DECODE_DEPTH_LIMIT = 16;

// stderr is drained continuously into this bounded in-memory ring and is never
// logged raw; it exists only so a human can classify a failure in a debugger.
constexpr size_t STDERR_RING_BYTES = 64 * 1024;

// sanitized_message budget: <= 900 UTF-8 bytes, codepoint-safe truncation.
constexpr size_t SANITIZED_MESSAGE_MAX_BYTES = 900;
// Storage-option values shorter than this are not substring-checkable without
// collapsing every message into a false positive; the first-line defense
// (static categories + numeric identity only) is what protects them.
constexpr size_t SECRET_SUBSTRING_MIN_BYTES = 4;

// D15: dedup entries survive completion and are retained until the FE deadline
// plus this grace, so a late redelivery of the same invocation_id can never
// re-execute external mutation side effects.
constexpr int64_t DEDUP_RETAIN_AFTER_DEADLINE_MS = 10 * 60 * 1000;

constexpr int64_t CHILD_NOFILE_LIMIT = 1024;
constexpr const char* CGROUP_ROOT = "/sys/fs/cgroup";
constexpr const char* CGROUP_CONTROLLERS = "+memory +pids";

int64_t epoch_millis_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

#ifdef __linux__

// ---------------------------------------------------------------------------
// Small filesystem helpers. All operate on parent-side (supervisor) state.
// ---------------------------------------------------------------------------

std::string trim_copy(std::string text) {
    while (!text.empty() &&
           (text.back() == '\n' || text.back() == '\r' || text.back() == ' ')) {
        text.pop_back();
    }
    return text;
}

bool read_file(const std::string& path, std::string* out, size_t cap) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    std::string data;
    char chunk[1024];
    for (;;) {
        ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            ::close(fd);
            return false;
        }
        if (n == 0) {
            break;
        }
        data.append(chunk, static_cast<size_t>(n));
        if (data.size() > cap) {
            ::close(fd);
            return false;
        }
    }
    ::close(fd);
    *out = std::move(data);
    return true;
}

// Writes the full content; cgroup attribute files require a single complete
// write of the value. Returns 0 or an errno.
int write_file(const std::string& path, const std::string& content) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0) {
        return errno;
    }
    size_t done = 0;
    while (done < content.size()) {
        ssize_t n = ::write(fd, content.data() + done, content.size() - done);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            int err = errno;
            ::close(fd);
            return err;
        }
        done += static_cast<size_t>(n);
    }
    ::close(fd);
    return 0;
}

// Parses a cgroup integer attribute; "max" maps to INT64_MAX.
bool read_cgroup_int64(const std::string& path, int64_t* out) {
    std::string text;
    if (!read_file(path, &text, 128)) {
        return false;
    }
    text = trim_copy(std::move(text));
    if (text == "max") {
        *out = INT64_MAX;
        return true;
    }
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
        if (value > static_cast<uint64_t>(INT64_MAX)) {
            return false;
        }
    }
    *out = static_cast<int64_t>(value);
    return true;
}

// Reads one "<key> <value>" row from a cgroup events file (memory.events or
// cgroup.events). Returns false when the file or the key is unreadable.
bool read_cgroup_event_value(const std::string& path, const char* key, int64_t* out) {
    std::string text;
    if (!read_file(path, &text, 4096)) {
        return false;
    }
    const size_t key_len = std::strlen(key);
    size_t pos = 0;
    while (pos < text.size()) {
        size_t eol = text.find('\n', pos);
        if (eol == std::string::npos) {
            eol = text.size();
        }
        std::string line = text.substr(pos, eol - pos);
        pos = eol + 1;
        if (line.compare(0, key_len, key) == 0 && line.size() > key_len && line[key_len] == ' ') {
            int64_t value = 0;
            bool digits = false;
            for (size_t i = key_len + 1; i < line.size(); ++i) {
                if (line[i] < '0' || line[i] > '9') {
                    return false;
                }
                digits = true;
                value = value * 10 + (line[i] - '0');
            }
            if (!digits) {
                return false;
            }
            *out = value;
            return true;
        }
    }
    return false;
}

// The v2 unified-hierarchy path (the "0::" line) of /proc/<pid>/cgroup,
// returned exactly as shown (leading '/'). pid < 0 means /proc/self/cgroup.
bool read_proc_cgroup_v2_path(pid_t pid, std::string* out) {
    std::string text;
    const std::string path =
            pid < 0 ? "/proc/self/cgroup" : fmt::format("/proc/{}/cgroup", pid);
    if (!read_file(path, &text, 8192)) {
        return false;
    }
    size_t pos = 0;
    while (pos < text.size()) {
        size_t eol = text.find('\n', pos);
        if (eol == std::string::npos) {
            eol = text.size();
        }
        std::string line = text.substr(pos, eol - pos);
        pos = eol + 1;
        if (line.compare(0, 3, "0::") == 0) {
            *out = line.substr(3);
            return !out->empty();
        }
    }
    return false;
}

// Absolute cgroupfs dir -> path as shown in /proc/*/cgroup (leading '/').
std::string cgroup_rel_of_abs(const std::string& abs_dir) {
    if (abs_dir == CGROUP_ROOT) {
        return "/";
    }
    return abs_dir.substr(std::strlen(CGROUP_ROOT));
}

int64_t oom_kill_count(const std::string& cgroup_dir) {
    int64_t value = -1;
    if (!read_cgroup_event_value(cgroup_dir + "/memory.events", "oom_kill", &value)) {
        return -1;
    }
    return value;
}

bool cgroup_populated_is_zero(const std::string& cgroup_dir, bool* is_zero) {
    int64_t populated = -1;
    if (!read_cgroup_event_value(cgroup_dir + "/cgroup.events", "populated", &populated)) {
        return false;
    }
    *is_zero = (populated == 0);
    return true;
}

// Bounded wait for populated=0: descendants may take a moment to disappear
// after the leader is reaped.
bool wait_populated_zero(const std::string& cgroup_dir, int64_t timeout_ms) {
    const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        bool zero = false;
        if (cgroup_populated_is_zero(cgroup_dir, &zero) && zero) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// Removes the cgroup only once populated=0 (never before — descendants can
// escape the process group). A persistently populated group is left in place
// (its limits still bind the leak) with a static-category warning.
void remove_cgroup_dir(const std::string& cgroup_dir) {
    if (wait_populated_zero(cgroup_dir, 1000)) {
        if (::rmdir(cgroup_dir.c_str()) == 0) {
            return;
        }
    }
    LOG(WARNING) << "lance supervisor: leaving populated or busy invocation cgroup "
                 << cgroup_dir;
}

// Writes the four boundary attributes of an invocation cgroup and reads every
// one back; any mismatch is an error naming the exact file.
Status write_cgroup_limits(const std::string& cgroup_dir, int64_t memory_limit_bytes,
                           int64_t pids_max) {
    struct Limit {
        const char* file;
        int64_t value;
    };
    const Limit limits[] = {{"memory.max", memory_limit_bytes},
                            {"memory.swap.max", 0},
                            {"pids.max", pids_max},
                            {"memory.oom.group", 1}};
    for (const Limit& limit : limits) {
        const std::string path = cgroup_dir + "/" + limit.file;
        const int err = write_file(path, std::to_string(limit.value));
        if (err != 0) {
            return Status::CgroupError("cgroup limit step '{}' write failed on {}: {}", limit.file,
                                       cgroup_dir, std::strerror(err));
        }
        int64_t read_back = -1;
        if (!read_cgroup_int64(path, &read_back) || read_back != limit.value) {
            return Status::CgroupError("cgroup limit step '{}' read-back mismatch on {}",
                                       limit.file, cgroup_dir);
        }
    }
    return Status::OK();
}

// Enables +memory +pids on dir's cgroup.subtree_control and verifies the
// read-back. Returns 0 or an errno; *detail describes the failure precisely.
int enable_memory_pids(const std::string& dir, std::string* detail) {
    const std::string control = dir + "/cgroup.subtree_control";
    const int err = write_file(control, CGROUP_CONTROLLERS);
    if (err != 0) {
        *detail = fmt::format("write '{}' to {} failed: {}", CGROUP_CONTROLLERS, control,
                              std::strerror(err));
        return err;
    }
    std::string read_back;
    if (!read_file(control, &read_back, 4096) || read_back.find("memory") == std::string::npos ||
        read_back.find("pids") == std::string::npos) {
        *detail = fmt::format("read-back of {} does not show memory+pids", control);
        return EINVAL;
    }
    return 0;
}

#endif // __linux__

// ---------------------------------------------------------------------------
// Thrift-compact frame codec (parent side). Same discipline as the worker:
// length capped before allocation, decoder recursion depth-limited, exactly
// one struct per frame.
// ---------------------------------------------------------------------------

template <typename T>
bool encode_compact(const T& value, std::vector<uint8_t>* out) {
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(transport);
        value.write(&protocol);
        uint8_t* buffer = nullptr;
        uint32_t size = 0;
        transport->getBuffer(&buffer, &size);
        out->assign(buffer, buffer + size);
    } catch (...) {
        return false;
    }
    return true;
}

template <typename T>
bool decode_compact(const std::string& frame, T* out) {
    try {
        auto config = std::make_shared<apache::thrift::TConfiguration>();
        config->setMaxMessageSize(static_cast<int>(frame.size()));
        config->setRecursionLimit(THRIFT_DECODE_DEPTH_LIMIT);
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
                const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(frame.data())),
                static_cast<uint32_t>(frame.size()),
                apache::thrift::transport::TMemoryBuffer::OBSERVE, config);
        apache::thrift::protocol::TCompactProtocol protocol(
                transport, static_cast<int32_t>(frame.size()), /*container_limit=*/1024);
        out->read(&protocol);
        return transport->available_read() == 0;
    } catch (...) {
        return false;
    }
}

uint32_t frame_be32(const char* header) {
    return (static_cast<uint32_t>(static_cast<uint8_t>(header[0])) << 24) |
           (static_cast<uint32_t>(static_cast<uint8_t>(header[1])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(header[2])) << 8) |
           static_cast<uint32_t>(static_cast<uint8_t>(header[3]));
}

// Incremental length-prefixed frame assembly. The buffer can never grow past
// 4 + cap + one read chunk because poll_frame() is consulted after every read.
struct FrameAssembler {
    uint32_t cap;
    std::string buf;

    // -1 malformed (zero/oversized length), 0 need more bytes, 1 one frame out.
    int poll_frame(std::string* payload_out) {
        if (buf.size() < 4) {
            return 0;
        }
        const uint32_t length = frame_be32(buf.data());
        if (length == 0 || length > cap) {
            return -1;
        }
        if (buf.size() < 4 + static_cast<size_t>(length)) {
            return 0;
        }
        payload_out->assign(buf, 4, length);
        buf.erase(0, 4 + static_cast<size_t>(length));
        return 1;
    }
};

// Bounded stderr capture: newest bytes win once the ring is full. Never logged.
struct StderrRing {
    std::string ring;
    size_t dropped = 0;

    void push(const char* data, size_t size) {
        if (ring.size() + size <= STDERR_RING_BYTES) {
            ring.append(data, size);
            return;
        }
        const size_t overflow = ring.size() + size - STDERR_RING_BYTES;
        const size_t drop = std::min(overflow, ring.size());
        ring.erase(0, drop);
        dropped += drop;
        if (size <= STDERR_RING_BYTES - ring.size()) {
            ring.append(data, size);
        } else {
            ring.append(data + (size - (STDERR_RING_BYTES - ring.size())),
                        STDERR_RING_BYTES - ring.size());
            dropped += size - (STDERR_RING_BYTES - ring.size());
        }
    }
};

#ifdef __linux__

// ---------------------------------------------------------------------------
// Pipe + fork-barrier machinery.
// ---------------------------------------------------------------------------

struct PipePair {
    int read_fd = -1;
    int write_fd = -1;

    void close_read() {
        if (read_fd >= 0) {
            ::close(read_fd);
            read_fd = -1;
        }
    }
    void close_write() {
        if (write_fd >= 0) {
            ::close(write_fd);
            write_fd = -1;
        }
    }
    void close_all() {
        close_read();
        close_write();
    }
};

// Moves an fd above STDERR_FILENO so the child's dup2(->0/1/2) mapping can
// never clobber a not-yet-mapped source fd.
int raise_fd_above_stdio(int fd) {
    if (fd > STDERR_FILENO) {
        return fd;
    }
    const int raised = ::fcntl(fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
    ::close(fd);
    return raised;
}

bool make_pipe(PipePair* pipe) {
    int fds[2];
    if (::pipe2(fds, O_CLOEXEC) != 0) {
        return false;
    }
    pipe->read_fd = raise_fd_above_stdio(fds[0]);
    pipe->write_fd = raise_fd_above_stdio(fds[1]);
    if (pipe->read_fd < 0 || pipe->write_fd < 0) {
        pipe->close_all();
        return false;
    }
    return true;
}

bool set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// Kernel < 5.9 fallback for close_range: raw getdents64 over /proc/self/fd.
// Runs in the forked child before exec, so it must not allocate (no DIR*).
struct Dirent64 {
    uint64_t d_ino;
    int64_t d_off;
    unsigned short d_reclen;
    unsigned char d_type;
    // Kernel linux_dirent64 layout; the name runs past the declared bound, so
    // size 1 keeps -Wpedantic happy (flexible array members are C99-only).
    char d_name[1];
};

void close_all_fds_from(unsigned int first) {
    if (static_cast<long>(::syscall(SYS_close_range, first, ~0u, 0)) == 0) {
        return;
    }
    const int dir_fd = ::open("/proc/self/fd", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0) {
        return; // still exec: the pipes are the only fds that matter and they
                // were placed onto 0/1/2; everything else of ours is CLOEXEC
    }
    alignas(8) char buffer[1024];
    for (;;) {
        const long count = ::syscall(SYS_getdents64, dir_fd, buffer, sizeof(buffer));
        if (count <= 0) {
            break;
        }
        for (long offset = 0; offset < count;) {
            const auto* dirent = reinterpret_cast<const Dirent64*>(buffer + offset);
            offset += dirent->d_reclen;
            unsigned int fd = 0;
            bool digits = false;
            for (const char* p = dirent->d_name; *p >= '0' && *p <= '9' && fd < 1000000; ++p) {
                fd = fd * 10 + static_cast<unsigned int>(*p - '0');
                digits = true;
            }
            if (digits && fd >= first && static_cast<int>(fd) != dir_fd) {
                ::close(static_cast<int>(fd));
            }
        }
    }
    ::close(dir_fd);
}

// Everything the child needs, fully built before fork(): the post-fork child
// of a multi-threaded process may only use async-signal-safe operations until
// execve (no heap, no logging, no locks).
struct ChildLaunchPlan {
    int barrier_read_fd;
    int stdin_read_fd;
    int stdout_write_fd;
    int stderr_write_fd;
    // The parent's ends of all four pipes: the child closes them on entry so
    // the barrier's EOF semantics hold (a parent death or abort reaches the
    // blocked reader) and the child can never write to its own stdin pipe.
    int parent_side_fds[4];
    pid_t expected_ppid;
    int64_t as_limit_bytes;
    int64_t cpu_limit_seconds;
    const std::vector<char*>* envp;
    // Exec target. Production: "/proc/self/exe" with kDefaultWorkerArgv.
    const char* exec_path;
    const char* const* exec_argv;
};

// Production worker argv (the UT exec-override seam substitutes its own).
static const char* const kDefaultWorkerArgv[] = {"doris_be", "--lance-worker", nullptr};

// Child path: arm PDEATHSIG and immediately re-check the parent (closing the
// arm-after-death race), block on the fork barrier (EOF/error => _exit, so a
// child whose parent failed the cgroup migration provably never execs), then
// the hygiene layer, then exec. Any failure is _exit(127).
[[noreturn]] void child_exec_or_die(const ChildLaunchPlan& plan) {
    ::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
    if (::getppid() != plan.expected_ppid) {
        ::_exit(127);
    }
    for (const int fd : plan.parent_side_fds) {
        if (fd >= 0) {
            ::close(fd);
        }
    }
    uint8_t release_byte = 0;
    for (;;) {
        const ssize_t n = ::read(plan.barrier_read_fd, &release_byte, 1);
        if (n == 1) {
            break;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        ::_exit(127); // EOF or error: never launched
    }
    ::setpgid(0, 0); // defensive; the parent already made us a group leader
    // Credentials must never reach a core file (pipe-style core_pattern hosts
    // ignore RLIMIT_CORE); survives the plain self-exec and is re-armed by the
    // worker library entry as a second line.
    ::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
    ::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
    struct rlimit limit;
    limit.rlim_cur = limit.rlim_max = 0;
    if (::setrlimit(RLIMIT_CORE, &limit) != 0) {
        ::_exit(127);
    }
    limit.rlim_cur = limit.rlim_max = static_cast<rlim_t>(CHILD_NOFILE_LIMIT);
    if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) {
        ::_exit(127);
    }
    limit.rlim_cur = limit.rlim_max = static_cast<rlim_t>(plan.as_limit_bytes);
    if (::setrlimit(RLIMIT_AS, &limit) != 0) {
        ::_exit(127);
    }
    limit.rlim_cur = limit.rlim_max = static_cast<rlim_t>(plan.cpu_limit_seconds);
    if (::setrlimit(RLIMIT_CPU, &limit) != 0) {
        ::_exit(127);
    }
    // The supervisor executor threads block SIGPIPE; the worker must not
    // inherit that mask across exec.
    sigset_t empty_mask;
    ::sigemptyset(&empty_mask);
    ::sigprocmask(SIG_SETMASK, &empty_mask, nullptr);
    if (::dup2(plan.stdin_read_fd, STDIN_FILENO) < 0) {
        ::_exit(127);
    }
    if (::dup2(plan.stdout_write_fd, STDOUT_FILENO) < 0) {
        ::_exit(127);
    }
    if (::dup2(plan.stderr_write_fd, STDERR_FILENO) < 0) {
        ::_exit(127);
    }
    close_all_fds_from(STDERR_FILENO + 1);
    ::execve(plan.exec_path, const_cast<char* const*>(plan.exec_argv), plan.envp->data());
    ::_exit(127);
}

// Validates one directory path component for the controlled LD_LIBRARY_PATH:
// absolute, no control chars, no ':' (would split the search list), exists.
bool validated_dir(const std::string& dir) {
    if (dir.empty() || dir.front() != '/') {
        return false;
    }
    for (const unsigned char c : dir) {
        if (c < 0x20 || c == 0x7f || c == ':') {
            return false;
        }
    }
    struct stat st;
    return ::stat(dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// Builds the controlled environment whitelist (D1/D4): LD_LIBRARY_PATH is
// derived ONLY from the resolved doris_be install lib dir plus an optional
// validated $JAVA_HOME/lib/server; LANG/LC_ALL/TZ pass through when sane;
// RUST_BACKTRACE is pinned off. Never inherits the caller's LD_LIBRARY_PATH.
Status build_child_env(std::vector<std::string>* storage, std::vector<char*>* envp) {
    char exe_path[4096];
    const ssize_t exe_len = ::readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (exe_len <= 0 || static_cast<size_t>(exe_len) >= sizeof(exe_path) - 1) {
        return Status::InternalError("lance worker launch: cannot resolve /proc/self/exe");
    }
    exe_path[exe_len] = '\0';
    const std::string exe(exe_path, static_cast<size_t>(exe_len));
    const size_t slash = exe.rfind('/');
    if (slash == std::string::npos || slash == 0) {
        return Status::InternalError("lance worker launch: unexpected exe path shape");
    }
    const std::string lib_dir = exe.substr(0, slash);
    if (!validated_dir(lib_dir)) {
        return Status::InternalError("lance worker launch: BE lib dir failed validation");
    }
    std::string ld_library_path = lib_dir;
    if (const char* java_home = std::getenv("JAVA_HOME")) {
        if (*java_home != '\0') {
            const std::string server_dir = std::string(java_home) + "/lib/server";
            if (validated_dir(server_dir)) {
                ld_library_path += ":" + server_dir;
            }
        }
    }
    storage->push_back("LD_LIBRARY_PATH=" + ld_library_path);
    // The supervisor's own pid, so the worker's exec-side PR_SET_PDEATHSIG
    // re-arm can recheck getppid() exactly (file-capabilities execve clears
    // the pre-exec arm; see run_index_worker). Fully controlled, never
    // inherited.
    storage->push_back(fmt::format("{}={}", WORKER_EXPECTED_PPID_ENV, ::getpid()));
    for (const char* name : {"LANG", "LC_ALL", "TZ"}) {
        if (const char* value = std::getenv(name)) {
            const size_t len = std::strlen(value);
            bool sane = len > 0 && len <= 128;
            for (size_t i = 0; sane && i < len; ++i) {
                const unsigned char c = static_cast<unsigned char>(value[i]);
                sane = c >= 0x20 && c != 0x7f;
            }
            if (sane) {
                storage->push_back(fmt::format("{}={}", name, value));
            }
        }
    }
    storage->push_back("RUST_BACKTRACE=0");
    envp->reserve(storage->size() + 1);
    for (std::string& entry : *storage) {
        envp->push_back(entry.data());
    }
    envp->push_back(nullptr);
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Reaping evidence. pidfd is the primary wait primitive (immune to the CDC
// manager's process-wide SIGCHLD reaper); waitid(P_PIDFD) consumes the exact
// child's status, and ECHILD means someone else (CDC) consumed it first.
// ---------------------------------------------------------------------------

struct ReapEvidence {
    bool pidfd_terminated = false; // poll observed the pidfd becoming readable
    bool reaped_exact = false;     // we consumed the exact child's exit status
    bool wait_echild = false;      // the status was already consumed elsewhere
    int wait_kind = 0;   // CLD_EXITED / CLD_KILLED when reaped_exact
    int wait_status = 0; // exit code or signal number when reaped_exact
};

// Bounded wait for child termination, then a non-blocking status consume.
void reap_child_bounded(int pidfd, pid_t pid, int64_t timeout_ms, ReapEvidence* evidence) {
    const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    if (pidfd >= 0) {
        struct pollfd pfd;
        pfd.fd = pidfd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        for (;;) {
            const int n = ::poll(&pfd, 1, 50);
            if (n > 0 && (pfd.revents & (POLLIN | POLLERR)) != 0) {
                evidence->pidfd_terminated = true;
                break;
            }
            if (n < 0 && errno != EINTR) {
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            pfd.revents = 0;
        }
        siginfo_t info;
        std::memset(&info, 0, sizeof(info));
        if (::waitid(PIDFD_IDTYPE, static_cast<id_t>(pidfd), &info, WEXITED | WNOHANG) == 0) {
            if (info.si_pid != 0) {
                evidence->reaped_exact = true;
                evidence->wait_kind = info.si_code;
                evidence->wait_status = info.si_status;
            }
        } else if (errno == ECHILD) {
            evidence->wait_echild = true;
        }
    } else {
        // No pidfd (early launch failure): the child was already SIGKILLed by
        // the caller. Poll waitpid(WNOHANG) directly — kill(pid, 0) alone keeps
        // succeeding on a zombie, so a kill-only loop would spin the whole
        // timeout on every preflight and every pre-pidfd launch failure.
        for (;;) {
            int status = 0;
            pid_t reaped;
            do {
                reaped = ::waitpid(pid, &status, WNOHANG);
            } while (reaped < 0 && errno == EINTR);
            if (reaped == pid) {
                evidence->reaped_exact = true;
                evidence->pidfd_terminated = true;
                if (WIFEXITED(status)) {
                    evidence->wait_kind = CLD_EXITED;
                    evidence->wait_status = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    evidence->wait_kind = CLD_KILLED;
                    evidence->wait_status = WTERMSIG(status);
                }
                break;
            }
            if (reaped < 0 && errno == ECHILD) {
                // Someone else (the CDC reaper) consumed the status; a reaped
                // child is provably terminated.
                evidence->wait_echild = true;
                evidence->pidfd_terminated = true;
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
}

#endif // __linux__

#ifdef __linux__

// ---------------------------------------------------------------------------
// launch_child: fork behind a barrier, migrate into the invocation cgroup,
// verify membership, and only then release the child to exec. Every failure
// before the barrier release is a provable NEVER_LAUNCHED rejection (the
// child cannot have exec'd: the release byte never left us).
// ---------------------------------------------------------------------------

struct Launch {
    pid_t pid = -1;
    int pidfd = -1;
    int stdin_write_fd = -1;
    int stdout_read_fd = -1;
    int stderr_read_fd = -1;

    void close_all() {
        if (pidfd >= 0) {
            ::close(pidfd);
            pidfd = -1;
        }
        if (stdin_write_fd >= 0) {
            ::close(stdin_write_fd);
            stdin_write_fd = -1;
        }
        if (stdout_read_fd >= 0) {
            ::close(stdout_read_fd);
            stdout_read_fd = -1;
        }
        if (stderr_read_fd >= 0) {
            ::close(stderr_read_fd);
            stderr_read_fd = -1;
        }
    }
};

Status launch_child(const ChildLaunchPlan& plan, PipePair* stdin_pipe, PipePair* stdout_pipe,
                    PipePair* stderr_pipe, PipePair* barrier_pipe,
                    const std::string& cgroup_dir, const std::string& cgroup_rel,
                    bool force_migration_failure, Launch* launch, const char** fail_category) {
    auto cleanup_unlaunched = [&]() {
        stdin_pipe->close_all();
        stdout_pipe->close_all();
        stderr_pipe->close_all();
        barrier_pipe->close_all();
        remove_cgroup_dir(cgroup_dir);
    };
    const pid_t child = ::fork();
    if (child < 0) {
        cleanup_unlaunched();
        *fail_category = "fork failed";
        return Status::CgroupError("lance worker fork failed: {}", std::strerror(errno));
    }
    if (child == 0) {
        child_exec_or_die(plan); // never returns
    }

    // Parent: the child holds duplicates of our pipe ends; drop them.
    stdin_pipe->close_read();
    stdout_pipe->close_write();
    stderr_pipe->close_write();
    barrier_pipe->close_read();

    int pidfd = -1;
    auto fail = [&](const char* category) {
        // Direct kill only: the child may still be in OUR process group here,
        // so kill(-pid) could take the whole BE down with it.
        ::kill(child, SIGKILL);
        barrier_pipe->close_write(); // EOF for a still-blocked child
        ReapEvidence evidence;
        reap_child_bounded(pidfd, child, 5000, &evidence);
        if (pidfd >= 0) {
            ::close(pidfd);
        }
        stdin_pipe->close_all();
        stdout_pipe->close_all();
        stderr_pipe->close_all();
        remove_cgroup_dir(cgroup_dir);
        *fail_category = category;
        return Status::CgroupError("lance worker launch failed at step: {}", category);
    };

    // Make the child a process-group leader while it provably has not exec'd
    // (a post-exec setpgid from here would fail with EACCES). From this point
    // on kill(-child) is safe and reaps the whole worker tree.
    if (::setpgid(child, child) != 0) {
        return fail("process-group setup failed");
    }
    pidfd = static_cast<int>(::syscall(SYS_pidfd_open, child, 0));
    if (pidfd < 0) {
        return fail("pidfd unavailable");
    }
    const int migrate_err = force_migration_failure
                                    ? EPERM
                                    : write_file(cgroup_dir + "/cgroup.procs",
                                                 std::to_string(child));
    if (migrate_err != 0) {
        return fail("cgroup migration failed");
    }
    // Read back the membership ourselves; never trust the child to be where we
    // put it. The child is still blocked on the barrier (still dumpable), so
    // /proc/<pid>/cgroup is readable by us.
    std::string actual_rel;
    if (!read_proc_cgroup_v2_path(child, &actual_rel) || actual_rel != cgroup_rel) {
        return fail("cgroup membership verification failed");
    }
    const uint8_t release_byte = 0x42;
    ssize_t written;
    do {
        written = ::write(barrier_pipe->write_fd, &release_byte, 1);
    } while (written < 0 && errno == EINTR);
    barrier_pipe->close_write();
    if (written != 1) {
        return fail("barrier release failed");
    }

    launch->pid = child;
    launch->pidfd = pidfd;
    launch->stdin_write_fd = stdin_pipe->write_fd;
    launch->stdout_read_fd = stdout_pipe->read_fd;
    launch->stderr_read_fd = stderr_pipe->read_fd;
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Handshake validation (D2 layer 1): magic/version first, then every
// self-reported value cross-checked against OUR OWN read-back of the
// invocation cgroup and the exact rlimit values the child was given. The
// report is never trusted on its own.
// ---------------------------------------------------------------------------

bool validate_handshake(const std::string& frame, const std::string& cgroup_rel,
                        const std::string& cgroup_dir, int64_t memory_limit_bytes, int64_t pids_max,
                        int64_t as_limit_bytes, int64_t cpu_limit_seconds,
                        const char** violation_category) {
    TLanceIndexWorkerHandshake handshake;
    if (!decode_compact(frame, &handshake)) {
        *violation_category = "undecodable handshake frame";
        return false;
    }
    if (handshake.protocol_magic != HANDSHAKE_PROTOCOL_MAGIC ||
        handshake.protocol_version != HANDSHAKE_PROTOCOL_VERSION) {
        *violation_category = "handshake magic or version mismatch";
        return false;
    }
    if (handshake.cgroup_path != cgroup_rel) {
        *violation_category = "handshake cgroup path mismatch";
        return false;
    }
    int64_t memory_read_back = -1;
    if (!read_cgroup_int64(cgroup_dir + "/memory.max", &memory_read_back) ||
        memory_read_back != memory_limit_bytes || handshake.memory_max_bytes != memory_read_back) {
        *violation_category = "handshake memory limit mismatch";
        return false;
    }
    int64_t pids_read_back = -1;
    if (!read_cgroup_int64(cgroup_dir + "/pids.max", &pids_read_back) ||
        pids_read_back != pids_max || handshake.pids_max != pids_read_back) {
        *violation_category = "handshake pids limit mismatch";
        return false;
    }
    if (handshake.rlimit_as_bytes != as_limit_bytes ||
        handshake.rlimit_cpu_seconds != cpu_limit_seconds ||
        handshake.rlimit_nofile != CHILD_NOFILE_LIMIT || handshake.rlimit_core != 0) {
        *violation_category = "handshake rlimit mismatch";
        return false;
    }
    return true;
}

bool identity_matches(const TLanceIndexJobReport& report, const TLanceIndexJobDispatch& dispatch) {
    return report.job_id == dispatch.job_id &&
           report.dispatch_revision == dispatch.dispatch_revision &&
           report.invocation_id == dispatch.invocation_id &&
           report.be_process_epoch == dispatch.be_process_epoch;
}

// Wire-domain membership of TLanceIndexJobResultCode (the thrift-compact decode
// of an i32 enum field performs no membership check). The real worker only ever
// emits codes from wire_result_code_for_native_error, so a complete
// identity-matched frame with an out-of-domain code means a corrupted or
// drifted worker: the frame is a protocol violation, never a trusted result.
bool is_known_wire_result_code(TLanceIndexJobResultCode::type code) {
    switch (code) {
    case TLanceIndexJobResultCode::PRE_INVOCATION_STALE_ADMISSION:
    case TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT:
    case TLanceIndexJobResultCode::PRE_INVOCATION_CREDENTIAL_EXPIRED:
    case TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED:
    case TLanceIndexJobResultCode::NATIVE_OK:
    case TLanceIndexJobResultCode::NATIVE_COMMIT_CONFLICT:
    case TLanceIndexJobResultCode::NATIVE_NOT_FOUND:
    case TLanceIndexJobResultCode::NATIVE_INVALID_ARGUMENT:
    case TLanceIndexJobResultCode::NATIVE_NOT_SUPPORTED:
    case TLanceIndexJobResultCode::NATIVE_INDEX:
    case TLanceIndexJobResultCode::NATIVE_IO:
    case TLanceIndexJobResultCode::NATIVE_INTERNAL:
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Preflight probes (D2 layer 3): dummy-child migration under a fork barrier,
// proving the whole delegation chain before the first real invocation.
// ---------------------------------------------------------------------------

Status probe_dummy_migration(const std::string& cgroup_dir, const std::string& cgroup_rel) {
    PipePair barrier;
    if (!make_pipe(&barrier)) {
        return Status::CgroupError("preflight step 'barrier' pipe creation failed: {}",
                                   std::strerror(errno));
    }
    const pid_t parent_pid = ::getpid();
    const pid_t child = ::fork();
    if (child < 0) {
        barrier.close_all();
        return Status::CgroupError("preflight step 'fork' failed: {}", std::strerror(errno));
    }
    if (child == 0) {
        // Dummy child: async-signal-safe only. Arm + recheck, drop the write
        // end (so EOF reaches us), then block until the parent releases us by
        // closing the pipe.
        ::prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
        if (::getppid() != parent_pid) {
            ::_exit(127);
        }
        ::close(barrier.write_fd);
        uint8_t byte = 0;
        ssize_t n;
        do {
            n = ::read(barrier.read_fd, &byte, 1);
        } while (n < 0 && errno == EINTR);
        ::_exit(0);
    }
    barrier.close_read();
    // Capability probe (kernel >= 5.9): the supervisor's reaping evidence rests
    // on pidfd_open (5.3) AND waitid(P_PIDFD) (5.9). Probing both here — instead
    // of discovering an EINVAL at the first real invocation — keeps the
    // no-soft-fallback rule honest: an unsupported kernel fails the preflight
    // and rejects every submission.
    const int pidfd = static_cast<int>(::syscall(SYS_pidfd_open, child, 0));
    Status status = Status::OK();
    if (pidfd < 0) {
        status = Status::CgroupError(
                "preflight step 'pidfd' failed: pidfd_open is unavailable (kernel >= 5.3 "
                "required): {}",
                std::strerror(errno));
    }
    if (status.ok()) {
        const int migrate_err = write_file(cgroup_dir + "/cgroup.procs", std::to_string(child));
        if (migrate_err != 0) {
            status = Status::CgroupError(
                    "preflight step 'migrate' failed writing cgroup.procs in {}: {} (class: "
                    "no-delegation or nsdelegate common-ancestor)",
                    cgroup_dir, std::strerror(migrate_err));
        } else {
            std::string actual_rel;
            if (!read_proc_cgroup_v2_path(child, &actual_rel) || actual_rel != cgroup_rel) {
                status = Status::CgroupError(
                        "preflight step 'membership' read-back mismatch in {}", cgroup_dir);
            }
        }
    }
    if (!status.ok()) {
        ::kill(child, SIGKILL);
    }
    barrier.close_write(); // EOF releases the dummy child
    ReapEvidence evidence;
    reap_child_bounded(pidfd, child, 5000, &evidence);
    if (pidfd >= 0) {
        ::close(pidfd);
    }
    if (status.ok() && !evidence.reaped_exact && !evidence.wait_echild) {
        // The EOF-released dummy child is dead (the pidfd poll observed it), so
        // reaching here without reaped_exact/ECHILD means the waitid(P_PIDFD)
        // call itself failed — EINVAL on kernels before 5.9.
        status = Status::CgroupError(
                "preflight step 'reap' failed: waitid(P_PIDFD) is unsupported on this kernel "
                "(kernel >= 5.9 required for termination proofs)");
    }
    return status;
}

// Best-effort reclaim of EMPTY probe leftovers of this pid (a BE SIGKILLed
// mid-probe cannot remove its own probe group). Never touches other pids'
// groups; a populated or busy leftover is left in place — the probe below runs
// under a fresh unique name, so a stubborn leftover is noise, not a failure.
void reclaim_preflight_leftovers(const std::string& parent, const std::string& base_name) {
    DIR* dir = ::opendir(parent.c_str());
    if (dir == nullptr) {
        return;
    }
    while (const struct dirent* entry = ::readdir(dir)) {
        const std::string name = entry->d_name;
        if (name.compare(0, base_name.size(), base_name) == 0) {
            ::rmdir((parent + "/" + name).c_str()); // empty leftovers only
        }
    }
    ::closedir(dir);
}

Status preflight_group_probe(const std::string& parent) {
    // Unique per probe (pid + monotonic nanos): a pid-recycled or restarting BE
    // can never collide with its own leftover, and a stale same-pid leftover is
    // reclaimed above rather than misread as a delegation failure.
    const std::string base = "lance-preflight-" + std::to_string(::getpid());
    reclaim_preflight_leftovers(parent, base);
    const int64_t unique =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
    const std::string dir = parent + "/" + base + "-" + std::to_string(unique);
    if (::mkdir(dir.c_str(), 0755) != 0) {
        return Status::CgroupError("preflight step 'mkdir' failed on {}: {}", dir,
                                   std::strerror(errno));
    }
    auto fail = [&dir](const Status& status) {
        remove_cgroup_dir(dir);
        return status;
    };
    // The controller attribute files appear in a child group only when the
    // parent's subtree_control really delegated them.
    for (const char* file : {"memory.max", "memory.swap.max", "pids.max", "memory.oom.group",
                             "cgroup.procs", "cgroup.events"}) {
        if (::access((dir + "/" + file).c_str(), F_OK) != 0) {
            return fail(Status::CgroupError(
                    "preflight step 'controllers': {} missing in the child group (class: "
                    "no-delegation)",
                    file));
        }
    }
    Status status = write_cgroup_limits(dir, config::lance_index_worker_memory_limit_bytes,
                                        config::lance_index_worker_pids_max);
    if (!status.ok()) {
        return fail(status);
    }
    status = probe_dummy_migration(dir, cgroup_rel_of_abs(dir));
    if (!status.ok()) {
        return fail(status);
    }
    if (!wait_populated_zero(dir, 1000)) {
        return fail(Status::CgroupError("preflight step 'populated' never reached zero in {}",
                                        dir));
    }
    if (::rmdir(dir.c_str()) != 0) {
        return Status::CgroupError("preflight step 'rmdir' failed on {}: {}", dir,
                                   std::strerror(errno));
    }
    return Status::OK();
}

#endif // __linux__

#ifdef __linux__

// ---------------------------------------------------------------------------
// The supervision event loop: one poll set over pidfd + stdin(write) + stdout
// + stderr with the wall-clock deadline, the bounded two-frame protocol, and
// the kill-escalation/reaping discipline. Produces a Supervision; the caller
// maps it onto (result | termination | silence) x proof.
// ---------------------------------------------------------------------------

struct LaunchLimits {
    int64_t memory_limit_bytes;
    int64_t pids_max;
    int64_t as_limit_bytes;
    int64_t cpu_limit_seconds;
    int64_t term_grace_seconds;
};

struct Supervision {
    // Ending classification.
    bool got_result = false;          // complete, identity-matched result frame
    TLanceIndexJobReport report;
    bool pre_ffi_violation = false;   // handshake-stage protocol violation
    const char* failure_category = nullptr; // static string only
    bool deadline_expired = false;
    bool child_exit_observed = false;
    // Evidence.
    ReapEvidence reap;
    bool populated_zero = false;
    int64_t oom_kill_delta = -1;

    // CHILD_REAPED requires the exact child provably reaped (by us, or by the
    // CDC reaper with pidfd-termination evidence) AND no surviving descendants
    // in the invocation cgroup (D16).
    bool child_reaped_proof() const {
        return (reap.reaped_exact || (reap.wait_echild &&
                                      (reap.pidfd_terminated || child_exit_observed))) &&
               populated_zero;
    }
};

// Bounded wait for the pidfd to signal exit; returns true when observed.
bool wait_pidfd_exit(int pidfd, int64_t timeout_ms) {
    const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        struct pollfd pfd;
        pfd.fd = pidfd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        const int64_t left = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     deadline - std::chrono::steady_clock::now())
                                     .count();
        if (left <= 0) {
            return false;
        }
        const int n = ::poll(&pfd, 1, static_cast<int>(std::min<int64_t>(left, 200)));
        if (n > 0 && (pfd.revents & (POLLIN | POLLERR)) != 0) {
            return true;
        }
        if (n < 0 && errno != EINTR) {
            return false;
        }
    }
}

Supervision supervise_child(const TLanceIndexJobDispatch& dispatch, const std::string& cgroup_dir,
                            const std::string& cgroup_rel, const std::string& dispatch_frame,
                            int64_t wall_seconds, const LaunchLimits& limits,
                            int64_t oom_kill_baseline, Launch* launch) {
    Supervision sup;
    const pid_t pid = launch->pid;
    if (!set_nonblocking(launch->stdin_write_fd) ||
        !set_nonblocking(launch->stdout_read_fd) ||
        !set_nonblocking(launch->stderr_read_fd)) {
        ::kill(-pid, SIGKILL);
        ::kill(pid, SIGKILL);
        sup.failure_category = "supervisor fd setup failure";
    }

    enum class Phase { WRITE_DISPATCH, READ_HANDSHAKE, READ_RESULT };
    Phase phase = Phase::WRITE_DISPATCH;
    FrameAssembler handshake {MAX_HANDSHAKE_FRAME_BYTES, {}};
    FrameAssembler result {MAX_RESULT_FRAME_BYTES, {}};
    StderrRing stderr_ring;
    size_t frame_written = 0;
    bool stdin_open = true;
    bool stdout_open = true;
    bool stderr_open = true;
    const auto wall_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(wall_seconds);

    // Validates a completed result frame payload; on success the supervision
    // carries the trusted report.
    auto finish_result = [&sup, &dispatch](const std::string& payload) {
        TLanceIndexJobReport decoded;
        if (!decode_compact(payload, &decoded)) {
            sup.failure_category = "undecodable result frame";
            return;
        }
        if (!identity_matches(decoded, dispatch)) {
            // Identity mismatch after acceptance is never a result (D5/D6):
            // fall into the termination-proof discipline.
            sup.failure_category = "result identity mismatch";
            return;
        }
        if (!is_known_wire_result_code(decoded.result_code)) {
            // Out-of-domain result code (corrupted/drifted worker): not a
            // trusted result, and NOT a pre-FFI violation either — the frame
            // arrived past a valid handshake, so a mutation may have committed;
            // the termination-proof discipline converges it.
            sup.failure_category = "result code outside the wire domain";
            return;
        }
        sup.report = std::move(decoded);
        sup.got_result = true;
    };

    while (!sup.got_result && sup.failure_category == nullptr) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= wall_deadline) {
            sup.deadline_expired = true;
            break;
        }
        const int64_t ms_left = std::chrono::duration_cast<std::chrono::milliseconds>(wall_deadline -
                                                                                      now)
                                        .count();
        struct pollfd fds[4];
        nfds_t nfds = 0;
        fds[nfds] = {launch->pidfd, POLLIN, 0};
        ++nfds;
        // stdin is a write-side concern of its own: an out-of-order worker may
        // complete its handshake (phase already READ_RESULT) while a large
        // dispatch frame is still draining, so the write must NOT be gated on
        // the read-side phase — stdin_open alone tracks it.
        const bool want_stdin = stdin_open;
        if (want_stdin) {
            fds[nfds] = {launch->stdin_write_fd, POLLOUT, 0};
            ++nfds;
        }
        if (stdout_open) {
            fds[nfds] = {launch->stdout_read_fd, POLLIN, 0};
            ++nfds;
        }
        if (stderr_open) {
            fds[nfds] = {launch->stderr_read_fd, POLLIN, 0};
            ++nfds;
        }
        const int ready = ::poll(fds, nfds, static_cast<int>(std::clamp<int64_t>(ms_left, 1, 1000)));
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            sup.failure_category = "supervisor poll failure";
            break;
        }
        if (ready == 0) {
            continue;
        }
        nfds_t index = 0;
        const short pidfd_events = fds[index++].revents;
        const short stdin_events = want_stdin ? fds[index++].revents : static_cast<short>(0);
        const short stdout_events = stdout_open ? fds[index++].revents : static_cast<short>(0);
        const short stderr_events = stderr_open ? fds[index++].revents : static_cast<short>(0);

        if ((pidfd_events & (POLLIN | POLLERR)) != 0) {
            sup.child_exit_observed = true;
        }

        // stdin: exactly one dispatch frame, then the write end closes.
        if (want_stdin && (stdin_events & (POLLOUT | POLLERR | POLLHUP)) != 0) {
            if ((stdin_events & POLLOUT) != 0 && frame_written < dispatch_frame.size()) {
                const ssize_t n = ::write(launch->stdin_write_fd,
                                          dispatch_frame.data() + frame_written,
                                          dispatch_frame.size() - frame_written);
                if (n > 0) {
                    frame_written += static_cast<size_t>(n);
                } else if (n < 0 && errno != EAGAIN && errno != EINTR) {
                    // EPIPE included: the child is gone; the reads below classify.
                    frame_written = dispatch_frame.size();
                }
            }
            if (frame_written >= dispatch_frame.size() ||
                (stdin_events & (POLLERR | POLLHUP)) != 0) {
                ::close(launch->stdin_write_fd);
                launch->stdin_write_fd = -1;
                stdin_open = false;
                // Never regress the phase: an out-of-order worker may have
                // already completed its handshake (a large dispatch frame can
                // still be draining here), and its result frame must keep the
                // result-side parsing and bounds.
                if (phase == Phase::WRITE_DISPATCH) {
                    phase = Phase::READ_HANDSHAKE;
                }
            }
        }

        // stdout: one handshake frame, then at most one result frame.
        if (stdout_open && (stdout_events & (POLLIN | POLLERR | POLLHUP)) != 0) {
            char chunk[4096];
            const ssize_t n = ::read(launch->stdout_read_fd, chunk, sizeof(chunk));
            if (n > 0) {
                FrameAssembler& current =
                        (phase == Phase::READ_RESULT) ? result : handshake;
                current.buf.append(chunk, static_cast<size_t>(n));
                std::string payload;
                const int frame_status = current.poll_frame(&payload);
                if (frame_status < 0) {
                    if (phase == Phase::READ_RESULT) {
                        sup.failure_category = "malformed result frame";
                    } else {
                        sup.pre_ffi_violation = true;
                        sup.failure_category = "malformed handshake frame";
                    }
                } else if (frame_status == 1) {
                    if (phase != Phase::READ_RESULT) {
                        const char* violation = nullptr;
                        if (!validate_handshake(payload, cgroup_rel, cgroup_dir,
                                                limits.memory_limit_bytes, limits.pids_max,
                                                limits.as_limit_bytes, limits.cpu_limit_seconds,
                                                &violation)) {
                            sup.pre_ffi_violation = true;
                            sup.failure_category = violation;
                        } else {
                            phase = Phase::READ_RESULT;
                            // Bytes past the handshake belong to the result frame.
                            result.buf.append(handshake.buf);
                            handshake.buf.clear();
                            std::string result_payload;
                            const int status = result.poll_frame(&result_payload);
                            if (status < 0) {
                                sup.failure_category = "malformed result frame";
                            } else if (status == 1) {
                                finish_result(result_payload);
                            }
                        }
                    } else {
                        finish_result(payload);
                    }
                }
            } else if (n == 0) {
                stdout_open = false;
                ::close(launch->stdout_read_fd);
                launch->stdout_read_fd = -1;
                // A complete buffered frame may have raced the EOF.
                std::string payload;
                if (phase == Phase::READ_RESULT && result.poll_frame(&payload) == 1) {
                    finish_result(payload);
                }
                if (!sup.got_result && sup.failure_category == nullptr) {
                    // Silent death is never a pre-FFI rejection: only a live,
                    // protocol-speaking violation is.
                    sup.failure_category =
                            phase == Phase::READ_RESULT
                                    ? "worker exited before completing the result frame"
                                    : "worker exited before completing the handshake";
                }
            } else if (errno != EAGAIN && errno != EINTR) {
                stdout_open = false;
                ::close(launch->stdout_read_fd);
                launch->stdout_read_fd = -1;
                sup.failure_category = "supervisor stdout read failure";
            }
        }

        // stderr: continuous bounded drain; content is never logged.
        if (stderr_open && (stderr_events & (POLLIN | POLLERR | POLLHUP)) != 0) {
            char chunk[4096];
            const ssize_t n = ::read(launch->stderr_read_fd, chunk, sizeof(chunk));
            if (n > 0) {
                stderr_ring.push(chunk, static_cast<size_t>(n));
            } else if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) {
                stderr_open = false;
                ::close(launch->stderr_read_fd);
                launch->stderr_read_fd = -1;
            }
        }

        if (sup.child_exit_observed && !stdout_open && !stderr_open && !sup.got_result &&
            sup.failure_category == nullptr) {
            sup.failure_category = "worker exited without a result frame";
        }
    }

    // Close any pipe ends the loop still owns; the pidfd stays for reaping.
    if (launch->stdin_write_fd >= 0) {
        ::close(launch->stdin_write_fd);
        launch->stdin_write_fd = -1;
    }
    if (launch->stdout_read_fd >= 0) {
        ::close(launch->stdout_read_fd);
        launch->stdout_read_fd = -1;
    }
    if (launch->stderr_read_fd >= 0) {
        ::close(launch->stderr_read_fd);
        launch->stderr_read_fd = -1;
    }

    // Kill discipline. The child leads its own process group (the parent moved
    // it pre-release), so kill(-pid) reaps the whole worker tree; the direct
    // kill is a backstop. A group leader can never leave its group.
    if (!sup.child_exit_observed) {
        if (sup.got_result) {
            // The one-shot worker exits right after its result frame; allow a
            // short grace before concluding it hung.
            if (wait_pidfd_exit(launch->pidfd, 5000)) {
                sup.child_exit_observed = true;
            } else {
                ::kill(-pid, SIGKILL);
                ::kill(pid, SIGKILL);
            }
        } else if (sup.deadline_expired) {
            ::kill(-pid, SIGTERM);
            if (wait_pidfd_exit(launch->pidfd, limits.term_grace_seconds * 1000)) {
                sup.child_exit_observed = true;
            } else {
                ::kill(-pid, SIGKILL);
                ::kill(pid, SIGKILL);
            }
        } else {
            ::kill(-pid, SIGKILL);
            ::kill(pid, SIGKILL);
        }
    }

    reap_child_bounded(launch->pidfd, pid, 10000, &sup.reap);
    launch->close_all(); // pidfd included; pipes are already closed
    sup.populated_zero = wait_populated_zero(cgroup_dir, 1000);
    const int64_t oom_after = oom_kill_count(cgroup_dir);
    if (oom_kill_baseline >= 0 && oom_after >= 0) {
        sup.oom_kill_delta = oom_after - oom_kill_baseline;
    }
    return sup;
}

// Static message categories per result code (D12): the only text that may
// ever ride a report. nullptr = no message (the typed result stands alone).
const char* category_for_result_code(TLanceIndexJobResultCode::type code) {
    switch (code) {
    case TLanceIndexJobResultCode::PRE_INVOCATION_STALE_ADMISSION:
        return "stale admission";
    case TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT:
        return "unsupported schema contract";
    case TLanceIndexJobResultCode::PRE_INVOCATION_CREDENTIAL_EXPIRED:
        return "credential expired";
    case TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED:
        return "resource rejected";
    case TLanceIndexJobResultCode::NATIVE_OK:
        return nullptr;
    case TLanceIndexJobResultCode::NATIVE_COMMIT_CONFLICT:
    case TLanceIndexJobResultCode::NATIVE_NOT_FOUND:
    case TLanceIndexJobResultCode::NATIVE_INVALID_ARGUMENT:
    case TLanceIndexJobResultCode::NATIVE_NOT_SUPPORTED:
    case TLanceIndexJobResultCode::NATIVE_INDEX:
    case TLanceIndexJobResultCode::NATIVE_IO:
    case TLanceIndexJobResultCode::NATIVE_INTERNAL:
        return "lance native error";
    }
    return "unmapped result code";
}

struct CountGuard {
    std::atomic<int>* counter;
    ~CountGuard() { counter->fetch_sub(1); }
};

#endif // __linux__

} // namespace

IndexJobSupervisor::IndexJobSupervisor() = default;

IndexJobSupervisor::~IndexJobSupervisor() {
    stop();
}

void IndexJobSupervisor::set_report_result_callback(ReportResultFn fn) {
    std::lock_guard<std::mutex> lock(_callback_mutex);
    _report_result_fn = std::move(fn);
}

void IndexJobSupervisor::set_report_termination_callback(ReportTerminationFn fn) {
    std::lock_guard<std::mutex> lock(_callback_mutex);
    _report_termination_fn = std::move(fn);
}

void IndexJobSupervisor::set_report_silent_callback(ReportSilentFn fn) {
    std::lock_guard<std::mutex> lock(_callback_mutex);
    _report_silent_fn = std::move(fn);
}

Status IndexJobSupervisor::resolve_cgroup_parent(const std::string& configured_parent,
                                                 std::string* resolved_parent) {
#ifdef __linux__
    auto reject = [](const char* failure_class, const std::string& detail) {
        return Status::CgroupError("cgroup parent resolution failed (class: {}): {}",
                                   failure_class, detail);
    };
    if (!configured_parent.empty()) {
        // Prefix check with a path-boundary: "/sys/fs/cgroup-anything" shares
        // the prefix but is not under the cgroup root.
        const size_t root_len = std::strlen(CGROUP_ROOT);
        const bool under_root =
                configured_parent == CGROUP_ROOT ||
                (configured_parent.size() > root_len &&
                 configured_parent.compare(0, root_len, CGROUP_ROOT) == 0 &&
                 configured_parent[root_len] == '/');
        if (!under_root || configured_parent.find("..") != std::string::npos) {
            return reject("invalid-config",
                          "lance_index_worker_cgroup_parent must be an absolute path under "
                          "/sys/fs/cgroup without '..'");
        }
        struct stat st;
        if (::stat(configured_parent.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
            return reject("invalid-config",
                          "configured parent cgroup does not exist: " + configured_parent);
        }
        std::string detail;
        const int err = enable_memory_pids(configured_parent, &detail);
        if (err != 0) {
            return reject(err == EBUSY ? "no-internal-process" : "no-delegation", detail);
        }
        *resolved_parent = configured_parent;
        return Status::OK();
    }

    struct statfs fs;
    if (::statfs(CGROUP_ROOT, &fs) != 0 ||
        static_cast<uint64_t>(fs.f_type) != static_cast<uint64_t>(CGROUP2_SUPER_MAGIC)) {
        return reject("not-v2", "cgroup v2 is not mounted at /sys/fs/cgroup");
    }
    std::string self_rel;
    if (!read_proc_cgroup_v2_path(-1, &self_rel)) {
        return reject("not-v2", "no unified hierarchy line in /proc/self/cgroup");
    }
    std::string dir = self_rel == "/" ? CGROUP_ROOT : CGROUP_ROOT + self_rel;
    bool saw_writable = false;
    bool saw_ebusy = false;
    bool saw_other_failure = false;
    std::string last_detail;
    for (;;) {
        if (::access(dir.c_str(), W_OK) == 0) {
            saw_writable = true;
            std::string detail;
            const int err = enable_memory_pids(dir, &detail);
            if (err == 0) {
                *resolved_parent = dir;
                return Status::OK();
            }
            last_detail = std::move(detail);
            if (err == EBUSY) {
                saw_ebusy = true;
            } else {
                saw_other_failure = true;
            }
        }
        if (dir == CGROUP_ROOT) {
            break;
        }
        dir = dir.substr(0, dir.rfind('/'));
    }
    if (!saw_writable) {
        return reject("read-only",
                      "no writable cgroup directory in the ancestry of " + self_rel);
    }
    if (saw_ebusy && !saw_other_failure) {
        return reject("no-internal-process",
                      "every writable ancestor has internal processes; last error: " +
                              last_detail);
    }
    return reject("no-delegation",
                  "no writable ancestor accepts +memory +pids; last error: " + last_detail);
#else
    (void)configured_parent;
    (void)resolved_parent;
    return Status::NotSupported("lance index worker isolation requires Linux");
#endif
}

Status IndexJobSupervisor::preflight() {
#ifdef __linux__
    if (_force_preflight_failure.load()) {
        LOG(WARNING) << "lance isolation preflight: forced failure (test hook)";
        return Status::CgroupError("lance isolation preflight failed: forced failure (test hook)");
    }
    if (!config::lance_index_isolation_preflight) {
        // The switch only skips the probe; isolation stays unverified and every
        // submission is still rejected (taskcard A.1.4).
        LOG(INFO) << "lance isolation preflight disabled by config; isolation stays "
                     "unverified and every submission will be rejected";
        return Status::OK();
    }
    std::string core_pattern;
    if (read_file("/proc/sys/kernel/core_pattern", &core_pattern, 4096)) {
        LOG(INFO) << "lance isolation preflight: kernel.core_pattern="
                  << trim_copy(std::move(core_pattern));
    }
    std::string parent;
    Status status = resolve_cgroup_parent(config::lance_index_worker_cgroup_parent, &parent);
    if (!status.ok()) {
        LOG(WARNING) << "lance isolation preflight failed at parent resolution: "
                     << status.to_string();
        return status;
    }
    status = preflight_group_probe(parent);
    if (!status.ok()) {
        LOG(WARNING) << "lance isolation preflight failed: " << status.to_string();
        return status;
    }
    _cgroup_parent_abs = parent;
    _isolation_verified.store(true);
    LOG(INFO) << "lance index worker isolation verified; delegated parent cgroup: " << parent;
    return Status::OK();
#else
    LOG(WARNING) << "lance index worker isolation is unsupported on this platform (not "
                    "Linux); every submission will be rejected";
    return Status::NotSupported("lance index worker isolation requires Linux");
#endif
}

Status IndexJobSupervisor::_ensure_started() {
    std::lock_guard<std::mutex> lock(_lifecycle_mutex);
    if (_started) {
        return Status::OK();
    }
    if (_stopping.load()) {
        return Status::Cancelled("lance index job supervisor is stopping");
    }
    _queue = std::make_unique<BlockingQueue<TLanceIndexJobDispatch>>(
            static_cast<uint32_t>(config::lance_index_worker_queue_size));
    const int executor_count = config::lance_index_worker_max_inflight;
    for (int i = 0; i < executor_count; ++i) {
        _executors.emplace_back([this]() { _executor_loop(); });
    }
    _started = true;
    return Status::OK();
}

Status IndexJobSupervisor::submit(const TLanceIndexJobDispatch& dispatch) {
    if (!_isolation_verified.load()) {
        return Status::CgroupError(
                "lance index worker isolation is not verified (startup preflight failed or "
                "was skipped); rejecting submission");
    }
    if (_stopping.load()) {
        return Status::Cancelled("lance index job supervisor is stopping");
    }
    const std::string& invocation_id = dispatch.invocation_id;
    if (invocation_id.empty()) {
        return Status::InvalidArgument("lance index dispatch has an empty invocation_id");
    }
    const int64_t now_ms = epoch_millis_now();
    {
        std::lock_guard<std::mutex> lock(_dedup_mutex);
        for (auto it = _dedup_erase_after_ms.begin(); it != _dedup_erase_after_ms.end();) {
            it = it->second <= now_ms ? _dedup_erase_after_ms.erase(it) : std::next(it);
        }
        if (_dedup_erase_after_ms.count(invocation_id) != 0) {
            return Status::AlreadyExist("lance index invocation {} is already accepted",
                                        invocation_id);
        }
        // Retained until deadline + grace, NOT removed at completion (D15). The
        // addition saturates: a pathological deadline_ms near INT64_MAX must not
        // wrap the retention into the past (that would let a late redelivery of
        // the same invocation_id re-execute external side effects).
        _dedup_erase_after_ms[invocation_id] =
                dispatch.deadline_ms > INT64_MAX - DEDUP_RETAIN_AFTER_DEADLINE_MS
                        ? INT64_MAX
                        : dispatch.deadline_ms + DEDUP_RETAIN_AFTER_DEADLINE_MS;
    }
    Status status = _ensure_started();
    if (!status.ok() || !_queue->try_put(dispatch)) {
        {
            std::lock_guard<std::mutex> lock(_dedup_mutex);
            _dedup_erase_after_ms.erase(invocation_id);
        }
        if (!status.ok()) {
            return status;
        }
        if (_stopping.load()) {
            // try_put failed because stop() shut the queue down between the
            // entry check and here — say so instead of crying "queue full".
            return Status::Cancelled("lance index job supervisor is stopping");
        }
        return Status::TooManyTasks("lance index job queue is full (capacity {})",
                                    _queue->get_capacity());
    }
    LOG(INFO) << "lance index invocation accepted: job_id=" << dispatch.job_id
              << " invocation_id=" << dispatch.invocation_id
              << " mutation_type=" << to_string(dispatch.mutation_type);
    return Status::OK();
}

void IndexJobSupervisor::stop() {
    _stopping.store(true);
    std::vector<std::thread> executors;
    {
        std::lock_guard<std::mutex> lock(_lifecycle_mutex);
        if (!_started) {
            return;
        }
        _queue->shutdown();
        executors.swap(_executors);
        _started = false;
    }
#ifdef __linux__
    // Best-effort terminate in-flight workers (correctness never depends on
    // this: the worker-side PDEATHSIG arm + getppid recheck is the backstop).
    std::vector<pid_t> children;
    {
        std::lock_guard<std::mutex> lock(_inflight_mutex);
        children = _inflight_children;
    }
    for (const pid_t pid : children) {
        ::kill(-pid, SIGTERM);
    }
    const int64_t grace_ms = std::min<int64_t>(
            config::lance_index_worker_term_grace_seconds * 1000, 5000);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(grace_ms);
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(_inflight_mutex);
            if (_inflight_children.empty()) {
                break;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    {
        std::lock_guard<std::mutex> lock(_inflight_mutex);
        children = _inflight_children;
    }
    for (const pid_t pid : children) {
        ::kill(-pid, SIGKILL);
        ::kill(pid, SIGKILL);
    }
#endif
    for (std::thread& executor : executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
}

void IndexJobSupervisor::_executor_loop() {
#ifdef __linux__
    // A dead child's stdin pipe must never raise SIGPIPE in this thread;
    // write() returns EPIPE instead and the read side classifies the death.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    pthread_setname_np(pthread_self(), "lance-idx-worker");
#endif
    for (;;) {
        TLanceIndexJobDispatch dispatch;
        if (!_queue->blocking_get(&dispatch)) {
            return;
        }
        if (_stopping.load()) {
            // stop() owns the lifecycle now: a drained dispatch must never start
            // a new worker past the kill snapshot (a late-launched child would
            // escape stop()'s in-flight signal set). It provably never exec'd,
            // so it still earns the full NEVER_LAUNCHED envelope; the report
            // callback (dropped RPC-side while stopping) releases the
            // invocation's accounting slot either way.
            _report_never_launched(dispatch, "supervisor is stopping");
            continue;
        }
        _execute(dispatch);
    }
}

void IndexJobSupervisor::_register_inflight_child(pid_t pid) {
    std::lock_guard<std::mutex> lock(_inflight_mutex);
    _inflight_children.push_back(pid);
}

void IndexJobSupervisor::_unregister_inflight_child(pid_t pid) {
    std::lock_guard<std::mutex> lock(_inflight_mutex);
    auto it = std::find(_inflight_children.begin(), _inflight_children.end(), pid);
    if (it != _inflight_children.end()) {
        _inflight_children.erase(it);
    }
}

void IndexJobSupervisor::_invoke_result_callback(const TLanceIndexJobReport& report) {
    // Terminal reports are delivered to the bound callback even while stopping:
    // the callback owner (LanceIndexJobService) decides to drop the RPC, and it
    // can only release the invocation's accounting slot if it sees the report.
    ReportResultFn fn;
    {
        std::lock_guard<std::mutex> lock(_callback_mutex);
        fn = _report_result_fn;
    }
    if (fn) {
        fn(report);
    } else {
        LOG(ERROR) << "lance supervisor has no result callback wired; dropping report: job_id="
                   << report.job_id << " invocation_id=" << report.invocation_id;
    }
}

void IndexJobSupervisor::_invoke_termination_callback(
        const TLanceIndexJobTerminationReport& report) {
    ReportTerminationFn fn;
    {
        std::lock_guard<std::mutex> lock(_callback_mutex);
        fn = _report_termination_fn;
    }
    if (fn) {
        fn(report);
    } else {
        LOG(ERROR) << "lance supervisor has no termination callback wired; dropping report: "
                      "job_id="
                   << report.job_id << " invocation_id=" << report.invocation_id;
    }
}

void IndexJobSupervisor::_invoke_silent_callback(int64_t job_id,
                                                 const std::string& invocation_id) {
    // Same delivery discipline as the report callbacks: fires even while
    // stopping, and for a silent ending it is the owner's ONLY terminal
    // notification — no report callback will ever run for this invocation,
    // so an unwired callback means the owner's gauge slot leaks.
    ReportSilentFn fn;
    {
        std::lock_guard<std::mutex> lock(_callback_mutex);
        fn = _report_silent_fn;
    }
    if (fn) {
        fn(job_id, invocation_id);
    } else {
        LOG(ERROR) << "lance supervisor has no silent-ending callback wired; the owner's gauge "
                      "slot leaks until restart: job_id="
                   << job_id << " invocation_id=" << invocation_id;
    }
}

// Echoes the per-dispatch invocation secret into an outgoing report: the FE
// compares it in constant time against the journaled secret before trusting any
// part of the envelope, so every report this supervisor builds carries it. The
// secret otherwise stays inside the BE trust boundary - the isolated worker
// never reads it out of its dispatch frame, and it is never logged. A dispatch
// without one (the rolling-upgrade shape) echoes nothing, which the FE pairs
// with its fail-closed read of a legacy record that journals no secret.
void echo_invocation_secret(const TLanceIndexJobDispatch& dispatch, TLanceIndexJobReport* report) {
    if (dispatch.__isset.invocation_secret) {
        report->__set_invocation_secret(dispatch.invocation_secret);
    }
}

void echo_invocation_secret(const TLanceIndexJobDispatch& dispatch,
                            TLanceIndexJobTerminationReport* report) {
    if (dispatch.__isset.invocation_secret) {
        report->__set_invocation_secret(dispatch.invocation_secret);
    }
}

void IndexJobSupervisor::_report_never_launched(const TLanceIndexJobDispatch& dispatch,
                                                const char* static_category) {
    // Full-envelope async rejection (D6 path 3): the invocation provably never
    // exec'd (pre-fork rejection, or killed before the barrier release), so the
    // trusted result code rides with the NEVER_LAUNCHED proof.
    TLanceIndexJobReport report;
    report.job_id = dispatch.job_id;
    report.dispatch_revision = dispatch.dispatch_revision;
    report.invocation_id = dispatch.invocation_id;
    report.be_process_epoch = dispatch.be_process_epoch;
    echo_invocation_secret(dispatch, &report);
    report.result_code = TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED;
    report.__set_termination_proof(TLanceIndexTerminationProof::NEVER_LAUNCHED);
    const std::string message = sanitize_message(static_category, dispatch);
    if (!message.empty()) {
        report.__set_sanitized_message(message);
    }
    LOG(WARNING) << "lance invocation rejected before launch: job_id=" << dispatch.job_id
                 << " invocation_id=" << dispatch.invocation_id
                 << " category=" << static_category;
    _invoke_result_callback(report);
}

void IndexJobSupervisor::_execute(const TLanceIndexJobDispatch& dispatch) {
#ifdef __linux__
    _inflight_count.fetch_add(1);
    CountGuard count_guard {&_inflight_count};

    // D3: budget recompute at dequeue (queue wait already consumed budget).
    // No positive lower clamp is ever applied over the FE deadline or the
    // configured ceiling.
    const int64_t remaining_ms = dispatch.deadline_ms - epoch_millis_now();
    const int64_t wallclock_s = _force_wallclock_seconds.load() >= 0
                                        ? _force_wallclock_seconds.load()
                                        : config::lance_index_worker_wallclock_limit_seconds;
    const int64_t margin_s = _force_report_margin_seconds.load() >= 0
                                     ? _force_report_margin_seconds.load()
                                     : config::lance_index_worker_report_margin_seconds;
    const int64_t grace_s = _force_term_grace_seconds.load() >= 0
                                    ? _force_term_grace_seconds.load()
                                    : config::lance_index_worker_term_grace_seconds;
    const int64_t available_s = remaining_ms / 1000 - margin_s - grace_s;
    const int64_t wall_s = std::min(wallclock_s, available_s);
    if (wall_s <= MIN_EXECUTABLE_BUDGET_SECONDS) {
        LOG(INFO) << "lance invocation rejected: insufficient deadline budget: job_id="
                  << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                  << " remaining_ms=" << remaining_ms;
        _report_never_launched(dispatch, "deadline budget exhausted");
        return;
    }

    // The single dispatch frame, bounded before it ever reaches a pipe.
    std::vector<uint8_t> payload;
    if (!encode_compact(dispatch, &payload) || payload.empty() ||
        payload.size() > MAX_DISPATCH_FRAME_BYTES) {
        _report_never_launched(dispatch, "dispatch frame encode failed");
        return;
    }
    std::string dispatch_frame;
    dispatch_frame.reserve(4 + payload.size());
    const uint32_t frame_length = static_cast<uint32_t>(payload.size());
    dispatch_frame.push_back(static_cast<char>(frame_length >> 24));
    dispatch_frame.push_back(static_cast<char>(frame_length >> 16));
    dispatch_frame.push_back(static_cast<char>(frame_length >> 8));
    dispatch_frame.push_back(static_cast<char>(frame_length));
    dispatch_frame.append(reinterpret_cast<const char*>(payload.data()), payload.size());

    // Controlled environment; all heap work happens before fork().
    std::vector<std::string> env_storage;
    std::vector<char*> envp;
    if (!build_child_env(&env_storage, &envp).ok()) {
        _report_never_launched(dispatch, "launch environment invalid");
        return;
    }

    PipePair stdin_pipe;
    PipePair stdout_pipe;
    PipePair stderr_pipe;
    PipePair barrier_pipe;
    if (!make_pipe(&stdin_pipe) || !make_pipe(&stdout_pipe) || !make_pipe(&stderr_pipe) ||
        !make_pipe(&barrier_pipe)) {
        stdin_pipe.close_all();
        stdout_pipe.close_all();
        stderr_pipe.close_all();
        barrier_pipe.close_all();
        _report_never_launched(dispatch, "pipe creation failed");
        return;
    }
    auto reject_with_pipes = [&](const char* category) {
        stdin_pipe.close_all();
        stdout_pipe.close_all();
        stderr_pipe.close_all();
        barrier_pipe.close_all();
        _report_never_launched(dispatch, category);
    };

    // Invocation cgroup: name carries job_id + a short invocation hash.
    const uint32_t invocation_hash =
            static_cast<uint32_t>(std::hash<std::string> {}(dispatch.invocation_id));
    const std::string cgroup_dir =
            _cgroup_parent_abs + "/" +
            fmt::format("lance-worker-{}-{:08x}", dispatch.job_id, invocation_hash);
    const std::string cgroup_rel = cgroup_rel_of_abs(cgroup_dir);
    const int64_t memory_limit = config::lance_index_worker_memory_limit_bytes;
    const int64_t pids_max = config::lance_index_worker_pids_max;
    if (::mkdir(cgroup_dir.c_str(), 0755) != 0) {
        LOG(WARNING) << "lance invocation cgroup creation failed: job_id=" << dispatch.job_id
                     << " invocation_id=" << dispatch.invocation_id
                     << " error=" << std::strerror(errno);
        reject_with_pipes("cgroup creation failed");
        return;
    }
    if (!write_cgroup_limits(cgroup_dir, memory_limit, pids_max).ok()) {
        remove_cgroup_dir(cgroup_dir);
        reject_with_pipes("cgroup limit setup failed");
        return;
    }
    const int64_t oom_baseline = oom_kill_count(cgroup_dir);

    // Fork behind the barrier; migrate; verify membership; release.
    const int64_t as_limit = config::lance_index_worker_as_limit_bytes;
    const int64_t cpu_limit =
            static_cast<int64_t>(config::lance_index_worker_cpu_limit_multiplier) * wall_s;
    // Exec target: production self-exec. BE_TEST builds may override it with a
    // protocol-speaking fake worker; the storage below outlives launch_child()
    // (the child consumes the pointers before execve, synchronously).
    const char* exec_path = "/proc/self/exe";
    const char* const* exec_argv = kDefaultWorkerArgv;
#ifdef BE_TEST
    std::vector<std::string> exec_argv_storage;
    std::vector<const char*> exec_argv_ptrs;
    if (!_force_exec_path.empty()) {
        exec_argv_storage.reserve(_force_exec_args.size() + 1);
        exec_argv_storage.push_back(_force_exec_path);
        for (const std::string& arg : _force_exec_args) {
            exec_argv_storage.push_back(arg);
        }
        exec_argv_ptrs.reserve(exec_argv_storage.size() + 1);
        for (const std::string& arg : exec_argv_storage) {
            exec_argv_ptrs.push_back(arg.c_str());
        }
        exec_argv_ptrs.push_back(nullptr);
        exec_path = exec_argv_storage.front().c_str();
        exec_argv = exec_argv_ptrs.data();
    }
#endif
    ChildLaunchPlan plan {barrier_pipe.read_fd,
                          stdin_pipe.read_fd,
                          stdout_pipe.write_fd,
                          stderr_pipe.write_fd,
                          {stdin_pipe.write_fd, stdout_pipe.read_fd, stderr_pipe.read_fd,
                           barrier_pipe.write_fd},
                          ::getpid(),
                          as_limit,
                          cpu_limit,
                          &envp,
                          exec_path,
                          exec_argv};
    Launch launch;
    const char* fail_category = nullptr;
    if (!launch_child(plan, &stdin_pipe, &stdout_pipe, &stderr_pipe, &barrier_pipe, cgroup_dir,
                      cgroup_rel, _force_cgroup_migration_failure.load(), &launch,
                      &fail_category)
                 .ok()) {
        // launch_child already killed/reaped before releasing, closed every fd,
        // and removed the cgroup; the child provably never exec'd.
        _report_never_launched(dispatch, fail_category);
        return;
    }
    _register_inflight_child(launch.pid);

    LaunchLimits limits {memory_limit, pids_max, as_limit, cpu_limit, grace_s};
    Supervision sup = supervise_child(dispatch, cgroup_dir, cgroup_rel, dispatch_frame, wall_s,
                                      limits, oom_baseline, &launch);
    _unregister_inflight_child(launch.pid);

    if (sup.populated_zero) {
        remove_cgroup_dir(cgroup_dir);
    } else {
        LOG(WARNING) << "lance supervisor: leaving populated invocation cgroup " << cgroup_dir
                     << " job_id=" << dispatch.job_id
                     << " invocation_id=" << dispatch.invocation_id;
    }

    const bool proof = sup.child_reaped_proof();
    if (sup.got_result) {
        // Ending (a): a complete, identity-matched result frame is the only
        // trusted result. The proof rides when the evidence allows it.
        TLanceIndexJobReport report = std::move(sup.report);
        // The worker's frame never carries the secret (the isolated worker stays
        // inside the BE trust boundary); the supervisor stamps the echo here.
        echo_invocation_secret(dispatch, &report);
        if (proof) {
            report.__set_termination_proof(TLanceIndexTerminationProof::CHILD_REAPED);
        }
        // D12: the supervisor produces the message; worker text never forwards.
        report.sanitized_message.clear();
        report.__isset.sanitized_message = false;
        if (const char* category = category_for_result_code(report.result_code)) {
            const std::string message = sanitize_message(category, dispatch);
            if (!message.empty()) {
                report.__set_sanitized_message(message);
            }
        }
        LOG(INFO) << "lance invocation completed: job_id=" << dispatch.job_id
                  << " invocation_id=" << dispatch.invocation_id
                  << " result_code=" << to_string(report.result_code)
                  << " child_reaped_proof=" << proof;
        _invoke_result_callback(report);
        return;
    }
    if (sup.pre_ffi_violation) {
        // Handshake-stage violation: the worker writes its handshake before
        // its first FFI call in program order and the kill was issued within
        // microseconds of the frame, so no mutation can have committed. The
        // result code is a trusted PRE_INVOCATION_RESOURCE_REJECTED, but a
        // forked rejection may only be reported once the exact child is
        // provably reaped (D6); without the proof no report is sent and the
        // FE-side slot waits for the epoch sweep. NEVER_LAUNCHED would be
        // wrong here (it DID exec).
        if (proof) {
            TLanceIndexJobReport report;
            report.job_id = dispatch.job_id;
            report.dispatch_revision = dispatch.dispatch_revision;
            report.invocation_id = dispatch.invocation_id;
            report.be_process_epoch = dispatch.be_process_epoch;
            echo_invocation_secret(dispatch, &report);
            report.result_code = TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED;
            report.__set_termination_proof(TLanceIndexTerminationProof::CHILD_REAPED);
            const std::string message = sanitize_message(
                    sup.failure_category != nullptr ? sup.failure_category
                                                    : "isolation self-check mismatch",
                    dispatch);
            if (!message.empty()) {
                report.__set_sanitized_message(message);
            }
            LOG(WARNING) << "lance invocation rejected at the isolation self-check: job_id="
                         << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                         << " category=" << sup.failure_category;
            _invoke_result_callback(report);
        } else {
            LOG(WARNING) << "lance isolation self-check failed but termination evidence is "
                            "insufficient; FE-side slot kept for the epoch sweep: job_id="
                         << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                         << " category=" << sup.failure_category;
            // No report fires on this ending, so no report callback would ever
            // release the invocation's gauge slot; the silent callback does it
            // here instead. FE-side semantics are unchanged: with no report the
            // deadline/epoch sweep still owns the invocation.
            _invoke_silent_callback(dispatch.job_id, dispatch.invocation_id);
        }
        return;
    }
    // Ending (b): no trusted result frame (exit/signal/timeout/OOM/malformed).
    // Never a result code; at most a termination proof.
    const char* category = sup.deadline_expired
                                   ? "wall-clock deadline expired"
                                   : (sup.failure_category != nullptr
                                              ? sup.failure_category
                                              : "worker exited without a result frame");
    LOG(WARNING) << "lance invocation ended without a trusted result: job_id="
                 << dispatch.job_id << " invocation_id=" << dispatch.invocation_id
                 << " category=" << category
                 << " exit_status=" << (sup.reap.reaped_exact ? sup.reap.wait_status : -1)
                 << " exit_kind=" << (sup.reap.reaped_exact ? sup.reap.wait_kind : -1)
                 << " oom_kill_delta=" << sup.oom_kill_delta
                 << " populated_zero=" << sup.populated_zero;
    if (proof) {
        TLanceIndexJobTerminationReport termination;
        termination.job_id = dispatch.job_id;
        termination.dispatch_revision = dispatch.dispatch_revision;
        termination.invocation_id = dispatch.invocation_id;
        termination.be_process_epoch = dispatch.be_process_epoch;
        echo_invocation_secret(dispatch, &termination);
        termination.proof = TLanceIndexTerminationProof::CHILD_REAPED;
        _invoke_termination_callback(termination);
    } else {
        LOG(WARNING) << "lance invocation termination evidence is insufficient (CDC reaper "
                        "race or surviving descendants); FE-side slot kept for the epoch "
                        "sweep: job_id="
                     << dispatch.job_id << " invocation_id=" << dispatch.invocation_id;
        // Same no-report accounting: the silent callback releases the local
        // gauge slot (nothing else ever will for this invocation); the FE-side
        // slot still converges through the epoch sweep, exactly as before.
        _invoke_silent_callback(dispatch.job_id, dispatch.invocation_id);
    }
#else
    (void)dispatch;
#endif
}

std::string IndexJobSupervisor::sanitize_message(const std::string& static_category,
                                                 const TLanceIndexJobDispatch& dispatch) {
    if (static_category.empty()) {
        return "";
    }
    const std::string raw =
            fmt::format("{}: job_id={} dispatch_revision={} invocation_id={} "
                        "be_process_epoch={}",
                        static_category, dispatch.job_id, dispatch.dispatch_revision,
                        dispatch.invocation_id, dispatch.be_process_epoch);
    // Escape control characters (identity fields are FE-generated but still
    // treated as untrusted text here).
    std::string escaped;
    escaped.reserve(raw.size() + 16);
    for (const unsigned char c : raw) {
        if (c < 0x20 || c == 0x7f) {
            char buffer[5];
            std::snprintf(buffer, sizeof(buffer), "\\x%02x", c);
            escaped.append(buffer);
        } else {
            escaped.push_back(static_cast<char>(c));
        }
    }
    // Codepoint-safe truncation to <= 900 bytes: never split a UTF-8 sequence.
    if (escaped.size() > SANITIZED_MESSAGE_MAX_BYTES) {
        size_t cut = SANITIZED_MESSAGE_MAX_BYTES;
        while (cut > 0 &&
               (static_cast<unsigned char>(escaped[cut]) & 0xC0) == 0x80) {
            --cut;
        }
        escaped.resize(cut);
    }
    // Second-line invariant (D12): worker frames never carry storage-option
    // values; if any ever appears here, drop the whole message rather than
    // risk forwarding a secret. Values shorter than the minimum are protected
    // by construction (static categories + numeric identity only).
    if (dispatch.__isset.storage_options) {
        for (const auto& option : dispatch.storage_options) {
            if (option.second.size() >= SECRET_SUBSTRING_MIN_BYTES &&
                escaped.find(option.second) != std::string::npos) {
                return "";
            }
        }
    }
    return escaped;
}

#ifdef BE_TEST
void IndexJobSupervisor::force_budgets_for_test(int64_t wallclock_seconds,
                                                int64_t term_grace_seconds,
                                                int64_t report_margin_seconds) {
    _force_wallclock_seconds.store(wallclock_seconds);
    _force_term_grace_seconds.store(term_grace_seconds);
    _force_report_margin_seconds.store(report_margin_seconds);
}

void IndexJobSupervisor::force_worker_exec_for_test(std::string path,
                                                    std::vector<std::string> args) {
    _force_exec_path = std::move(path);
    _force_exec_args = std::move(args);
}

uint32_t IndexJobSupervisor::queue_depth_for_test() const {
    std::lock_guard<std::mutex> lock(_lifecycle_mutex);
    return _queue != nullptr ? _queue->get_size() : 0;
}
#endif

} // namespace doris::lance
