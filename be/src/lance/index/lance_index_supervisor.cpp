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

#include "lance/index/lance_index_supervisor.h"

#include <thrift/TConfiguration.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fmt/format.h>
#include <memory>
#include <string>
#include <thread>
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
#include "gen_cpp/LanceIndexWorker_constants.h"
#include "lance/index/lance_index_worker.h"
#include "lance/index/lance_index_worker_frame.h"
#include "util/debug_points.h"

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
// Protocol bounds. The dispatch/result/handshake caps mirror the worker-side
// defaults (lance_index_worker.cpp); both sides cap BEFORE any allocation.
// The codec itself (depth/container limits, length-before-allocation, exactly
// one struct per frame) lives in lance_index_worker_frame.h and is shared with
// the worker library.
// ---------------------------------------------------------------------------
constexpr uint32_t MAX_DISPATCH_FRAME_BYTES = 512 * 1024;
constexpr uint32_t MAX_HANDSHAKE_FRAME_BYTES = 4 * 1024;
constexpr uint32_t MAX_RESULT_FRAME_BYTES = 8 * 1024;

// stderr is drained continuously into this bounded in-memory ring and is never
// logged raw; it exists only so a human can classify a failure in a debugger.
constexpr size_t STDERR_RING_BYTES = 64 * 1024;

// sanitized_message budget: <= 900 UTF-8 bytes, codepoint-safe truncation.
constexpr size_t SANITIZED_MESSAGE_MAX_BYTES = 900;
// Storage-option values shorter than this are not substring-checkable without
// collapsing every message into a false positive; the first-line defense
// (static categories + numeric identity only) is what protects them.
constexpr size_t SECRET_SUBSTRING_MIN_BYTES = 4;

// The kill ladder's hard bounds (D3): the SIGKILL reap wait and the
// populated=0 wait. Together they are kReapBoundMs.
constexpr int64_t REAP_AFTER_KILL_BOUND_MS = 10 * 1000;
constexpr int64_t POPULATED_ZERO_BOUND_MS = 1 * 1000;
// A worker that already wrote its result frame gets this long to exit on its
// own before the ladder concludes it hung (the one-shot worker exits right
// after the frame; pr4a value, still inside the reservation).
constexpr int64_t POST_RESULT_EXIT_GRACE_MS = 5 * 1000;

constexpr int64_t CHILD_NOFILE_LIMIT = 1024;
constexpr const char* CGROUP_ROOT = "/sys/fs/cgroup";
constexpr const char* CGROUP_CONTROLLERS = "+memory +pids";

int64_t epoch_millis_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

const char* mutation_type_name(TLanceIndexWorkerMutationType::type type) {
    switch (type) {
    case TLanceIndexWorkerMutationType::CREATE:
        return "CREATE";
    case TLanceIndexWorkerMutationType::REPLACE:
        return "REPLACE";
    case TLanceIndexWorkerMutationType::DROP:
        return "DROP";
    }
    return "UNKNOWN";
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
// Bounded stderr capture: newest bytes win once the ring is full. Never logged.
// ---------------------------------------------------------------------------
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
    // The supervisor's calling threads block SIGPIPE (a dead child's stdin
    // write must return EPIPE, not kill the BE); the worker must not inherit
    // that mask across exec.
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

// Builds the controlled environment whitelist (D5): LD_LIBRARY_PATH is
// derived ONLY from the resolved doris_be install lib dir plus an optional
// validated $JAVA_HOME/lib/server; LANG/LC_ALL/TZ pass through when sane;
// RUST_BACKTRACE is pinned off. Never inherits the caller's LD_LIBRARY_PATH
// (a net env self-exec would die 127 on the operator's library mix).
// `debug_points_env_value` is the pre-filtered names-only handoff of the
// dispatch frame's field 18 (see worker_debug_points_env_value); empty means
// "set no debug env" (D11: names only, and only the fixed allowlist, ever).
Status build_child_env(std::vector<std::string>* storage, std::vector<char*>* envp,
                       const std::string& debug_points_env_value) {
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
    if (!debug_points_env_value.empty()) {
        storage->push_back(fmt::format("{}={}", WORKER_DEBUG_POINTS_ENV, debug_points_env_value));
    }
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

// ---------------------------------------------------------------------------
// launch_child: fork behind a barrier, migrate into the invocation cgroup,
// verify membership, and only then release the child to exec. Every failure
// before the barrier release is a provable never-launched rejection (the
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
// Handshake validation: magic/version first, then every self-reported value
// cross-checked against OUR OWN read-back of the invocation cgroup and the
// exact rlimit values the child was given. The report is never trusted on its
// own. Frames received before a valid handshake are discarded (the worker
// protocol emits exactly one handshake first; anything else is a violation).
// ---------------------------------------------------------------------------

bool validate_handshake(const std::string& frame, const std::string& cgroup_rel,
                        const std::string& cgroup_dir, int64_t memory_limit_bytes, int64_t pids_max,
                        int64_t as_limit_bytes, int64_t cpu_limit_seconds,
                        const char** violation_category) {
    TLanceIndexWorkerHandshake handshake;
    if (!lance_worker_decode_compact(frame, &handshake)) {
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

// The result-frame contract (the pipe protocol's own band rules, mirrored from
// gensrc/thrift/LanceIndexWorker.thrift): phase 1 pairs with exactly one of
// the five LANCE_WORKER_REJECTED_* codes, phase 2 with a non-negative native
// passthrough code. The thrift-compact decode of the i32 enum performs no
// membership check, so a frame outside the contract (a drifted or corrupted
// worker) is a protocol violation — never a trusted result, and never a
// pre-FFI violation either (it arrived past a valid handshake, so a mutation
// may have committed).
bool result_frame_in_contract(const TLanceIndexWorkerResult& result) {
    const auto& constants = g_LanceIndexWorker_constants;
    switch (result.phase) {
    case TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED:
        return result.lance_error_code == constants.LANCE_WORKER_REJECTED_STALE_ADMISSION ||
               result.lance_error_code ==
                       constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT ||
               result.lance_error_code == constants.LANCE_WORKER_REJECTED_CREDENTIAL_EXPIRED ||
               result.lance_error_code == constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED ||
               result.lance_error_code == constants.LANCE_WORKER_REJECTED_INDEX_UNRESOLVED;
    case TLanceIndexWorkerResultPhase::NATIVE_RETURN:
        return result.lance_error_code >= 0;
    }
    return false;
}

// The static message category of a trusted result (D12 second line): the
// supervisor rebuilds every operator-facing message from ITS OWN table and
// never forwards worker text. The only passthrough is the authoritative
// pre-invocation IF no-op note (phase 2, code 0), whose two fixed spellings
// are matched exactly — anything else the worker wrote in the message slot is
// dropped whole.
const char* NO_OP_CREATE_CATEGORY = "index exists; create if not exists no-op";
const char* NO_OP_DROP_CATEGORY = "index absent; drop if exists no-op";

const char* result_message_category(const TLanceIndexWorkerResult& result) {
    if (result.phase == TLanceIndexWorkerResultPhase::NATIVE_RETURN) {
        if (result.lance_error_code == 0) {
            if (result.__isset.message && result.message == NO_OP_CREATE_CATEGORY) {
                return NO_OP_CREATE_CATEGORY;
            }
            if (result.__isset.message && result.message == NO_OP_DROP_CATEGORY) {
                return NO_OP_DROP_CATEGORY;
            }
            return nullptr; // a plain native success carries no message
        }
        return "lance native error";
    }
    switch (result.lance_error_code) {
    case -1:
        return "stale admission";
    case -2:
        return "unsupported schema contract";
    case -3:
        return "credential expired";
    case -5:
        return "index unresolved";
    }
    return "resource rejected";
}

// ---------------------------------------------------------------------------
// Preflight probes: dummy-child migration under a fork barrier, proving the
// whole delegation chain before the first real invocation.
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
    // and rejects every invocation.
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
// the kill-escalation/reaping discipline. Produces a Supervision; execute()
// maps it onto the WorkerExecution classification.
// ---------------------------------------------------------------------------

struct LaunchLimits {
    int64_t memory_limit_bytes;
    int64_t pids_max;
    int64_t as_limit_bytes;
    int64_t cpu_limit_seconds;
    int64_t term_grace_ms;
};

struct Supervision {
    // Ending classification.
    bool got_result = false;    // complete result frame inside the contract
    TLanceIndexWorkerResult result;
    bool pre_ffi_violation = false; // handshake-stage protocol violation
    const char* failure_category = nullptr; // static string only
    bool deadline_expired = false;
    bool child_exit_observed = false;
    // Evidence.
    ReapEvidence reap;
    bool populated_zero = false;
    int64_t oom_kill_delta = -1;

    // The D16 dual evidence: the exact child provably reaped (by us, or by the
    // CDC reaper with pidfd-termination evidence) AND no surviving descendants
    // in the invocation cgroup.
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

Supervision supervise_child(const std::string& cgroup_dir, const std::string& cgroup_rel,
                            const std::string& dispatch_frame, int64_t wall_ms,
                            const LaunchLimits& limits, int64_t oom_kill_baseline,
                            Launch* launch) {
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
    LanceWorkerFrameAssembler handshake {MAX_HANDSHAKE_FRAME_BYTES, {}};
    LanceWorkerFrameAssembler result {MAX_RESULT_FRAME_BYTES, {}};
    StderrRing stderr_ring;
    size_t frame_written = 0;
    bool stdin_open = true;
    bool stdout_open = true;
    bool stderr_open = true;
    const auto wall_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(wall_ms);

    // Validates a completed result frame payload; on success the supervision
    // carries the trusted result.
    auto finish_result = [&sup](const std::string& payload) {
        TLanceIndexWorkerResult decoded;
        if (!lance_worker_decode_compact(payload, &decoded)) {
            sup.failure_category = "undecodable result frame";
            return;
        }
        if (!result_frame_in_contract(decoded)) {
            // Phase/code outside the pipe-protocol contract (corrupted or
            // drifted worker): not a trusted result, and NOT a pre-FFI
            // violation either — the frame arrived past a valid handshake, so
            // a mutation may have committed; the termination discipline
            // converges it as indeterminate.
            sup.failure_category = "result phase or code outside the frame contract";
            return;
        }
        sup.result = std::move(decoded);
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
                LanceWorkerFrameAssembler& current =
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
            if (wait_pidfd_exit(launch->pidfd, POST_RESULT_EXIT_GRACE_MS)) {
                sup.child_exit_observed = true;
            } else {
                ::kill(-pid, SIGKILL);
                ::kill(pid, SIGKILL);
            }
        } else if (sup.deadline_expired) {
            ::kill(-pid, SIGTERM);
            if (wait_pidfd_exit(launch->pidfd, limits.term_grace_ms)) {
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

    reap_child_bounded(launch->pidfd, pid, REAP_AFTER_KILL_BOUND_MS, &sup.reap);
    launch->close_all(); // pidfd included; pipes are already closed
    sup.populated_zero = wait_populated_zero(cgroup_dir, POPULATED_ZERO_BOUND_MS);
    const int64_t oom_after = oom_kill_count(cgroup_dir);
    if (oom_kill_baseline >= 0 && oom_after >= 0) {
        sup.oom_kill_delta = oom_after - oom_kill_baseline;
    }
    return sup;
}

struct CountGuard {
    std::atomic<int>* counter;
    ~CountGuard() { counter->fetch_sub(1); }
};

// Blocks SIGPIPE on the calling thread for the duration (a dead child's stdin
// write must return EPIPE, not kill the BE) and restores the prior mask.
struct SigpipeGuard {
    sigset_t saved;
    bool was_blocked = false;
    SigpipeGuard() {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGPIPE);
        pthread_sigmask(SIG_BLOCK, &mask, &saved);
        was_blocked = sigismember(&saved, SIGPIPE) == 1;
    }
    ~SigpipeGuard() {
        if (!was_blocked) {
            pthread_sigmask(SIG_SETMASK, &saved, nullptr);
        }
    }
};

#endif // __linux__

} // namespace

LanceIndexSupervisor::LanceIndexSupervisor() = default;

LanceIndexSupervisor::~LanceIndexSupervisor() {
    stop();
}

Status LanceIndexSupervisor::resolve_cgroup_parent(const std::string& configured_parent,
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

Status LanceIndexSupervisor::preflight() {
#ifdef __linux__
    if (_force_preflight_failure.load()) {
        LOG(WARNING) << "lance isolation preflight: forced failure (test hook)";
        return Status::CgroupError("lance isolation preflight failed: forced failure (test hook)");
    }
    if (!config::lance_index_isolation_preflight) {
        // The switch only skips the probe; isolation stays unverified and every
        // invocation is still rejected.
        LOG(INFO) << "lance isolation preflight disabled by config; isolation stays "
                     "unverified and every invocation will be rejected";
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
                    "Linux); every invocation will be rejected";
    return Status::NotSupported("lance index worker isolation requires Linux");
#endif
}

std::string LanceIndexSupervisor::invocation_cgroup_name(const std::string& invocation_id) {
    // Path-safe projection of the raw id: [A-Za-z0-9._-] only, first 64 bytes.
    // The raw-id hash suffix keeps sanitization collisions distinct and lets
    // series 3 map a cgroup path back to its invocation id.
    constexpr size_t MAX_ID_BYTES = 64;
    std::string safe;
    safe.reserve(std::min(invocation_id.size(), MAX_ID_BYTES));
    for (const unsigned char c : invocation_id) {
        if (safe.size() >= MAX_ID_BYTES) {
            break;
        }
        const bool keep = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                          (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (keep) {
            safe.push_back(static_cast<char>(c));
        }
    }
    if (safe.empty()) {
        safe = "inv";
    }
    const uint32_t hash = static_cast<uint32_t>(std::hash<std::string> {}(invocation_id));
    return fmt::format("lance-worker-{}-{:08x}", safe, hash);
}

std::string LanceIndexSupervisor::worker_debug_points_env_value(
        const std::vector<std::string>& names) {
    // Names only, fixed allowlist, canonical order (D11): nothing but the two
    // known worker-fault names may ever cross the exec boundary, whatever the
    // dispatch frame carried.
    std::string value;
    for (const char* allowed : {WORKER_DEBUG_POINT_HANG, WORKER_DEBUG_POINT_SKIP_RESULT}) {
        if (std::find(names.begin(), names.end(), allowed) != names.end()) {
            if (!value.empty()) {
                value.push_back(',');
            }
            value += allowed;
        }
    }
    return value;
}

Status LanceIndexSupervisor::execute(const TLanceIndexWorkerDispatch& dispatch,
                                     int64_t kill_deadline_ms, WorkerExecution* execution) {
    if (execution != nullptr) {
        *execution = WorkerExecution {};
    }
#ifdef __linux__
    // Never-launched rejections: the invocation provably never exec'd, so the
    // caller maps the non-OK status onto the wire's confirmed-failure path.
    auto never_launched = [&dispatch](const char* static_category) {
        return Status::CgroupError("{}", sanitize_message(static_category, dispatch));
    };
    if (!_isolation_verified.load()) {
        return Status::CgroupError(
                "lance index worker isolation is not verified (startup preflight failed or "
                "was skipped); rejecting invocation");
    }
    if (_stopping.load()) {
        return Status::Cancelled("lance index supervisor is stopping; rejecting invocation {}",
                                 dispatch.invocation_id);
    }
    if (dispatch.invocation_id.empty()) {
        return Status::InvalidArgument("lance index dispatch has an empty invocation_id");
    }

    // Fault-injection point (D11, phase = after the dequeue/admission gates,
    // before ANY cgroup/fork work): a deterministic never-launched rejection
    // that works on every runner (the negative path of the no-delegation
    // environments). The invocation provably never exec'd.
    DBUG_EXECUTE_IF("LanceIndexSupervisor.reject_after_enqueue", {
        return never_launched("debug point rejection");
    });

    SigpipeGuard sigpipe_guard;
    _inflight_count.fetch_add(1);
    CountGuard count_guard {&_inflight_count};

    // D3 dequeue rail: the caller computed kill_deadline once from the request
    // deadline; an invocation whose remaining kill budget cannot fund a minimal
    // worker run is rejected instead of launched into certain expiry.
    const int64_t remaining_to_kill_ms = kill_deadline_ms - epoch_millis_now();
    if (remaining_to_kill_ms < kMinWorkerRunMs) {
        LOG(INFO) << "lance invocation rejected: insufficient kill budget: invocation_id="
                  << dispatch.invocation_id << " be_process_epoch=" << dispatch.be_process_epoch
                  << " remaining_to_kill_ms=" << remaining_to_kill_ms;
        return never_launched("deadline budget exhausted");
    }

    // The wall watch: the caller's kill deadline, still capped by the
    // configured ceiling (a request with an hour of budget never funds an
    // hour-long worker when the operator capped it lower).
    const int64_t wallcap_ms =
            std::max<int64_t>(1, config::lance_index_worker_wallclock_limit_seconds) * 1000;
    const int64_t wall_ms = std::max<int64_t>(1, std::min(remaining_to_kill_ms, wallcap_ms));

    // The single dispatch frame, bounded before it ever reaches a pipe. Field
    // 19 (worker_deadline_ms) is stamped here from the authoritative
    // kill_deadline parameter. Informational to the worker: the worker
    // performs no deadline logic of its own and does not read the value;
    // this monotonic watch is the enforcing deadline.
    TLanceIndexWorkerDispatch wire_dispatch = dispatch;
    wire_dispatch.__set_worker_deadline_ms(kill_deadline_ms);
    std::vector<uint8_t> payload;
    if (!lance_worker_encode_compact(wire_dispatch, &payload) || payload.empty() ||
        payload.size() > MAX_DISPATCH_FRAME_BYTES) {
        return never_launched("dispatch frame encode failed");
    }
    std::vector<uint8_t> wrapped;
    lance_worker_frame_wrap(payload, &wrapped);
    const std::string dispatch_frame(reinterpret_cast<const char*>(wrapped.data()),
                                     wrapped.size());

    // Controlled environment; all heap work happens before fork(). The debug
    // points handoff is the names-only allowlist filter of frame field 18.
    std::vector<std::string> env_storage;
    std::vector<char*> envp;
    if (!build_child_env(&env_storage, &envp,
                         dispatch.__isset.debug_points
                                 ? worker_debug_points_env_value(dispatch.debug_points)
                                 : std::string())
                 .ok()) {
        return never_launched("launch environment invalid");
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
        return never_launched("pipe creation failed");
    }
    auto reject_with_pipes = [&](const char* category) {
        stdin_pipe.close_all();
        stdout_pipe.close_all();
        stderr_pipe.close_all();
        barrier_pipe.close_all();
        return never_launched(category);
    };

    // Invocation cgroup: named after the invocation id (D5 — the name is the
    // series-3 precise-kill primitive).
    const std::string cgroup_name = invocation_cgroup_name(dispatch.invocation_id);
    const std::string cgroup_dir = _cgroup_parent_abs + "/" + cgroup_name;
    const std::string cgroup_rel = cgroup_rel_of_abs(cgroup_dir);
    const int64_t memory_limit = config::lance_index_worker_memory_limit_bytes;
    const int64_t pids_max = config::lance_index_worker_pids_max;
    if (::mkdir(cgroup_dir.c_str(), 0755) != 0) {
        LOG(WARNING) << "lance invocation cgroup creation failed: invocation_id="
                     << dispatch.invocation_id << " error=" << std::strerror(errno);
        return reject_with_pipes("cgroup creation failed");
    }
    if (!write_cgroup_limits(cgroup_dir, memory_limit, pids_max).ok()) {
        remove_cgroup_dir(cgroup_dir);
        return reject_with_pipes("cgroup limit setup failed");
    }
    const int64_t oom_baseline = oom_kill_count(cgroup_dir);

    // Fork behind the barrier; migrate; verify membership; release.
    const int64_t as_limit = config::lance_index_worker_as_limit_bytes;
    const int64_t cpu_limit = std::max<int64_t>(
            1, static_cast<int64_t>(config::lance_index_worker_cpu_limit_multiplier) * wall_ms /
                       1000);
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
        return never_launched(fail_category != nullptr ? fail_category : "launch failed");
    }
    _register_inflight_child(launch.pid);

    LaunchLimits limits {memory_limit,
                         pids_max,
                         as_limit,
                         cpu_limit,
                         std::max<int64_t>(1, config::lance_index_worker_term_grace_seconds) *
                                 1000};
    Supervision sup = supervise_child(cgroup_dir, cgroup_rel, dispatch_frame, wall_ms, limits,
                                      oom_baseline, &launch);
    _unregister_inflight_child(launch.pid);

    if (sup.populated_zero) {
        remove_cgroup_dir(cgroup_dir);
    } else {
        LOG(WARNING) << "lance supervisor: leaving populated invocation cgroup " << cgroup_dir
                     << " invocation_id=" << dispatch.invocation_id;
    }

    // Respond-after-reap (D10): everything below runs only after the ladder,
    // the bounded reap, and the populated wait have completed — execute()
    // returning IS the domain-termination confirmation the caller waits for.
    const bool proof = sup.child_reaped_proof();
    if (execution != nullptr) {
        execution->child_reaped_proof = proof;
        execution->failure_category = sup.got_result ? nullptr : sup.failure_category;
    }
    if (sup.got_result) {
        // Ending (a): a complete, in-contract result frame is the only trusted
        // result. The message is rebuilt from the supervisor's own category
        // table (worker text never forwards; D12).
        const int32_t result_code = sup.result.lance_error_code;
        const int result_phase = sup.result.phase ==
                                         TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED
                                 ? 1
                                 : 2;
        if (execution != nullptr) {
            execution->kind = WorkerExecution::Kind::TRUSTED_RESULT;
            execution->result = std::move(sup.result);
            if (const char* category = result_message_category(*execution->result)) {
                execution->message = sanitize_message(category, dispatch);
            }
        }
        LOG(INFO) << "lance invocation completed: invocation_id=" << dispatch.invocation_id
                  << " be_process_epoch=" << dispatch.be_process_epoch
                  << " mutation_type=" << mutation_type_name(dispatch.mutation_type)
                  << " phase=" << result_phase << " lance_error_code=" << result_code
                  << " child_reaped_proof=" << proof;
        return Status::OK();
    }
    if (sup.pre_ffi_violation) {
        // Handshake-stage violation (S5#1): the worker writes its handshake
        // before its first FFI call in program order and the kill was issued
        // within microseconds of the frame, so no mutation can have committed.
        // WITH the reap proof this is a trusted pre-invocation rejection;
        // without it the ending is a protocol ambiguity (indeterminate) —
        // never a fabricated confirmed answer.
        if (proof) {
            if (execution != nullptr) {
                execution->kind = WorkerExecution::Kind::PROVEN_PRE_INVOCATION_REJECTION;
                TLanceIndexWorkerResult rejection;
                rejection.phase = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
                rejection.lance_error_code =
                        g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED;
                execution->result = std::move(rejection);
                execution->message = sanitize_message(
                        sup.failure_category != nullptr ? sup.failure_category
                                                        : "isolation self-check mismatch",
                        dispatch);
            }
            LOG(WARNING) << "lance invocation rejected at the isolation self-check: "
                            "invocation_id="
                         << dispatch.invocation_id << " category=" << sup.failure_category;
            return Status::OK();
        }
        LOG(WARNING) << "lance isolation self-check failed but termination evidence is "
                        "insufficient; converging indeterminate: invocation_id="
                     << dispatch.invocation_id << " category=" << sup.failure_category;
        // Fall through to the indeterminate ending.
    }
    // Ending (b): no trusted result frame (exit/signal/timeout/OOM/malformed/
    // band-crossing/unproven violation). Indeterminate, no code; the ladder
    // evidence rides for logs only and never rewrites the classification.
    {
        const char* category = sup.deadline_expired
                                       ? "wall-clock deadline expired"
                                       : (sup.failure_category != nullptr
                                                  ? sup.failure_category
                                                  : "worker exited without a result frame");
        if (execution != nullptr) {
            execution->kind = WorkerExecution::Kind::INDETERMINATE;
            if (execution->failure_category == nullptr) {
                execution->failure_category = category;
            }
            execution->message = sanitize_message(category, dispatch);
        }
        LOG(WARNING) << "lance invocation ended without a trusted result: invocation_id="
                     << dispatch.invocation_id << " category=" << category << " exit_status="
                     << (sup.reap.reaped_exact ? sup.reap.wait_status : -1)
                     << " exit_kind=" << (sup.reap.reaped_exact ? sup.reap.wait_kind : -1)
                     << " oom_kill_delta=" << sup.oom_kill_delta
                     << " populated_zero=" << sup.populated_zero
                     << " child_reaped_proof=" << proof;
    }
    return Status::OK();
#else
    (void)dispatch;
    (void)kill_deadline_ms;
    (void)execution;
    return Status::NotSupported("lance index worker isolation requires Linux");
#endif
}

void LanceIndexSupervisor::stop() {
    _stopping.store(true);
#ifdef __linux__
    // Best-effort terminate in-flight workers (correctness never depends on
    // this: the worker-side PDEATHSIG arm + getppid recheck is the backstop).
    // In-flight execute() calls converge on their own caller threads; this
    // loop only accelerates them and waits a bounded grace for the registry
    // to drain before the KILL backstop.
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
}

void LanceIndexSupervisor::_register_inflight_child(pid_t pid) {
    std::lock_guard<std::mutex> lock(_inflight_mutex);
    _inflight_children.push_back(pid);
}

void LanceIndexSupervisor::_unregister_inflight_child(pid_t pid) {
    std::lock_guard<std::mutex> lock(_inflight_mutex);
    auto it = std::find(_inflight_children.begin(), _inflight_children.end(), pid);
    if (it != _inflight_children.end()) {
        _inflight_children.erase(it);
    }
}

std::string LanceIndexSupervisor::sanitize_message(const std::string& static_category,
                                                   const TLanceIndexWorkerDispatch& dispatch) {
    if (static_category.empty()) {
        return "";
    }
    const std::string raw =
            fmt::format("{}: invocation_id={} be_process_epoch={}", static_category,
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
        while (cut > 0 && (static_cast<unsigned char>(escaped[cut]) & 0xC0) == 0x80) {
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
void LanceIndexSupervisor::force_worker_exec_for_test(std::string path,
                                                      std::vector<std::string> args) {
    _force_exec_path = std::move(path);
    _force_exec_args = std::move(args);
}
#endif

} // namespace doris::lance
