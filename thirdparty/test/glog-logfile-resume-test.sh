#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Run after rebuilding the patched glog with build-thirdparty.sh glog.
# TP_INSTALL_DIR can point to an isolated install of either patched glog version.
# Exercise the real static library, including fdopen failure via the GNU linker.

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
TP_INSTALL_DIR="${TP_INSTALL_DIR:-${ROOT}/installed}"

if [[ "$(uname -s)" != 'Linux' ]]; then
    echo "SKIP: this test requires /proc/self/fd and GNU ld --wrap"
    exit 0
fi

[[ -f "${TP_INSTALL_DIR}/lib/libglog.a" ]] || {
    echo "FAIL: ${TP_INSTALL_DIR}/lib/libglog.a not found; rebuild glog first" >&2
    exit 1
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

cat >"${tmpdir}/test.cc" <<'EOF'
#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

static bool fail_next_fdopen = false;
static int failed_fdopens = 0;

extern "C" FILE* __real_fdopen(int fd, const char* mode);
extern "C" FILE* __wrap_fdopen(int fd, const char* mode) {
    if (fail_next_fdopen) {
        fail_next_fdopen = false;
        ++failed_fdopens;
        errno = ENOMEM;
        return nullptr;
    }
    return __real_fdopen(fd, mode);
}

static void require(bool condition, const char* message) {
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        exit(1);
    }
}

static int check_fds(const std::string& log_dir, const char* stage, int expected) {
    DIR* dir = opendir("/proc/self/fd");
    require(dir != nullptr, "cannot inspect /proc/self/fd");
    int held = 0;
    int seeds = 0;
    int deleted = 0;
    bool close_on_exec = true;
    while (dirent* entry = readdir(dir)) {
        const std::string path = std::string("/proc/self/fd/") + entry->d_name;
        char target[PATH_MAX];
        const ssize_t len = readlink(path.c_str(), target, sizeof(target) - 1);
        if (len < 0) continue;
        target[len] = '\0';
        const std::string name(target);
        if (name.find(log_dir + "/be.") != 0) continue;
        const int fd = atoi(entry->d_name);
        struct stat st;
        require(fstat(fd, &st) == 0, "fstat failed");
        if (st.st_nlink == 0) ++deleted;
        close_on_exec = close_on_exec && (fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0;
        ++held;
        if (name.find(".seed") != std::string::npos) ++seeds;
    }
    closedir(dir);
    printf("%s: held=%d expected=%d deleted=%d\n", stage, held, expected, deleted);
    require(held == expected, "unexpected number of log fds");
    require(deleted == 0, "deleted log still has an open fd");
    require(close_on_exec, "log fd lost FD_CLOEXEC");
    return seeds;
}

int main(int argc, char** argv) {
    require(argc == 3, "usage: test LOG_DIRECTORY MODE");
    const std::string log_dir = argv[1];
    const std::string mode = argv[2];
    const bool seeded = mode != "empty";
    const google::LogSeverity severities[] = {google::GLOG_INFO, google::GLOG_WARNING};
    const char* names[] = {"INFO", "WARNING"};
    const std::string large(1024 * 1024, 's');
    const std::string marker = "resume\n";
    std::string seeds[2];
    for (int i = 0; i < 2; ++i) {
        seeds[i] = log_dir + "/be." + names[i] + ".log.seed";
        if (seeded) {
            std::ofstream seed(seeds[i]);
            seed << large;
            require(seed.good(), "cannot seed log");
        }
    }

    FLAGS_log_dir = log_dir;
    FLAGS_log_split_method = "size";
    FLAGS_log_filenum_quota = mode == "quota-one" ? 1 : 2;
    FLAGS_max_log_size = 1;
    google::InitGoogleLogging("be");
    time_t timestamp = 1800000000;
    for (int i = 0; i < 2; ++i) {
        fail_next_fdopen = mode == "fdopen-failure";
        google::base::GetLogger(severities[i])
                ->Write(true, timestamp, marker.data(), marker.size());
        if (mode == "resume") {
            struct stat st;
            require(stat(seeds[i].c_str(), &st) == 0, "existing log disappeared");
            require(st.st_size == static_cast<off_t>(large.size() + marker.size()),
                    "existing log was not appended");
        }
    }
    const int held_seeds = check_fds(log_dir, "initial write", 2);
    if (mode == "resume") {
        require(held_seeds == 2, "did not resume both existing logs");
        // The seeded files already exceed max_log_size. Their saved length must
        // trigger rotation on the next write, even though this message is tiny.
        for (auto severity : severities) {
            google::base::GetLogger(severity)->Write(true, timestamp + 1, marker.data(),
                                                     marker.size());
        }
        require(check_fds(log_dir, "rotation from saved length", 2) == 0,
                "resumed file length was not preserved");
    }
    if (mode == "fdopen-failure") {
        require(failed_fdopens == 2, "did not inject both resume failures");
    }

    // Rotate enough times for the quota to unlink both seeded inodes. A leaked
    // raw fd would keep the deleted files open even after closing each FILE*.
    for (int round = 0; round < 4; ++round) {
        timestamp += 2;
        for (auto severity : severities) {
            google::base::GetLogger(severity)->Write(true, timestamp, large.data(), large.size());
            google::base::GetLogger(severity)->Write(true, timestamp + 1, marker.data(),
                                                     marker.size());
        }
        check_fds(log_dir, "quota rotation", 2);
    }
    for (const auto& seed : seeds) {
        require(access(seed.c_str(), F_OK) != 0 && errno == ENOENT,
                "quota did not remove seeded log");
    }
    google::ShutdownGoogleLogging();
    check_fds(log_dir, "shutdown", 0);
    printf("PASS: %s\n", mode.c_str());
}
EOF

extra_libs=()
[[ -f "${TP_INSTALL_DIR}/lib/libgflags.a" ]] && extra_libs+=("${TP_INSTALL_DIR}/lib/libgflags.a")

"${CXX:-c++}" -std=c++11 -O0 -g -Wall -Wextra -Werror \
    -I"${TP_INSTALL_DIR}/include" "${tmpdir}/test.cc" \
    "${TP_INSTALL_DIR}/lib/libglog.a" "${extra_libs[@]}" \
    -Wl,--wrap=fdopen -pthread -ldl -o "${tmpdir}/test"

for mode in resume empty quota-one fdopen-failure; do
    mkdir "${tmpdir}/${mode}"
    "${tmpdir}/test" "${tmpdir}/${mode}" "${mode}"
done
