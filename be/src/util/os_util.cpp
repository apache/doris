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

#include "util/os_util.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "env/env_util.h"
#include "gutil/macros.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "util/faststring.h"

using std::string;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace doris {

// Ensure that Impala compiles on earlier kernels. If the target kernel does not support
// _SC_CLK_TCK, sysconf(_SC_CLK_TCK) will return -1.
#ifndef _SC_CLK_TCK
#define _SC_CLK_TCK 2
#endif

static const int64_t kTicksPerSec = sysconf(_SC_CLK_TCK);

// Offsets into the ../stat file array of per-thread statistics.
//
// They are themselves offset by two because the pid and comm fields of the
// file are parsed separately.
static const int64_t kUserTicks = 13 - 2;
static const int64_t kKernelTicks = 14 - 2;
static const int64_t kIoWait = 41 - 2;

// Largest offset we are interested in, to check we get a well formed stat file.
static const int64_t kMaxOffset = kIoWait;

Status parse_stat(const std::string& buffer, std::string* name, ThreadStats* stats) {
    DCHECK(stats != nullptr);

    // The thread name should be the only field with parentheses. But the name
    // itself may contain parentheses.
    size_t open_paren = buffer.find('(');
    size_t close_paren = buffer.rfind(')');
    if (open_paren == string::npos ||       // '(' must exist
        close_paren == string::npos ||      // ')' must exist
        open_paren >= close_paren ||        // '(' must come before ')'
        close_paren + 2 == buffer.size()) { // there must be at least two chars after ')'
        return Status::IOError("Unrecognised /proc format");
    }
    string extracted_name = buffer.substr(open_paren + 1, close_paren - (open_paren + 1));
    string rest = buffer.substr(close_paren + 2);
    std::vector<string> splits = Split(rest, " ", strings::SkipEmpty());
    if (splits.size() < kMaxOffset) {
        return Status::IOError("Unrecognised /proc format");
    }

    int64_t tmp;
    if (safe_strto64(splits[kUserTicks], &tmp)) {
        stats->user_ns = tmp * (1e9 / kTicksPerSec);
    }
    if (safe_strto64(splits[kKernelTicks], &tmp)) {
        stats->kernel_ns = tmp * (1e9 / kTicksPerSec);
    }
    if (safe_strto64(splits[kIoWait], &tmp)) {
        stats->iowait_ns = tmp * (1e9 / kTicksPerSec);
    }
    if (name != nullptr) {
        *name = extracted_name;
    }
    return Status::OK();
}

Status get_thread_stats(int64_t tid, ThreadStats* stats) {
    DCHECK(stats != nullptr);
    if (kTicksPerSec <= 0) {
        return Status::NotSupported("ThreadStats not supported");
    }
    faststring buf;
    RETURN_IF_ERROR(env_util::read_file_to_string(
            Env::Default(), strings::Substitute("/proc/self/task/$0/stat", tid), &buf));

    return parse_stat(buf.ToString(), nullptr, stats);
}
void disable_core_dumps() {
    struct rlimit lim;
    PCHECK(getrlimit(RLIMIT_CORE, &lim) == 0);
    lim.rlim_cur = 0;
    PCHECK(setrlimit(RLIMIT_CORE, &lim) == 0);

    // Set coredump_filter to not dump any parts of the address space.
    // Although the above disables core dumps to files, if core_pattern
    // is set to a pipe rather than a file, it's not sufficient. Setting
    // this pattern results in piping a very minimal dump into the core
    // processor (eg abrtd), thus speeding up the crash.
    int f;
    RETRY_ON_EINTR(f, open("/proc/self/coredump_filter", O_WRONLY));
    if (f >= 0) {
        ssize_t ret;
        RETRY_ON_EINTR(ret, write(f, "00000000", 8));
        int close_ret;
        RETRY_ON_EINTR(close_ret, close(f));
    }
}

bool is_being_debugged() {
#ifndef __linux__
    return false;
#else
    // Look for the TracerPid line in /proc/self/status.
    // If this is non-zero, we are being ptraced, which is indicative of gdb or strace
    // being attached.
    faststring buf;
    Status s = env_util::read_file_to_string(Env::Default(), "/proc/self/status", &buf);
    if (!s.ok()) {
        LOG(WARNING) << "could not read /proc/self/status: " << s.to_string();
        return false;
    }
    StringPiece buf_sp(reinterpret_cast<const char*>(buf.data()), buf.size());
    std::vector<StringPiece> lines = Split(buf_sp, "\n");
    for (const auto& l : lines) {
        if (!HasPrefixString(l, "TracerPid:")) continue;
        std::pair<StringPiece, StringPiece> key_val = Split(l, "\t");
        int64_t tracer_pid = -1;
        if (!safe_strto64(key_val.second.data(), key_val.second.size(), &tracer_pid)) {
            LOG(WARNING) << "Invalid line in /proc/self/status: " << l;
            return false;
        }
        return tracer_pid != 0;
    }
    LOG(WARNING) << "Could not find TracerPid line in /proc/self/status";
    return false;
#endif // __linux__
}

} // namespace doris