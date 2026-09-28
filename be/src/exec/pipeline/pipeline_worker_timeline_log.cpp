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

#include "exec/pipeline/pipeline_worker_timeline_log.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iterator>
#include <memory>
#include <system_error>

#include "common/config.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

constexpr size_t WRITE_BUFFER_SIZE = 1 << 20;
// Bounds how much of the timeline is lost if the BE dies without running thread exit hooks.
constexpr int64_t FLUSH_INTERVAL_MS = 1000;

std::filesystem::path base_log_dir() {
    const char* env_log_dir = std::getenv("LOG_DIR");
    if (env_log_dir != nullptr && env_log_dir[0] != '\0') {
        return std::filesystem::path(env_log_dir);
    }
    if (!config::sys_log_dir.empty()) {
        return std::filesystem::path(config::sys_log_dir);
    }
    return std::filesystem::current_path();
}

std::string startup_time_str() {
    std::time_t now = std::time(nullptr);
    std::tm local_tm {};
    localtime_r(&now, &local_tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y%m%d-%H%M%S", &local_tm);
    return buf;
}

bool create_log_dir() {
    std::error_code ec;
    std::filesystem::create_directories(worker_timeline_log_dir(), ec);
    if (ec) {
        LOG(WARNING) << "failed to create pipeline worker timeline log dir "
                     << worker_timeline_log_dir() << ": " << ec.message();
        return false;
    }
    return true;
}

class WorkerTimelineLogger {
public:
    ~WorkerTimelineLogger() {
        if (_file != nullptr) {
            std::fclose(_file);
        }
    }

    void record(const std::string& scheduler, int index, const TUniqueId& query_id, bool start) {
        if (_file == nullptr && !_open(scheduler, index)) {
            return;
        }
        fmt::memory_buffer line;
        fmt::format_to(std::back_inserter(line), "{} {} {}\n", UnixMicros(), print_id(query_id),
                       start ? "START" : "END");
        std::fwrite(line.data(), 1, line.size(), _file);

        const int64_t now_ms = MonotonicMillis();
        if (now_ms - _last_flush_ms >= FLUSH_INTERVAL_MS) {
            std::fflush(_file);
            _last_flush_ms = now_ms;
        }
    }

private:
    bool _open(const std::string& scheduler, int index) {
        if (_open_failed) {
            return false;
        }
        _open_failed = true;
        if (!create_log_dir()) {
            return false;
        }
        std::string file_name = fmt::format("{}_{}.log", scheduler, index);
        std::replace(file_name.begin(), file_name.end(), '/', '_');
        const auto path = worker_timeline_log_dir() / file_name;
        _file = std::fopen(path.c_str(), "a");
        if (_file == nullptr) {
            LOG(WARNING) << "failed to open pipeline worker timeline log " << path;
            return false;
        }
        _buffer = std::make_unique<char[]>(WRITE_BUFFER_SIZE);
        std::setvbuf(_file, _buffer.get(), _IOFBF, WRITE_BUFFER_SIZE);
        _last_flush_ms = MonotonicMillis();
        _open_failed = false;
        return true;
    }

    FILE* _file = nullptr;
    // Backs the stdio buffer of `_file`, so it must outlive the fclose() in the destructor.
    std::unique_ptr<char[]> _buffer;
    bool _open_failed = false;
    int64_t _last_flush_ms = 0;
};

} // namespace

const std::filesystem::path& worker_timeline_log_dir() {
    static const std::filesystem::path dir =
            base_log_dir() / "worker_timeline" / startup_time_str();
    return dir;
}

void init_worker_timeline_log_dir() {
    if (!config::enable_pipeline_worker_timeline_log) {
        return;
    }
    if (create_log_dir()) {
        LOG(INFO) << "pipeline worker timeline log dir: " << worker_timeline_log_dir();
    }
}

void worker_timeline_record(const std::string& scheduler, int index, const TUniqueId& query_id,
                            bool start) {
    if (!config::enable_pipeline_worker_timeline_log) {
        return;
    }
    thread_local WorkerTimelineLogger logger;
    logger.record(scheduler, index, query_id, start);
}

#include "common/compile_check_end.h"
} // namespace doris
