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

#include <ctype.h>
#include <stdint.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

static bool logging_initialized = false;

static std::mutex logging_mutex;

// Implement the custom log format: I20250118 10:53:06.239614 1318521 timezone_utils.cpp:115] Preloaded653 timezones.
struct StdoutLogSink : google::LogSink {
    void send(google::LogSeverity severity, const char* /*full_filename*/,
              const char* base_filename, int line, const google::LogMessageTime& time,
              const char* message, std::size_t message_len) override {
        // 1.  Convert log severity to corresponding character (I/W/E/F)
        char severity_char;
        switch (severity) {
        case google::GLOG_INFO:
            severity_char = 'I';
            break;
        case google::GLOG_WARNING:
            severity_char = 'W';
            break;
        case google::GLOG_ERROR:
            severity_char = 'E';
            break;
        case google::GLOG_FATAL:
            severity_char = 'F';
            break;
        default:
            severity_char = '?';
            break;
        }
        // Set output formatting flags
        std::cout << std::setfill('0');

        // 1. Log severity (I/W/E/F)
        std::cout << severity_char;

        // 2. Date (YYYYMMDD)
        // Note: tm_year is years since 1900, tm_mon is 0-based (0-11)
        std::cout << std::setw(4) << (time.year() + 1900) << std::setw(2) << std::setfill('0')
                  << (time.month() + 1) << std::setw(2) << std::setfill('0') << time.day();

        // 3. Time (HH:MM:SS.ffffff)
        std::cout << " " << std::setw(2) << std::setfill('0') << time.hour() << ":" << std::setw(2)
                  << std::setfill('0') << time.min() << ":" << std::setw(2) << std::setfill('0')
                  << time.sec() << "." << std::setw(6) << std::setfill('0') << time.usec();

        // 4. Process ID
        std::cout << " " << getpid();

        // 5. Filename and line number
        std::cout << " " << base_filename << ":" << line << "] ";

        // 6. Log message
        std::cout.write(message, message_len);

        // Add newline and flush
        std::cout << std::endl;
    }
};

static StdoutLogSink stdout_log_sink;

static bool iequals(const std::string& a, const std::string& b) {
    unsigned int sz = a.size();
    if (b.size() != sz) {
        return false;
    }
    for (unsigned int i = 0; i < sz; ++i) {
        if (tolower(a[i]) != tolower(b[i])) {
            return false;
        }
    }
    return true;
}

// if custom_date_time_format = false, same format as in be.log
// The following is same as default log format. eg:
// I20240605 15:25:15.677153 1763151 wal_manager.cpp:481] msg...
template <bool add_runtime_logger_prefix = false, bool custom_date_time_format = false>
void custom_prefix(std::ostream& s, const google::LogMessageInfo& l, void* arg) {
    if constexpr (add_runtime_logger_prefix) {
        // Add prefix "RuntimeLogger ".
        s << "RuntimeLogger ";
    }
    s << l.severity[0];

    // Add a space if custom_date_time_format.
    if constexpr (custom_date_time_format) {
        s << ' ';
    }

    std::tm tm_time = {};
    tm_time.tm_year = l.time.year();
    tm_time.tm_mon = l.time.month();
    tm_time.tm_mday = l.time.day();
    tm_time.tm_hour = l.time.hour();
    tm_time.tm_min = l.time.min();
    tm_time.tm_sec = l.time.sec();

    if constexpr (custom_date_time_format) {
        s << std::put_time(&tm_time, config::sys_log_custom_date_time_format.c_str());
        if (!config::sys_log_custom_date_time_ms_format.empty()) {
            s << fmt::format(config::sys_log_custom_date_time_ms_format, l.time.usec() / 1000);
        }
    } else {
        s << std::put_time(&tm_time, "%Y%m%d %H:%M:%S");
        s << "." << std::setw(6) << l.time.usec();
    }

    s << ' ';
    s << std::setfill(' ') << std::setw(5);
    s << l.thread_id << std::setfill('0');
    s << ' ';
    s << l.filename << ':' << l.line_number << "]";
}

bool init_glog(const char* basename) {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);

    if (logging_initialized) {
        return true;
    }

    bool log_to_console = (getenv("DORIS_LOG_TO_STDERR") != nullptr);
    if (log_to_console) {
        if (doris::config::enable_file_logger) {
            // will output log to be.info and output log to stdout
            google::AddLogSink(&stdout_log_sink);
        } else {
            // enable_file_logger is false, will only output log to stdout
            // Not output to stderr because be.out will output log to stderr
            FLAGS_logtostdout = true;
        }
    }

    // don't log to stderr except fatal level
    // so fatal log can output to be.out .
    FLAGS_stderrthreshold = google::FATAL;
    // set glog log dir
    // ATTN: sys_log_dir is deprecated, this is just for compatibility
    std::string log_dir = config::sys_log_dir;
    if (log_dir == "") {
        log_dir = getenv("LOG_DIR");
    }
    FLAGS_log_dir = log_dir;
    // 0 means buffer INFO only
    FLAGS_logbuflevel = 0;
    // buffer log messages for at most this many seconds
    FLAGS_logbufsecs = 30;
    // set roll num
    FLAGS_log_filenum_quota = config::sys_log_roll_num;

    // set log level
    std::string& loglevel = config::sys_log_level;
    if (iequals(loglevel, "INFO")) {
        FLAGS_minloglevel = 0;
    } else if (iequals(loglevel, "WARNING")) {
        FLAGS_minloglevel = 1;
    } else if (iequals(loglevel, "ERROR")) {
        FLAGS_minloglevel = 2;
    } else if (iequals(loglevel, "FATAL")) {
        FLAGS_minloglevel = 3;
    } else {
        std::cerr << "sys_log_level needs to be INFO, WARNING, ERROR, FATAL" << std::endl;
        return false;
    }

    // set log buffer level
    // default is 0
    std::string& logbuflevel = config::log_buffer_level;
    if (iequals(logbuflevel, "-1")) {
        FLAGS_logbuflevel = -1;
    } else if (iequals(logbuflevel, "0")) {
        FLAGS_logbuflevel = 0;
    }

    // set log roll mode
    std::string& rollmode = config::sys_log_roll_mode;
    std::string sizeflag = "SIZE-MB-";
    bool ok = false;
    if (rollmode.compare("TIME-DAY") == 0) {
        FLAGS_log_split_method = "day";
        ok = true;
    } else if (rollmode.compare("TIME-HOUR") == 0) {
        FLAGS_log_split_method = "hour";
        ok = true;
    } else if (rollmode.substr(0, sizeflag.length()).compare(sizeflag) == 0) {
        FLAGS_log_split_method = "size";
        std::string sizestr = rollmode.substr(sizeflag.size(), rollmode.size() - sizeflag.size());
        if (sizestr.size() != 0) {
            char* end = nullptr;
            errno = 0;
            const char* sizecstr = sizestr.c_str();
            int64_t ret64 = strtoll(sizecstr, &end, 10);
            if ((errno == 0) && (end == sizecstr + strlen(sizecstr))) {
                int32_t retval = static_cast<int32_t>(ret64);
                if (retval == ret64) {
                    FLAGS_max_log_size = retval;
                    ok = true;
                }
            }
        }
    } else {
        ok = false;
    }
    if (!ok) {
        std::cerr << "sys_log_roll_mode needs to be TIME-DAY, TIME-HOUR, SIZE-MB-nnn" << std::endl;
        return false;
    }

    // set verbose modules.
    FLAGS_v = config::sys_log_verbose_flags_v;
    std::vector<std::string>& verbose_modules = config::sys_log_verbose_modules;
    int32_t vlog_level = config::sys_log_verbose_level;
    for (size_t i = 0; i < verbose_modules.size(); i++) {
        if (verbose_modules[i].size() != 0) {
            google::SetVLOGLevel(verbose_modules[i].c_str(), vlog_level);
        }
    }

    if (log_to_console) {
        // Add prefix if log output to stderr
        if (config::sys_log_enable_custom_date_time_format) {
            google::InitGoogleLogging(basename, &custom_prefix<true, true>);
        } else {
            google::InitGoogleLogging(basename, &custom_prefix<true, false>);
        }
    } else {
        if (config::sys_log_enable_custom_date_time_format) {
            google::InitGoogleLogging(basename, &custom_prefix<false, true>);
        } else {
            google::InitGoogleLogging(basename);
        }
    }

    logging_initialized = true;

    return true;
}

void shutdown_logging() {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);
    google::RemoveLogSink(&stdout_log_sink);
    google::ShutdownGoogleLogging();
}

void update_logging(const std::string& name, const std::string& value) {
    if ("sys_log_level" == name) {
        if (iequals(value, "INFO")) {
            FLAGS_minloglevel = 0;
        } else if (iequals(value, "WARNING")) {
            FLAGS_minloglevel = 1;
        } else if (iequals(value, "ERROR")) {
            FLAGS_minloglevel = 2;
        } else if (iequals(value, "FATAL")) {
            FLAGS_minloglevel = 3;
        } else {
            LOG(WARNING) << "update sys_log_level failed, need to be INFO, WARNING, ERROR, FATAL";
        }
    }
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
