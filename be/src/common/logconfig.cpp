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

static bool logging_initialized = false;

static std::mutex logging_mutex;

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

void custom_prefix(std::ostream& s, const google::LogMessageInfo& l, void*) {
    // Add prefix "RuntimeLogger ".
    s << "RuntimeLogger ";
    // Same as in fe.log
    // The following is same as default log format. eg:
    // I20240605 15:25:15.677153 1763151 wal_manager.cpp:481] msg...
    s << l.severity[0];
    s << std::setw(4) << 1900 + l.time.year();
    s << std::setw(2) << 1 + l.time.month();
    s << std::setw(2) << l.time.day();
    s << ' ';
    s << std::setw(2) << l.time.hour() << ':';
    s << std::setw(2) << l.time.min() << ':';
    s << std::setw(2) << l.time.sec() << ".";
    s << std::setw(6) << l.time.usec();
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
        if (config::enable_file_logger) {
            FLAGS_alsologtostderr = true;
        } else {
            FLAGS_logtostderr = true;
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
        // Only add prefix if log output to stderr
        google::InitGoogleLogging(basename, &custom_prefix);
    } else {
        google::InitGoogleLogging(basename);
    }

    logging_initialized = true;

    return true;
}

void shutdown_logging() {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);
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

} // namespace doris
