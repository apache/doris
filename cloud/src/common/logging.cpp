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

#include "logging.h"

#include <bthread/bthread.h>
#include <bthread/types.h>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>

#include <iomanip>
#include <iostream>
#include <mutex>

#include "config.h"

namespace doris::cloud {

static butil::LinkedList<AnnotateTag>* get_annotate_tag_list() {
    static std::once_flag log_annotated_tags_key_once;
    static bthread_key_t log_annotated_tags_key;
    std::call_once(
            log_annotated_tags_key_once,
            +[](bthread_key_t* key) {
                bthread_key_create(
                        key, +[](void* value) {
                            delete reinterpret_cast<butil::LinkedList<AnnotateTag>*>(value);
                        });
            },
            &log_annotated_tags_key);

    auto* tag_list = reinterpret_cast<butil::LinkedList<AnnotateTag>*>(
            bthread_getspecific(log_annotated_tags_key));
    if (!tag_list) {
        tag_list = new butil::LinkedList<AnnotateTag>();
        bthread_setspecific(log_annotated_tags_key, tag_list);
    }
    return tag_list;
}

AnnotateTag::AnnotateTag(default_tag_t, std::string_view key, std::string value)
        : key_(key), value_(std::move(value)) {
    get_annotate_tag_list()->Append(this);
}

AnnotateTag::AnnotateTag(std::string_view key, std::string_view value)
        : AnnotateTag(default_tag, key, fmt::format("\"{}\"", value)) {}

AnnotateTag::~AnnotateTag() {
    RemoveFromList();
}

void AnnotateTag::format_tag_list(std::ostream& stream) {
    butil::LinkedList<AnnotateTag>* list = get_annotate_tag_list();
    butil::LinkNode<AnnotateTag>* head = list->head();
    const butil::LinkNode<AnnotateTag>* end = list->end();
    for (; head != end; head = head->next()) {
        stream << ' ' << head->value()->key_ << '=' << head->value()->value_;
    }
}

void custom_prefix(std::ostream& s, const google::LogMessageInfo& l, void*) {
    // Add prefix "RuntimeLogger ".
    s << "RuntimeLogger ";
    // Same as in fe.log
    // The following is same as default log format. eg:
    // I20240605 15:25:15.677153 1763151 meta_service_txn.cpp:481] msg...
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

// Implement the custom log format for stdout output in K8S environment
// Format: I20240605 15:25:15.677153 1763151 meta_service_txn.cpp:481] msg...
struct StdoutLogSink : google::LogSink {
    void send(google::LogSeverity severity, const char* /*full_filename*/,
              const char* base_filename, int line, const google::LogMessageTime& time,
              const char* message, std::size_t message_len) override {
        // Convert log severity to corresponding character (I/W/E/F)
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

        // 4. Thread ID
        std::cout << " " << std::setfill(' ') << std::setw(5) << getpid() << std::setfill('0');

        // 5. Filename and line number
        std::cout << " " << base_filename << ":" << line << "] ";

        // 6. Log message
        std::cout.write(message, message_len);

        // Add newline and flush
        std::cout << std::endl;
    }
};

static StdoutLogSink stdout_log_sink;

/**
 * @param basename the basename of log file
 * @return true for success
 */
bool init_glog(const char* basename) {
    static std::mutex mtx;
    static bool inited = false;
    std::lock_guard<std::mutex> logging_lock(mtx);
    if (inited) return true;

    bool log_to_console = (getenv("DORIS_LOG_TO_STDERR") != nullptr);
    if (log_to_console) {
        if (config::enable_file_logger) {
            // will output log to log file and output log to stdout
            google::AddLogSink(&stdout_log_sink);
        } else {
            // enable_file_logger is false, will only output log to stdout
            // Not output to stderr because doris_cloud.out will output log to stderr
            FLAGS_logtostdout = true;
        }
    } else {
        FLAGS_alsologtostderr = false;
    }

    // Don't log to stderr except fatal level
    // so fatal log can output to doris_cloud.out .
    FLAGS_stderrthreshold = google::FATAL;

    // Set glog log dir
    FLAGS_log_dir = config::log_dir;
    // Buffer log messages for at most this many seconds
    FLAGS_logbufsecs = 1;
    // Set log roll mode
    // Candidates: day, hour, size
    FLAGS_log_split_method = "size";
    // Sets the maximum log file size (in MB).
    FLAGS_max_log_size = config::log_size_mb;
    // Set roll num
    FLAGS_log_filenum_quota = config::log_filenum_quota;

#ifdef GLOG_HAS_WARN_LOG_FILENUM_QUOTA
    // Set warn log roll num
    FLAGS_warn_log_filenum_quota = config::warn_log_filenum_quota;
#endif

    // clang-format off
    // set log level
    std::string& loglevel = config::log_level;
    // Can be 0 1 2 3 ... the larger the higher level for logging,
    // corrensponding to INFO WARNING ERROR FATAL
    // const int GLOG_INFO = 0, GLOG_WARNING = 1, GLOG_ERROR = 2, GLOG_FATAL = 3, NUM_SEVERITIES = 4;
    auto tolower = [](std::string s) { for (auto& i : s) i |= 0x20; return s; };
    FLAGS_minloglevel = tolower(loglevel) == "info"  ? 0
                      : tolower(loglevel) == "warn"  ? 1
                      : tolower(loglevel) == "error" ? 2
                      : tolower(loglevel) == "fatal" ? 3
                      :                                0; // Default INFO
    // clang-format on

    // Log messages at a level <= this flag are buffered.
    // Log messages at a higher level are flushed immediately.
    FLAGS_logbuflevel = config::log_immediate_flush ? -1 : 0;

    // Set verbose modules
    FLAGS_v = -1;
    for (auto& i : config::log_verbose_modules) {
        if (i.empty()) continue;
        google::SetVLOGLevel(i.c_str(), config::log_verbose_level);
    }
    if (log_to_console) {
        // Only add prefix if log output to stderr
        google::InitGoogleLogging(basename, &custom_prefix);
    } else {
        google::InitGoogleLogging(basename);
    }
    inited = true;
    return true;
}

} // namespace doris::cloud
