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

#ifndef DORIS_BE_SRC_COMMON_UTIL_LOGGING_H
#define DORIS_BE_SRC_COMMON_UTIL_LOGGING_H

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include <atomic>
#include <string>

#include "common/logging.h"
#include "util/uid_util.h"
#include "gutil/walltime.h"

namespace doris {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
bool init_glog(const char* basename);

// Shuts down the google logging library. Call before exit to ensure that log files are
// flushed. May only be called once.
void shutdown_logging();

// Format a timestamp in the same format as used by GLog.
std::string FormatTimestampForLog(MicrosecondsInt64 micros_since_epoch);

class DorisAWSLogger final : public Aws::Utils::Logging::LogSystemInterface {
public:
    DorisAWSLogger() : _log_level(Aws::Utils::Logging::LogLevel::Info) {}
    DorisAWSLogger(Aws::Utils::Logging::LogLevel log_level) : _log_level(log_level) {}
    ~DorisAWSLogger() final = default;
    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return _log_level; }
    void Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str,
             ...) final {
        _log_impl(log_level, tag, format_str);
    }
    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag,
                   const Aws::OStringStream& message_stream) final {
        _log_impl(log_level, tag, message_stream.str().c_str());
    }

    void Flush() final {}

private:
    void _log_impl(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* message) {
        switch (log_level) {
        case Aws::Utils::Logging::LogLevel::Off:
            break;
        case Aws::Utils::Logging::LogLevel::Fatal:
            LOG(FATAL) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Error:
            LOG(ERROR) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Warn:
            LOG(WARNING) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Info:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Debug:
            VLOG_ROW << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Trace:
            VLOG_ROW << "[" << tag << "] " << message;
            break;
        default:
            break;
        }
    }

    std::atomic<Aws::Utils::Logging::LogLevel> _log_level;
};

/// Wrap a glog stream and tag on the log. usage:
///   TAG(LOG(INFO)).tag("query_id", queryId).log("here is an info for a query");
///
/// TAG is the macro to TaggableLogger, which use method tag(key, value) to add tags
/// and log(fmt, ...) to flush and emit the log. Usually the tag key is determined,
/// like "query_id", so we use specified tag methods more often, like query_id(id).
/// You can add a new tag method if needed.
///
/// You can custom your tagged logging format in logconfig.cpp, the default is like
/// "#message#|k1=v1|k2=v2". You can also custom all the tag names. For example, if
/// you wish to use camelCase style, just set tag name constants like QUERY_ID to
/// "queryId". The constants is also listed in logconfig.cpp.
///
/// The transfer from the variable of tag method to string is immediate. If a tagged
/// logging has time-consuming to-string procedure and is logged to VLOG(10) which
/// may not be processed, check VLOG_IS_ON(n) first.
#define TAG doris::TaggableLogger

class TaggableLogger {
public:
    TaggableLogger(std::ostream& _stream) : _stream(_stream), _tags(nullptr) {};

    ~TaggableLogger() {
        flush();
    }

    void flush();

    TaggableLogger& log(std::string&& message) {
        _message = std::move(message);
        return *this;
    }

    inline TaggableLogger& tag(const std::string& key, const std::string& value) {
        _tags = new Tags(key, value, _tags);
        return *this;
    }

    inline TaggableLogger& tag(const std::string& key, std::string&& value) {
        _tags = new Tags(key, std::move(value), _tags);
        return *this;
    }

private:
    std::ostream& _stream;
    std::string _message;

    struct Tags {
        const std::string key;
        const std::string value;
        Tags* next;

        Tags(const std::string& key, const std::string& value, Tags* next) : key(key), value(value), next(next) {}
        Tags(const std::string& key, std::string&& value, Tags* next) : key(key), value(std::move(value)), next(next) {}
    };

    Tags* _tags;

public:
    // add tag method here
    const static std::string QUERY_ID;

    TaggableLogger& query_id(const std::string& query_id) {
        return tag(QUERY_ID, query_id);
    }

    TaggableLogger& query_id(const TUniqueId& query_id) {
        return tag(QUERY_ID, print_id(query_id));
    }

    TaggableLogger& query_id(const PUniqueId& query_id) {
        return tag(QUERY_ID, print_id(query_id));
    }

    const static std::string INSTANCE_ID;

    TaggableLogger& instance_id(const std::string& instance_id) {
        return tag(INSTANCE_ID, instance_id);
    }

    TaggableLogger& instance_id(const TUniqueId& instance_id) {
        return tag(INSTANCE_ID, print_id(instance_id));
    }

    TaggableLogger& instance_id(const PUniqueId& instance_id) {
        return tag(INSTANCE_ID, print_id(instance_id));
    }
};

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_UTIL_LOGGING_H
