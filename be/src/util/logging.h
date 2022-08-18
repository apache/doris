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

#pragma once

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include <atomic>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/logging.h"
#include "gutil/walltime.h"
#include "util/uid_util.h"

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
///   TAG(LOG(INFO)).log("here is an info for a query").tag("query_id", queryId);
///
/// TAG is the macro to TaggableLogger, which use method tag(key, value) to add tags
/// and log(fmt, ...) to flush and emit the log. Usually the tag key is determined,
/// like "query_id", so we use specified tag methods more often, like query_id(id).
/// You can add a new tag method if needed.
#define TAG doris::TaggableLogger

#define TAG_LOG(severity) TAG(LOG(severity))

class TaggableLogger {
public:
    TaggableLogger(std::ostream& _stream) : _stream(_stream) {};

    TaggableLogger& log(std::string_view message) {
        _stream << message;
        return *this;
    }

    template <typename V>
    TaggableLogger& tag(std::string_view key, const V& value) {
        _stream << '|' << key << '=' << value;
        return *this;
    }

private:
    std::ostream& _stream;

public:
    template <typename V>
    TaggableLogger& query_id(const V& value) {
        _stream << "|query_id=";
        if constexpr (std::is_same_v<V, TUniqueId> || std::is_same_v<V, PUniqueId>) {
            _stream << print_id(value);
        } else {
            _stream << value;
        }
        return *this;
    }

    template <typename V>
    TaggableLogger& instance_id(const V& value) {
        _stream << "|instance_id=";
        if constexpr (std::is_same_v<V, TUniqueId> || std::is_same_v<V, PUniqueId>) {
            _stream << print_id(value);
        } else {
            _stream << value;
        }
        return *this;
    }

    template <typename E>
    TaggableLogger& error(const E& error) {
        _stream << "|error=" << error;
        return *this;
    }
};

} // namespace doris
