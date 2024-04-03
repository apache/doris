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

#include <memory>

// GLOG defines this based on the system but doesn't check if it's already
// been defined.  undef it first to avoid warnings.
// glog MUST be included before gflags.  Instead of including them,
// our files should include this file instead.
#undef _XOPEN_SOURCE
// This is including a glog internal file.  We want this to expose the
// function to get the stack trace.
#include <glog/logging.h> // IWYU pragma: export
#undef MutexLock

// Define VLOG levels.  We want display per-row info less than per-file which
// is less than per-query.  For now per-connection is the same as per-query.
#define VLOG_CONNECTION VLOG(1)
#define VLOG_RPC VLOG(8)
#define VLOG_QUERY VLOG(1)
#define VLOG_FILE VLOG(2)
#define VLOG_ROW VLOG(10)
#define VLOG_PROGRESS VLOG(2)
#define VLOG_TRACE VLOG(10)
#define VLOG_DEBUG VLOG(7)
#define VLOG_NOTICE VLOG(3)
#define VLOG_CRITICAL VLOG(1)

#define VLOG_CONNECTION_IS_ON VLOG_IS_ON(1)
#define VLOG_RPC_IS_ON VLOG_IS_ON(8)
#define VLOG_QUERY_IS_ON VLOG_IS_ON(1)
#define VLOG_FILE_IS_ON VLOG_IS_ON(2)
#define VLOG_ROW_IS_ON VLOG_IS_ON(10)
#define VLOG_TRACE_IS_ON VLOG_IS_ON(10)
#define VLOG_DEBUG_IS_ON VLOG_IS_ON(7)
#define VLOG_NOTICE_IS_ON VLOG_IS_ON(3)
#define VLOG_CRITICAL_IS_ON VLOG_IS_ON(1)

/// Define a wrapper around DCHECK for strongly typed enums that print a useful error
/// message on failure.
#define DCHECK_ENUM_EQ(a, b)                                                 \
    DCHECK(a == b) << "[ " #a " = " << static_cast<int>(a) << " , " #b " = " \
                   << static_cast<int>(b) << " ]"

#include <fmt/format.h>

#include "util/uid_util.h"

namespace doris {

// glog doesn't allow multiple invocations of InitGoogleLogging. This method conditionally
// calls InitGoogleLogging only if it hasn't been called before.
bool init_glog(const char* basename);

// Shuts down the google logging library. Call before exit to ensure that log files are
// flushed. May only be called once.
void shutdown_logging();

void update_logging(const std::string& name, const std::string& value);

class TaggableLogger {
public:
    TaggableLogger(const char* file, int line, google::LogSeverity severity)
            : _msg(file, line, severity) {}

    template <typename... Args>
    TaggableLogger& operator()(const std::string_view& fmt, Args&&... args) {
        if constexpr (sizeof...(args) == 0) {
            _msg.stream() << fmt;
        } else {
            _msg.stream() << fmt::format(fmt, std::forward<Args&&>(args)...);
        }
        return *this;
    }

    template <typename V>
    TaggableLogger& tag(std::string_view key, V&& value) {
        _msg.stream() << '|' << key << '=';
        if constexpr (std::is_same_v<V, TUniqueId> || std::is_same_v<V, PUniqueId>) {
            _msg.stream() << print_id(value);
        } else {
            _msg.stream() << value;
        }
        return *this;
    }

    template <typename E>
    TaggableLogger& error(E&& error) {
        _msg.stream() << "|error=" << error;
        return *this;
    }

private:
    google::LogMessage _msg;
};

// Very very important!!!!
// Never define LOG_DEBUG or LOG_TRACE. because the tagged logging method will
// always generated string and then check the log level, its performane is bad.
// glog's original method will first check log level if it is not satisfied,
// the log message is not generated.
#define LOG_INFO TaggableLogger(__FILE__, __LINE__, google::GLOG_INFO)
#define LOG_WARNING TaggableLogger(__FILE__, __LINE__, google::GLOG_WARNING)
#define LOG_ERROR TaggableLogger(__FILE__, __LINE__, google::GLOG_ERROR)
#define LOG_FATAL TaggableLogger(__FILE__, __LINE__, google::GLOG_FATAL)

} // namespace doris
