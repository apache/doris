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

// GLOG defines this based on the system but doesn't check if it's already
// been defined.  undef it first to avoid warnings.
// glog MUST be included before gflags.  Instead of including them,
// our files should include this file instead.
#undef _XOPEN_SOURCE
// This is including a glog internal file.  We want this to expose the
// function to get the stack trace.
#include <glog/logging.h>
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

/// Wrap a glog stream and tag on the log. usage:
///   LOG_INFO("here is an info for a {} query", query_type).tag("query_id", queryId);
#define LOG_INFO(...) doris::TaggableLogger(LOG(INFO), ##__VA_ARGS__)
#define LOG_WARNING(...) doris::TaggableLogger(LOG(WARNING), ##__VA_ARGS__)
#define LOG_ERROR(...) doris::TaggableLogger(LOG(ERROR), ##__VA_ARGS__)
#define LOG_FATAL(...) doris::TaggableLogger(LOG(FATAL), ##__VA_ARGS__)

class TaggableLogger {
public:
    template <typename... Args>
    TaggableLogger(std::ostream& stream, std::string_view fmt, Args&&... args) : _stream(stream) {
        if constexpr (sizeof...(args) == 0) {
            _stream << fmt;
        } else {
            _stream << fmt::format(fmt, std::forward<Args>(args)...);
        }
    };

    template <typename V>
    TaggableLogger& tag(std::string_view key, const V& value) {
        _stream << '|' << key << '=';
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

private:
    std::ostream& _stream;
};

} // namespace doris
