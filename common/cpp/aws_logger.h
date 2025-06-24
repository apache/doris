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
#include <glog/logging.h>

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
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Trace:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        default:
            break;
        }
    }

    std::atomic<Aws::Utils::Logging::LogLevel> _log_level;
};