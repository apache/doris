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

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace doris::vectorized {

class SpillLogSink : public google::LogSink {
public:
    SpillLogSink() { google::AddLogSink(this); }
    ~SpillLogSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& /*time*/,
              const char* message, std::size_t message_len) override {
        std::string_view text(message, message_len);
        if (text.starts_with("[spill]")) {
            std::lock_guard lock(_mutex);
            _entries.push_back({severity, std::string(text)});
        }
    }

    size_t count(google::LogSeverity severity, std::string_view prefix) const {
        std::lock_guard lock(_mutex);
        return static_cast<size_t>(std::ranges::count_if(_entries, [&](const auto& entry) {
            return entry.severity == severity && entry.message.starts_with(prefix);
        }));
    }

    std::string text() const {
        std::lock_guard lock(_mutex);
        std::string result;
        for (const auto& entry : _entries) {
            result += entry.message + '\n';
        }
        return result;
    }

private:
    struct Entry {
        google::LogSeverity severity;
        std::string message;
    };
    mutable std::mutex _mutex;
    std::vector<Entry> _entries;
};

} // namespace doris::vectorized
