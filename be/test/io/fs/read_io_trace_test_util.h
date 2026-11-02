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

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"

namespace doris::io {

// Scoped capture of the actual JSON submitted to the writer, shared by end-to-end IO tests.
class ReadIOTraceCapture {
public:
    ReadIOTraceCapture() : _old_enabled(config::enable_read_io_trace) {
        config::enable_read_io_trace = true;
        SyncPoint::get_instance()->set_call_back(
                "ReadIOTrace::record",
                [this](auto&& args) {
                    std::lock_guard lock(_mutex);
                    _lines.push_back(*try_any_cast<std::string*>(args[0]));
                },
                &_guard);
        SyncPoint::get_instance()->enable_processing();
    }

    ~ReadIOTraceCapture() {
        SyncPoint::get_instance()->disable_processing();
        config::enable_read_io_trace = _old_enabled;
    }

    std::vector<rapidjson::Document> events(const char* event_name) {
        std::lock_guard lock(_mutex);
        std::vector<rapidjson::Document> result;
        for (const auto& line : _lines) {
            rapidjson::Document document;
            document.Parse(line.data(), line.size());
            if (document.HasParseError()) {
                ADD_FAILURE() << "Malformed trace: " << line;
                continue;
            }
            if (document["event"].GetString() == std::string(event_name)) {
                result.emplace_back(std::move(document));
            }
        }
        return result;
    }

private:
    const bool _old_enabled;
    std::mutex _mutex;
    std::vector<std::string> _lines;
    SyncPoint::CallbackGuard _guard;
};

} // namespace doris::io
