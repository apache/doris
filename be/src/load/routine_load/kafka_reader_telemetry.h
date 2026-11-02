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

#include <array>
#include <cstdint>

#include "util/uid_util.h"

namespace doris {

class KafkaReaderTelemetry {
public:
    struct Window {
        int32_t partition = -1;
        int64_t records = 0;
        int64_t first_ms = 0;
        int64_t last_ms = 0;
    };

    KafkaReaderTelemetry(int64_t job_id, const UniqueId& reader, bool share, bool kafka = true);
    ~KafkaReaderTelemetry();
    bool enabled() const { return _enabled; }
    void received(int32_t partition, int64_t now_ms);
    void flush();
    void timing(const char* stage, int64_t elapsed_ns, int64_t records = -1) const;

private:
    void emit(Window& window) const;
    bool _enabled = false;
    int64_t _job_id;
    UniqueId _reader;
    bool _share;
    std::array<Window, 64> _windows {};
};

} // namespace doris
