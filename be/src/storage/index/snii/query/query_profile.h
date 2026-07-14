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

#include <chrono>
#include <cstdint>

#include "storage/index/snii/io/io_metrics.h"

namespace doris::snii::io {
class FileReader;
}

namespace doris::snii::query {

struct QueryProfile {
    uint64_t elapsed_ns = 0;
    bool has_io_metrics = false;
    io::IoMetrics io_before;
    io::IoMetrics io_after;
    io::IoMetrics io_delta;
};

class QueryProfileScope {
public:
    QueryProfileScope(io::FileReader* reader, QueryProfile* profile);
    ~QueryProfileScope();
    QueryProfileScope(const QueryProfileScope&) = delete;
    QueryProfileScope& operator=(const QueryProfileScope&) = delete;

    void finish();

private:
    io::FileReader* reader_ = nullptr;
    QueryProfile* profile_ = nullptr;
    std::chrono::steady_clock::time_point start_;
    bool finished_ = false;
};

} // namespace doris::snii::query
