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

#include "io/fs/read_ahead_range_buffer.h"
#include "storage/segment/column_read_ahead.h"

namespace doris {
struct OlapReaderStatistics;
namespace segment_v2 {
class ColumnReader;
struct ColumnReadAheadResources;

/// One iterator's foreground-owned policy. Async workers retain only their range handles and
/// source readers; completion never calls back into this controller or its column window.
class ColumnReadAheadController {
public:
    ColumnReadAheadController(std::shared_ptr<ColumnReader> reader, OlapReaderStatistics* stats,
                              std::shared_ptr<const ColumnReadAheadResources> resources);
    ~ColumnReadAheadController();

    Status prepare(const ColumnReadAheadHint& hint);
    io::ReadAheadRangeBuffer* before_page_read(int32_t page_index);
    void after_page_read(int32_t page_index, io::FileRange range);

private:
    Status _init_window(ColumnReadAheadOptions options, bool reverse);
    Status _apply_plan(const ColumnReadAheadPlan& plan);
    Status _submit_pages(const std::vector<ColumnReadAheadPage>& misses);

    const std::shared_ptr<ColumnReader> _reader;
    OlapReaderStatistics* const _stats;
    const std::shared_ptr<const ColumnReadAheadResources> _resources;
    std::unique_ptr<ColumnReadAhead> _window;
    io::ReadAheadRangeBuffer _buffer;
    bool _page_driven {false};
};

} // namespace segment_v2
} // namespace doris
