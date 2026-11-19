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

#include <functional>
#include <memory>

#include "io/cache/file_range_planner.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/read_ahead_range_buffer.h"
#include "storage/segment/column_read_ahead.h"

namespace doris {
class ExecEnv;
class StorageReadOptions;
namespace segment_v2 {

using ReadAheadPageCacheProbe = std::function<bool(const io::FileRange&)>;

struct ColumnReadAheadIOOptions {
    io::FileRangePlanOptions range_plan;
    ReadAheadPageCacheProbe page_cache_probe {};
    io::ReadAheadRangeConsumerFactory range_consumer_factory {};
};

/// Shared immutable services for a file's column iterators. All columns use the same query byte
/// budget and cancellation context, but each owns and submits its own page predictions.
struct ColumnReadAheadResources {
    ColumnReadAheadResources(io::FileReaderSPtr source_reader,
                             io::FileRangeReadScheduler* scheduler,
                             std::shared_ptr<io::FileRangeReadContext> context,
                             io::FileRangeReadIOContext io_context,
                             ColumnReadAheadIOOptions options,
                             ColumnReadAheadOptions eager_options = {},
                             ColumnReadAheadOptions lazy_options = {});

    static bool enabled();
    static Status create_for_query(io::FileReaderSPtr source_reader, ExecEnv* exec_env,
                                   std::shared_ptr<io::FileRangeReadContext> context,
                                   const StorageReadOptions& read_options,
                                   std::shared_ptr<const ColumnReadAheadResources>* output);

    const io::FileReaderSPtr source_reader;
    io::FileRangeReadScheduler* const scheduler;
    const std::shared_ptr<io::FileRangeReadContext> context;
    const io::FileRangeReadIOContext io_context;
    const ColumnReadAheadIOOptions options;
    const ColumnReadAheadOptions eager_options;
    const ColumnReadAheadOptions lazy_options;
    const std::shared_ptr<io::ReadAheadStatistics> statistics;
};

ReadAheadPageCacheProbe make_storage_page_cache_probe(const io::FileReaderSPtr& file_reader);

} // namespace segment_v2
} // namespace doris
