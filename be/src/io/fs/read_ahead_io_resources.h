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

namespace doris::io {
class PartialBlockWritebackManager;

using ReadAheadPageCacheProbe = std::function<bool(const FileRange&)>;

struct ReadAheadIOOptions {
    FileRangePlanOptions range_plan;
    ReadAheadPageCacheProbe page_cache_probe {};
    ReadAheadRangeConsumerFactory range_consumer_factory {};
};

// One query owns admission and cancellation. File/reader statistics are bound separately.
struct QueryReadAheadResources {
    QueryReadAheadResources(FileRangeReadScheduler* scheduler,
                            PartialBlockWritebackManager* writeback_manager,
                            FileRangePlanOptions range_plan, size_t eager_window_bytes,
                            size_t lazy_window_bytes);
    static Status create(FileRangeReadScheduler* scheduler,
                         PartialBlockWritebackManager* writeback_manager,
                         std::shared_ptr<const QueryReadAheadResources>* output);
    void cancel() const;

    FileRangeReadScheduler* const scheduler;
    PartialBlockWritebackManager* const writeback_manager;
    const std::shared_ptr<FileRangeReadContext> context;
    const FileRangePlanOptions range_plan;
    const size_t eager_window_bytes;
    const size_t lazy_window_bytes;
};

// Bound to one source file and reader-local statistics, never shared across unrelated readers.
struct FileReadAheadResources {
    FileReadAheadResources(FileReaderSPtr source_reader, FileRangeReadScheduler* scheduler,
                           std::shared_ptr<FileRangeReadContext> context,
                           FileRangeReadIOContext io_context, ReadAheadIOOptions options,
                           size_t eager_window_bytes = 1, size_t lazy_window_bytes = 1);
    static std::shared_ptr<const FileReadAheadResources> create(
            FileReaderSPtr source_reader, const IOContext& io_context,
            std::shared_ptr<ReadAheadStatistics> statistics, ReadAheadPageCacheProbe probe);

    const FileReaderSPtr source_reader;
    FileRangeReadScheduler* const scheduler;
    const std::shared_ptr<FileRangeReadContext> context;
    const FileRangeReadIOContext io_context;
    const ReadAheadIOOptions options;
    const size_t eager_window_bytes;
    const size_t lazy_window_bytes;
    const std::shared_ptr<ReadAheadStatistics> statistics;
};
} // namespace doris::io
