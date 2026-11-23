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

#include "io/fs/read_ahead_io_resources.h"

#include <optional>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/range_cache_writeback.h"
#include "io/fs/read_ahead_metrics.h"

namespace doris::io {
namespace {
FileRangeReadIOContext with_statistics(FileRangeReadIOContext context) {
    if (context.statistics == nullptr) {
        context.statistics = std::make_shared<ReadAheadStatistics>();
    }
    return context;
}
} // namespace

QueryReadAheadResources::QueryReadAheadResources(FileRangeReadScheduler* scheduler,
                                                 PartialBlockWritebackManager* writeback_manager,
                                                 FileRangePlanOptions range_plan,
                                                 size_t eager_window_bytes,
                                                 size_t lazy_window_bytes)
        : scheduler(scheduler),
          writeback_manager(writeback_manager),
          context(scheduler->create_context()),
          range_plan(std::move(range_plan)),
          eager_window_bytes(eager_window_bytes),
          lazy_window_bytes(lazy_window_bytes) {}

Status QueryReadAheadResources::create(FileRangeReadScheduler* scheduler,
                                       PartialBlockWritebackManager* writeback_manager,
                                       std::shared_ptr<const QueryReadAheadResources>* output) {
    DORIS_CHECK(scheduler != nullptr);
    DORIS_CHECK(output != nullptr);
    output->reset();
    if (config::read_ahead_eager_window_bytes <= 0 || config::read_ahead_lazy_window_bytes <= 0) {
        return Status::InvalidArgument("column read-ahead window bytes must be positive");
    }
    FileRangePlanOptions options {
            .coalesce_options = {.max_gap_bytes =
                                         cast_set<size_t>(config::read_ahead_max_gap_bytes),
                                 .max_range_bytes =
                                         cast_set<size_t>(config::read_ahead_max_range_bytes),
                                 .max_read_amplification_ratio =
                                         config::read_ahead_max_read_amplification_ratio},
            .cache_block_size = cast_set<size_t>(config::file_cache_each_block_size),
            .block_fill_min_coverage = config::read_ahead_block_fill_min_coverage,
    };
    *output = std::make_shared<QueryReadAheadResources>(
            scheduler, writeback_manager, std::move(options),
            cast_set<size_t>(config::read_ahead_eager_window_bytes),
            cast_set<size_t>(config::read_ahead_lazy_window_bytes));
    return Status::OK();
}

void QueryReadAheadResources::cancel() const {
    context->cancel();
}

FileReadAheadResources::FileReadAheadResources(FileReaderSPtr source_reader,
                                               FileRangeReadScheduler* scheduler,
                                               std::shared_ptr<FileRangeReadContext> context,
                                               FileRangeReadIOContext io_context,
                                               ReadAheadIOOptions options,
                                               size_t eager_window_bytes, size_t lazy_window_bytes)
        : source_reader(std::move(source_reader)),
          scheduler(scheduler),
          context(std::move(context)),
          io_context(with_statistics(std::move(io_context))),
          options(std::move(options)),
          eager_window_bytes(eager_window_bytes),
          lazy_window_bytes(lazy_window_bytes),
          statistics(this->io_context.statistics) {
    DORIS_CHECK(this->source_reader != nullptr);
    DORIS_CHECK(scheduler != nullptr);
    DORIS_CHECK(this->context != nullptr);
}

std::shared_ptr<const FileReadAheadResources> FileReadAheadResources::create(
        FileReaderSPtr source_reader, const IOContext& caller,
        std::shared_ptr<ReadAheadStatistics> statistics, ReadAheadPageCacheProbe probe) {
    DORIS_CHECK(source_reader != nullptr);
    DORIS_CHECK(caller.query_read_ahead != nullptr);
    const auto& query = *caller.query_read_ahead;
    auto* file_cache_stats = caller.file_cache_stats;
    auto* file_reader_stats = caller.file_reader_stats;
    std::shared_ptr<io::RangeCacheWriteback> range_writeback;
    auto cached_reader = std::dynamic_pointer_cast<io::CachedRemoteFileReader>(source_reader);
    if (cached_reader != nullptr) {
        auto* partial_block_manager = query.writeback_manager;
        DORIS_CHECK(partial_block_manager != nullptr);
        range_writeback = cached_reader->make_range_cache_writeback(caller, partial_block_manager);
    }
    io::ReadAheadRangeConsumerFactory range_consumer_factory;
    if (file_cache_stats != nullptr || file_reader_stats != nullptr || range_writeback != nullptr) {
        range_consumer_factory = [file_cache_stats, file_reader_stats, statistics,
                                  range_writeback]() -> io::ReadAheadRangeConsumer {
            std::optional<io::AsyncCacheWriteEpoch> write_epoch;
            if (range_writeback != nullptr) {
                write_epoch = range_writeback->capture_write_epoch();
            }
            return [file_cache_stats, file_reader_stats, range_writeback, statistics,
                    write_epoch = std::move(write_epoch)](const io::FileRangeRead& read) {
                const auto stats = read.stats();
                if (file_cache_stats != nullptr) {
                    file_cache_stats->merge_from(stats.file_cache);
                }
                if (file_reader_stats != nullptr) {
                    file_reader_stats->merge_from(stats.file_reader);
                }
                // Cache and inflight-buffer hits already have a complete cache-block owner.
                if (write_epoch.has_value() && stats.file_cache.bytes_read_from_remote > 0) {
                    static_cast<void>(range_writeback->submit_consumed_range(
                            read.range(), read.data(), *write_epoch, statistics.get(),
                            read.trace_id()));
                }
            };
        };
    }

    auto snapshot = FileRangeReadIOContext::from_caller(caller);
    snapshot.statistics = std::move(statistics);
    ReadAheadIOOptions options {
            .range_plan = query.range_plan,
            .page_cache_probe = std::move(probe),
            .range_consumer_factory = std::move(range_consumer_factory),
    };
    return std::make_shared<FileReadAheadResources>(
            std::move(source_reader), query.scheduler, query.context, std::move(snapshot),
            std::move(options), query.eager_window_bytes, query.lazy_window_bytes);
}

} // namespace doris::io
