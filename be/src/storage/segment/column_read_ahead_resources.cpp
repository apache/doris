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

#include "storage/segment/column_read_ahead_resources.h"

#include <optional>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/range_cache_writeback.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/exec_env.h"
#include "storage/cache/page_cache.h"
#include "storage/iterators.h"

namespace doris::segment_v2 {
namespace {
io::FileRangeReadIOContext with_statistics(io::FileRangeReadIOContext context) {
    if (context.statistics == nullptr) {
        context.statistics = std::make_shared<io::ReadAheadStatistics>();
    }
    return context;
}
} // namespace

ColumnReadAheadResources::ColumnReadAheadResources(
        io::FileReaderSPtr source, io::FileRangeReadScheduler* read_scheduler,
        std::shared_ptr<io::FileRangeReadContext> query_context,
        io::FileRangeReadIOContext read_io_context, ColumnReadAheadIOOptions read_options,
        ColumnReadAheadOptions eager_window, ColumnReadAheadOptions lazy_window)
        : source_reader(std::move(source)),
          scheduler(read_scheduler),
          context(std::move(query_context)),
          io_context(with_statistics(std::move(read_io_context))),
          options(std::move(read_options)),
          eager_options(eager_window),
          lazy_options(lazy_window),
          statistics(io_context.statistics) {
    DORIS_CHECK(source_reader != nullptr);
    DORIS_CHECK(scheduler != nullptr);
    DORIS_CHECK(context != nullptr);
}

bool ColumnReadAheadResources::enabled() {
    // Read-ahead fetches with NO_WRITE and relies on the async writer after consumption.
    // With only read-ahead enabled, retain the original reader so cold pages still fill cache.
    return config::enable_query_read_ahead && config::enable_async_file_cache_write &&
           config::is_cloud_mode();
}

Status ColumnReadAheadResources::create_for_query(
        io::FileReaderSPtr source_reader, ExecEnv* exec_env,
        std::shared_ptr<io::FileRangeReadContext> context, const StorageReadOptions& read_options,
        std::shared_ptr<const ColumnReadAheadResources>* output) {
    DORIS_CHECK(source_reader != nullptr);
    DORIS_CHECK(exec_env != nullptr);
    DORIS_CHECK(output != nullptr);
    output->reset();
    if (!enabled() || read_options.io_ctx.reader_type != ReaderType::READER_QUERY) {
        return Status::OK();
    }
    DORIS_CHECK(context != nullptr);

    ColumnReadAheadOptions eager_options {
            .window_bytes = cast_set<size_t>(config::read_ahead_eager_window_bytes),
    };
    ColumnReadAheadOptions lazy_options {
            .window_bytes = cast_set<size_t>(config::read_ahead_lazy_window_bytes),
    };
    RETURN_IF_ERROR(eager_options.validate());
    RETURN_IF_ERROR(lazy_options.validate());

    auto statistics =
            read_options.stats != nullptr ? read_options.stats->read_ahead_stats : nullptr;
    if (statistics == nullptr) {
        statistics = std::make_shared<io::ReadAheadStatistics>();
        if (read_options.stats != nullptr) {
            read_options.stats->read_ahead_stats = statistics;
        }
    }

    auto* file_cache_stats = read_options.io_ctx.file_cache_stats;
    auto* file_reader_stats = read_options.io_ctx.file_reader_stats;
    std::shared_ptr<io::RangeCacheWriteback> range_writeback;
    auto cached_reader = std::dynamic_pointer_cast<io::CachedRemoteFileReader>(source_reader);
    if (cached_reader != nullptr) {
        auto* partial_block_manager = exec_env->partial_block_writeback_manager();
        DORIS_CHECK(partial_block_manager != nullptr);
        range_writeback = cached_reader->make_range_cache_writeback(read_options.io_ctx,
                                                                    partial_block_manager);
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

    ColumnReadAheadIOOptions options {
            .range_plan = {.coalesce_options =
                                   {.max_gap_bytes =
                                            cast_set<size_t>(config::read_ahead_max_gap_bytes),
                                    .max_range_bytes =
                                            cast_set<size_t>(config::read_ahead_max_range_bytes),
                                    .max_read_amplification_ratio =
                                            config::read_ahead_max_read_amplification_ratio},
                           .cache_block_size = cast_set<size_t>(config::file_cache_each_block_size),
                           .block_fill_min_coverage = config::read_ahead_block_fill_min_coverage},
            .page_cache_probe = read_options.use_page_cache
                                        ? make_storage_page_cache_probe(source_reader)
                                        : ReadAheadPageCacheProbe {},
            .range_consumer_factory = std::move(range_consumer_factory),
    };
    auto* scheduler = exec_env->file_range_read_scheduler();
    DORIS_CHECK(scheduler != nullptr);
    auto io_context = io::FileRangeReadIOContext::from_caller(read_options.io_ctx);
    io_context.statistics = std::move(statistics);
    *output = std::make_shared<ColumnReadAheadResources>(
            std::move(source_reader), scheduler, std::move(context), std::move(io_context),
            std::move(options), eager_options, lazy_options);
    return Status::OK();
}

ReadAheadPageCacheProbe make_storage_page_cache_probe(const io::FileReaderSPtr& file_reader) {
    DORIS_CHECK(file_reader != nullptr);
    return [file_reader](const io::FileRange& page_range) {
        auto* cache = StoragePageCache::instance();
        if (cache == nullptr) {
            return false;
        }
        PageCacheHandle handle;
        StoragePageCache::CacheKey key(file_reader->path().native(), file_reader->size(),
                                       cast_set<int64_t>(page_range.offset));
        return cache->lookup(key, &handle, DATA_PAGE);
    };
}

} // namespace doris::segment_v2
