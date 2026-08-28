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

#include "storage/segment/segment_read_ahead.h"

#include <butil/iobuf.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/logging.h"
#include "storage/cache/page_cache.h"

namespace doris::segment_v2 {
namespace {

struct PendingPage {
    ColumnReadAhead* column {nullptr};
    ColumnReadAheadPage page;
};

} // namespace

SegmentReadAheadFileReader::SegmentReadAheadFileReader(io::FileReaderSPtr inner)
        : _inner(std::move(inner)) {
    DORIS_CHECK(_inner != nullptr);
}

void SegmentReadAheadFileReader::_register_page(ColumnReadAhead* column,
                                                const ColumnReadAheadPage& page,
                                                std::shared_ptr<BufferedRange> range,
                                                size_t buffer_offset) {
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK(range != nullptr);
    std::lock_guard lock(_mutex);
    const PageKey key {.offset = page.range.offset, .size = page.range.size};
    auto [slot, inserted] = _pages.try_emplace(key);
    if (inserted) {
        slot->second.range = std::move(range);
        slot->second.buffer_offset = buffer_offset;
    } else {
        DORIS_CHECK(slot->second.range == range);
        DORIS_CHECK(slot->second.buffer_offset == buffer_offset);
    }
    const PageOwner owner {.column = column, .page_index = page.page_index};
    DORIS_CHECK(std::ranges::find(slot->second.owners, owner) == slot->second.owners.end());
    slot->second.owners.push_back(owner);
}

void SegmentReadAheadFileReader::_release_page(ColumnReadAhead* column,
                                               const ColumnReadAheadPage& page) {
    DORIS_CHECK(column != nullptr);
    std::lock_guard lock(_mutex);
    const PageKey key {.offset = page.range.offset, .size = page.range.size};
    auto slot = _pages.find(key);
    if (slot == _pages.end()) {
        return;
    }
    const PageOwner owner {.column = column, .page_index = page.page_index};
    auto entry = std::ranges::find(slot->second.owners, owner);
    if (entry == slot->second.owners.end()) {
        return;
    }
    slot->second.owners.erase(entry);
    if (slot->second.owners.empty()) {
        _pages.erase(slot);
    }
}

bool SegmentReadAheadFileReader::_try_read(const PageKey& key, Slice output, Status* status) {
    std::shared_ptr<BufferedRange> range;
    size_t buffer_offset = 0;
    {
        std::lock_guard lock(_mutex);
        const auto slot = _pages.find(key);
        if (slot == _pages.end()) {
            return false;
        }
        range = slot->second.range;
        buffer_offset = slot->second.buffer_offset;
    }

    *status = range->read->wait();
    if (!status->ok()) {
        _finish_page(key, false);
        return true;
    }
    DORIS_CHECK(output.size == key.size);
    const Slice source = range->read->slice(buffer_offset, key.size);
    std::memcpy(output.data, source.data, source.size);
    _finish_page(key, true);
    return true;
}

void SegmentReadAheadFileReader::_finish_page(const PageKey& key, bool consumed) {
    std::vector<PageOwner> owners;
    std::shared_ptr<BufferedRange> consumed_range;
    {
        std::lock_guard lock(_mutex);
        auto slot = _pages.find(key);
        if (slot == _pages.end()) {
            return;
        }
        owners = std::move(slot->second.owners);
        auto range = std::move(slot->second.range);
        _pages.erase(slot);
        if (consumed && !range->consumed) {
            range->consumed = true;
            consumed_range = std::move(range);
        }
    }

    for (const auto& owner : owners) {
        owner.column->complete(owner.page_index);
    }
    if (consumed_range != nullptr && consumed_range->consumer) {
        auto consumer = std::move(consumed_range->consumer);
        consumer(consumed_range->read->range(), consumed_range->read->data());
    }
}

Status SegmentReadAheadFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                                const io::IOContext* io_ctx) {
    Status read_ahead_status;
    if (_try_read({.offset = offset, .size = result.size}, result, &read_ahead_status)) {
        if (read_ahead_status.ok()) {
            *bytes_read = result.size;
            return Status::OK();
        }
    }
    return _inner->read_at(offset, result, bytes_read, io_ctx);
}

Status SegmentReadAheadFileReader::read_at_iobuf_impl(size_t offset, size_t bytes_req,
                                                      butil::IOBuf* out, size_t* bytes_read,
                                                      const io::IOContext* io_ctx) {
    return _inner->read_at_iobuf(offset, bytes_req, out, bytes_read, io_ctx);
}

SegmentReadAhead::SegmentReadAhead(io::FileReaderSPtr source_reader,
                                   io::FileRangeReadScheduler* scheduler,
                                   std::shared_ptr<io::FileRangeReadContext> context,
                                   io::FileRangeReadIOContext io_context,
                                   SegmentReadAheadOptions options)
        : _source_reader(std::move(source_reader)),
          _scheduler(scheduler),
          _context(std::move(context)),
          _io_context(std::move(io_context)),
          _options(std::move(options)),
          _reader(std::shared_ptr<SegmentReadAheadFileReader>(
                  new SegmentReadAheadFileReader(_source_reader))) {
    DORIS_CHECK(_source_reader != nullptr);
    DORIS_CHECK(_scheduler != nullptr);
    DORIS_CHECK(_context != nullptr);
}

SegmentReadAheadResult SegmentReadAhead::apply_plans(std::vector<ColumnReadAheadPlan> plans) {
    SegmentReadAheadResult result;
    std::vector<PendingPage> misses;

    for (auto& plan : plans) {
        DORIS_CHECK(plan.column != nullptr);
        for (const auto& page : plan.released_pages) {
            _reader->_release_page(plan.column, page);
        }
        for (const auto& page : plan.new_pages) {
            ++result.new_pages;
            if (_options.page_cache_probe && _options.page_cache_probe(page.range)) {
                plan.column->complete(page.page_index);
                ++result.page_cache_hits;
            } else {
                misses.push_back({.column = plan.column, .page = page});
            }
        }
    }

    if (misses.empty()) {
        return result;
    }

    std::ranges::sort(misses, [](const PendingPage& left, const PendingPage& right) {
        return left.page.range.offset < right.page.range.offset ||
               (left.page.range.offset == right.page.range.offset &&
                left.page.range.size < right.page.range.size);
    });
    std::vector<io::FileRange> input_ranges;
    input_ranges.reserve(misses.size());
    for (const auto& miss : misses) {
        if (input_ranges.empty() || input_ranges.back() != miss.page.range) {
            input_ranges.push_back(miss.page.range);
        }
    }

    io::FileRangePlan range_plan;
    result.status = io::FileRangePlanner::plan(input_ranges, _source_reader->size(),
                                               _options.range_plan, &range_plan);
    if (!result.status.ok()) {
        for (const auto& miss : misses) {
            miss.column->complete(miss.page.page_index);
        }
        return result;
    }

    for (const auto& range : range_plan.ranges) {
        result.submitted_bytes += range.size;
    }
    ReadAheadRangeConsumer consumer;
    if (_options.range_consumer_factory) {
        consumer = _options.range_consumer_factory();
    }
    auto submit_result =
            _scheduler->try_submit(range_plan.ranges, _source_reader, _io_context, _context);
    result.reject_reason = submit_result.reject_reason;
    result.status = submit_result.status;
    if (!submit_result.accepted()) {
        result.submitted_bytes = 0;
        for (const auto& miss : misses) {
            miss.column->complete(miss.page.page_index);
        }
        return result;
    }

    result.submitted_ranges = submit_result.reads.size();
    DORIS_CHECK(submit_result.reads.size() == range_plan.ranges.size());
    std::vector<std::shared_ptr<SegmentReadAheadFileReader::BufferedRange>> ranges;
    ranges.reserve(range_plan.ranges.size());
    for (size_t index = 0; index < range_plan.ranges.size(); ++index) {
        ranges.push_back(std::make_shared<SegmentReadAheadFileReader::BufferedRange>(
                SegmentReadAheadFileReader::BufferedRange {
                        .read = std::move(submit_result.reads[index]), .consumer = consumer}));
    }

    DORIS_CHECK(range_plan.input_locations.size() == input_ranges.size());
    size_t input_index = 0;
    for (size_t index = 0; index < misses.size(); ++index) {
        if (index > 0 && misses[index - 1].page.range != misses[index].page.range) {
            ++input_index;
        }
        DORIS_CHECK(input_index < range_plan.input_locations.size());
        const auto& location = range_plan.input_locations[input_index];
        DORIS_CHECK(location.range_index < ranges.size());
        _reader->_register_page(misses[index].column, misses[index].page,
                                ranges[location.range_index], location.buffer_offset);
    }
    return result;
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
