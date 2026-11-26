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

#include "storage/segment/column_read_ahead_controller.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/runtime_profile.h"
#include "storage/segment/column_read_ahead_resources.h"
#include "storage/segment/column_reader.h"

namespace doris::segment_v2 {

ColumnReadAheadController::ColumnReadAheadController(
        std::shared_ptr<ColumnReader> reader, OlapReaderStatistics* stats,
        std::shared_ptr<const ColumnReadAheadResources> resources)
        : _reader(std::move(reader)),
          _stats(stats),
          _resources(std::move(resources)),
          _buffer(_resources->statistics) {
    _stats->read_ahead_stats = _resources->statistics;
}

ColumnReadAheadController::~ColumnReadAheadController() = default;

Status ColumnReadAheadController::_init_window(ColumnReadAheadOptions options, bool reverse) {
    if (_window != nullptr) {
        DORIS_CHECK(_window->options() == options);
        DORIS_CHECK(_window->reverse() == reverse);
        return Status::OK();
    }
    OrdinalIndexReader* index = nullptr;
    RETURN_IF_ERROR(_reader->get_ordinal_index_reader(index, _stats));
    std::vector<ColumnReadAheadPage> pages;
    pages.reserve(index->num_data_pages());
    for (auto page = index->begin(); page.valid(); page.next()) {
        pages.push_back({.page_index = page.page_index(),
                         .first_ordinal = page.first_ordinal(),
                         .last_ordinal = page.last_ordinal(),
                         .range = {.offset = page.page().offset, .size = page.page().size}});
    }
    return ColumnReadAhead::create(std::move(pages), options, reverse, &_window);
}

Status ColumnReadAheadController::prepare(const ColumnReadAheadHint& hint) {
    if (_page_driven) {
        const auto& scan = std::get<ScanReadAheadHint>(hint.request);
        DORIS_CHECK(_window->reverse() == scan.reverse);
        DORIS_CHECK(_window->options() == (scan.role == ColumnReadAheadRole::EAGER
                                                   ? _resources->eager_options
                                                   : _resources->lazy_options));
        return Status::OK();
    }
    auto* statistics = _resources->statistics.get();
    ColumnReadAheadPlan plan;
    {
        SCOPED_TIMER(&statistics->column_plan_time);
        if (const auto* scan = std::get_if<ScanReadAheadHint>(&hint.request)) {
            DORIS_CHECK(scan->candidate_rows != nullptr);
            DORIS_CHECK(!scan->candidate_rows->isEmpty());
            const auto options = scan->role == ColumnReadAheadRole::EAGER
                                         ? _resources->eager_options
                                         : _resources->lazy_options;
            {
                SCOPED_TIMER(&statistics->column_init_time);
                RETURN_IF_ERROR(_init_window(options, scan->reverse));
                _window->start(*scan->candidate_rows, &plan);
                _page_driven = true;
            }
        } else {
            const auto rowids = std::get<ExactRowReadAheadHint>(hint.request).rowids;
            DORIS_CHECK(!rowids.empty());
            DORIS_CHECK(std::is_sorted(rowids.begin(), rowids.end()));
            DORIS_CHECK(std::adjacent_find(rowids.begin(), rowids.end()) == rowids.end());
            DORIS_CHECK(!_page_driven);
            {
                SCOPED_TIMER(&statistics->column_init_time);
                RETURN_IF_ERROR(_init_window(_resources->eager_options, false));
            }
            roaring::Roaring selection;
            selection.addMany(rowids.size(), rowids.data());
            _window->plan(rowids.data(), rowids.size(), selection, &plan);
        }
        plan.update_statistics(statistics);
    }
    return _apply_plan(plan);
}

io::ReadAheadRangeBuffer* ColumnReadAheadController::before_page_read(int32_t page_index) {
    // init() may inspect the last data page for dictionary encoding before scan pruning. There
    // is no prediction until prepare() receives the actual selection and dependency role.
    if (_window == nullptr) {
        return nullptr;
    }
    if (_page_driven) {
        auto* statistics = _resources->statistics.get();
        ColumnReadAheadPlan plan;
        {
            SCOPED_TIMER(&statistics->column_plan_time);
            SCOPED_TIMER(&statistics->page_advance_time);
            _window->advance(page_index, &plan);
            plan.update_statistics(statistics);
        }
        const auto status = _apply_plan(plan);
        if (!status.ok()) {
            LOG_EVERY_N(WARNING, 100) << "failed to advance column read-ahead: " << status;
        }
    }
    return &_buffer;
}

void ColumnReadAheadController::after_page_read(int32_t page_index, io::FileRange range) {
    _buffer.release(range);
    // Cache probes and admission rejection may have already completed this page. A page loaded
    // during init(), before prepare(), has no window entry at all.
    if (_window != nullptr && _window->pending(page_index)) {
        _window->complete(page_index);
    }
}

Status ColumnReadAheadController::_apply_plan(const ColumnReadAheadPlan& plan) {
    if (plan.empty()) {
        return Status::OK();
    }
    auto* statistics = _resources->statistics.get();
    COUNTER_UPDATE(&statistics->plan_calls, 1);
    SCOPED_TIMER(&statistics->plan_time);
    for (const auto& page : plan.released_pages) {
        _buffer.release(page.range);
    }
    COUNTER_UPDATE(&statistics->released_pages, static_cast<int64_t>(plan.released_pages.size()));
    COUNTER_UPDATE(&statistics->candidate_pages, static_cast<int64_t>(plan.new_pages.size()));
    std::vector<ColumnReadAheadPage> misses;
    for (const auto& page : plan.new_pages) {
        COUNTER_UPDATE(&statistics->candidate_bytes, static_cast<int64_t>(page.range.size));
        if (_resources->options.page_cache_probe &&
            _resources->options.page_cache_probe(page.range)) {
            _window->complete(page.page_index);
            COUNTER_UPDATE(&statistics->page_cache_hits, 1);
            COUNTER_UPDATE(&statistics->page_cache_hit_bytes,
                           static_cast<int64_t>(page.range.size));
        } else if (!_buffer.contains(page.range)) {
            misses.push_back(page);
        }
    }
    if (misses.empty()) {
        return Status::OK();
    }
    std::ranges::sort(misses, {}, [](const auto& page) { return page.range.offset; });
    return _submit_pages(misses);
}

Status ColumnReadAheadController::_submit_pages(const std::vector<ColumnReadAheadPage>& misses) {
    auto* statistics = _resources->statistics.get();
    std::vector<io::FileRange> inputs;
    inputs.reserve(misses.size());
    for (const auto& page : misses) {
        inputs.push_back(page.range);
    }
    auto complete_misses = [&] {
        for (const auto& page : misses) {
            _window->complete(page.page_index);
        }
    };
    io::FileRangePlan ranges;
    auto status = io::FileRangePlanner::plan(inputs, _resources->source_reader->size(),
                                             _resources->options.range_plan, &ranges, statistics);
    if (!status.ok()) {
        complete_misses();
        return status;
    }
    io::ReadAheadRangeConsumer consumer;
    if (_resources->options.range_consumer_factory) {
        consumer = _resources->options.range_consumer_factory();
    }
    auto submitted = _resources->scheduler->try_submit(ranges.ranges, _resources->source_reader,
                                                       _resources->io_context, _resources->context);
    if (!submitted.accepted()) {
        complete_misses();
        return submitted.status;
    }
    std::vector<std::shared_ptr<io::ReadAheadRangeBuffer::Range>> buffers(ranges.ranges.size());
    for (size_t index = 0; index < buffers.size(); ++index) {
        if (submitted.reads[index] != nullptr) {
            buffers[index] = std::make_shared<io::ReadAheadRangeBuffer::Range>(
                    io::ReadAheadRangeBuffer::Range {.read = std::move(submitted.reads[index]),
                                                     .consumer = consumer});
        }
    }
    for (size_t index = 0; index < misses.size(); ++index) {
        const auto& page = misses[index];
        const auto& location = ranges.input_locations[index];
        if (buffers[location.range_index] == nullptr) {
            _window->complete(page.page_index);
        } else {
            _buffer.register_slice(page.range, buffers[location.range_index],
                                   location.buffer_offset);
        }
    }
    return Status::OK();
}

} // namespace doris::segment_v2
