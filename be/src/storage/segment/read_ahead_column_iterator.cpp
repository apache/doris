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

#include "storage/segment/read_ahead_column_iterator.h"

#include <utility>

#include "common/logging.h"
#include "storage/segment/column_read_ahead_controller.h"

namespace doris::segment_v2 {

ReadAheadColumnIterator::ReadAheadColumnIterator(ColumnIteratorUPtr inner)
        : _inner(std::move(inner)) {
    DORIS_CHECK(_inner != nullptr);
}

ReadAheadColumnIterator::~ReadAheadColumnIterator() {
    _inner->set_page_read_observer(nullptr);
}

Status ReadAheadColumnIterator::init(const ColumnIteratorOptions& opts) {
    _inner->set_page_read_observer(nullptr);
    _read_ahead.reset();
    if (opts.read_ahead != nullptr) {
        auto reader = _inner->get_column_reader();
        DORIS_CHECK(reader != nullptr);
        _read_ahead = std::make_unique<ColumnReadAheadController>(std::move(reader), opts.stats,
                                                                  opts.read_ahead);
        // Dictionary-encoding inspection can load a page from inner init(). Install the observer
        // first so those loads retain retry behavior without starting a scan window.
        _inner->set_page_read_observer(this);
    }
    auto inner_options = opts;
    inner_options.read_ahead.reset();
    return _inner->init(inner_options);
}

Status ReadAheadColumnIterator::prepare_read_ahead(const ColumnReadAheadHint& hint) {
    if (_read_ahead == nullptr) {
        return _inner->prepare_read_ahead(hint);
    }
    if (!_inner->need_to_read()) {
        return Status::OK();
    }
    return _read_ahead->prepare(hint);
}

Status ReadAheadColumnIterator::init_prefetcher(const SegmentPrefetchParams& params) {
    if (_read_ahead != nullptr) {
        return Status::OK();
    }
    return _inner->init_prefetcher(params);
}

void ReadAheadColumnIterator::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    if (_read_ahead == nullptr) {
        _inner->collect_prefetchers(prefetchers, init_method);
    }
}

ColumnPageReadContext ReadAheadColumnIterator::before_page_read(int32_t page_index,
                                                                io::FileRange range) {
    return {.buffer = _read_ahead->before_page_read(page_index), .retry_on_error = true};
}

void ReadAheadColumnIterator::after_page_read(int32_t page_index, io::FileRange range) {
    _read_ahead->after_page_read(page_index, range);
}

} // namespace doris::segment_v2
