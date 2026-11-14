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

#include "storage/segment/read_ahead_file_column_iterator.h"

#include <utility>

#include "storage/segment/column_read_ahead_controller.h"

namespace doris::segment_v2 {

template <typename Base>
ReadAheadIterator<Base>::ReadAheadIterator(std::shared_ptr<ColumnReader> reader)
        : Base(std::move(reader)) {}

template <typename Base>
ReadAheadIterator<Base>::~ReadAheadIterator() = default;

template <typename Base>
Status ReadAheadIterator<Base>::init(const ColumnIteratorOptions& opts) {
    _read_ahead.reset();
    if (opts.read_ahead != nullptr) {
        _read_ahead = std::make_unique<ColumnReadAheadController>(this->get_reader(), opts.stats,
                                                                  opts.read_ahead);
    }
    // A predicate dictionary column may load its last data page in Base::init(). Bind resources
    // first so its virtual page hooks can retry a corrupt probe without starting a scan window.
    return Base::init(opts);
}

template <typename Base>
Status ReadAheadIterator<Base>::prepare_read_ahead(const ColumnReadAheadHint& hint) {
    if (_read_ahead == nullptr) {
        return Base::prepare_read_ahead(hint);
    }
    if (!this->need_to_read()) {
        return Status::OK();
    }
    return _read_ahead->prepare(hint);
}

template <typename Base>
Status ReadAheadIterator<Base>::init_prefetcher(const SegmentPrefetchParams& params) {
    if (_read_ahead != nullptr) {
        return Status::OK();
    }
    return Base::init_prefetcher(params);
}

template <typename Base>
void ReadAheadIterator<Base>::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    if (_read_ahead == nullptr) {
        Base::collect_prefetchers(prefetchers, init_method);
    }
}

template <typename Base>
auto ReadAheadIterator<Base>::_before_page_read(int32_t page_index) -> PageReadContext {
    if (_read_ahead == nullptr) {
        return {};
    }
    return {.buffer = _read_ahead->before_page_read(page_index), .retry_on_error = true};
}

template <typename Base>
void ReadAheadIterator<Base>::_after_page_read(int32_t page_index, const PagePointer& page) {
    if (_read_ahead != nullptr) {
        _read_ahead->after_page_read(page_index, {.offset = page.offset, .size = page.size});
    }
}

template class ReadAheadIterator<FileColumnIterator>;
template class ReadAheadIterator<StringFileColumnIterator>;

} // namespace doris::segment_v2
