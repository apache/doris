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

#include "storage/segment/lazy_init_segment_iterator.h"

#include <algorithm>

#include "storage/segment/empty_segment_iterator.h"
#include "storage/segment/segment_loader.h"

namespace doris::segment_v2 {

LazyInitSegmentIterator::LazyInitSegmentIterator(BetaRowsetSharedPtr rowset,
                                                 RowsetSegmentRef segment, bool should_use_cache,
                                                 ReadSchemaSPtr schema,
                                                 const StorageReadOptions& opts)
        : _rowset(std::move(rowset)),
          _segment(segment),
          _should_use_cache(should_use_cache),
          _schema(std::move(schema)),
          _read_options(opts) {}

/// See where the iterator is created in `BetaRowsetReader::get_segment_iterators`
Status LazyInitSegmentIterator::init(const StorageReadOptions& opts) {
    _need_lazy_init = false;
    if (_inner_iterator) {
        return Status::OK();
    }

    auto* work_limit = _read_options.seq_map_candidate_work_limit;
    if (work_limit != nullptr &&
        (work_limit->exceeded || work_limit->remaining_segment_read_calls == 0 ||
         work_limit->remaining_rows <= 0 || work_limit->remaining_bytes <= 0)) {
        work_limit->exceeded = true;
        _inner_iterator = std::make_unique<EmptySegmentIterator>(*_schema);
        return Status::OK();
    }

    const int64_t rows_before = work_limit != nullptr ? _read_options.stats->raw_rows_read : 0;
    const int64_t bytes_before =
            work_limit != nullptr ? _read_options.stats->seq_map_candidate_work_bytes() : 0;
    auto status = [&]() {
        std::shared_ptr<Segment> segment;
        {
            SegmentCacheHandle segment_cache_handle;
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                    _rowset, _segment, &segment_cache_handle, _should_use_cache, false, opts.stats,
                    &opts.io_ctx));
            const auto& tmp_segments = segment_cache_handle.get_segments();
            segment = tmp_segments[0];
        }
        RETURN_IF_ERROR(segment->new_iterator(_schema, _read_options, &_inner_iterator));
        return _inner_iterator->init(_read_options);
    }();
    if (work_limit != nullptr) {
        const int64_t rows = std::max<int64_t>(0, _read_options.stats->raw_rows_read - rows_before);
        const int64_t bytes = std::max<int64_t>(
                0, _read_options.stats->seq_map_candidate_work_bytes() - bytes_before);
        if (rows > work_limit->remaining_rows) {
            work_limit->remaining_rows = 0;
            work_limit->exceeded = true;
        } else {
            work_limit->remaining_rows -= rows;
        }
        if (bytes > work_limit->remaining_bytes) {
            work_limit->remaining_bytes = 0;
            work_limit->exceeded = true;
        } else {
            work_limit->remaining_bytes -= bytes;
        }
    }
    return status;
}

} // namespace doris::segment_v2
