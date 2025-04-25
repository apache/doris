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

#include "olap/rowset/segment_v2/lazy_init_segment_iterator.h"

#include "olap/segment_loader.h"

namespace doris::segment_v2 {

LazyInitSegmentIterator::LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, int64_t segment_id,
                                                 bool should_use_cache, SchemaSPtr schema,
                                                 const StorageReadOptions& opts)
        : _rowset(std::move(rowset)),
          _segment_id(segment_id),
          _should_use_cache(should_use_cache),
          _schema(std::move(schema)),
          _read_options(opts) {}

/// See where the iterator is created in `BetaRowsetReader::get_segment_iterators`
Status LazyInitSegmentIterator::init(const StorageReadOptions& opts) {
    _need_lazy_init = false;
    if (_inner_iterator) {
        return Status::OK();
    }

    std::shared_ptr<Segment> segment;
    {
        SegmentCacheHandle segment_cache_handle;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                _rowset, _segment_id, &segment_cache_handle, _should_use_cache, false, opts.stats));
        const auto& tmp_segments = segment_cache_handle.get_segments();
        segment = tmp_segments[0];
    }
    RETURN_IF_ERROR(segment->new_iterator(_schema, _read_options, &_inner_iterator));
    return _inner_iterator->init(_read_options);
}

} // namespace doris::segment_v2
