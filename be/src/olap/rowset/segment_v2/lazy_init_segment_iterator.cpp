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

namespace doris::segment_v2 {

LazyInitSegmentIterator::LazyInitSegmentIterator(std::shared_ptr<Segment> segment,
                                                 SchemaSPtr schema, const StorageReadOptions& opts)
        : _schema(std::move(schema)), _segment(std::move(segment)), _read_options(opts) {}

/// Here do not use the argument of `opts`,
/// see where the iterator is created in `BetaRowsetReader::get_segment_iterators`
Status LazyInitSegmentIterator::init(const StorageReadOptions& /*opts*/) {
    _need_lazy_init = false;
    if (_inner_iterator) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_segment->new_iterator(_schema, _read_options, &_inner_iterator));
    return _inner_iterator->init(_read_options);
}

Status LazyInitSegmentIterator::_ensure_initialized() {
    if (UNLIKELY(_need_lazy_init)) {
        RETURN_IF_ERROR(init(_read_options));
    }
    if (UNLIKELY(!_inner_iterator)) {
        return Status::InternalError("LazyInitSegmentIterator inner iterator not initialized");
    }
    return Status::OK();
}

Status LazyInitSegmentIterator::prepare_prefetch_batch(
        std::set<std::pair<uint64_t, uint32_t>>* pages_to_prefetch, bool* has_more) {
    RETURN_IF_ERROR(_ensure_initialized());
    auto* planner = dynamic_cast<PrefetchPlanner*>(_inner_iterator.get());
    if (planner == nullptr) {
        if (has_more != nullptr) {
            *has_more = false;
        }
        return Status::OK();
    }
    return planner->prepare_prefetch_batch(pages_to_prefetch, has_more);
}

Status LazyInitSegmentIterator::submit_prefetch_batch(
        const std::set<std::pair<uint64_t, uint32_t>>& pages_to_prefetch) {
    RETURN_IF_ERROR(_ensure_initialized());
    auto* planner = dynamic_cast<PrefetchPlanner*>(_inner_iterator.get());
    if (planner == nullptr) {
        return Status::OK();
    }
    return planner->submit_prefetch_batch(pages_to_prefetch);
}

} // namespace doris::segment_v2
