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

#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/query/spi/index_source.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The split source that drives the segment walk: the binding's, else the first split one among
// the sources, the bindings and the fields; null when no source is split by segment.
inline index_query::IndexSourcePtr find_segmented_source(const QueryExecutionContext& context,
                                                         const std::string& binding_key) {
    if (!binding_key.empty()) {
        if (auto it = context.source_bindings.find(binding_key);
            it != context.source_bindings.end()) {
            return it->second->segments().empty() ? nullptr : it->second;
        }
    }

    for (const auto& source : context.sources) {
        if (!source->segments().empty()) {
            return source;
        }
    }
    for (const auto& [_, source] : context.source_bindings) {
        if (!source->segments().empty()) {
            return source;
        }
    }
    for (const auto& [_, source] : context.field_sources) {
        if (!source->segments().empty()) {
            return source;
        }
    }
    return nullptr;
}

// A split source must be split like the driver: the same segments, bases and sizes.
inline void validate_segment_topology(const index_query::IndexSourcePtr& source,
                                      const std::vector<index_query::IndexSegment>& driver) {
    const auto segments = source->segments();
    if (segments.empty()) {
        return;
    }
    DCHECK_EQ(segments.size(), driver.size());
    for (size_t i = 0; i < driver.size(); ++i) {
        DCHECK_EQ(segments[i].doc_base, driver[i].doc_base);
        DCHECK_EQ(segments[i].source->doc_count(), driver[i].source->doc_count());
    }
}

inline void validate_segment_topologies(const QueryExecutionContext& context,
                                        const std::vector<index_query::IndexSegment>& driver) {
    for (const auto& source : context.sources) {
        validate_segment_topology(source, driver);
    }
    for (const auto& [_, source] : context.source_bindings) {
        validate_segment_topology(source, driver);
    }
    for (const auto& [_, source] : context.field_sources) {
        validate_segment_topology(source, driver);
    }
}

// The part of `source` for one segment, or the source itself when it is not split.
inline index_query::IndexSourcePtr source_for_segment(const index_query::IndexSourcePtr& source,
                                                      size_t segment_index) {
    const auto segments = source->segments();
    if (segments.empty()) {
        return source;
    }
    DCHECK_LT(segment_index, segments.size());
    return segments[segment_index].source;
}

class SegmentNullBitmapResolver final : public NullBitmapResolver {
public:
    SegmentNullBitmapResolver(const QueryExecutionContext& source, uint32_t base, uint32_t count)
            : _source(source.null_resolver),
              _source_owner(source.null_resolver_owner),
              _base(base),
              _end(uint64_t(base) + count) {}

    segment_v2::IndexIterator* iterator_for(const Scorer& scorer,
                                            const std::string& logical_field) const override {
        return _source->iterator_for(scorer, logical_field);
    }

    void localize_null_rows(roaring::Roaring& rows) const override {
        _source->localize_null_rows(rows);
        rows.removeRange(0, _base);
        rows.removeRange(_end, uint64_t(1) << 32);
        if (_base != 0 && !rows.isEmpty()) {
            auto* shifted = roaring::api::roaring_bitmap_add_offset(&rows.roaring, -int64_t(_base));
            if (shifted == nullptr) {
                throw Exception(ErrorCode::MEM_ALLOC_FAILED, "Failed to rebase segment NULL rows");
            }
            rows = roaring::Roaring(shifted);
        }
    }

private:
    const NullBitmapResolver* _source;
    std::shared_ptr<const NullBitmapResolver> _source_owner;
    uint32_t _base;
    uint64_t _end;
};

inline QueryExecutionContext create_segment_context(const QueryExecutionContext& original_ctx,
                                                    size_t segment_index, uint32_t segment_num_rows,
                                                    uint32_t segment_doc_base,
                                                    const std::string& binding_key) {
    QueryExecutionContext seg_ctx;

    for (const auto& source : original_ctx.sources) {
        seg_ctx.sources.push_back(source_for_segment(source, segment_index));
    }

    seg_ctx.segment_num_rows = segment_num_rows;

    for (const auto& [key, source] : original_ctx.source_bindings) {
        seg_ctx.source_bindings[key] = source_for_segment(source, segment_index);
    }
    for (const auto& [field, source] : original_ctx.field_sources) {
        seg_ctx.field_sources[field] = source_for_segment(source, segment_index);
    }

    if (!binding_key.empty() && !seg_ctx.sources.empty() &&
        !seg_ctx.source_bindings.contains(binding_key)) {
        seg_ctx.source_bindings[binding_key] = seg_ctx.sources.front();
    }

    seg_ctx.binding_fields = original_ctx.binding_fields;
    if (original_ctx.null_resolver != nullptr) {
        seg_ctx.null_resolver_owner = std::make_shared<SegmentNullBitmapResolver>(
                original_ctx, segment_doc_base, segment_num_rows);
        seg_ctx.null_resolver = seg_ctx.null_resolver_owner.get();
    }

    return seg_ctx;
}

template <typename SegmentCallback>
void for_each_index_segment(const QueryExecutionContext& context, const std::string& binding_key,
                            SegmentCallback&& callback) {
    auto driver = find_segmented_source(context, binding_key);
    if (!driver) {
        // No source available (e.g., AllQuery/MatchAllDocsQuery which doesn't resolve fields).
        // Fall back to using the original context directly, as AllScorer only needs segment_num_rows.
        if (context.segment_num_rows > 0) {
            callback(context, 0);
        }
        return;
    }

    const auto segments = driver->segments();
    validate_segment_topologies(context, segments);
    for (size_t i = 0; i < segments.size(); ++i) {
        QueryExecutionContext seg_ctx = create_segment_context(
                context, i, segments[i].source->doc_count(), segments[i].doc_base, binding_key);
        callback(seg_ctx, segments[i].doc_base);
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
