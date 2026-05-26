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

#include "storage/index/inverted/query_v2/weight.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include "CLucene.h"
#include "CLucene/index/MultiReader.h"
#include "CLucene/index/_MultiSegmentReader.h"
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

namespace doris::segment_v2::inverted_index::query_v2 {

inline std::shared_ptr<lucene::index::IndexReader> non_owning_reader(
        lucene::index::IndexReader* reader) {
    return {reader, [](lucene::index::IndexReader*) {}};
}

inline const lucene::util::ArrayBase<lucene::index::IndexReader*>* sub_readers(
        lucene::index::IndexReader* reader) {
    if (auto* multi_segment_reader = dynamic_cast<lucene::index::MultiSegmentReader*>(reader)) {
        return multi_segment_reader->getSubReaders();
    }
    if (auto* multi_reader = dynamic_cast<lucene::index::MultiReader*>(reader)) {
        return multi_reader->getSubReaders();
    }
    return nullptr;
}

inline const int32_t* segment_starts(lucene::index::IndexReader* reader) {
    if (auto* multi_segment_reader = dynamic_cast<lucene::index::MultiSegmentReader*>(reader)) {
        return multi_segment_reader->getStarts();
    }
    return nullptr;
}

inline uint32_t segment_base(lucene::index::IndexReader* reader, size_t segment_index) {
    const auto* starts = segment_starts(reader);
    if (starts != nullptr) {
        return static_cast<uint32_t>(starts[segment_index]);
    }

    const auto* segments = sub_readers(reader);
    DCHECK(segments != nullptr);
    uint32_t base = 0;
    for (size_t i = 0; i < segment_index; ++i) {
        base += (*segments)[i]->maxDoc();
    }
    return base;
}

inline std::shared_ptr<lucene::index::IndexReader> find_segmented_reader(
        const QueryExecutionContext& context, const std::string& binding_key) {
    if (!binding_key.empty()) {
        if (auto it = context.reader_bindings.find(binding_key);
            it != context.reader_bindings.end()) {
            return sub_readers(it->second.get()) != nullptr ? it->second : nullptr;
        }
    }

    for (const auto& reader : context.readers) {
        if (sub_readers(reader.get()) != nullptr) {
            return reader;
        }
    }
    for (const auto& [_, reader] : context.reader_bindings) {
        if (sub_readers(reader.get()) != nullptr) {
            return reader;
        }
    }
    for (const auto& [_, reader] : context.field_reader_bindings) {
        if (sub_readers(reader.get()) != nullptr) {
            return reader;
        }
    }
    return nullptr;
}

inline void validate_segment_topology(
        const std::shared_ptr<lucene::index::IndexReader>& reader,
        const std::shared_ptr<lucene::index::IndexReader>& driver_reader) {
    const auto* segments = sub_readers(reader.get());
    if (segments == nullptr) {
        return;
    }

    const auto* driver_segments = sub_readers(driver_reader.get());
    DCHECK(driver_segments != nullptr);
    DCHECK_EQ(segments->length, driver_segments->length);
    for (size_t i = 0; i < driver_segments->length; ++i) {
        DCHECK_EQ(segment_base(reader.get(), i), segment_base(driver_reader.get(), i));
        DCHECK_EQ((*segments)[i]->maxDoc(), (*driver_segments)[i]->maxDoc());
    }
}

inline void validate_segment_topologies(
        const QueryExecutionContext& context,
        const std::shared_ptr<lucene::index::IndexReader>& driver_reader) {
    for (const auto& reader : context.readers) {
        validate_segment_topology(reader, driver_reader);
    }
    for (const auto& [_, reader] : context.reader_bindings) {
        validate_segment_topology(reader, driver_reader);
    }
    for (const auto& [_, reader] : context.field_reader_bindings) {
        validate_segment_topology(reader, driver_reader);
    }
}

inline std::shared_ptr<lucene::index::IndexReader> reader_for_segment(
        const std::shared_ptr<lucene::index::IndexReader>& reader, size_t segment_index) {
    const auto* segments = sub_readers(reader.get());
    if (segments != nullptr) {
        DCHECK_LT(segment_index, segments->length);
        return non_owning_reader((*segments)[segment_index]);
    }
    return reader;
}

inline QueryExecutionContext create_segment_context(const QueryExecutionContext& original_ctx,
                                                    size_t segment_index, uint32_t segment_num_rows,
                                                    const std::string& binding_key) {
    QueryExecutionContext seg_ctx;

    for (const auto& reader : original_ctx.readers) {
        seg_ctx.readers.push_back(reader_for_segment(reader, segment_index));
    }

    seg_ctx.segment_num_rows = segment_num_rows;

    for (const auto& [key, reader] : original_ctx.reader_bindings) {
        seg_ctx.reader_bindings[key] = reader_for_segment(reader, segment_index);
    }
    for (const auto& [field, reader] : original_ctx.field_reader_bindings) {
        seg_ctx.field_reader_bindings[field] = reader_for_segment(reader, segment_index);
    }

    if (!binding_key.empty() && !seg_ctx.readers.empty() &&
        seg_ctx.reader_bindings.find(binding_key) == seg_ctx.reader_bindings.end()) {
        seg_ctx.reader_bindings[binding_key] = seg_ctx.readers.front();
    }

    seg_ctx.binding_fields = original_ctx.binding_fields;
    seg_ctx.null_resolver = original_ctx.null_resolver;

    return seg_ctx;
}

template <typename SegmentCallback>
void for_each_index_segment(const QueryExecutionContext& context, const std::string& binding_key,
                            SegmentCallback&& callback) {
    auto segmented_reader = find_segmented_reader(context, binding_key);
    if (!segmented_reader) {
        // No reader available (e.g., AllQuery/MatchAllDocsQuery which doesn't resolve fields).
        // Fall back to using the original context directly, as AllScorer only needs segment_num_rows.
        if (context.segment_num_rows > 0) {
            callback(context, 0);
        }
        return;
    }

    const auto* sub_readers = query_v2::sub_readers(segmented_reader.get());
    if (!sub_readers || sub_readers->length == 0) {
        return;
    }

    validate_segment_topologies(context, segmented_reader);
    for (size_t i = 0; i < sub_readers->length; ++i) {
        auto seg_base = segment_base(segmented_reader.get(), i);
        QueryExecutionContext seg_ctx =
                create_segment_context(context, i, (*sub_readers)[i]->numDocs(), binding_key);
        callback(seg_ctx, seg_base);
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
