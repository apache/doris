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

#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#endif
#include "CLucene.h"
#include "CLucene/index/_MultiSegmentReader.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2::inverted_index::query_v2 {

inline QueryExecutionContext create_segment_context(lucene::index::IndexReader* seg_reader,
                                                    const QueryExecutionContext& original_ctx,
                                                    const std::string& binding_key) {
    QueryExecutionContext seg_ctx;
    seg_ctx.segment_num_rows = seg_reader->numDocs();

    auto reader_ptr = std::shared_ptr<lucene::index::IndexReader>(
            seg_reader, [](lucene::index::IndexReader*) {});
    seg_ctx.readers.push_back(reader_ptr);

    if (!binding_key.empty()) {
        seg_ctx.reader_bindings[binding_key] = reader_ptr;
    }

    seg_ctx.binding_fields = original_ctx.binding_fields;
    seg_ctx.null_resolver = original_ctx.null_resolver;

    return seg_ctx;
}

template <typename SegmentCallback>
void for_each_index_segment(const QueryExecutionContext& context, const std::string& binding_key,
                            SegmentCallback&& callback) {
    auto* reader = context.readers.empty() ? nullptr : context.readers.front().get();
    if (!reader) {
        return;
    }

    auto* multi_reader = dynamic_cast<lucene::index::MultiSegmentReader*>(reader);
    if (multi_reader == nullptr) {
        callback(context, 0);
        return;
    }

    const auto* sub_readers = multi_reader->getSubReaders();
    const auto* starts = multi_reader->getStarts();

    if (!sub_readers || sub_readers->length == 0) {
        return;
    }

    for (size_t i = 0; i < sub_readers->length; ++i) {
        auto* seg_reader = (*sub_readers)[i];
        auto seg_base = static_cast<uint32_t>(starts[i]);
        QueryExecutionContext seg_ctx = create_segment_context(seg_reader, context, binding_key);
        callback(seg_ctx, seg_base);
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
