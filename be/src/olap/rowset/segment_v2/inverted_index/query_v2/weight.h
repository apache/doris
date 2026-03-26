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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace lucene::index {
class IndexReader;
}

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2::inverted_index::query_v2 {

struct FieldBindingContext {
    std::string logical_field_name;
    std::string stored_field_name;
    std::wstring stored_field_wstr;
};

struct QueryExecutionContext {
    uint32_t segment_num_rows = 0;
    std::vector<std::shared_ptr<lucene::index::IndexReader>> readers;
    std::unordered_map<std::string, std::shared_ptr<lucene::index::IndexReader>> reader_bindings;
    std::unordered_map<std::wstring, std::shared_ptr<lucene::index::IndexReader>>
            field_reader_bindings;
    std::unordered_map<std::string, FieldBindingContext> binding_fields;
    const NullBitmapResolver* null_resolver = nullptr;
};

class Weight {
public:
    using PruningCallback = std::function<float(uint32_t doc_id, float score)>;

    Weight() = default;
    virtual ~Weight() = default;

    virtual ScorerPtr scorer(const QueryExecutionContext& context) { return scorer(context, {}); }
    virtual ScorerPtr scorer(const QueryExecutionContext& context, const std::string& binding_key) {
        (void)binding_key;
        return scorer(context);
    }

    virtual void for_each_pruning(const QueryExecutionContext& context, float threshold,
                                  PruningCallback callback) {
        auto sc = scorer(context);
        if (!sc) {
            return;
        }
        for_each_pruning_scorer(sc, threshold, std::move(callback));
    }

    virtual void for_each_pruning(const QueryExecutionContext& context,
                                  const std::string& binding_key, float threshold,
                                  PruningCallback callback) {
        (void)binding_key;
        for_each_pruning(context, threshold, std::move(callback));
    }

    static void for_each_pruning_scorer(const ScorerPtr& scorer, float threshold,
                                        PruningCallback callback) {
        int32_t doc = scorer->doc();
        while (doc != TERMINATED) {
            float score = scorer->score();
            if (score > threshold) {
                threshold = callback(doc, score);
            }
            doc = scorer->advance();
        }
    }

protected:
    const FieldBindingContext* get_field_binding(const QueryExecutionContext& ctx,
                                                 const std::string& binding_key) const {
        auto it = ctx.binding_fields.find(binding_key);
        if (it != ctx.binding_fields.end()) {
            return &it->second;
        }
        return nullptr;
    }

    std::string logical_field_or_fallback(const QueryExecutionContext& ctx,
                                          const std::string& binding_key,
                                          const std::wstring& fallback) const {
        const auto* binding = get_field_binding(ctx, binding_key);
        if (binding != nullptr) {
            if (!binding->logical_field_name.empty()) {
                return binding->logical_field_name;
            }
            if (!binding->stored_field_name.empty()) {
                return binding->stored_field_name;
            }
        }
        return std::string(fallback.begin(), fallback.end());
    }

    std::shared_ptr<lucene::index::IndexReader> lookup_reader(
            const std::wstring& field, const QueryExecutionContext& ctx,
            const std::string& binding_key) const {
        if (!binding_key.empty()) {
            if (auto it = ctx.reader_bindings.find(binding_key); it != ctx.reader_bindings.end()) {
                return it->second;
            }
        }
        if (auto it = ctx.field_reader_bindings.find(field);
            it != ctx.field_reader_bindings.end()) {
            return it->second;
        }
        if (!ctx.readers.empty()) {
            return ctx.readers.front();
        }
        return nullptr;
    }

    SegmentPostingsPtr create_term_posting(lucene::index::IndexReader* reader,
                                           const std::wstring& field, const std::string& term,
                                           bool enable_scoring, const SimilarityPtr& similarity,
                                           const io::IOContext* io_ctx) const {
        return create_term_posting(reader, field, StringHelper::to_wstring(term), enable_scoring,
                                   similarity, io_ctx);
    }

    SegmentPostingsPtr create_term_posting(lucene::index::IndexReader* reader,
                                           const std::wstring& field, const std::wstring& term,
                                           bool enable_scoring, const SimilarityPtr& similarity,
                                           const io::IOContext* io_ctx) const {
        auto t = make_term_ptr(field.c_str(), term.c_str());
        auto iter = make_term_doc_ptr(reader, t.get(), enable_scoring, io_ctx);
        return iter ? make_segment_postings(std::move(iter), enable_scoring, similarity) : nullptr;
    }

    SegmentPostingsPtr create_position_posting(lucene::index::IndexReader* reader,
                                               const std::wstring& field, const std::string& term,
                                               bool enable_scoring, const SimilarityPtr& similarity,
                                               const io::IOContext* io_ctx) const {
        return create_position_posting(reader, field, StringHelper::to_wstring(term),
                                       enable_scoring, similarity, io_ctx);
    }

    SegmentPostingsPtr create_position_posting(lucene::index::IndexReader* reader,
                                               const std::wstring& field, const std::wstring& term,
                                               bool enable_scoring, const SimilarityPtr& similarity,
                                               const io::IOContext* io_ctx) const {
        auto t = make_term_ptr(field.c_str(), term.c_str());
        auto iter = make_term_positions_ptr(reader, t.get(), enable_scoring, io_ctx);
        return iter ? make_segment_postings(std::move(iter), enable_scoring, similarity) : nullptr;
    }
};

using WeightPtr = std::shared_ptr<Weight>;

} // namespace doris::segment_v2::inverted_index::query_v2
