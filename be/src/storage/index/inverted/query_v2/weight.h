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
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/query/spi/index_source.h"

namespace doris::segment_v2::inverted_index::query_v2 {

struct FieldBindingContext {
    std::string logical_field_name;
    std::string stored_field_name;
    std::wstring stored_field_wstr;
};

struct QueryExecutionContext {
    uint32_t segment_num_rows = 0;
    // The index sources a query reads: every bound one, by binding key, and by stored field.
    std::vector<index_query::IndexSourcePtr> sources;
    std::unordered_map<std::string, index_query::IndexSourcePtr> source_bindings;
    std::unordered_map<std::wstring, index_query::IndexSourcePtr> field_sources;
    std::unordered_map<std::string, FieldBindingContext> binding_fields;
    const NullBitmapResolver* null_resolver = nullptr;
    std::shared_ptr<const NullBitmapResolver> null_resolver_owner;
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

    // The source bound to `binding_key`, else the one bound to `field`, else the first.
    index_query::IndexSourcePtr lookup_source(const std::wstring& field,
                                              const QueryExecutionContext& ctx,
                                              const std::string& binding_key) const {
        if (!binding_key.empty()) {
            if (auto it = ctx.source_bindings.find(binding_key); it != ctx.source_bindings.end()) {
                return it->second;
            }
        }
        if (auto it = ctx.field_sources.find(field); it != ctx.field_sources.end()) {
            return it->second;
        }
        if (!ctx.sources.empty()) {
            return ctx.sources.front();
        }
        return nullptr;
    }

    // The postings of a UTF-8 term on `source`, or null when the source lacks the term.
    SegmentPostingsPtr open_postings(
            index_query::IndexSource& source, std::string_view term, bool positions,
            bool enable_scoring, const index_query::ScoringContextPtr<float>& similarity) const {
        std::unique_ptr<index_query::PostingsCursor> cursor;
        THROW_IF_ERROR(source.open_term(term, positions, enable_scoring, &cursor));
        if (cursor == nullptr) {
            return nullptr;
        }
        return make_segment_postings(std::move(cursor), enable_scoring, similarity);
    }
};

using WeightPtr = std::shared_ptr<Weight>;

} // namespace doris::segment_v2::inverted_index::query_v2
