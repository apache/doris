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
#include <string_view>

#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2 {

// A raw query or a borrowed leaf already lowered by the caller.
struct LeafRequest {
    InvertedIndexQueryType query_type;
    // Null for a raw query, which is lowered only after a cache miss.
    const index_query::logical::Node* leaf = nullptr;
    // The longest value the STRING_TYPE ignore_above limit applies to.
    size_t longest_value_bytes = 0;
    // The value messages quote, and the pattern a gram query compiles.
    std::string_view text;
    // The analyzer that cuts a MATCH value; null when the index's own does, and for a leaf.
    const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr;
};

// What a format decides about a request before the result cache is read.
struct Admission {
    // Whether the result may enter the result cache.
    bool cacheable = true;
    // Whether concurrent identical full-segment queries share one execution.
    bool coalesce = false;
    // Whether the request is planned before the index opens, so a value its analyzer rejects
    // fails without reading the index. A format whose analysis depends on the opened segment
    // plans after it.
    bool plan_before_open = true;
    // Rewrites a failure to open the index; the failure stands when unset.
    std::function<Status(Status)> open_failed;
    // Rejects an opened index that cannot answer the request exactly.
    std::function<Status(OpenedIndex&)> check_open;
};

// Runs MATCH queries and SEARCH leaves through shared caching, candidate filtering, and execution.
// Format-specific hooks provide index access and admission rules.
class TextIndexReader : public InvertedIndexReader {
public:
    using InvertedIndexReader::InvertedIndexReader;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status query_with_null_bitmap(const IndexQueryContextPtr& context,
                                  const std::string& column_name, const Field& query_value,
                                  InvertedIndexQueryType query_type,
                                  std::shared_ptr<roaring::Roaring>& bit_map,
                                  InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                                  const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status query_leaf(const IndexQueryContextPtr& context, const std::string& column_name,
                      const index_query::logical::Node& leaf,
                      std::shared_ptr<roaring::Roaring>& bit_map,
                      InvertedIndexQueryCacheHandle* null_bitmap_cache_handle = nullptr) override;
    Status try_query(const IndexQueryContextPtr& /*context*/, const std::string& /*column_name*/,
                     const Field& /*query_value*/, InvertedIndexQueryType /*query_type*/,
                     size_t* /*count*/) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "a text index reader does not support try_query");
    }
    Status open_source(const IndexQueryContextPtr& context, const std::wstring& field,
                       std::unique_ptr<OpenedIndex>* opened,
                       index_query::IndexSourcePtr* source) override;

protected:
    // Lowers a MATCH value and runs it, keyed by the raw value.
    Status _query_raw(const IndexQueryContextPtr& context, const std::string& column_name,
                      const std::string& value, InvertedIndexQueryType query_type,
                      std::shared_ptr<roaring::Roaring>& bit_map,
                      InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                      const InvertedIndexAnalyzerCtx* analyzer_ctx);
    Status _execute(const IndexQueryContextPtr& context, const std::string& column_name,
                    const LeafRequest& request, std::shared_ptr<roaring::Roaring>& bit_map,
                    InvertedIndexQueryCacheHandle* null_bitmap_cache_handle);
    // Answers `request` into `bit_map`; `index` keeps what it opened, and `count_shaped` tells
    // a count-only answer from row ids.
    Status _run_request(const IndexQueryContextPtr& context, const std::string& column_name,
                        const LeafRequest& request, std::shared_ptr<roaring::Roaring>& bit_map,
                        std::unique_ptr<OpenedIndex>* index, bool* count_shaped);
    // Lowers the raw value of `request`, analyzed unless the index or the analyzer keeps it whole.
    Status _lower(const IndexQueryContextPtr& context, const LeafRequest& request, bool keyword,
                  index_query::logical::Node* out);
    // Answers a COUNT_ON_INDEX scan of one exact term from its document frequency: `out` holds
    // that many ids, off the NULL rows, and never enters the cache. Declines a reader without the
    // segment's row count and an ARRAY column on a segment with NULL rows, whose postings may
    // hold them.
    Status _count_from_df(const IndexQueryContextPtr& context, const std::string& column_name,
                          OpenedIndex& index, const std::string& term, bool* handled,
                          std::shared_ptr<roaring::Roaring>* out);

    // Opens the index through the searcher cache, reusing an already constructed index key.
    virtual Status _open_index(const IndexQueryContextPtr& context,
                               std::unique_ptr<OpenedIndex>* out,
                               const std::string& index_file_key = {}) = 0;
    // The engine's source for `field` over the open index.
    virtual index_query::IndexSourcePtr _bind_source(const IndexQueryContextPtr& context,
                                                     const std::wstring& field,
                                                     OpenedIndex& index) = 0;
    // The number of documents holding `term`, read from the dictionary, and the number of
    // documents the index covers.
    virtual Status _term_document_frequency(const std::string& column_name, OpenedIndex& index,
                                            const std::string& term, uint64_t* df,
                                            uint64_t* document_count) = 0;
    // The name of this index's statistics in the query's CollectionStatistics.
    std::wstring _statistics_field(const std::wstring& field) const {
        return match_statistics_field(field, _index_meta.index_id());
    }
    // Runs `leaf` over the open index into `out`. With `candidates`, a phrase matches only those
    // rows; with `scoring`, the BM25 values reach the context's collection similarity.
    virtual Status _run_leaf(const IndexQueryContextPtr& context, const std::string& column_name,
                             OpenedIndex& index, const index_query::logical::Node& leaf,
                             const roaring::Roaring* candidates, bool scoring,
                             std::shared_ptr<roaring::Roaring>* out) = 0;
    // Reads the null bitmap through the query cache, from `index` when it is open.
    virtual Status _read_null_bitmap(const IndexQueryContextPtr& context,
                                     InvertedIndexQueryCacheHandle* cache_handle,
                                     OpenedIndex* /*index*/) {
        return read_null_bitmap(context, cache_handle);
    }
    // Decides, before the result cache is read, whether the index answers `request` and what
    // `admission` holds for it. A text index answers no gram query unless its format does.
    virtual Status _admit(const IndexQueryContextPtr& context, const std::string& column_name,
                          const LeafRequest& request, Admission* admission);
    // Runs the gram query of `request` (LIKE or REGEXP over the index's grams) into `out`.
    virtual Status _run_gram(const IndexQueryContextPtr& context, OpenedIndex& index,
                             const LeafRequest& request, std::shared_ptr<roaring::Roaring>* out);
};

// The query_v2 query that runs a logical leaf on the bound source of `field`. With `candidates`,
// a phrase only matches those rows.
Status plan_query(const index_query::logical::Node& leaf, const IndexQueryContextPtr& context,
                  const std::wstring& field, const std::string& binding_key,
                  const roaring::Roaring* candidates,
                  std::shared_ptr<inverted_index::query_v2::Query>* out);

// Adds the leaf's matching rows and optional BM25 scores, restricting phrase matches to candidates.
// Exceptions from the query engine propagate to the caller.
Status run_leaf(const IndexQueryContextPtr& context, const std::wstring& field,
                const index_query::logical::Node& leaf, const roaring::Roaring* candidates,
                bool scoring, index_query::IndexSourcePtr source, uint32_t doc_count,
                const std::shared_ptr<roaring::Roaring>& result);

} // namespace doris::segment_v2
