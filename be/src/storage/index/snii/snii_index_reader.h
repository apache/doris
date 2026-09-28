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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/query/logical/node.h"

namespace doris::snii::reader {
class LogicalIndexReader;
} // namespace doris::snii::reader

namespace doris::snii::query {
struct PhraseMatch;
} // namespace doris::snii::query

namespace doris::segment_v2 {

// The query SNII's executors run for a logical leaf.
struct NativeQuery {
    InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
    InvertedIndexQueryInfo query_info;
};

// Fills `out`, which has no terms yet, with the query that runs `leaf`, moving the terms out of
// `leaf`.
Status plan_native_query(index_query::logical::Node&& leaf, NativeQuery* out);

// All query inputs passed to _compute_query_bitmap after opening the logical reader.
struct SniiQueryBitmapRequest {
    InvertedIndexQueryType query_type;
    const InvertedIndexQueryInfo& query_info;
    std::string_view search_str;
    int32_t max_expansions = 0;
    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
    // Scan candidates a multi-term phrase is restricted to; null for a full-segment query.
    const roaring::Roaring* candidates = nullptr;
};

class SniiIndexReader final : public TextIndexReader {
    ENABLE_FACTORY_CREATOR(SniiIndexReader);

public:
    SniiIndexReader(const TabletIndex* index_meta,
                    const std::shared_ptr<IndexFileReader>& index_file_reader,
                    InvertedIndexReaderType reader_type, uint64_t rows_of_segment,
                    bool column_is_array)
            : TextIndexReader(index_meta, index_file_reader, rows_of_segment, column_is_array),
              _reader_type(reader_type) {}

    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) override;
    InvertedIndexReaderType type() override { return _reader_type; }

private:
    Status _open_index(const IndexQueryContextPtr& context,
                       std::unique_ptr<OpenedIndex>* out) override;
    Status _term_document_frequency(const std::string& column_name, OpenedIndex& index,
                                    const std::string& term, uint64_t* df,
                                    uint64_t* document_count) override;
    // Plans the native query, runs it and scores its rows afterwards.
    Status _run_leaf(const IndexQueryContextPtr& context, const std::string& column_name,
                     OpenedIndex& index, const index_query::logical::Node& leaf,
                     const roaring::Roaring* candidates, bool scoring,
                     std::shared_ptr<roaring::Roaring>* out) override;
    Status _read_null_bitmap(const IndexQueryContextPtr& context,
                             InvertedIndexQueryCacheHandle* cache_handle,
                             OpenedIndex* index) override;
    Status _get_logical_reader(
            const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
            std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
            const ::doris::snii::reader::LogicalIndexReader** logical_reader);
    Status _read_snii_null_bitmap(
            const IndexQueryContextPtr& context, InvertedIndexQueryCacheHandle* cache_handle,
            const ::doris::snii::reader::LogicalIndexReader* preopened_reader);
    // Runs the planned query over the open reader, producing the result bitmap.
    Status _compute_query_bitmap(const IndexQueryContextPtr& context,
                                 const SniiQueryBitmapRequest& request,
                                 std::vector<std::string>* terms,
                                 std::shared_ptr<roaring::Roaring>* out,
                                 std::vector<::doris::snii::query::PhraseMatch>* phrase_matches);
#ifdef BE_TEST
    Status _compute_query_bitmap(const IndexQueryContextPtr& context,
                                 InvertedIndexQueryType query_type,
                                 const InvertedIndexQueryInfo& query_info,
                                 std::string_view search_str, std::vector<std::string>* terms,
                                 int32_t max_expansions, std::shared_ptr<roaring::Roaring>* out);
#endif
#ifdef BE_TEST
    // The count-only fast path over `preopened_reader`, or the reader the session opens.
    Status _try_count_only_fastpath(
            const IndexQueryContextPtr& context, InvertedIndexQueryType query_type,
            const InvertedIndexQueryInfo& query_info, const std::vector<std::string>& terms,
            bool* handled, std::shared_ptr<roaring::Roaring>* out,
            const ::doris::snii::reader::LogicalIndexReader* preopened_reader = nullptr);
#endif

    InvertedIndexReaderType _reader_type;
};

} // namespace doris::segment_v2
