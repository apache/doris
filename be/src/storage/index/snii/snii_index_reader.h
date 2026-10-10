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

namespace doris::segment_v2::gram {
struct GramScheme;
} // namespace doris::segment_v2::gram

namespace doris::snii::reader {
class LogicalIndexReader;
} // namespace doris::snii::reader

namespace doris::segment_v2 {

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
    // A policy that cannot be resolved names no index a gram query could use; an analyzed query
    // reports that failure where it matters.
    bool is_gram_family() const override {
        std::optional<segment_v2::gram::GramScheme> scheme;
        return _current_gram_scheme(nullptr, &scheme).ok() && scheme.has_value();
    }

private:
    Status _open_index(const IndexQueryContextPtr& context, std::unique_ptr<OpenedIndex>* out,
                       const std::string& index_file_key = {}) override;
    Status _term_document_frequency(const std::string& column_name, OpenedIndex& index,
                                    const std::string& term, uint64_t* df,
                                    uint64_t* document_count) override;
    // Runs the leaf on the shared engine over the index's source.
    Status _run_leaf(const IndexQueryContextPtr& context, const std::string& column_name,
                     OpenedIndex& index, const index_query::logical::Node& leaf,
                     const roaring::Roaring* candidates, bool scoring,
                     std::shared_ptr<roaring::Roaring>* out) override;
    Status _read_null_bitmap(const IndexQueryContextPtr& context,
                             InvertedIndexQueryCacheHandle* cache_handle,
                             OpenedIndex* index) override;
    index_query::IndexSourcePtr _bind_source(const IndexQueryContextPtr& context,
                                             const std::wstring& field,
                                             OpenedIndex& index) override;
    // Declines a gram query on an index no analyzer policy could have cut into grams, and keeps
    // an analyzed query on a gram index out of the cache and off a segment cut another way.
    Status _admit(const IndexQueryContextPtr& context, const std::string& column_name,
                  const LeafRequest& request, Admission* admission) override;
    Status _run_gram(const IndexQueryContextPtr& context, OpenedIndex& index,
                     const LeafRequest& request, std::shared_ptr<roaring::Roaring>* out) override;
    // The scheme the current analyzer cuts query terms with: the provider of `analyzer_ctx`, or
    // the analyzer the index properties name.
    Status _current_gram_scheme(const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                std::optional<segment_v2::gram::GramScheme>* out) const;
    Status _get_logical_reader(
            const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
            std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
            const ::doris::snii::reader::LogicalIndexReader** logical_reader,
            const std::string& index_file_key = {});
    Status _read_snii_null_bitmap(
            const IndexQueryContextPtr& context, InvertedIndexQueryCacheHandle* cache_handle,
            const ::doris::snii::reader::LogicalIndexReader* preopened_reader);
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
