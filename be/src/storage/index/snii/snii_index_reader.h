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
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::snii::reader {
class LogicalIndexReader;
} // namespace doris::snii::reader

namespace doris::segment_v2 {

class SniiIndexReader final : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(SniiIndexReader);

public:
    SniiIndexReader(const TabletIndex* index_meta,
                    const std::shared_ptr<IndexFileReader>& index_file_reader,
                    InvertedIndexReaderType reader_type)
            : InvertedIndexReader(index_meta, index_file_reader), _reader_type(reader_type) {}

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) override;
    InvertedIndexReaderType type() override { return _reader_type; }

private:
    Status _parse_query_terms(const IndexQueryContextPtr& context, std::string search_str,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx,
                              InvertedIndexQueryInfo* query_info);
    Status _get_logical_reader(
            const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
            std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
            const ::doris::snii::reader::LogicalIndexReader** logical_reader);
    // Opens the segment index and runs the query, producing the result bitmap. Invoked as the
    // single-flight "compute" step by query(); see SingleFlight for the concurrency rationale.
    Status _compute_query_bitmap(const IndexQueryContextPtr& context,
                                 InvertedIndexQueryType query_type,
                                 const InvertedIndexQueryInfo& query_info,
                                 std::string_view search_str, const std::vector<std::string>& terms,
                                 int32_t max_expansions, std::shared_ptr<roaring::Roaring>* out);
    // G02 count-only fast path. Only called when the caller (SegmentIterator)
    // set context->count_on_index_fastpath, i.e. the match count alone decides
    // the scan result. On *handled = true, *out is a bitmap of cardinality df
    // (row ids NOT real) built from dict entries WITHOUT decoding postings:
    // single exact term -> dict-entry df; 2-term MATCH_PHRASE with a surviving
    // (dict-HIT) G01 bigram -> bigram df. On a segment without a null bitmap
    // the fabricated ids are the dense range [0, df); on a segment WITH one
    // they are the first df NON-NULL row ids (see
    // fabricate_null_disjoint_count_bitmap) so that the unconditional
    // FunctionMatchBase -> mask_out_null subtraction of the real null bitmap
    // is a no-op and the cardinality stays df -- which is already the exact
    // match count, because postings never contain null docs. Falls through
    // (*handled = false) for every other shape: multi-term OR/AND,
    // prefix/regexp/wildcard expansion, sloppy phrase, pruned/absent bigram.
    Status _try_count_only_fastpath(const IndexQueryContextPtr& context,
                                    InvertedIndexQueryType query_type,
                                    const InvertedIndexQueryInfo& query_info,
                                    const std::vector<std::string>& terms, bool* handled,
                                    std::shared_ptr<roaring::Roaring>* out);

    InvertedIndexReaderType _reader_type;
};

} // namespace doris::segment_v2
