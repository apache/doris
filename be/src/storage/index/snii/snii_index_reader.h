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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/common_grams/common_grams_query_cost.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::snii::reader {
class LogicalIndexReader;
} // namespace doris::snii::reader

namespace doris::snii::query {
struct PhraseMatch;
} // namespace doris::snii::query

namespace doris::segment_v2 {

// One query plus the plan the caller chose for it. This is a parameter object rather than a
// parameter list because _compute_query_bitmap() took fourteen positional arguments, two of them
// adjacent bools (common_grams_query_shape, force_plain) that no call site could tell apart
// without counting commas.
struct SniiQueryBitmapRequest {
    InvertedIndexQueryType query_type;
    const InvertedIndexQueryInfo& query_info;
    std::string_view search_str;
    int32_t max_expansions = 0;

    // Plan decisions the caller has already made. Both bools are false on the plain path.
    bool common_grams_query_shape = false;
    bool force_plain = false;
    inverted_index::CommonGramsPlanCostModel common_grams_cost_model {};
    const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr;
    // Identifies the physical query for the single-flight key; empty when unused.
    std::string_view physical_raw_query_key {};

    const ::doris::snii::reader::LogicalIndexReader* logical_reader = nullptr;
};

class SniiIndexReader final : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(SniiIndexReader);

public:
#ifdef BE_TEST
    using SingleFlightFollowerJoinedObserver = void (*)(void*) noexcept;
    using SingleFlightLeaderBeforeComputeObserver = void (*)(void*) noexcept;
    using SearcherOpenObserver = void (*)(void*) noexcept;
#endif

    SniiIndexReader(const TabletIndex* index_meta,
                    const std::shared_ptr<IndexFileReader>& index_file_reader,
                    InvertedIndexReaderType reader_type)
            : InvertedIndexReader(index_meta, index_file_reader), _reader_type(reader_type) {}

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
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) override;
    InvertedIndexReaderType type() override { return _reader_type; }

#ifdef BE_TEST
    void set_single_flight_follower_joined_observer_for_test(
            SingleFlightFollowerJoinedObserver observer, void* opaque) {
        _single_flight_follower_joined_observer = observer;
        _single_flight_follower_joined_opaque = opaque;
    }
    void set_single_flight_leader_before_compute_observer_for_test(
            SingleFlightLeaderBeforeComputeObserver observer, void* opaque) {
        _single_flight_leader_before_compute_observer = observer;
        _single_flight_leader_before_compute_opaque = opaque;
    }
    void set_searcher_open_observer_for_test(SearcherOpenObserver observer, void* opaque) {
        _searcher_open_observer = observer;
        _searcher_open_opaque = opaque;
    }
#endif

private:
    Status _query(const IndexQueryContextPtr& context, const std::string& column_name,
                  const Field& query_value, InvertedIndexQueryType query_type,
                  std::shared_ptr<roaring::Roaring>& bit_map,
                  InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                  const InvertedIndexAnalyzerCtx* analyzer_ctx);
    Status _parse_query_terms(
            const IndexQueryContextPtr& context, std::string search_str,
            InvertedIndexQueryType query_type, const InvertedIndexAnalyzerCtx* analyzer_ctx,
            InvertedIndexQueryInfo* query_info,
            std::optional<inverted_index::AnalysisPurpose> purpose_override = std::nullopt);
    Status _get_logical_reader(
            const IndexQueryContextPtr& context, InvertedIndexCacheHandle* searcher_cache_handle,
            std::unique_ptr<::doris::snii::reader::LogicalIndexReader>* uncached_reader,
            const ::doris::snii::reader::LogicalIndexReader** logical_reader);
    Status _read_null_bitmap(const IndexQueryContextPtr& context,
                             InvertedIndexQueryCacheHandle* cache_handle,
                             const ::doris::snii::reader::LogicalIndexReader* preopened_reader);
    // Opens the segment index and runs the query, producing the result bitmap. Invoked as the
    // single-flight "compute" step by query(); see SingleFlight for the concurrency rationale.
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
    // G02 count-only fast path. Only called when the caller (SegmentIterator)
    // set context->count_on_index_fastpath, i.e. the match count alone decides
    // the scan result. On *handled = true, *out is a bitmap of cardinality df
    // (row ids NOT real) built from a single exact term's dict-entry df WITHOUT
    // decoding postings. On a segment without a null bitmap the fabricated ids
    // are the dense range [0, df); on a segment WITH one they are the first df
    // NON-NULL row ids (see
    // fabricate_null_disjoint_count_bitmap) so that the unconditional
    // FunctionMatchBase -> mask_out_null subtraction of the real null bitmap
    // is a no-op and the cardinality stays df -- which is already the exact
    // match count, because postings never contain null docs. Falls through
    // (*handled = false) for every other shape: every multi-term query
    // (including phrase and OR/AND), prefix/regexp/wildcard/phrase-prefix
    // expansion. Multi-term sloppy phrases fall through with every other
    // multi-term shape; a single-term phrase remains exactly one posting df.
    // On *handled = true, query() also raises
    // context->count_on_index_fastpath_hit (G03) so the SegmentIterator may
    // short-circuit row emission for the count-shaped bitmap.
    Status _try_count_only_fastpath(
            const IndexQueryContextPtr& context, InvertedIndexQueryType query_type,
            const InvertedIndexQueryInfo& query_info, const std::vector<std::string>& terms,
            bool* handled, std::shared_ptr<roaring::Roaring>* out,
            const ::doris::snii::reader::LogicalIndexReader* preopened_reader = nullptr);

    InvertedIndexReaderType _reader_type;
#ifdef BE_TEST
    SingleFlightFollowerJoinedObserver _single_flight_follower_joined_observer = nullptr;
    void* _single_flight_follower_joined_opaque = nullptr;
    SingleFlightLeaderBeforeComputeObserver _single_flight_leader_before_compute_observer = nullptr;
    void* _single_flight_leader_before_compute_opaque = nullptr;
    SearcherOpenObserver _searcher_open_observer = nullptr;
    void* _searcher_open_opaque = nullptr;
#endif
};

} // namespace doris::segment_v2
