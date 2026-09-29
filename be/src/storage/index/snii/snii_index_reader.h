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

#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"

namespace doris::segment_v2::gram {
struct GramScheme;
} // namespace doris::segment_v2::gram

namespace doris::snii::reader {
class LogicalIndexReader;
} // namespace doris::snii::reader

namespace doris::snii::query {
struct PhraseMatch;
} // namespace doris::snii::query

namespace doris::segment_v2 {

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

class SniiIndexReader final : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(SniiIndexReader);

public:
#ifdef BE_TEST
    using SingleFlightFollowerJoinedObserver = void (*)(void*) noexcept;
    using SingleFlightLeaderBeforeComputeObserver = void (*)(void*) noexcept;
    using SearcherOpenObserver = void (*)(void*) noexcept;
#endif

    // The segment row count bounds row ids emitted by the count shortcut.
    // The array flag describes how the indexed column was written.
    SniiIndexReader(const TabletIndex* index_meta,
                    const std::shared_ptr<IndexFileReader>& index_file_reader,
                    InvertedIndexReaderType reader_type, uint64_t rows_of_segment,
                    bool column_is_array)
            : InvertedIndexReader(index_meta, index_file_reader),
              _reader_type(reader_type),
              _rows_of_segment(rows_of_segment),
              _column_is_array(column_is_array) {}

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
    // A policy that cannot be resolved names no index a gram query could use; an analyzed query
    // reports that failure where it matters.
    bool is_gram_family() const override {
        std::optional<segment_v2::gram::GramScheme> scheme;
        return _current_gram_scheme(nullptr, &scheme).ok() && scheme.has_value();
    }

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
    Status _current_gram_scheme(const InvertedIndexAnalyzerCtx* analyzer_ctx,
                                std::optional<segment_v2::gram::GramScheme>* out) const;
    Status _parse_query_terms(const IndexQueryContextPtr& context, std::string search_str,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx,
                              InvertedIndexQueryInfo* query_info);
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
    // Returns a count-sized bitmap from a single term's df when SegmentIterator requests count-only evaluation. Fabricated IDs avoid null rows; all other query shapes fall back to normal posting decode.
    Status _try_count_only_fastpath(
            const IndexQueryContextPtr& context, InvertedIndexQueryType query_type,
            const InvertedIndexQueryInfo& query_info, const std::vector<std::string>& terms,
            bool* handled, std::shared_ptr<roaring::Roaring>* out,
            const ::doris::snii::reader::LogicalIndexReader* preopened_reader = nullptr);

    InvertedIndexReaderType _reader_type;
    // Row count of the segment this reader belongs to, straight from
    // Segment::_num_rows. The count-only fast path bounds the index's own
    // document domain against it; see _try_count_only_fastpath.
    uint64_t _rows_of_segment = 0;
    // True when the indexed column is an ARRAY. Disqualifies the count-only fast
    // path on a segment that has nulls; see _try_count_only_fastpath.
    bool _column_is_array = false;
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
