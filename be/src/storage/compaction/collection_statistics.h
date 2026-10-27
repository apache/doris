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
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/be_mock_util.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/olap_common.h"
#include "storage/predicate_collector.h"

namespace lucene::index {
class IndexReader;
} // namespace lucene::index

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
#include "common/compile_check_begin.h"

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
struct IOContext;
} // namespace io

struct RowSetSplits;

class DeleteBitmap;

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

namespace segment_v2 {
class TermIterator;
} // namespace segment_v2

// The rows of a merge-on-write tablet that its segments still hold but a scan at `version` does
// not return: rows a later load overwrote or deleted, which the delete bitmap marks, and rows
// written with the delete sign, which queries filter out. Compaction removes both, on each
// replica at its own time, so statistics counting them would differ between the replicas of a
// tablet, and so would every BM25 score. Rows written with the delete sign are left out even when
// the query returns them (`skip_delete_sign`, `show_hidden_columns`), and rows deleted by a delete
// predicate (`enable_mow_light_delete`) are still counted, as the delete bitmap does not mark them.
//
// The cache flags carry the query's choices to the segment and column reads of the delete sign;
// the file cache choices come with the I/O context.
struct MowHiddenRows {
    std::shared_ptr<DeleteBitmap> delete_bitmap;
    int64_t version = -1;
    bool use_segment_cache = true;
    bool use_page_cache = true;
};

class CollectionStatistics {
public:
    CollectionStatistics() = default;
    virtual ~CollectionStatistics() = default;

    // With `hidden_rows`, the statistics describe only the rows a merge-on-write scan returns,
    // so every replica of a tablet scores alike whatever it has compacted: the hidden rows are
    // left out of the document count and the term document frequencies, and the token count is
    // the sum of the document lengths the norms of the returned non-NULL rows stand for, or the
    // returned row count for an index without norms.
    Status collect(RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
                   const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   const MowHiddenRows* hidden_rows = nullptr);

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

    // Replaces the collected counters with those of `other`. The derived idf/avgdl caches are
    // reset rather than copied, so the copy can be used by another thread without sharing them.
    void copy_collected_from(const CollectionStatistics& other);

private:
    Status extract_collect_info(RuntimeState* state,
                                const VExprContextSPtrs& common_expr_ctxs_push_down,
                                const TabletSchemaSPtr& tablet_schema,
                                CollectInfoMap* collect_infos);
    Status process_segment(const RowsetSharedPtr& rowset, int32_t seg_id,
                           const TabletSchema* tablet_schema, const CollectInfoMap& collect_infos,
                           const MowHiddenRows* hidden_rows, io::IOContext* io_ctx);

    uint64_t get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                      const std::wstring& term);
    uint64_t get_total_term_cnt_by_col(const std::wstring& lucene_col_name);
    uint64_t get_doc_num() const;

    uint64_t _total_num_docs = 0;
    // Whether a collected segment held rows the scan does not return, so that no documents can
    // mean every row is hidden.
    bool _counted_hidden_docs = false;
    std::unordered_map<std::wstring, uint64_t> _total_num_tokens;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>> _term_doc_freqs;

    std::unordered_map<std::wstring, float> _avg_dl_by_col;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, float>> _idf_by_col_term;

    MOCK_DEFINE(friend class BM25SimilarityTest;)
    MOCK_DEFINE(friend class CollectionStatisticsTest;)
    MOCK_DEFINE(friend class BooleanQueryTest;)
    MOCK_DEFINE(friend class OccurBooleanQueryTest;)
};
using CollectionStatisticsPtr = std::shared_ptr<CollectionStatistics>;

// BM25 statistics of a whole tablet, shared by every scanner the tablet is split into.
//
// A parallel scan divides a tablet's rowsets among several scanners. If each scanner
// collected statistics over only its own rowsets, rows with identical content would score
// differently depending on which scanner read them, and the split itself depends on the
// tablet's row count and the number of cores. Collecting once over the entire read source
// gives every row of the tablet the same idf and avgdl.
class TabletCollectionStatistics {
public:
    explicit TabletCollectionStatistics(std::vector<RowSetSplits> rs_splits);
    ~TabletCollectionStatistics();

    // Collects the statistics on the first call, then copies them into `statistics`.
    // Later callers wait for the first collection and receive its result or error.
    Status copy_to(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   const MowHiddenRows* hidden_rows, CollectionStatistics* statistics);

private:
    std::mutex _mutex;
    bool _collected = false;
    Status _status;
    std::vector<RowSetSplits> _rs_splits;
    CollectionStatistics _statistics;
};
using TabletCollectionStatisticsPtr = std::shared_ptr<TabletCollectionStatistics>;

// Implementation details of the visible-row statistics, surfaced for collection_statistics_test.
namespace collection_statistics_detail {

// How many of the ascending `docs` the term's postings list: leaping over the postings between
// them when the docs are much rarer than the postings, otherwise decoding every posting once.
uint64_t count_listed_docs(const segment_v2::TermIterator& postings,
                           std::span<const uint32_t> docs);

// The sum of the lengths the norms of `field` stand for, of every document of `reader`, read one
// CLucene segment at a time into a buffer of the statistics' own.
Status sum_norm_lengths(lucene::index::IndexReader* reader, const std::wstring& field,
                        uint64_t* out);

// The sum of the lengths the norms of `field` stand for, of the ascending `docs` of `reader`.
Status sum_norm_lengths_of(lucene::index::IndexReader* reader, const std::wstring& field,
                           std::span<const uint32_t> docs, uint64_t* out);

// The sum of the lengths the norms of a segment's documents other than the ascending `null_docs`
// stand for. A NULL row is a document without tokens, though its norm stands for one. Rows that are
// indexed without tokens but are not NULL keep a norm that cannot be told from a one-token row's:
// an analyzed empty string.
Status sum_non_null_norm_lengths(lucene::index::IndexReader* reader, const std::wstring& field,
                                 std::span<const uint32_t> null_docs, uint64_t* out);

// The scoring statistics of one field of a segment's index.
struct SegmentScoringStats {
    uint64_t doc_count = 0;
    uint64_t token_count = 0;
    bool has_norms = false;
};

// Sums the lengths the norms of ascending rows of a segment's index stand for.
using SumNormLengthsOf = std::function<Status(std::span<const uint32_t> docs, uint64_t* out)>;

// Turns a segment's scoring statistics into those of its documents other than the ascending
// `hidden_docs`: their count, and the sum of the lengths the norms of the non-NULL ones stand
// for, or their count when the index has no norms. `non_null_length_sum` is the segment's
// sum_non_null_norm_lengths(), and `null_docs` its NULL rows; both are unused without norms. The
// lengths of the hidden non-NULL rows are summed by `sum_norm_lengths_of`.
Status exclude_hidden_docs(std::span<const uint32_t> hidden_docs, const roaring::Roaring& null_docs,
                           uint64_t non_null_length_sum,
                           const SumNormLengthsOf& sum_norm_lengths_of, SegmentScoringStats* stats);

// Takes from a term's document frequency the documents of `hidden_docs` its postings list.
Status exclude_hidden_term_docs(const segment_v2::TermIterator& postings,
                                std::span<const uint32_t> hidden_docs, uint64_t* doc_frequency);

} // namespace collection_statistics_detail

#include "common/compile_check_end.h"
} // namespace doris
