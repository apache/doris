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

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iterator>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/common/query_info.h"
#include "storage/index/inverted/reader/predicate_collector.h"
#include "storage/olap_common.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {

namespace index_query {
class IndexSource;
struct IndexScoringStats;
} // namespace index_query

namespace io {
class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;
struct IOContext;
} // namespace io

struct RowSetSplits;

class DeleteBitmap;

class Rowset;
class RowsetSegmentView;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

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

// The `limit` smallest terms, in dictionary order, that a prefix expanded to in the segments seen
// so far: the terms an index-wide expansion of Lucene would take. A term only enters or stays
// while fewer than `limit` smaller terms are known, and one dropped never returns, whatever
// order the segments come in.
//
// That is what keeps the document frequencies whole. A term of the final set has fewer than
// `limit` smaller terms in every segment holding it, so each of those segments expanded to it
// and counted its documents. With a limit of 1, segment A holding {rose} and segment B holding
// {red, rose} leave {red}: B's expansion is red alone, but rose is not scored, so its document
// frequency, which only A counted, is not used.
class PrefixExpansionSet {
public:
    explicit PrefixExpansionSet(int32_t limit) : _limit(limit) { DORIS_CHECK_GT(limit, 0); }

    void insert(const std::wstring& term) {
        _terms.insert(term);
        if (_terms.size() > static_cast<size_t>(_limit)) {
            _terms.erase(std::prev(_terms.end()));
        }
    }

    int32_t limit() const { return _limit; }
    const std::set<std::wstring>& terms() const { return _terms; }

private:
    int32_t _limit;
    std::set<std::wstring> _terms;
};
// The terms each prefix expanded to, by prefix.
using FieldPrefixExpansions = std::unordered_map<std::wstring, PrefixExpansionSet>;

class CollectionStatistics {
public:
    explicit CollectionStatistics(PredicateCollectorPtr search_collector = nullptr)
            : _search_collector(std::move(search_collector)) {}
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
    Status collect_full_collection(RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
                                   const TabletSchemaSPtr& tablet_schema,
                                   const VExprContextSPtrs& common_expr_ctxs_push_down,
                                   io::IOContext* io_ctx,
                                   const MowHiddenRows* hidden_rows = nullptr);

    MOCK_FUNCTION float get_or_calculate_idf(const std::wstring& lucene_col_name,
                                             const std::wstring& term);
    MOCK_FUNCTION float get_or_calculate_avg_dl(const std::wstring& lucene_col_name);

    // The terms the collected `prefix` expanded to in any segment of the tablet and that some
    // returned row holds, in dictionary order; their idfs are in get_or_calculate_idf. None for a
    // prefix that was not collected, as for a SEARCH predicate.
    MOCK_FUNCTION std::vector<std::wstring> get_prefix_expansions(
            const std::wstring& lucene_col_name, const std::wstring& prefix);

    // Replaces the collected counters with those of `other`. The derived idf/avgdl caches are
    // reset rather than copied, so the copy can be used by another thread without sharing them.
    void copy_collected_from(const CollectionStatistics& other);

private:
    // The terms each collected prefix expanded to, by field and prefix.
    using PrefixExpansions = std::unordered_map<std::wstring, FieldPrefixExpansions>;

    struct SniiScoringSegmentAccumulator {
        uint64_t doc_count = 0;
        std::unordered_map<std::wstring, uint64_t> token_counts;
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>> term_doc_freqs;
        PrefixExpansions prefix_expansions;
    };

    Status extract_collect_info(RuntimeState* state,
                                const VExprContextSPtrs& common_expr_ctxs_push_down,
                                const TabletSchemaSPtr& tablet_schema,
                                CollectInfoMap* collect_infos);
    Status process_segment(const RowsetSharedPtr& rowset, const RowsetSegmentView& seg,
                           const TabletSchema* tablet_schema, const CollectInfoMap& collect_infos,
                           const MowHiddenRows* hidden_rows, io::IOContext* io_ctx);
    Status admit_snii_scoring_segment(const std::wstring& field_name, uint64_t index_doc_count,
                                      uint64_t sum_total_term_freq, bool has_positions,
                                      bool has_norms,
                                      SniiScoringSegmentAccumulator* segment_accumulator,
                                      bool is_analyzed = true, bool single_term_normalizer = false);
    void commit_snii_scoring_segment(SniiScoringSegmentAccumulator&& segment_accumulator);
    void clear();

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
    PrefixExpansions _prefix_expansions;

    std::unordered_map<std::wstring, float> _avg_dl_by_col;
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, float>> _idf_by_col_term;
    PredicateCollectorPtr _search_collector;

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
    TabletCollectionStatistics(std::vector<RowsetSharedPtr> rowsets,
                               PredicateCollectorPtr search_collector)
            : _rowsets(std::move(rowsets)), _statistics(std::move(search_collector)) {}

    enum class Turn {
        COLLECTED, // the statistics are ready to copy
        COLLECT,   // the caller collects them
        WAIT,      // another scanner collects them; the caller is woken up once they are ready
    };

    // Called by a scanner of the tablet before it prepares. While another scanner collects the
    // statistics, `wake_up` is kept and called once they are collected, so the waiting scanner
    // can give its scanner thread back instead of blocking it.
    Turn take_turn(std::function<void()> wake_up);

    // Collects the statistics on the first call, then copies them into `statistics`.
    // Later callers receive the result or error of that collection. A caller that did not get
    // COLLECTED or COLLECT from take_turn() may block until the collection finishes.
    Status copy_to(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx,
                   const MowHiddenRows* hidden_rows, CollectionStatistics* statistics);

    // Called by the scanner that got COLLECT when it stops before collecting, so a waiting
    // scanner collects instead.
    void give_up_turn();

private:
    enum class State { NOT_STARTED, CLAIMED, COLLECTING, COLLECTED };

    // Takes the wake-up callbacks out under _mutex, so they run after it is released.
    std::vector<std::function<void()>> _take_waiters();

    std::mutex _mutex;
    std::condition_variable _collected_cv;
    State _state = State::NOT_STARTED;
    std::vector<std::function<void()>> _waiters;
    Status _status;
    std::vector<RowsetSharedPtr> _rowsets;
    CollectionStatistics _statistics;
};
using TabletCollectionStatisticsPtr = std::shared_ptr<TabletCollectionStatistics>;

// Implementation details of the SNII scoring-segment admission math, surfaced so
// collection_statistics_test.cpp can exercise them without compiling the .cpp a
// second time via #include.
namespace collection_statistics_detail {

struct SniiScoringSegmentStats {
    uint64_t doc_count = 0;
    uint64_t token_count = 0;
};

// Analyzed SNII indexes need positions and norms. Keywords and lowercase-only
// normalizers can use unit frequency and norm when those sections are absent.
Result<SniiScoringSegmentStats> resolve_snii_scoring_segment(uint64_t index_doc_count,
                                                             uint64_t sum_total_term_freq,
                                                             bool has_positions, bool has_norms,
                                                             bool is_analyzed = true,
                                                             bool single_term_normalizer = false);

void add_term_doc_frequency(
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>*
                logical_frequencies,
        const std::wstring& field, const std::wstring& logical_term, uint64_t doc_frequency);

// The sum of the lengths the norms of a segment's documents other than the ascending `null_docs`
// stand for. A NULL row is a document without tokens, though its norm stands for one. Rows that are
// indexed without tokens but are not NULL keep a norm that cannot be told from a one-token row's:
// an analyzed empty string on both formats, and on SNII any text that yields no token.
Status sum_non_null_norm_lengths(index_query::IndexSource& source,
                                 std::span<const uint32_t> null_docs, uint64_t* out);

// Turns a segment's scoring statistics into those of its documents other than the ascending
// `hidden_docs`: their count, and the sum of the lengths the norms of the non-NULL ones stand
// for, or their count when the index has no norms. `non_null_length_sum` is the segment's
// sum_non_null_norm_lengths(), and `null_docs` its NULL rows; both are unused without norms.
Status exclude_hidden_docs(index_query::IndexSource& source, std::span<const uint32_t> hidden_docs,
                           const roaring::Roaring& null_docs, uint64_t non_null_length_sum,
                           index_query::IndexScoringStats* stats);

// Sums the lengths the norms of ascending rows of a segment's index stand for.
using SumNormLengthsOf = std::function<Status(std::span<const uint32_t> docs, uint64_t* out)>;

// exclude_hidden_docs, the lengths of the hidden non-NULL rows summed by `sum_norm_lengths_of`.
Status exclude_hidden_docs(std::span<const uint32_t> hidden_docs, const roaring::Roaring& null_docs,
                           uint64_t non_null_length_sum,
                           const SumNormLengthsOf& sum_norm_lengths_of,
                           index_query::IndexScoringStats* stats);

// The terms whose document frequencies a segment adds for `collect_info`: its scoring terms, then
// each term its prefixes expand to in `source`, so that a phrase prefix is scored on the same
// tablet statistics as the other terms. A term met twice is listed once, as its documents count
// once. The terms each prefix expands to in this segment are added to `expansions`, which keeps
// the smallest ones of the segments seen (PrefixExpansionSet).
//
// Only terms that a returned row holds are expanded to, as `visible_doc_freq` tells: the document
// frequency of a term in this segment without its hidden rows. A term held by hidden rows alone
// must not take one of the `max_expansions` places, or the scores of the same visible rows would
// change when a compaction drops the hidden rows. With a limit of 1, and `ra` held by hidden rows
// and `rb` by visible ones, the prefix "r" expands to rb before and after the compaction.
// The dictionary is therefore read again, with twice the limit, while it still has terms and the
// terms found hold fewer than `max_expansions` visible ones. A term whose bytes are not valid UTF-8
// has no wide-string key of its own and is skipped like a hidden one.
//
// Example: "red r" collects red and, in a segment holding red, rose and run, the terms red (already
// listed), rose and run, and records {red, rose, run} for the prefix "r".
using VisibleDocFreq = std::function<Status(const std::string& term, uint64_t* doc_freq)>;
Status segment_scoring_terms(index_query::IndexSource& source, const CollectInfo& collect_info,
                             const VisibleDocFreq& visible_doc_freq,
                             FieldPrefixExpansions* expansions, std::vector<std::string>* terms);

// Takes from a term's document frequency the documents of `hidden_docs` that hold it.
Status exclude_hidden_term_docs(index_query::IndexSource& source, std::string_view term,
                                std::span<const uint32_t> hidden_docs, uint64_t* doc_frequency);

// The document frequency of a term in `source` without the rows of `hidden_docs`, computed once
// per term for the lifetime of the returned function. With hidden rows every term costs an
// intersection of its postings with them, and the terms a prefix expands to are asked for twice:
// to choose the ones to keep, and to add up the frequencies of the ones kept.
// `source` and `hidden_docs` must outlive the function.
VisibleDocFreq cached_visible_doc_freq(index_query::IndexSource& source,
                                       std::span<const uint32_t> hidden_docs);

} // namespace collection_statistics_detail

} // namespace doris
