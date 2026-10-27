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

#include "storage/compaction/collection_statistics.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include <CLucene/index/MultiReader.h>
#include <CLucene/index/_MultiSegmentReader.h>
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <algorithm>
#include <array>
#include <iterator>
#include <set>
#include <sstream>
#include <stack>

#include "common/exception.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_factory.hpp"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/inverted/util/term_iterator.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/iterators.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {
namespace {

Result<std::shared_ptr<lucene::search::IndexSearcher>> open_optional_clucene_stats_searcher(
        const segment_v2::IndexFileReader& file_reader, const TabletIndex* index,
        const io::IOContext* io_ctx) {
    auto compound = file_reader.open(index, io_ctx);
    if (!compound.has_value()) {
        // Variant rowsets can contain only `a` or only `b`. Missing `a` must not discard
        // this segment's available `b` statistics when scoring SEARCH('a:hit OR b:hit').
        const auto& status = compound.error();
        if (status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() ||
            status.is<ErrorCode::INVERTED_INDEX_BYPASS>()) {
            return std::shared_ptr<lucene::search::IndexSearcher> {};
        }
        return ResultError(status);
    }
    lucene::index::IndexReader* reader = nullptr;
    try {
        reader = lucene::index::IndexReader::open(compound.value().get());
    } catch (const CLuceneError& e) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occurred opening the index for BM25 statistics: {}", e.what()));
    }
    // As the query path does once the index meta is read: the searcher may be cached and read
    // by later queries, which must not use this query's I/O context, and later reads are of
    // index data, not meta.
    auto* stream = compound.value()->getDorisIndexInput();
    stream->setIoContext(nullptr);
    stream->setIndexFile(false);
    return std::make_shared<lucene::search::IndexSearcher>(reader, true);
}

} // namespace

#include "common/compile_check_begin.h"

namespace collection_statistics_detail {

namespace {

// How many postings a scan decodes in the time a leap costs.
constexpr uint64_t kPostingsPerSeek = 32;

// Leaps to each of the ascending `docs` in turn.
uint64_t count_listed_docs_by_leaping(const segment_v2::TermIterator& postings,
                                      std::span<const uint32_t> docs) {
    uint64_t listed = 0;
    int64_t doc = -1;
    size_t next = 0;
    while (next < docs.size()) {
        if (doc < docs[next]) {
            // skipTo always moves past the current document, so only leap forward.
            doc = postings.advance(cast_set<int32_t>(docs[next]));
            if (doc == INT_MAX) {
                break;
            }
        }
        if (doc == docs[next]) {
            ++listed;
            ++next;
        } else {
            next = static_cast<size_t>(
                    std::lower_bound(docs.begin() + next, docs.end(), static_cast<uint32_t>(doc)) -
                    docs.begin());
        }
    }
    return listed;
}

// Decodes every posting once and walks the ascending `docs` alongside.
uint64_t count_listed_docs_by_scan(const segment_v2::TermIterator& postings,
                                   std::span<const uint32_t> docs) {
    uint64_t listed = 0;
    size_t next = 0;
    DocRange range;
    // A block without documents only comes from a corrupt image and ends the term.
    while (next < docs.size() && postings.read_range(&range) && range.doc_many_size_ != 0) {
        if (range.type_ == DocRangeType::kRange) {
            const auto begin =
                    std::lower_bound(docs.begin() + next, docs.end(), range.doc_range.first);
            const auto end = std::lower_bound(begin, docs.end(), range.doc_range.second);
            listed += static_cast<uint64_t>(end - begin);
            next = static_cast<size_t>(end - docs.begin());
            continue;
        }
        for (uint32_t i = 0; i < range.doc_many_size_; ++i) {
            const uint32_t doc = (*range.doc_many)[i];
            while (next < docs.size() && docs[next] < doc) {
                ++next;
            }
            if (next == docs.size()) {
                break;
            }
            if (docs[next] == doc) {
                ++listed;
                ++next;
            }
        }
    }
    return listed;
}

// The length each encoded norm stands for, as BM25 scoring decodes it.
const std::array<uint64_t, 256>& norm_length_table() {
    static const std::array<uint64_t, 256> table = [] {
        std::array<uint64_t, 256> lengths {};
        for (size_t norm = 0; norm < lengths.size(); ++norm) {
            lengths[norm] = static_cast<uint64_t>(static_cast<float>(
                    segment_v2::BM25Similarity::byte4_to_int(static_cast<uint8_t>(norm))));
        }
        return lengths;
    }();
    return table;
}

// A CLucene segment of a reader and the range of the reader's documents it holds.
struct ReaderPartition {
    lucene::index::IndexReader* reader;
    uint32_t begin;
    uint32_t end;
};

const lucene::util::ArrayBase<lucene::index::IndexReader*>* sub_readers(
        lucene::index::IndexReader* reader) {
    if (auto* multi_segment = dynamic_cast<lucene::index::MultiSegmentReader*>(reader)) {
        return multi_segment->getSubReaders();
    }
    if (auto* multi = dynamic_cast<lucene::index::MultiReader*>(reader)) {
        return multi->getSubReaders();
    }
    return nullptr;
}

void append_partitions(lucene::index::IndexReader* reader, uint32_t begin,
                       std::vector<ReaderPartition>* partitions) {
    if (const auto* subs = sub_readers(reader); subs != nullptr && subs->length != 0) {
        for (size_t i = 0; i < subs->length; ++i) {
            auto* child = (*subs)[i];
            append_partitions(child, begin, partitions);
            begin += static_cast<uint32_t>(child->maxDoc());
        }
    } else {
        partitions->push_back({reader, begin, begin + static_cast<uint32_t>(reader->maxDoc())});
    }
}

// The statistics read a field's norms into a buffer of their own, freed after use: asking a reader
// for its norms makes it keep them, uncharged to the searcher cache, for every segment the
// statistics visit, scored or not. Reading per segment also avoids the copy of all segments' norms
// a reader of several segments would keep. A segment without the field's norms reads as the
// default norm, as that reader would.
std::vector<ReaderPartition> norm_partitions(lucene::index::IndexReader* reader) {
    std::vector<ReaderPartition> partitions;
    append_partitions(reader, 0, &partitions);
    return partitions;
}

void read_norms(const ReaderPartition& partition, const std::wstring& field,
                DorisVector<uint8_t>* norms) {
    norms->assign(partition.end - partition.begin, 0);
    partition.reader->norms(field.c_str(), norms->data());
}

Status clucene_norms_error(const char* what) {
    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "CLuceneError occurred reading norms for BM25 statistics: {}", what);
}

} // namespace

uint64_t count_listed_docs(const segment_v2::TermIterator& postings,
                           std::span<const uint32_t> docs) {
    const auto doc_freq = postings.doc_freq();
    if (docs.empty() || doc_freq <= 0) {
        return 0;
    }
    // Leaping costs a seek per wanted document, scanning a decode per posting: leap only over
    // documents much rarer than the postings.
    if (docs.size() * kPostingsPerSeek >= static_cast<uint64_t>(doc_freq)) {
        return count_listed_docs_by_scan(postings, docs);
    }
    return count_listed_docs_by_leaping(postings, docs);
}

Status sum_norm_lengths(lucene::index::IndexReader* reader, const std::wstring& field,
                        uint64_t* out) {
    *out = 0;
    try {
        if (!reader->hasNorms(field.c_str())) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "field {} has no norms", StringHelper::to_string(field));
        }
        const auto& lengths = norm_length_table();
        DorisVector<uint8_t> norms;
        for (const ReaderPartition& partition : norm_partitions(reader)) {
            read_norms(partition, field, &norms);
            for (const uint8_t norm : norms) {
                *out += lengths[norm];
            }
        }
    } catch (CLuceneError& e) {
        return clucene_norms_error(e.what());
    }
    return Status::OK();
}

Status sum_norm_lengths_of(lucene::index::IndexReader* reader, const std::wstring& field,
                           std::span<const uint32_t> docs, uint64_t* out) {
    *out = 0;
    if (docs.empty()) {
        return Status::OK();
    }
    try {
        if (!reader->hasNorms(field.c_str())) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "field {} has no norms", StringHelper::to_string(field));
        }
        const auto& lengths = norm_length_table();
        const auto count = static_cast<uint32_t>(reader->maxDoc());
        if (docs.back() >= count) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                    "document {} is beyond the {} documents of the index", docs.back(), count);
        }
        DorisVector<uint8_t> norms;
        // `docs` ascend, so each partition's documents are contiguous in them.
        size_t next = 0;
        for (const ReaderPartition& partition : norm_partitions(reader)) {
            if (next == docs.size()) {
                break;
            }
            if (docs[next] >= partition.end) {
                continue;
            }
            read_norms(partition, field, &norms);
            for (; next < docs.size() && docs[next] < partition.end; ++next) {
                *out += lengths[norms[docs[next] - partition.begin]];
            }
        }
    } catch (CLuceneError& e) {
        return clucene_norms_error(e.what());
    }
    return Status::OK();
}

Status sum_non_null_norm_lengths(lucene::index::IndexReader* reader, const std::wstring& field,
                                 std::span<const uint32_t> null_docs, uint64_t* out) {
    const auto doc_count = static_cast<uint32_t>(reader->maxDoc());
    if (!null_docs.empty() && null_docs.back() >= doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "NULL row {} is beyond the {} documents of the index", null_docs.back(), doc_count);
    }
    uint64_t all_lengths = 0;
    RETURN_IF_ERROR(sum_norm_lengths(reader, field, &all_lengths));
    uint64_t null_lengths = 0;
    RETURN_IF_ERROR(sum_norm_lengths_of(reader, field, null_docs, &null_lengths));
    DORIS_CHECK(null_lengths <= all_lengths);
    *out = all_lengths - null_lengths;
    return Status::OK();
}

Status exclude_hidden_docs(std::span<const uint32_t> hidden_docs, const roaring::Roaring& null_docs,
                           uint64_t non_null_length_sum,
                           const SumNormLengthsOf& sum_norm_lengths_of,
                           SegmentScoringStats* stats) {
    DORIS_CHECK(stats != nullptr);
    if (!hidden_docs.empty() && hidden_docs.back() >= stats->doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "hidden row {} is beyond the {} documents of the index", hidden_docs.back(),
                stats->doc_count);
    }
    stats->doc_count -= hidden_docs.size();
    if (!stats->has_norms) {
        // Without norms every document stands for one token.
        stats->token_count = stats->doc_count;
        return Status::OK();
    }
    // The exact token count of the hidden rows is not stored anywhere, and norms hold lengths
    // only approximately, so count the lengths the norms of the returned rows stand for. A
    // replica that has compacted the hidden rows away counts the same rows with the same norms.
    // Most segments have no NULL rows, and then the hidden rows are summed as they are.
    DorisVector<uint32_t> hidden_non_null_docs;
    std::span<const uint32_t> summed_docs = hidden_docs;
    if (!null_docs.isEmpty()) {
        std::ranges::copy_if(hidden_docs, std::back_inserter(hidden_non_null_docs),
                             [&null_docs](uint32_t doc) { return !null_docs.contains(doc); });
        summed_docs = hidden_non_null_docs;
    }
    uint64_t hidden_lengths = 0;
    if (!summed_docs.empty()) {
        RETURN_IF_ERROR(sum_norm_lengths_of(summed_docs, &hidden_lengths));
    }
    DORIS_CHECK(hidden_lengths <= non_null_length_sum);
    stats->token_count = non_null_length_sum - hidden_lengths;
    return Status::OK();
}

Status exclude_hidden_term_docs(const segment_v2::TermIterator& postings,
                                std::span<const uint32_t> hidden_docs, uint64_t* doc_frequency) {
    DORIS_CHECK(doc_frequency != nullptr);
    const uint64_t hidden = count_listed_docs(postings, hidden_docs);
    if (hidden > *doc_frequency) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "term lists {} hidden rows but only {} documents", hidden, *doc_frequency);
    }
    *doc_frequency -= hidden;
    return Status::OK();
}

} // namespace collection_statistics_detail

namespace {

// The rows of a segment that a scan with `hidden_rows` does not return, ascending.
Status collect_hidden_docs(const RowsetSharedPtr& rowset, int32_t seg_id,
                           const TabletSchema& tablet_schema, const MowHiddenRows& hidden_rows,
                           const io::IOContext* io_ctx, DorisVector<uint32_t>* hidden_docs) {
    DORIS_CHECK(hidden_rows.delete_bitmap != nullptr);
    DORIS_CHECK(hidden_rows.version >= 0);
    roaring::Roaring hidden = *hidden_rows.delete_bitmap->get_agg(
            {rowset->rowset_id(), cast_set<uint32_t>(seg_id), hidden_rows.version});

    // A row written with the delete sign stays readable until base compaction removes it, while
    // queries filter it out by predicate.
    if (tablet_schema.delete_sign_idx() >= 0) {
        OlapReaderStatistics stats;
        SegmentCacheHandle cache_handle;
        // The delete sign is column data, read as the query reads its columns.
        io::IOContext data_io_ctx;
        if (io_ctx != nullptr) {
            data_io_ctx = *io_ctx;
        }
        data_io_ctx.is_inverted_index = false;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                std::static_pointer_cast<BetaRowset>(rowset), seg_id, &cache_handle,
                hidden_rows.use_segment_cache, false, &stats, &data_io_ctx));
        DORIS_CHECK(cache_handle.get_segments().size() == 1);
        const auto& segment = cache_handle.get_segments()[0];
        const TabletColumn& delete_sign = tablet_schema.column(tablet_schema.delete_sign_idx());

        bool may_hold_deletes = true;
        std::shared_ptr<segment_v2::ColumnReader> column_reader;
        auto status = segment->get_column_reader(delete_sign, &column_reader, &stats, &data_io_ctx);
        if (status.is<ErrorCode::NOT_FOUND>()) {
            // A segment written before the column existed reads its default, 0.
            may_hold_deletes = false;
        } else {
            RETURN_IF_ERROR(status);
            DORIS_CHECK(column_reader != nullptr);
            if (column_reader->has_zone_map()) {
                segment_v2::ZoneMap zone_map;
                RETURN_IF_ERROR(column_reader->get_segment_zone_map(&zone_map));
                // Any sign other than 0 deletes the row. Bounds the zone map cannot vouch for say
                // nothing.
                may_hold_deletes =
                        zone_map.pass_all ||
                        (zone_map.has_not_null && (zone_map.min_value.get<TYPE_TINYINT>() != 0 ||
                                                   zone_map.max_value.get<TYPE_TINYINT>() != 0));
            }
        }
        if (may_hold_deletes) {
            StorageReadOptions read_options;
            read_options.stats = &stats;
            read_options.io_ctx = data_io_ctx;
            std::unique_ptr<segment_v2::ColumnIterator> iterator;
            RETURN_IF_ERROR(segment->new_column_iterator(delete_sign, &iterator, &read_options));
            segment_v2::ColumnIteratorOptions iterator_options {
                    .use_page_cache =
                            hidden_rows.use_page_cache && !config::disable_storage_page_cache,
                    .file_reader = segment->file_reader().get(),
                    .stats = &stats,
                    .io_ctx = read_options.io_ctx,
            };
            RETURN_IF_ERROR(iterator->init(iterator_options));
            RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
            auto data_type = DataTypeFactory::instance().create_data_type(delete_sign);
            constexpr size_t kBatch = 4096;
            for (uint32_t begin = 0; begin < segment->num_rows();) {
                size_t rows = std::min<size_t>(kBatch, segment->num_rows() - begin);
                MutableColumnPtr column = data_type->create_column();
                bool has_null = false;
                RETURN_IF_ERROR(iterator->next_batch(&rows, column, &has_null));
                if (rows == 0) {
                    return Status::Corruption("segment {} ended after {} of its {} rows",
                                              segment->id(), begin, segment->num_rows());
                }
                const auto& signs = assert_cast<const ColumnInt8&>(*column).get_data();
                for (size_t i = 0; i < rows; ++i) {
                    if (signs[i] != 0) {
                        hidden.add(begin + cast_set<uint32_t>(i));
                    }
                }
                begin += cast_set<uint32_t>(rows);
            }
        }
    }

    hidden_docs->resize(hidden.cardinality());
    hidden.toUint32Array(hidden_docs->data());
    return Status::OK();
}

} // namespace

Status CollectionStatistics::collect(RuntimeState* state,
                                     const std::vector<RowSetSplits>& rs_splits,
                                     const TabletSchemaSPtr& tablet_schema,
                                     const VExprContextSPtrs& common_expr_ctxs_push_down,
                                     io::IOContext* io_ctx, const MowHiddenRows* hidden_rows) {
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));
    if (collect_infos.empty()) {
        LOG(WARNING) << "Index statistics collection: no collect info extracted.";
        return Status::OK();
    }

    for (const auto& rs_split : rs_splits) {
        const auto& rs_reader = rs_split.rs_reader;
        auto rowset = rs_reader->rowset();
        auto num_segments = rowset->num_segments();
        for (int32_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto status = process_segment(rowset, seg_id, tablet_schema.get(), collect_infos,
                                          hidden_rows, io_ctx);
            if (!status.ok()) {
                if (status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                    status.code() == ErrorCode::INVERTED_INDEX_BYPASS) {
                    LOG(ERROR) << "Index statistics collection failed: " << status.to_string();
                } else {
                    return status;
                }
            }
        }
    }

    // Build a single-line log with query_id, tablet_ids, and per-field term statistics
    if (VLOG_IS_ON(1)) {
        std::set<int64_t> tablet_ids;
        for (const auto& rs_split : rs_splits) {
            if (rs_split.rs_reader && rs_split.rs_reader->rowset()) {
                tablet_ids.insert(rs_split.rs_reader->rowset()->rowset_meta()->tablet_id());
            }
        }

        std::ostringstream oss;
        oss << "CollectionStatistics: query_id=" << print_id(state->query_id());

        oss << ", tablet_ids=[";
        bool first_tablet = true;
        for (int64_t tid : tablet_ids) {
            if (!first_tablet) oss << ",";
            oss << tid;
            first_tablet = false;
        }
        oss << "]";

        oss << ", total_num_docs=" << _total_num_docs;

        for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
            oss << ", {field=" << StringHelper::to_string(ws_field_name)
                << ", num_tokens=" << num_tokens << ", terms=[";

            bool first_term = true;
            for (const auto& [term, doc_freq] : _term_doc_freqs.at(ws_field_name)) {
                if (!first_term) oss << ", ";
                oss << "(" << StringHelper::to_string(term) << ":" << doc_freq << ")";
                first_term = false;
            }
            oss << "]}";
        }

        VLOG(1) << oss.str();
    }

    return Status::OK();
}

Status CollectionStatistics::extract_collect_info(
        RuntimeState* state, const VExprContextSPtrs& common_expr_ctxs_push_down,
        const TabletSchemaSPtr& tablet_schema, CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    std::unordered_map<TExprNodeType::type, PredicateCollectorPtr> collectors;
    collectors[TExprNodeType::MATCH_PRED] = std::make_unique<MatchPredicateCollector>();
    collectors[TExprNodeType::SEARCH_EXPR] = std::make_unique<SearchPredicateCollector>();

    for (const auto& root_expr_ctx : common_expr_ctxs_push_down) {
        const auto& root_expr = root_expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::stack<VExprSPtr> stack;
        stack.emplace(root_expr);

        while (!stack.empty()) {
            auto expr = stack.top();
            stack.pop();

            if (!expr) {
                continue;
            }

            auto collector_it = collectors.find(expr->node_type());
            if (collector_it != collectors.end()) {
                RETURN_IF_ERROR(
                        collector_it->second->collect(state, tablet_schema, expr, collect_infos));
            }

            const auto& children = expr->children();
            for (const auto& child : children) {
                stack.push(child);
            }
        }
    }

    LOG(INFO) << "Extracted collect info for " << collect_infos->size() << " fields";

    return Status::OK();
}

Status CollectionStatistics::process_segment(const RowsetSharedPtr& rowset, int32_t seg_id,
                                             const TabletSchema* tablet_schema,
                                             const CollectInfoMap& collect_infos,
                                             const MowHiddenRows* hidden_rows,
                                             io::IOContext* io_ctx) {
    auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
    auto rowset_meta = rowset->rowset_meta();

    auto idx_file_reader = std::make_unique<IndexFileReader>(
            rowset_meta->fs(),
            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format(),
            rowset_meta->inverted_index_file_info(seg_id), rowset_meta->tablet_id());
    RETURN_IF_ERROR(idx_file_reader->init(config::inverted_index_read_buffer_size, io_ctx));

    // Row ID lists are as large as the segment's hidden or NULL rows, so they are charged to the
    // query's memory.
    DorisVector<uint32_t> hidden_docs;
    if (hidden_rows != nullptr) {
        RETURN_IF_ERROR(collect_hidden_docs(rowset, seg_id, *tablet_schema, *hidden_rows, io_ctx,
                                            &hidden_docs));
    }

    int32_t total_seg_num_docs = 0;

    for (const auto& [ws_field_name, collect_info] : collect_infos) {
        lucene::search::IndexSearcher* index_searcher = nullptr;
        lucene::index::IndexReader* index_reader = nullptr;

#ifdef BE_TEST
        auto searcher_ptr = DORIS_TRY(open_optional_clucene_stats_searcher(
                *idx_file_reader, collect_info.index_meta, io_ctx));
        if (searcher_ptr == nullptr) {
            continue;
        }
        index_searcher = searcher_ptr.get();
        index_reader = index_searcher->getReader();
#else
        InvertedIndexCacheHandle inverted_index_cache_handle;
        auto index_file_key = idx_file_reader->get_index_file_cache_key(collect_info.index_meta);
        InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);

        if (!InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                            &inverted_index_cache_handle)) {
            auto searcher_ptr = DORIS_TRY(open_optional_clucene_stats_searcher(
                    *idx_file_reader, collect_info.index_meta, io_ctx));
            if (searcher_ptr == nullptr) {
                continue;
            }
            index_reader = searcher_ptr->getReader();
            size_t reader_size = index_reader->getTermInfosRAMUsed();
            auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
                    std::move(searcher_ptr), reader_size, UnixMillis());
            InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                           &inverted_index_cache_handle);
        }

        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        auto index_searcher_ptr = std::get<FulltextIndexSearcherPtr>(searcher_variant);
        index_searcher = index_searcher_ptr.get();
        index_reader = index_searcher->getReader();
#endif
        // BM25 on an analyzed index needs the record length of every row, and CLucene keeps
        // them, together with the field's token count, in the norms. A segment written without
        // norms would feed a zero avgdl, or rank its rows as zero-length documents next to the
        // segments that have norms, so refuse to score the collection. Keyword indexes and
        // built-in lowercase normalizers can use unit frequency and length instead.
        const auto token_count = index_reader->sumTotalTermFreq(ws_field_name.c_str());
        const bool single_term_normalizer = segment_v2::IndexReaderHelper::is_normalizer_only(
                collect_info.index_meta->properties());
        if (!token_count.has_value() && !single_term_normalizer &&
            segment_v2::inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    collect_info.index_meta->properties())) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "BM25 scoring requires norms, but segment {} was written without norms for "
                    "field {}. Norms are left out when the index sets \"norms\" = \"false\" or, "
                    "for a variant path, when inverted_index_skip_norms_for_variant is on",
                    seg_path, StringHelper::to_string(ws_field_name));
        }
        // Missing term totals imply one token per row for keyword and lowercase indexes.
        collection_statistics_detail::SegmentScoringStats scoring_stats {
                .doc_count = static_cast<uint64_t>(index_reader->maxDoc()),
                .token_count = token_count.value_or(index_reader->maxDoc()),
                .has_norms = token_count.has_value()};
        if (hidden_rows != nullptr) {
            std::shared_ptr<roaring::Roaring> null_docs = std::make_shared<roaring::Roaring>();
            uint64_t non_null_length_sum = 0;
            if (scoring_stats.has_norms) {
                segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
                RETURN_IF_ERROR(segment_v2::read_inverted_index_null_bitmap(
                        *idx_file_reader, *collect_info.index_meta, io_ctx,
                        &null_bitmap_cache_handle));
                null_docs = null_bitmap_cache_handle.get_bitmap();
                DORIS_CHECK(null_docs != nullptr);
                auto sum_lengths = [index_reader, &ws_field_name, &null_docs](uint64_t* out) {
                    DorisVector<uint32_t> null_doc_ids(null_docs->cardinality());
                    null_docs->toUint32Array(null_doc_ids.data());
                    return collection_statistics_detail::sum_non_null_norm_lengths(
                            index_reader, ws_field_name, null_doc_ids, out);
                };
#ifdef BE_TEST
                RETURN_IF_ERROR(sum_lengths(&non_null_length_sum));
#else
                RETURN_IF_ERROR(inverted_index_cache_handle.get_index_cache_value()
                                        ->non_null_norm_length_sum(ws_field_name, sum_lengths,
                                                                   &non_null_length_sum));
#endif
            }
            // The statistics keep no norms, so the lengths of a segment's hidden rows are summed
            // once per set of hidden rows and kept with the searcher.
            auto sum_hidden_lengths = [&](std::span<const uint32_t> docs, uint64_t* out) {
                auto sum = [index_reader, &ws_field_name, docs](uint64_t* lengths) {
                    return collection_statistics_detail::sum_norm_lengths_of(
                            index_reader, ws_field_name, docs, lengths);
                };
#ifdef BE_TEST
                return sum(out);
#else
                return inverted_index_cache_handle.get_index_cache_value()->hidden_norm_length_sum(
                        ws_field_name, docs, sum, out);
#endif
            };
            RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_docs(
                    hidden_docs, *null_docs, non_null_length_sum, sum_hidden_lengths,
                    &scoring_stats));
        }
        total_seg_num_docs =
                std::max(total_seg_num_docs, cast_set<int32_t>(scoring_stats.doc_count));
        _total_num_tokens[ws_field_name] += scoring_stats.token_count;

        for (const auto& term_info : collect_info.term_infos) {
            auto iter = TermIterator::create(io_ctx, false, index_reader, ws_field_name,
                                             term_info.get_single_term());
            auto doc_frequency = static_cast<uint64_t>(iter->doc_freq());
            if (!hidden_docs.empty()) {
                RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_term_docs(
                        *iter, hidden_docs, &doc_frequency));
            }
            _term_doc_freqs[ws_field_name][iter->term()] += doc_frequency;
        }
    }

    _total_num_docs += total_seg_num_docs;
    _counted_hidden_docs = _counted_hidden_docs || !hidden_docs.empty();

    return Status::OK();
}

void CollectionStatistics::copy_collected_from(const CollectionStatistics& other) {
    _total_num_docs = other._total_num_docs;
    _counted_hidden_docs = other._counted_hidden_docs;
    _total_num_tokens = other._total_num_tokens;
    _term_doc_freqs = other._term_doc_freqs;
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();
}

TabletCollectionStatistics::TabletCollectionStatistics(std::vector<RowSetSplits> rs_splits)
        : _rs_splits(std::move(rs_splits)) {}

TabletCollectionStatistics::~TabletCollectionStatistics() = default;

Status TabletCollectionStatistics::copy_to(RuntimeState* state,
                                           const TabletSchemaSPtr& tablet_schema,
                                           const VExprContextSPtrs& common_expr_ctxs_push_down,
                                           io::IOContext* io_ctx, const MowHiddenRows* hidden_rows,
                                           CollectionStatistics* statistics) {
    DORIS_CHECK(statistics != nullptr);
    std::lock_guard lock(_mutex);
    if (!_collected) {
        // Every scanner of the tablet reads with the same delete bitmap and version.
        _status = _statistics.collect(state, _rs_splits, tablet_schema, common_expr_ctxs_push_down,
                                      io_ctx, hidden_rows);
        _collected = true;
        // Do not hold the rowsets any longer than needed.
        _rs_splits.clear();
    }
    RETURN_IF_ERROR(_status);
    statistics->copy_collected_from(_statistics);
    return Status::OK();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    if (!_term_doc_freqs.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    if (!_term_doc_freqs[lucene_col_name].contains(term)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such term {}",
                        StringHelper::to_string(term));
    }

    return _term_doc_freqs[lucene_col_name][term];
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    if (!_total_num_tokens.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return _total_num_tokens[lucene_col_name];
}

uint64_t CollectionStatistics::get_doc_num() const {
    // A tablet whose rows are all hidden has no document a query can match, so its statistics
    // are empty rather than missing.
    if (_total_num_docs == 0 && !_counted_hidden_docs) {
        throw Exception(
                ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                "Index statistics collection failed: No data available for SimilarityCollector");
    }

    return _total_num_docs;
}

float CollectionStatistics::get_or_calculate_avg_dl(const std::wstring& lucene_col_name) {
    auto iter = _avg_dl_by_col.find(lucene_col_name);
    if (iter != _avg_dl_by_col.end()) {
        return iter->second;
    }

    const uint64_t total_term_cnt = get_total_term_cnt_by_col(lucene_col_name);
    const uint64_t total_doc_cnt = get_doc_num();
    float avg_dl = total_doc_cnt > 0 ? float((double)total_term_cnt / (double)total_doc_cnt) : 1.0F;
    _avg_dl_by_col[lucene_col_name] = avg_dl;
    return avg_dl;
}

float CollectionStatistics::get_or_calculate_idf(const std::wstring& lucene_col_name,
                                                 const std::wstring& term) {
    auto iter = _idf_by_col_term.find(lucene_col_name);
    if (iter != _idf_by_col_term.end()) {
        auto term_iter = iter->second.find(term);
        if (term_iter != iter->second.end()) {
            return term_iter->second;
        }
    }

    const uint64_t doc_num = get_doc_num();
    const uint64_t doc_freq = get_term_doc_freq_by_col(lucene_col_name, term);
    auto idf = (float)std::log(1 + ((double)doc_num - (double)doc_freq + (double)0.5) /
                                           ((double)doc_freq + (double)0.5));
    _idf_by_col_term[lucene_col_name][term] = idf;
    return idf;
}

#include "common/compile_check_end.h"
} // namespace doris
