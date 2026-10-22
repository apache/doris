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

#include "storage/index/inverted/reader/visible_collection.h"

#include <CLucene/index/_FieldInfos.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <numeric>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_common.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/reader/predicate_collector.h"
#include "storage/iterator/block_reader.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/schema.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using segment_v2::inverted_index::StringHelper;

namespace {

RowLocation scoring_row_location(const Block& block, int32_t ordinal, size_t row) {
    // The synthetic physical-rowid column is filtered together with data columns,
    // including an empty EOF block; no separate row-location buffer can go stale.
    const auto& column = assert_cast<const ColumnString&>(*block.get_by_position(ordinal).column);
    const auto value = column.get_data_at(row);
    DCHECK_EQ(value.size, sizeof(RowLocation));
    RowLocation location;
    std::memcpy(&location, value.data, sizeof(RowLocation));
    return location;
}

} // namespace

Status ExactScoringDocLengths::get_or_load(const Loader& loader,
                                           const std::vector<uint64_t>** result,
                                           uint64_t* total_tokens,
                                           const std::function<Status()>& check_cancel) {
    std::unique_lock lock(_mutex);
    while (_loading) {
        // A short-lived query must be able to cancel while another query performs
        // the first index scan; never hold the metadata mutex across that scan.
        if (check_cancel) {
            RETURN_IF_ERROR(check_cancel());
        }
        _changed.wait_for(lock, std::chrono::milliseconds(50));
    }
    if (!_ready) {
        if (check_cancel) {
            RETURN_IF_ERROR(check_cancel());
        }
        _loading = true;
        lock.unlock();
        std::vector<uint64_t> lengths;
        Status status;
        try {
            status = loader(&lengths);
        } catch (...) {
            lock.lock();
            _loading = false;
            lock.unlock();
            _changed.notify_all();
            throw;
        }
        const uint64_t total =
                status.ok() ? std::accumulate(lengths.begin(), lengths.end(), uint64_t {0}) : 0;
        lock.lock();
        if (status.ok()) {
            _lengths = std::move(lengths);
            _total_tokens = total;
            _ready = true;
        }
        _loading = false;
        _changed.notify_all();
        RETURN_IF_ERROR(status);
    }
    *result = &_lengths;
    if (total_tokens != nullptr) {
        *total_tokens = _total_tokens;
    }
    return Status::OK();
}

Status collect_visible_scoring_documents(const std::shared_ptr<BaseTablet>& tablet,
                                         const TabletReadSource& read_source, int64_t version,
                                         const std::shared_ptr<TabletSchema>& tablet_schema,
                                         RuntimeState* state, VisibleScoringDocuments* visible) {
    visible->segments.clear();
    uint64_t physical_rows = 0;
    TabletReadSource source;
    source.delete_predicates = read_source.delete_predicates;
    source.delete_bitmap = read_source.delete_bitmap;
    for (const auto& split : read_source.rs_splits) {
        const auto& rowset = split.rs_reader->rowset();
        physical_rows += rowset->num_rows();
        for (auto segment : rowset->segments()) {
            visible->segments[rowset->rowset_id()].try_emplace(static_cast<uint32_t>(segment.id()));
        }
        // Clone readers and omit scanner ranges: visibility belongs to the complete
        // captured collection, including rowsets assigned to another parallel scanner.
        source.rs_splits.emplace_back(split.rs_reader->clone());
    }
    if (physical_rows == 0) {
        return Status::OK();
    }

    std::vector<TabletColumnPtr> columns;
    for (size_t ordinal = 0; ordinal < tablet_schema->num_columns(); ++ordinal) {
        if (ordinal < tablet_schema->num_key_columns() ||
            static_cast<int32_t>(ordinal) == tablet_schema->delete_sign_idx() ||
            static_cast<int32_t>(ordinal) == tablet_schema->sequence_col_idx()) {
            columns.push_back(tablet_schema->columns()[ordinal]);
        }
    }
    TabletReader::ReaderParams params;
    params.tablet = tablet;
    params.tablet_schema = tablet_schema;
    params.version = {0, version};
    params.reader_type = ReaderType::READER_QUERY;
    params.runtime_state = state;
    params.batch_size = 1024;
    params.use_page_cache = !config::disable_storage_page_cache;
    params.read_schema = std::make_shared<ReadSchema>(std::move(columns));
    // Reuse the storage reader's synthetic physical-rowid column. Its name is
    // historical: it is generated while reading and does not require compaction.
    params.read_schema->append_compaction_rowid_column();
    params.rs_splits = std::move(source.rs_splits);
    params.delete_predicates = std::move(source.delete_predicates);
    params.delete_bitmap = state != nullptr && state->skip_delete_bitmap()
                                   ? nullptr
                                   : std::move(source.delete_bitmap);
    for (const auto& column : params.read_schema->columns()) {
        params.output_columns.insert(column->unique_id());
    }

    BlockReader reader;
    RETURN_IF_ERROR(reader.init(params));
    Block block = params.read_schema->create_read_block();
    const int sign_ordinal = params.read_schema->delete_sign_ordinal();
    bool eof = false;
    while (!eof) {
        if (state != nullptr) {
            RETURN_IF_CANCELLED(state);
        }
        block.clear_column_data();
        RETURN_IF_ERROR(reader.next_block_with_aggregation(&block, &eof));
        for (size_t row = 0; row < block.rows(); ++row) {
            // Query readers normally rely on the FE's delete-sign predicate. The
            // statistics reader has no user predicates; remove tombstones after
            // storage has applied the captured delete bitmap and delete conditions.
            if (sign_ordinal >= 0 &&
                block.get_by_position(sign_ordinal).column->get_int(row) != 0) {
                continue;
            }
            const auto location =
                    scoring_row_location(block, params.read_schema->rowid_ordinal(), row);
            visible->segments.at(location.rowset_id).at(location.segment_id).add(location.row_id);
        }
    }
    return Status::OK();
}

namespace visible_collection_detail {

using segment_v2::inverted_index::InvertedIndexAnalyzer;

Status count_raw_tokens(const IColumn& column, size_t row, const InvertedIndexAnalyzerCtx& analyzer,
                        const std::map<std::string, std::string>& properties, uint64_t ignore_above,
                        uint64_t* count) {
    if (const auto* constant = check_and_get_column<ColumnConst>(&column)) {
        return count_raw_tokens(constant->get_data_column(), 0, analyzer, properties, ignore_above,
                                count);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        if (nullable->is_null_at(row)) {
            return Status::OK();
        }
        return count_raw_tokens(nullable->get_nested_column(), row, analyzer, properties,
                                ignore_above, count);
    }
    if (const auto* array = check_and_get_column<ColumnArray>(&column)) {
        const auto& offsets = array->get_offsets();
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        for (size_t element = begin; element < offsets[row]; ++element) {
            RETURN_IF_ERROR(count_raw_tokens(array->get_data(), element, analyzer, properties,
                                             ignore_above, count));
        }
        return Status::OK();
    }
    const auto* text = check_and_get_column<ColumnString>(&column);
    if (text == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Exact BM25 lengths require a string or string-array scoring column");
    }
    const auto value = text->get_data_at(row);
    const bool analyzed = InvertedIndexAnalyzer::should_analyzer(properties);
    // Match add_values/add_array_values: ignore_above applies to untokenized
    // values; empty analyzed values and NULLs contribute no tokens.
    if ((analyzed && value.size == 0) || (!analyzed && value.size > ignore_above)) {
        return Status::OK();
    }
    std::vector<segment_v2::TermInfo> terms;
    const auto status = InvertedIndexAnalyzer::analyze(std::string_view(value.data, value.size),
                                                       &analyzer, properties, &terms);
    if (!status.ok()) {
        // The query analyzer's error includes its input. Here that input is a
        // stored document, so do not copy its text into diagnostics.
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Stored-text tokenization failed while collecting exact BM25 lengths");
    }
    for (const auto& term : terms) {
        *count += term.is_multi_terms() ? term.get_multi_terms().size() : 1;
    }
    return Status::OK();
}

} // namespace visible_collection_detail

namespace {
using visible_collection_detail::count_raw_tokens;

Status count_raw_length_block(const Block& block, int32_t rowid_ordinal,
                              const RowsetSegmentView& segment, const CollectInfo& info,
                              const InvertedIndexAnalyzerCtx& analyzer, uint64_t ignore_above,
                              RuntimeState* state, std::vector<uint64_t>* lengths) {
    const auto& column = *block.get_by_position(0).column;
    for (size_t row = 0; row < block.rows(); ++row) {
        if ((row & 31) == 0 && state != nullptr) {
            RETURN_IF_CANCELLED(state);
        }
        const auto location = scoring_row_location(block, rowid_ordinal, row);
        // A mismatched index/data row count must fail rather than index past
        // the dense length vector and publish invalid scoring statistics.
        if (location.segment_id != segment.id() || location.row_id >= lengths->size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                    "Index/data document IDs disagree while collecting BM25 lengths");
        }
        RETURN_IF_ERROR(count_raw_tokens(column, row, analyzer, info.index_meta->properties(),
                                         ignore_above, &(*lengths)[location.row_id]));
    }
    return Status::OK();
}

Status load_raw_doc_lengths(const std::shared_ptr<Rowset>& rowset, const RowsetSegmentView& segment,
                            const CollectInfo& info, RuntimeState* state,
                            std::vector<uint64_t>* lengths) {
    DORIS_CHECK(info.read_column != nullptr);
    auto analyzer = DORIS_TRY(analyzer_context_from_properties(info.index_meta->properties()));
    const auto ignore_above = std::stoull(
            get_parser_ignore_above_value_from_properties(info.index_meta->properties()));
    RowsetReaderContext context;
    context.reader_type = ReaderType::READER_QUERY;
    context.version = {0, rowset->end_version()};
    context.runtime_state = state;
    context.need_ordered_result = false;
    context.use_page_cache = !config::disable_storage_page_cache;
    context.read_schema = std::make_shared<ReadSchema>(
            std::vector<TabletColumnPtr> {std::make_shared<TabletColumn>(*info.read_column)});
    RETURN_IF_ERROR(
            context.read_schema->init_from_tablet_schema(*rowset->tablet_schema(), false, false));
    context.read_schema->append_compaction_rowid_column();
    RowsetReaderSharedPtr reader;
    RETURN_IF_ERROR(rowset->create_reader(&reader));
    RowSetSplits split(reader);
    split.segment_offsets = {segment.pos(), segment.pos() + 1};
    // This cache is immutable-index data, so do not apply a query's deletions.
    // A later query at another version applies its own visible document mask.
    RETURN_IF_ERROR(reader->init(&context, split));
    Block block = context.read_schema->create_read_block();
    size_t read_rows = 0;
    while (true) {
        if (state != nullptr) {
            RETURN_IF_CANCELLED(state);
        }
        block.clear_column_data();
        auto status = reader->next_batch(&block);
        if (!status.ok() && !status.is<ErrorCode::END_OF_FILE>()) {
            return status;
        }
        if (block.rows() != 0) {
            RETURN_IF_ERROR(count_raw_length_block(block, context.read_schema->rowid_ordinal(),
                                                   segment, info, analyzer, ignore_above, state,
                                                   lengths));
            read_rows += block.rows();
        }
        if (status.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
    }
    if (read_rows != lengths->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "Index/data document counts disagree: {} vs {}", lengths->size(), read_rows);
    }
    return Status::OK();
}

Status load_posting_doc_lengths(lucene::index::IndexReader* reader, const std::wstring& field,
                                RuntimeState* state, const io::IOContext* io_ctx,
                                std::vector<uint64_t>* lengths) {
    using namespace segment_v2;
    ErrorContext error_context;
    std::unique_ptr<lucene::index::TermEnum> terms;
    TermDocsPtr docs;
    Status status;
    try {
        auto start = make_term_ptr(field.c_str(), L"");
        terms.reset(reader->terms(start.get()));
        docs = make_term_doc_ptr(reader, true, io_ctx);
        do {
            const auto* term = terms->term(false);
            if (term == nullptr || field != term->field()) {
                break;
            }
            if (state != nullptr && state->is_cancelled()) {
                status = state->cancel_reason();
                break;
            }
            docs->seek(terms.get());
            size_t visited = 0;
            while (docs->next()) {
                if ((++visited & 16383) == 0 && state != nullptr && state->is_cancelled()) {
                    status = state->cancel_reason();
                    break;
                }
                if (docs->doc() < 0 || static_cast<size_t>(docs->doc()) >= lengths->size()) {
                    status = Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                            "Posting document ID exceeds the BM25 length vector");
                    break;
                }
                (*lengths)[docs->doc()] += static_cast<uint64_t>(docs->freq());
            }
        } while (status.ok() && terms->next());
    } catch (CLuceneError& error) {
        error_context.eptr = std::current_exception();
        error_context.err_msg = error.what();
    }
    FINALLY({
        FINALLY_CLOSE(docs);
        FINALLY_CLOSE(terms);
    })
    return status;
}

struct ExactLengthsCacheValue final : segment_v2::InvertedIndexSearcherCache::CacheValue {
    explicit ExactLengthsCacheValue(size_t documents)
            : CacheValue(sizeof(ExactLengthsCacheValue) + documents * sizeof(uint64_t),
                         UnixMillis()) {}
    ExactScoringDocLengths lengths;
};

struct CluceneVisibleStatisticsFieldContext {
    lucene::index::IndexReader* reader;
    const std::shared_ptr<Rowset>& rowset;
    const RowsetSegmentView& segment;
    const CollectInfo& info;
    const std::wstring& name;
    const std::string& cache_key;
    RuntimeState* state;
    const io::IOContext* io_ctx;
};

std::string make_visible_lengths_key(const CluceneVisibleStatisticsFieldContext& field) {
    const auto name = StringHelper::to_string(field.name);
    std::string key =
            field.cache_key + "/exact_bm25_lengths_v1/" + std::to_string(name.size()) + ":" + name;
    for (const auto& [property, value] : field.info.index_meta->properties()) {
        key += std::to_string(property.size()) + ":" + property + std::to_string(value.size()) +
               ":" + value;
    }
    return key;
}

Status sum_cached_visible_tokens(const CluceneVisibleStatisticsFieldContext& field,
                                 const roaring::Roaring& selected, bool subtract_excluded,
                                 size_t documents, uint64_t* token_count) {
    using namespace segment_v2;
    const auto* field_info = field.reader->getFieldInfos()->fieldInfo(field.name.c_str());
    DORIS_CHECK(field_info != nullptr);
    const std::string key = make_visible_lengths_key(field);
#ifdef BE_TEST
    ExactLengthsCacheValue local_value(documents);
    auto* value = &local_value;
#else
    InvertedIndexCacheHandle handle;
    InvertedIndexSearcherCache::CacheKey cache_key(key);
    if (!InvertedIndexSearcherCache::instance()->lookup(cache_key, &handle)) {
        // Concurrent cold queries can miss together. Recheck under a creation
        // lock so they share one loader; heavy loading uses only the entry mutex.
        static std::mutex creation_mutex;
        std::lock_guard lock(creation_mutex);
        if (!InvertedIndexSearcherCache::instance()->lookup(cache_key, &handle)) {
            InvertedIndexSearcherCache::instance()->insert(
                    cache_key, new ExactLengthsCacheValue(documents), &handle);
        }
    }
    auto* value = handle.get<ExactLengthsCacheValue>();
#endif
    const std::vector<uint64_t>* lengths = nullptr;
    uint64_t total_tokens = 0;
    RETURN_IF_ERROR(value->lengths.get_or_load(
            [&](std::vector<uint64_t>* loaded) {
                loaded->assign(documents, 0);
                if (field_info->hasProx) {
                    return load_posting_doc_lengths(field.reader, field.name, field.state,
                                                    field.io_ctx, loaded);
                }
                // Frequencies were omitted at write time. Quantized byte norms
                // cannot recover exact lengths (41 can decode as 40).
                return load_raw_doc_lengths(field.rowset, field.segment, field.info, field.state,
                                            loaded);
            },
            &lengths, &total_tokens,
            [&] {
                return field.state != nullptr && field.state->is_cancelled()
                               ? field.state->cancel_reason()
                               : Status::OK();
            }));
    DORIS_CHECK_EQ(lengths->size(), documents);
    // Sparse deletions cost O(deleted documents) after lengths are cached.
    *token_count = subtract_excluded ? total_tokens : 0;
    for (uint32_t doc : selected) {
        if (subtract_excluded) {
            *token_count -= (*lengths)[doc];
        } else {
            *token_count += (*lengths)[doc];
        }
    }
    return Status::OK();
}

Status collect_visible_term_frequencies(
        const CluceneVisibleStatisticsFieldContext& field, const roaring::Roaring& selected,
        bool subtract_excluded, std::unordered_map<std::wstring, uint64_t>* term_doc_freqs) {
    using namespace segment_v2;
    ErrorContext error_context;
    TermDocsPtr docs;
    Status status;
    try {
        docs = make_term_doc_ptr(field.reader, false, field.io_ctx);
        for (const auto& text : field.info.unique_terms) {
            const auto term_text = StringHelper::to_wstring(text);
            auto term = make_term_ptr(field.name.c_str(), term_text.c_str());
            docs->seek(term.get());
            const auto physical_df = static_cast<uint64_t>(docs->docFreq());
            uint64_t selected_matches = 0;
            int32_t current_doc = -1;
            size_t visited = 0;
            for (uint32_t doc : selected) {
                // skipTo may advance even when its current document is already past
                // the target. Retain that position across adjacent selected IDs.
                if (std::cmp_less(current_doc, doc)) {
                    if (!docs->skipTo(static_cast<int32_t>(doc))) {
                        break;
                    }
                    current_doc = docs->doc();
                }
                selected_matches += std::cmp_equal(current_doc, doc);
                if ((++visited & 16383) == 0 && field.state != nullptr &&
                    field.state->is_cancelled()) {
                    status = field.state->cancel_reason();
                    break;
                }
            }
            if (!status.ok()) {
                break;
            }
            if (selected_matches > physical_df) {
                status = Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                        "Selected document frequency exceeds physical document frequency");
                break;
            }
            (*term_doc_freqs)[term_text] =
                    subtract_excluded ? physical_df - selected_matches : selected_matches;
            if (field.state != nullptr && field.state->is_cancelled()) {
                status = field.state->cancel_reason();
                break;
            }
        }
    } catch (CLuceneError& error) {
        error_context.eptr = std::current_exception();
        error_context.err_msg = error.what();
    }
    FINALLY({ FINALLY_CLOSE(docs); })
    return status;
}

} // namespace

Status collect_clucene_visible_field_statistics(
        lucene::index::IndexReader* index_reader, const std::shared_ptr<Rowset>& rowset,
        const RowsetSegmentView& segment, const CollectInfo& info, const std::wstring& field_name,
        const std::string& index_cache_key, const roaring::Roaring& visible, RuntimeState* state,
        const io::IOContext* io_ctx, bool unit_length, uint64_t* token_count,
        std::unordered_map<std::wstring, uint64_t>* term_doc_freqs) {
    using namespace segment_v2;
    if (state != nullptr) {
        RETURN_IF_CANCELLED(state);
    }
    const auto documents = static_cast<size_t>(index_reader->maxDoc());
    if (!visible.isEmpty() && visible.maximum() >= documents) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "Visible document ID exceeds index document count");
    }
    const bool subtract_excluded = visible.cardinality() > documents / 2;
    roaring::Roaring excluded;
    if (subtract_excluded) {
        excluded.addRange(0, documents);
        excluded -= visible;
    }
    const auto& selected = subtract_excluded ? excluded : visible;
    *token_count = 0;
    term_doc_freqs->clear();
    const CluceneVisibleStatisticsFieldContext field {.reader = index_reader,
                                                      .rowset = rowset,
                                                      .segment = segment,
                                                      .info = info,
                                                      .name = field_name,
                                                      .cache_key = index_cache_key,
                                                      .state = state,
                                                      .io_ctx = io_ctx};
    if (unit_length) {
        *token_count = visible.cardinality();
    } else {
        RETURN_IF_ERROR(sum_cached_visible_tokens(field, selected, subtract_excluded, documents,
                                                  token_count));
    }
    return collect_visible_term_frequencies(field, selected, subtract_excluded, term_doc_freqs);
}

} // namespace doris
