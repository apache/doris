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

#include "storage/index/inverted/inverted_index_reader.h"

#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/Query.h>
#include <CLucene/search/RangeQuery.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/bkd/bkd_docid_iterator.h>
#include <CLucene/util/stringUtil.h>

#include <algorithm>
#include <map>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/type_limit.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/bkd_field_encoding.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/common/single_flight.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/listed_terms.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/expand_query/expand_query.h"
#include "storage/index/inverted/query_v2/phrase_prefix_query/phrase_prefix_query.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/spi/clucene_index_source.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/docid_set_ops.h"
#include "storage/index/query/logical/search_lowering.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/query/term_pattern.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/types.h"
#include "util/defer_op.h"
#include "util/faststring.h"

namespace {

// Sentinel values are sourced from the compute-layer `type_limit<CppType>` and
// then projected onto the storage-layer POD via `PrimitiveTypeConvertor<PT>`.
// Routing through the compute layer keeps the +/- infinity constants
// single-sourced (e.g. DecimalV2 max lives only in DecimalV2Value::get_max_decimal,
// DATE bounds only in VecDateTimeValue::datetime_min/max_value), so types like
// decimal12_t and uint24_t — which have no std::numeric_limits specialisation —
// no longer need their own type_limit<> entries.
template <doris::PrimitiveType PT>
static void bkd_encode_min(const doris::KeyCoder* coder, std::string* out) {
    using compute_t = typename doris::PrimitiveTypeTraits<PT>::CppType;
    auto compute_v = doris::type_limit<compute_t>::min();
    auto v = doris::PrimitiveTypeConvertor<PT>::to_storage_field_type(compute_v);
    coder->full_encode_ascending(&v, out);
}

template <doris::PrimitiveType PT>
static void bkd_encode_max(const doris::KeyCoder* coder, std::string* out) {
    using compute_t = typename doris::PrimitiveTypeTraits<PT>::CppType;
    auto compute_v = doris::type_limit<compute_t>::max();
    auto v = doris::PrimitiveTypeConvertor<PT>::to_storage_field_type(compute_v);
    coder->full_encode_ascending(&v, out);
}

// encode_bkd_field_ascending now lives in storage/index/bkd_field_encoding.h so
// the SNII-native BKD reader encodes query values through the exact same
// definition (INV-1); only the +/- infinity sentinels below stay here, being an
// artifact of this visitor's always-closed bounds.
using doris::encode_bkd_field_ascending;

static doris::Status encode_bkd_min_ascending(doris::FieldType ft, const doris::KeyCoder* coder,
                                              std::string* out) {
#define CASE(FT, PT)                                          \
    case doris::FieldType::FT:                                \
        bkd_encode_min<doris::PrimitiveType::PT>(coder, out); \
        return doris::Status::OK();
    switch (ft) {
        DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(CASE)
    default:
        break;
    }
#undef CASE
    return doris::Status::InternalError("unsupported BKD field type {}", static_cast<int>(ft));
}

static doris::Status encode_bkd_max_ascending(doris::FieldType ft, const doris::KeyCoder* coder,
                                              std::string* out) {
#define CASE(FT, PT)                                          \
    case doris::FieldType::FT:                                \
        bkd_encode_max<doris::PrimitiveType::PT>(coder, out); \
        return doris::Status::OK();
    switch (ft) {
        DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(CASE)
    default:
        break;
    }
#undef CASE
    return doris::Status::InternalError("unsupported BKD field type {}", static_cast<int>(ft));
}

} // anonymous namespace

namespace doris::segment_v2 {

std::string InvertedIndexReader::get_index_file_path() {
    return _index_file_reader->get_index_file_path(&_index_meta);
}

Status InvertedIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    DORIS_CHECK(null_bitmap_cache_handle != nullptr);
    RETURN_IF_ERROR(query(context, column_name, query_value, query_type, bit_map, analyzer_ctx));
    if (!has_null()) {
        return Status::OK();
    }
    return read_null_bitmap(context, null_bitmap_cache_handle);
}

Status InvertedIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                             InvertedIndexQueryCacheHandle* cache_handle,
                                             lucene::store::Directory* dir) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_null_bitmap_timer);
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    bool owned_dir = false;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {
                index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
        auto* cache = InvertedIndexQueryCache::instance();
        if (cache->lookup(cache_key, cache_handle)) {
            return Status::OK();
        }

        if (!dir) {
            // TODO: ugly code here, try to refact.
            auto st = _index_file_reader->init(config::inverted_index_read_buffer_size,
                                               context->io_ctx);
            if (!st.ok()) {
                LOG(WARNING) << st;
                return st;
            }
            auto directory = DORIS_TRY(_index_file_reader->open(&_index_meta, context->io_ctx));
            dir = directory.release();
            owned_dir = true;
        }

        // ownership of null_bitmap and its deletion will be transfered to cache
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        const char* null_bitmap_file_name =
                InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        if (dir->fileExists(null_bitmap_file_name)) {
            null_bitmap_in = dir->openInput(null_bitmap_file_name);
            auto null_bitmap_size = cast_set<int32_t>(null_bitmap_in->length());
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(buf.data(), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            cache->insert(cache_key, null_bitmap, cache_handle);
            FINALIZE_INPUT(null_bitmap_in);
        } else {
            cache->insert(cache_key, null_bitmap, cache_handle);
        }
        if (owned_dir) {
            FINALIZE_INPUT(dir);
        }
    } catch (CLuceneError& e) {
        FINALLY_FINALIZE_INPUT(null_bitmap_in);
        if (owned_dir) {
            FINALLY_FINALIZE_INPUT(dir);
        }
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index read null bitmap error occurred, reason={}", e.what());
    }

    return Status::OK();
}

bool InvertedIndexReader::handle_query_cache(const IndexQueryContextPtr& context,
                                             InvertedIndexQueryCache* cache,
                                             const InvertedIndexQueryCache::CacheKey& cache_key,
                                             InvertedIndexQueryCacheHandle* cache_handler,
                                             std::shared_ptr<roaring::Roaring>& bit_map,
                                             bool enabled) {
    const auto& query_options = context->runtime_state->query_options();
    if (!enabled || !query_options.enable_inverted_index_query_cache) {
        return false;
    }

    context->stats->inverted_index_query_cache_lookup++;
    SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
    const bool cache_hit = cache->lookup(cache_key, cache_handler);
    if (cache_hit) {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_hit", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache hit");
        });
        context->stats->inverted_index_query_cache_hit++;
        SCOPED_RAW_TIMER(&context->stats->inverted_index_query_bitmap_copy_timer);
        bit_map = cache_handler->get_bitmap();
        return true;
    }

    DBUG_EXECUTE_IF("InvertedIndexReader.handle_query_cache_miss", {
        return Status::Error<ErrorCode::INTERNAL_ERROR>("handle query cache miss");
    });
    context->stats->inverted_index_query_cache_miss++;
    return false;
}

void InvertedIndexReader::insert_query_cache(const IndexQueryContextPtr& context,
                                             InvertedIndexQueryCache* cache,
                                             const InvertedIndexQueryCache::CacheKey& cache_key,
                                             std::shared_ptr<roaring::Roaring> bit_map,
                                             InvertedIndexQueryCacheHandle* cache_handler,
                                             bool enabled) {
    if (!enabled || !context->runtime_state->query_options().enable_inverted_index_query_cache) {
        return;
    }
    cache->insert(cache_key, std::move(bit_map), cache_handler);
    context->stats->inverted_index_query_cache_insert++;
}

Status InvertedIndexReader::handle_searcher_cache(
        const IndexQueryContextPtr& context, InvertedIndexCacheHandle* inverted_index_cache_handle,
        const std::string& index_file_key) {
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(
            index_file_key.empty() ? _index_file_reader->get_index_file_cache_key(&_index_meta)
                                   : index_file_key);
    const auto& query_options = context->runtime_state->query_options();

    bool cache_hit = false;
    if (query_options.enable_inverted_index_searcher_cache) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_lookup_timer);
        cache_hit = InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                                   inverted_index_cache_handle);
    }

    if (cache_hit) {
        DBUG_EXECUTE_IF("InvertedIndexReader.handle_searcher_cache_hit", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle searcher cache hit");
        });
        context->stats->inverted_index_searcher_cache_hit++;
        return Status::OK();
    } else {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_open_timer);

        DBUG_EXECUTE_IF("InvertedIndexReader.handle_searcher_cache_miss", {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("handle searcher cache miss");
        });
        // searcher cache miss
        context->stats->inverted_index_searcher_cache_miss++;
        auto mem_tracker = std::make_unique<MemTracker>("InvertedIndexSearcherCacheWithRead");

        IndexSearcherPtr searcher;
        auto st =
                _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return st;
        }
        auto dir = DORIS_TRY(_index_file_reader->open(&_index_meta, context->io_ctx));

        DBUG_EXECUTE_IF(
                "InvertedIndexReader.handle_searcher_cache.io_ctx", ({
                    if (dir) {
                        auto* stream = dir->getDorisIndexInput();
                        const auto* cur_io_ctx = (const io::IOContext*)stream->getIoContext();
                        if (cur_io_ctx->file_cache_stats) {
                            if (cur_io_ctx->file_cache_stats != &context->stats->file_cache_stats) {
                                LOG(FATAL) << "io context file cache stats is not equal to "
                                              "stats file cache "
                                              "stats: "
                                           << cur_io_ctx->file_cache_stats << ", "
                                           << &context->stats->file_cache_stats;
                            }
                        }
                    }
                }));

        // try to reuse index_searcher's directory to read null_bitmap to cache
        // to avoid open directory additionally for null_bitmap
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(read_null_bitmap(context, &null_bitmap_cache_handle, dir.get()));
        size_t reader_size = 0;
        auto index_searcher_builder =
                DORIS_TRY(IndexSearcherBuilder::create_index_searcher_builder(type()));
        RETURN_IF_ERROR(create_index_searcher(index_searcher_builder.get(), dir.get(), &searcher,
                                              reader_size));
        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(std::move(searcher),
                                                                       reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       inverted_index_cache_handle);
        return Status::OK();
    }
}

Status InvertedIndexReader::create_index_searcher(IndexSearcherBuilder* index_searcher_builder,
                                                  lucene::store::Directory* dir,
                                                  IndexSearcherPtr* searcher, size_t& reader_size) {
    auto searcher_result = DORIS_TRY(index_searcher_builder->get_index_searcher(dir));
    *searcher = searcher_result;

    // When the meta information has been read, the ioContext needs to be reset to prevent it from being used by other queries.
    auto* stream = static_cast<DorisCompoundReader*>(dir)->getDorisIndexInput();
    stream->setIoContext(nullptr);
    stream->setIndexFile(false);

    reader_size = index_searcher_builder->get_reader_size();
    return Status::OK();
};

namespace {

namespace logical = index_query::logical;
namespace query_v2 = inverted_index::query_v2;

query_v2::QueryPtr term_query(const IndexQueryContextPtr& context, const std::wstring& field,
                              const std::string& term) {
    return std::make_shared<query_v2::TermQuery>(context, field, term);
}

// One term is queried as itself; several form the boolean the set asks for. SEARCH counts a
// threshold before a set reaches here.
query_v2::QueryPtr term_set_query(const IndexQueryContextPtr& context, const std::wstring& field,
                                  const std::string& binding_key, const logical::TermSet& set) {
    DORIS_CHECK(set.min_should_match == 0);
    if (set.terms.size() == 1) {
        return term_query(context, field, set.terms.front());
    }
    auto builder = query_v2::create_operator_boolean_query_builder(
            set.require_all ? query_v2::OperatorType::OP_AND : query_v2::OperatorType::OP_OR);
    for (const auto& term : set.terms) {
        builder->add(term_query(context, field, term), binding_key);
    }
    return builder->build();
}

Status phrase_query(const IndexQueryContextPtr& context, const std::wstring& field,
                    const logical::Phrase& phrase, const roaring::Roaring* candidates,
                    query_v2::QueryPtr* out) {
    const bool single_terms = std::ranges::all_of(
            phrase.slots, [](const TermInfo& slot) { return slot.is_single_term(); });
    if (phrase.prefix) {
        if (!single_terms) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "a phrase prefix with several terms at one position is not supported");
        }
        *out = std::make_shared<query_v2::PhrasePrefixQuery>(context, field, phrase.slots,
                                                             candidates, phrase.suffix);
        return Status::OK();
    }
    const index_query::PhraseQueryOptions options {
            .slop = static_cast<uint32_t>(phrase.slop),
            .ordered = phrase.ordered,
            .candidates = candidates,
            .candidate_rows_consumed =
                    candidates == nullptr ? nullptr : &context->candidate_rows_consumed};
    if (single_terms) {
        *out = std::make_shared<query_v2::PhraseQuery>(context, field, phrase.slots, options);
    } else {
        *out = std::make_shared<query_v2::MultiPhraseQuery>(context, field, phrase.slots, options);
    }
    return Status::OK();
}

index_query::TermPatternKind pattern_kind(logical::ExpandKind kind) {
    switch (kind) {
    case logical::ExpandKind::kPrefix:
        return index_query::TermPatternKind::kPrefix;
    case logical::ExpandKind::kRegexp:
        return index_query::TermPatternKind::kRegexp;
    case logical::ExpandKind::kContains:
        return index_query::TermPatternKind::kContains;
    case logical::ExpandKind::kWildcard:
    default:
        return index_query::TermPatternKind::kWildcard;
    }
}

} // namespace

Status TextIndexReader::open_source(const IndexQueryContextPtr& context, const std::wstring& field,
                                    std::unique_ptr<OpenedIndex>* opened,
                                    index_query::IndexSourcePtr* source) {
    RETURN_IF_ERROR(_open_index(context, opened));
    *source = _bind_source(context, field, **opened);
    return Status::OK();
}

Status run_leaf(const IndexQueryContextPtr& context, const std::wstring& field,
                const logical::Node& leaf, const roaring::Roaring* candidates, bool scoring,
                index_query::IndexSourcePtr source, uint32_t doc_count,
                const std::shared_ptr<roaring::Roaring>& result) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_timer);
    if (const auto* expand = leaf.as<logical::Expand>(); expand != nullptr && !scoring) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
        if (doc_count == 0) {
            return Status::OK();
        }
        const auto kind = pattern_kind(expand->kind);
        index_query::TermPattern pattern;
        THROW_IF_ERROR(index_query::TermPattern::create(kind, expand->pattern, &pattern));
        THROW_IF_ERROR(query_v2::collect_expanded_rows(
                *source, pattern,
                index_query::expansion_limit(kind, index_query::max_expansions(*context)), nullptr,
                result.get()));
        return Status::OK();
    }
    std::span<const std::string> terms;
    if (const auto* term = leaf.as<logical::Term>(); term != nullptr) {
        terms = std::span(&term->term, 1);
    } else if (const auto* set = leaf.as<logical::TermSet>();
               set != nullptr && (!set->require_all || set->terms.size() == 1) &&
               set->min_should_match == 0) {
        terms = set->terms;
    }
    if (!terms.empty() && (!scoring || terms.size() > 1)) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
        if (!scoring) {
            index_query::RoaringDocIdSink sink(*result);
            return source->collect_terms(terms, sink);
        }
        query_v2::ListedTerms listed(std::move(source), nullptr);
        for (size_t i = 0; i < terms.size(); ++i) {
            auto similarity = std::make_shared<BM25Similarity>();
            similarity->for_one_term(context, field,
                                     inverted_index::StringHelper::to_wstring(terms[i]));
            listed.add(i, terms[i], std::move(similarity));
        }
        const auto scorer = listed.scored_disjunction();
        if (context->collection_similarity != nullptr) {
            *result |= query_v2::collect_scored_rows(scorer, 0, *context->collection_similarity);
        } else {
            query_v2::collect_true_rows(scorer, result.get());
        }
        return Status::OK();
    }
    query_v2::WeightPtr weight;
    {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_init_timer);
        query_v2::QueryPtr query;
        RETURN_IF_ERROR(plan_query(leaf, context, field, "", candidates, &query));
        weight = query->weight(scoring);
    }
    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = doc_count;
    exec_ctx.field_sources.emplace(field, std::move(source));
    query_v2::collect_multi_segment_doc_set(weight, exec_ctx, "", result,
                                            context->collection_similarity, scoring);
    return Status::OK();
}

Status run_clucene_leaf(const IndexQueryContextPtr& context, std::wstring field,
                        const logical::Node& leaf, const roaring::Roaring* candidates, bool scoring,
                        const FulltextIndexSearcherPtr& searcher,
                        const std::shared_ptr<roaring::Roaring>& result) {
    auto* reader = searcher->getReader();
    if (context->runtime_state != nullptr &&
        context->runtime_state->query_options().inverted_index_compatible_read) {
        reader->setCompatibleRead(true);
    }
    try {
        auto source =
                clucene_index_source(std::shared_ptr<lucene::index::IndexReader>(searcher, reader),
                                     std::move(field), context->io_ctx);
        const auto& source_field = source->field();
        RETURN_IF_ERROR(run_leaf(context, source_field, leaf, candidates, scoring,
                                 std::move(source), reader->maxDoc(), result));
    } catch (const CLuceneError& e) {
        return clucene_error_status(fmt::format("CLuceneError occurred: {}", e.what()));
    } catch (const Exception& e) {
        return clucene_error_status(fmt::format("Exception occurred: {}", e.what()));
    }
    return Status::OK();
}

Status plan_query(const logical::Node& leaf, const IndexQueryContextPtr& context,
                  const std::wstring& field, const std::string& binding_key,
                  const roaring::Roaring* candidates, query_v2::QueryPtr* out) {
    if (const auto* term = leaf.as<logical::Term>()) {
        *out = term_query(context, field, term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        *out = term_set_query(context, field, binding_key, *set);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        return phrase_query(context, field, *phrase, candidates, out);
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        *out = std::make_shared<query_v2::ExpandQuery>(context, field, pattern_kind(expand->kind),
                                                       expand->pattern);
    } else if (leaf.as<logical::Exists>() != nullptr) {
        *out = std::make_shared<query_v2::AllQuery>(field, /*nullable=*/true);
    } else if (leaf.as<logical::Empty>() != nullptr) {
        *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
    } else {
        return Status::InternalError("leaf kind {} cannot run on a CLucene field",
                                     leaf.value.index());
    }
    return Status::OK();
}

namespace {

// Runs `compute` once for concurrent identical queries: the first caller leads and the others
// take its bitmap. A leader's failure leaves each follower to compute for itself.
template <typename Compute>
Status run_query_single_flight(
        inverted_index::SingleFlight<std::pair<Status, std::shared_ptr<roaring::Roaring>>>& flight,
        const std::string& key, std::shared_ptr<roaring::Roaring>* result,
#ifdef BE_TEST
        InvertedIndexReader::SingleFlightFollowerJoinedObserver follower_joined_observer,
        void* follower_joined_opaque,
        InvertedIndexReader::SingleFlightLeaderBeforeComputeObserver leader_before_compute_observer,
        void* leader_before_compute_opaque,
#endif
        Compute&& compute) {
    auto follower = flight.join_or_lead(key);
    if (follower.has_value()) {
#ifdef BE_TEST
        if (follower_joined_observer != nullptr) {
            follower_joined_observer(follower_joined_opaque);
        }
#endif
        auto [leader_status, leader_bitmap] = follower->get();
        if (leader_status.ok() && leader_bitmap != nullptr) {
            *result = std::move(leader_bitmap);
            return Status::OK();
        }
    }
    const bool is_leader = !follower.has_value();
#ifdef BE_TEST
    if (is_leader && leader_before_compute_observer != nullptr) {
        leader_before_compute_observer(leader_before_compute_opaque);
    }
#endif
    Status status = Status::OK();
    std::shared_ptr<roaring::Roaring> bitmap;
    {
        // Followers learn the outcome on every exit path, errors included.
        DEFER(if (is_leader) { flight.publish(key, std::make_pair(status, bitmap)); });
        status = compute(&bitmap);
    }
    RETURN_IF_ERROR(status);
    *result = std::move(bitmap);
    return Status::OK();
}

// The one exact term a COUNT_ON_INDEX scan of `leaf` can answer from the dictionary.
const std::string* single_exact_term(const logical::Node& leaf) {
    if (const auto* term = leaf.as<logical::Term>()) {
        return &term->term;
    }
    if (const auto* set = leaf.as<logical::TermSet>(); set != nullptr && set->terms.size() == 1) {
        return &set->terms.front();
    }
    const auto* phrase = leaf.as<logical::Phrase>();
    if (phrase != nullptr && phrase->slots.size() == 1 && !phrase->prefix && !phrase->suffix &&
        phrase->slots.front().is_single_term()) {
        return &phrase->slots.front().get_single_term();
    }
    return nullptr;
}

// Only a phrase of several slots reads positions row by row, so only it narrows to the scan's
// candidate rows; its partial result stays out of the cache and the flight.
bool consumes_candidates(const logical::Node& leaf) {
    const auto* phrase = leaf.as<logical::Phrase>();
    return phrase != nullptr && phrase->slots.size() > 1;
}

// The longest term of a leaf, which the ignore_above limit of a keyword index applies to.
size_t longest_term(const logical::Node& leaf) {
    size_t longest = 0;
    const auto note = [&longest](const std::string& term) {
        longest = std::max(longest, term.size());
    };
    if (const auto* term = leaf.as<logical::Term>()) {
        note(term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        std::ranges::for_each(set->terms, note);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        for (const auto& slot : phrase->slots) {
            if (slot.is_single_term()) {
                note(slot.get_single_term());
            } else {
                std::ranges::for_each(slot.get_multi_terms(), note);
            }
        }
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        note(expand->pattern);
    }
    return longest;
}

} // namespace

Status TextIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(type(), dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

Status TextIndexReader::query(const IndexQueryContextPtr& context, const std::string& column_name,
                              const Field& query_value, InvertedIndexQueryType query_type,
                              std::shared_ptr<roaring::Roaring>& bit_map,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    return _query_raw(context, column_name, query_value.get<PrimitiveType::TYPE_STRING>(),
                      query_type, bit_map, nullptr, analyzer_ctx);
}

Status TextIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    DORIS_CHECK(null_bitmap_cache_handle != nullptr);
    return _query_raw(context, column_name, query_value.get<PrimitiveType::TYPE_STRING>(),
                      query_type, bit_map, null_bitmap_cache_handle, analyzer_ctx);
}

Status TextIndexReader::query_leaf(const IndexQueryContextPtr& context,
                                   const std::string& column_name, const logical::Node& leaf,
                                   std::shared_ptr<roaring::Roaring>& bit_map,
                                   InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    const LeafRequest request {.query_type = logical::leaf_query_type(leaf),
                               .leaf = &leaf,
                               .longest_value_bytes = longest_term(leaf),
                               .text = {}};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

Status TextIndexReader::_query_raw(const IndexQueryContextPtr& context,
                                   const std::string& column_name, const std::string& value,
                                   InvertedIndexQueryType query_type,
                                   std::shared_ptr<roaring::Roaring>& bit_map,
                                   InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                                   const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    const LeafRequest request {.query_type = query_type,
                               .longest_value_bytes = value.size(),
                               .text = value,
                               .analyzer_ctx = analyzer_ctx};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

Status TextIndexReader::_admit(const IndexQueryContextPtr& /*context*/,
                               const std::string& column_name, const LeafRequest& request,
                               Admission* /*admission*/) {
    // CLucene indexes do not persist the gram tokenizer contract needed to compile a pattern.
    if (is_gram_query(request.query_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "{} requires SNII storage format for column {}",
                query_type_to_string(request.query_type), column_name);
    }
    return Status::OK();
}

Status TextIndexReader::_run_gram(const IndexQueryContextPtr& /*context*/, OpenedIndex& /*index*/,
                                  const LeafRequest& request,
                                  std::shared_ptr<roaring::Roaring>* /*out*/) {
    return Status::InternalError("a text index admitted {} it cannot run",
                                 query_type_to_string(request.query_type));
}

// Keep the cache, count-only, candidate and single-flight decisions in one linear path.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status TextIndexReader::_execute(const IndexQueryContextPtr& context,
                                 const std::string& column_name, const LeafRequest& request,
                                 std::shared_ptr<roaring::Roaring>& bit_map,
                                 InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    const InvertedIndexQueryType query_type = request.query_type;
    const bool track_requested_null_time = null_bitmap_cache_handle != nullptr;
    const int64_t query_ns_before =
            track_requested_null_time ? context->stats->inverted_index_query_timer : 0;
    int64_t requested_null_ns = 0;
    DEFER({
        if (!track_requested_null_time) {
            return;
        }
        const int64_t inclusive_query_ns =
                context->stats->inverted_index_query_timer - query_ns_before;
        DORIS_CHECK_GE(inclusive_query_ns, 0);
        const int64_t exclusive_query_ns =
                inclusive_query_ns > requested_null_ns ? inclusive_query_ns - requested_null_ns : 0;
        context->stats->inverted_index_query_timer = query_ns_before + exclusive_query_ns;
    });
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);
    // Fresh per-search reply: only the query about to run decides whether its result is
    // candidate-restricted.
    context->candidate_rows_consumed = false;
    Admission admission;
    RETURN_IF_ERROR(_admit(context, column_name, request, &admission));
    const auto finish_query = [&](OpenedIndex* index) -> Status {
        if (null_bitmap_cache_handle == nullptr) {
            return Status::OK();
        }
        const int64_t null_ns_before = context->stats->inverted_index_query_null_bitmap_timer;
        Status status = _read_null_bitmap(context, null_bitmap_cache_handle, index);
        const int64_t null_ns_after = context->stats->inverted_index_query_null_bitmap_timer;
        DORIS_CHECK_GE(null_ns_after, null_ns_before);
        requested_null_ns += null_ns_after - null_ns_before;
        return status;
    };

    const bool keyword = type() == InvertedIndexReaderType::STRING_TYPE;
    if (keyword) {
        // A keyword index drops values longer than ignore_above: a longer query value finds
        // nothing in it, and a contains match may still need such a value, so rows answer both.
        if (query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "a keyword index does not run MATCH_PHRASE_EDGE; evaluating by function");
        }
        if (int ignore_above = std::stoi(
                    get_parser_ignore_above_value_from_properties(_index_meta.properties()));
            std::cmp_greater(request.longest_value_bytes, ignore_above)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "query value is too long, evaluate skipped.");
        }
    }

    // A scoring query publishes its scores while it runs, so neither a cached bitmap nor a
    // leader's can answer it.
    const bool scoring = context->collection_similarity != nullptr &&
                         IndexReaderHelper::is_need_similarity_score(query_type, &_index_meta);
    const bool allow_result_cache = !scoring && admission.cacheable;
    InvertedIndexQueryCache::CacheKey cache_key;
    std::string index_file_key;
    if (allow_result_cache) {
        index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        const int32_t max_expansions = index_query::max_expansions(*context);
        if (request.leaf != nullptr) {
            cache_key = {index_file_key, column_name, query_type,
                         InvertedIndexLeafSemantic {.leaf = request.leaf,
                                                    .max_expansions = max_expansions}
                                 .encode()};
        } else {
            cache_key = {index_file_key, column_name, query_type,
                         InvertedIndexRawQuerySemantic {.raw_query_bytes = request.text,
                                                        .query_type = query_type,
                                                        .max_expansions = max_expansions}};
        }
    }
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map,
                           allow_result_cache)) {
        return finish_query(nullptr);
    }

    // A gram query runs over the index's grams; any other request is planned, before the index
    // opens unless its format admits the segment first.
    const bool gram = is_gram_query(query_type);
    logical::Node lowered;
    const logical::Node* leaf = request.leaf;
    const auto plan = [&]() -> Status {
        if (leaf != nullptr) {
            return Status::OK();
        }
        SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
        logical::AnalyzeValue analyze;
        const bool analyzed = request.analyzer_ctx != nullptr
                                      ? request.analyzer_ctx->requires_analysis()
                                      : !keyword;
        if (analyzed) {
            analyze = [&](std::string_view text, std::vector<TermInfo>* tokens) {
                return inverted_index::InvertedIndexAnalyzer::analyze(
                        text, request.analyzer_ctx, _index_meta.properties(), tokens);
            };
        }
        RETURN_IF_ERROR(logical::lower_match(query_type, request.text, analyze, &lowered));
        leaf = &lowered;
        return Status::OK();
    };
    if (!gram && admission.plan_before_open) {
        RETURN_IF_ERROR(plan());
    }
    std::unique_ptr<OpenedIndex> index;
    if (Status status = _open_index(context, &index, index_file_key); !status.ok()) {
        return admission.open_failed == nullptr ? status : admission.open_failed(std::move(status));
    }
    if (admission.check_open != nullptr) {
        RETURN_IF_ERROR(admission.check_open(*index));
    }
    if (!gram && !admission.plan_before_open) {
        RETURN_IF_ERROR(plan());
    }
    if (!gram && leaf->as<logical::Empty>() != nullptr) {
        auto msg = fmt::format("token parser result is empty for query '{}'", request.text);
        if (is_match_query(query_type)) {
            LOG(WARNING) << msg;
            bit_map = std::make_shared<roaring::Roaring>();
            insert_query_cache(context, cache, cache_key, bit_map, &cache_handler,
                               allow_result_cache);
            return finish_query(index.get());
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
    }

    // Under a cold cache, parallel scanners open and decode the same segment's index for the
    // same query; identical concurrent queries collapse into one execution (see SingleFlight),
    // which caches the result it computes.
    static inverted_index::SingleFlight<std::pair<Status, std::shared_ptr<roaring::Roaring>>>
            query_single_flight;
    const auto run_shared = [&](const auto& run, std::shared_ptr<roaring::Roaring>* out) {
        return run_query_single_flight(
                query_single_flight, cache_key.encode(), out,
#ifdef BE_TEST
                _single_flight_follower_joined_observer, _single_flight_follower_joined_opaque,
                _single_flight_leader_before_compute_observer,
                _single_flight_leader_before_compute_opaque,
#endif
                [&](std::shared_ptr<roaring::Roaring>* shared) {
                    Status run_status = run(shared);
                    if (run_status.ok()) {
                        insert_query_cache(context, cache, cache_key, *shared, &cache_handler,
                                           allow_result_cache);
                    }
                    return run_status;
                });
    };
    std::shared_ptr<roaring::Roaring> result;

    if (gram) {
        const auto run_gram = [&](std::shared_ptr<roaring::Roaring>* out) {
            return _run_gram(context, *index, request, out);
        };
        RETURN_IF_ERROR(allow_result_cache ? run_shared(run_gram, &result) : run_gram(&result));
        bit_map = std::move(result);
        return finish_query(index.get());
    }

    // A count-only scan of one exact term is answered from the dictionary. The cache came first,
    // since a cached row-accurate bitmap counts correctly, and the count-shaped bitmap never
    // enters the cache or the flight, which serve real row ids.
    if (context->count_on_index_fastpath) {
        if (const std::string* term = single_exact_term(*leaf); term != nullptr) {
            bool handled = false;
            std::shared_ptr<roaring::Roaring> count_bitmap;
            RETURN_IF_ERROR(
                    _count_from_df(context, column_name, *index, *term, &handled, &count_bitmap));
            if (handled) {
                bit_map = std::move(count_bitmap);
                RETURN_IF_ERROR(finish_query(index.get()));
                // Tells the scan the bitmap is count-shaped, so it may emit the count without
                // iterating rows. Never set on a cache hit or a shared result.
                context->count_on_index_fastpath_hit = true;
                return Status::OK();
            }
        }
    }

    const bool consume_candidates =
            context->candidate_rows != nullptr && consumes_candidates(*leaf);
    const roaring::Roaring* candidates = consume_candidates ? context->candidate_rows : nullptr;
    if (!allow_result_cache || consume_candidates) {
        RETURN_IF_ERROR(
                _run_leaf(context, column_name, *index, *leaf, candidates, scoring, &result));
        // A phrase with a slot the index lacks stops before the candidates: its result is empty
        // for the whole segment, so it may be cached.
        if (consume_candidates && !context->candidate_rows_consumed) {
            DORIS_CHECK(result->isEmpty());
            insert_query_cache(context, cache, cache_key, result, &cache_handler,
                               allow_result_cache);
        }
    } else {
        RETURN_IF_ERROR(run_shared(
                [&](std::shared_ptr<roaring::Roaring>* out) {
                    return _run_leaf(context, column_name, *index, *leaf, nullptr, false, out);
                },
                &result));
    }
    DORIS_CHECK(result != nullptr);
    bit_map = std::move(result);
    return finish_query(index.get());
}

Status TextIndexReader::_count_from_df(const IndexQueryContextPtr& context,
                                       const std::string& column_name, OpenedIndex& index,
                                       const std::string& term, bool* handled,
                                       std::shared_ptr<roaring::Roaring>* out) {
    *handled = false;
    if (_rows_of_segment == 0) {
        return Status::OK();
    }
    std::shared_ptr<roaring::Roaring> nulls;
    const auto read_nulls = [&]() -> Status {
        InvertedIndexQueryCacheHandle handle;
        RETURN_IF_ERROR(_read_null_bitmap(context, &handle, &index));
        nulls = handle.get_bitmap();
        DORIS_CHECK(nulls != nullptr);
        return Status::OK();
    };
    if (_column_is_array) {
        RETURN_IF_ERROR(read_nulls());
        if (!nulls->isEmpty()) {
            return Status::OK();
        }
    }
    uint64_t df = 0;
    uint64_t document_count = 0;
    RETURN_IF_ERROR(_term_document_frequency(column_name, index, term, &df, &document_count));
    // The fabricated ids lie inside the index's document domain, which lies inside the segment;
    // the segment's own row count is the one bound a corrupt image cannot move.
    if (df > document_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "count fast path: term df {} exceeds the index document count {}", df,
                document_count);
    }
    if (document_count > _rows_of_segment) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "count fast path: index document count {} exceeds the segment row count {}",
                document_count, _rows_of_segment);
    }
    auto result = std::make_shared<roaring::Roaring>();
    if (df > 0) {
        if (nulls == nullptr) {
            RETURN_IF_ERROR(read_nulls());
        }
        // The scan subtracts the null bitmap from every result, so the ids avoid the NULL rows and
        // the count stays df. Ids that do not fit belong to a corrupt index, which decoding
        // answers.
        if (nulls->isEmpty()) {
            result->addRange(0, df);
        } else if (!index_query::fabricate_null_disjoint_count_bitmap(df, *nulls, result.get())
                            .ok()) {
            return Status::OK();
        }
    }
    *out = std::move(result);
    *handled = true;
    return Status::OK();
}

namespace {

struct CluceneOpenedIndex : OpenedIndex {
    InvertedIndexCacheHandle handle;
    FulltextIndexSearcherPtr searcher;
};

} // namespace

Status CluceneTextIndexReader::_open_index(const IndexQueryContextPtr& context,
                                           std::unique_ptr<OpenedIndex>* out,
                                           const std::string& index_file_key) {
    auto opened = std::make_unique<CluceneOpenedIndex>();
    try {
        RETURN_IF_ERROR(handle_searcher_cache(context, &opened->handle, index_file_key));
    } catch (const CLuceneError& e) {
        return clucene_error_status(fmt::format("CLuceneError occurred: {}", e.what()));
    }
    auto variant = opened->handle.get_index_searcher();
    auto* fulltext = std::get_if<FulltextIndexSearcherPtr>(&variant);
    // A text index always builds a full-text searcher.
    DORIS_CHECK(fulltext != nullptr);
    opened->searcher = *fulltext;
    *out = std::move(opened);
    return Status::OK();
}

index_query::IndexSourcePtr CluceneTextIndexReader::_bind_source(
        const IndexQueryContextPtr& context, const std::wstring& field, OpenedIndex& index) {
    const auto& searcher = static_cast<CluceneOpenedIndex&>(index).searcher;
    return clucene_index_source(
            std::shared_ptr<lucene::index::IndexReader>(searcher, searcher->getReader()), field,
            context->io_ctx);
}

Status CluceneTextIndexReader::_term_document_frequency(const std::string& column_name,
                                                        OpenedIndex& index, const std::string& term,
                                                        uint64_t* df, uint64_t* document_count) {
    auto* reader = static_cast<CluceneOpenedIndex&>(index).searcher->getReader();
    const std::wstring field = StringUtil::string_to_wstring(column_name);
    const std::wstring text = inverted_index::StringHelper::to_wstring(term);
    try {
        lucene::index::Term key(field.c_str(), text.c_str());
        *df = reader->docFreq(&key);
        // Every row has a document, a NULL row an empty one.
        *document_count = reader->maxDoc();
    } catch (const CLuceneError& e) {
        return clucene_error_status(fmt::format("CLuceneError occurred: {}", e.what()));
    }
    return Status::OK();
}

Status CluceneTextIndexReader::_run_leaf(const IndexQueryContextPtr& context,
                                         const std::string& column_name, OpenedIndex& index,
                                         const logical::Node& leaf,
                                         const roaring::Roaring* candidates, bool scoring,
                                         std::shared_ptr<roaring::Roaring>* out) {
    const bool publish_scores = scoring && leaf.as<logical::Expand>() == nullptr;
    auto result = std::make_shared<roaring::Roaring>();
    RETURN_IF_ERROR(run_clucene_leaf(context, StringUtil::string_to_wstring(column_name), leaf,
                                     candidates, publish_scores,
                                     static_cast<CluceneOpenedIndex&>(index).searcher, result));
    result->runOptimize();
    *out = std::move(result);
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(const IndexQueryContextPtr& context,
                                            const std::string& column_name,
                                            const Field& query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<roaring::Roaring>& bit_map,
                                            const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/) {
    // CLucene indexes do not persist the gram tokenizer contract needed to compile a pattern.
    if (is_gram_query(query_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "{} requires SNII storage format for column {}", query_type_to_string(query_type),
                column_name);
    }
    switch (query_type) {
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::EQUAL_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY:
    case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY:
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY:
        // An untokenized index's own properties analyze a value to itself.
        return TextIndexReader::query(context, column_name, query_value, query_type, bit_map,
                                      nullptr);
    default:
        break;
    }
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);

    std::string search_str = query_value.get<PrimitiveType::TYPE_STRING>();

    // If the written value exceeds ignore_above, it will be written as null.
    // The queried value exceeds ignore_above means the written value cannot be found.
    // The query needs to be downgraded to read from the segment file.
    if (int ignore_above =
                std::stoi(get_parser_ignore_above_value_from_properties(_index_meta.properties()));
        search_str.size() > ignore_above) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }

    VLOG_DEBUG << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    try {
        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        // try to get query bitmap result from cache and return immediately on cache hit
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     search_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
            return Status::OK();
        }

        std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

        // A range query reads no candidate rows, so a flag left by an earlier search is cleared.
        context->candidate_rows_consumed = false;
        auto result = std::make_shared<roaring::Roaring>();
        FulltextIndexSearcherPtr* searcher_ptr = nullptr;
        InvertedIndexCacheHandle inverted_index_cache_handle;
        RETURN_IF_ERROR(handle_searcher_cache(context, &inverted_index_cache_handle));
        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr) {
            switch (query_type) {
            case InvertedIndexQueryType::LESS_THAN_QUERY:
            case InvertedIndexQueryType::LESS_EQUAL_QUERY:
            case InvertedIndexQueryType::GREATER_THAN_QUERY:
            case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
                std::wstring search_str_ws = StringUtil::string_to_wstring(search_str);
                // unique_ptr with custom deleter
                std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term {
                        _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
                        [](lucene::index::Term* term) { _CLDECDELETE(term); }};
                std::unique_ptr<lucene::search::Query> query;

                bool include_upper = query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY;
                bool include_lower = query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY;

                if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
                    query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY) {
                    query = std::make_unique<lucene::search::RangeQuery>(nullptr, term.get(),
                                                                         include_upper);
                } else { // GREATER_THAN_QUERY or GREATER_EQUAL_QUERY
                    query = std::make_unique<lucene::search::RangeQuery>(term.get(), nullptr,
                                                                         include_lower);
                }

                SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_timer);
                (*searcher_ptr)
                        ->_search(query.get(),
                                  [&result](const int32_t docid, const float_t /*score*/) {
                                      result->add(docid);
                                  });
                break;
            }
            default:
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                        "invalid query type when query untokenized inverted index");
            }
        }
        // add to cache (unless a candidate-consuming query made it partial)
        result->runOptimize();
        if (!context->candidate_rows_consumed) {
            insert_query_cache(context, cache, cache_key, result, &cache_handler);
        }

        bit_map = result;
        return Status::OK();
    } catch (const CLuceneError& e) {
        if (is_range_query(query_type) && e.number() == CL_ERR_TooManyClauses) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "range query term exceeds limits, try to downgrade from inverted index, "
                    "column "
                    "name:{}, search_str:{}",
                    column_name, search_str);
        } else {
            LOG(ERROR) << "CLuceneError occurred, error msg: " << e.what()
                       << ", column name: " << column_name << ", search_str: " << search_str;
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occurred, error msg: {}, column name: {}, search_str: {}",
                    e.what(), column_name, search_str);
        }
    }
}

Status StringTypeInvertedIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    if (!is_range_query(query_type)) {
        return TextIndexReader::query_with_null_bitmap(context, column_name, query_value,
                                                       query_type, bit_map,
                                                       null_bitmap_cache_handle, nullptr);
    }
    return InvertedIndexReader::query_with_null_bitmap(context, column_name, query_value,
                                                       query_type, bit_map,
                                                       null_bitmap_cache_handle, analyzer_ctx);
}

Status BkdIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(InvertedIndexReaderType::BKD,
                         dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

template <InvertedIndexQueryType QT>
Status BkdIndexReader::construct_bkd_query_value(const Field& query_value,
                                                 std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                                 InvertedIndexVisitor<QT>* visitor) {
    if constexpr (QT == InvertedIndexQueryType::EQUAL_QUERY) {
        RETURN_IF_ERROR(encode_bkd_field_ascending(_type, query_value, _value_key_coder,
                                                   &visitor->query_max));
        RETURN_IF_ERROR(encode_bkd_field_ascending(_type, query_value, _value_key_coder,
                                                   &visitor->query_min));
    } else if constexpr (QT == InvertedIndexQueryType::LESS_THAN_QUERY ||
                         QT == InvertedIndexQueryType::LESS_EQUAL_QUERY) {
        RETURN_IF_ERROR(encode_bkd_field_ascending(_type, query_value, _value_key_coder,
                                                   &visitor->query_max));
        RETURN_IF_ERROR(encode_bkd_min_ascending(_type, _value_key_coder, &visitor->query_min));
    } else if constexpr (QT == InvertedIndexQueryType::GREATER_THAN_QUERY ||
                         QT == InvertedIndexQueryType::GREATER_EQUAL_QUERY) {
        RETURN_IF_ERROR(encode_bkd_field_ascending(_type, query_value, _value_key_coder,
                                                   &visitor->query_min));
        RETURN_IF_ERROR(encode_bkd_max_ascending(_type, _value_key_coder, &visitor->query_max));
    } else {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "invalid query type when query bkd index");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_try_query(const IndexQueryContextPtr& context,
                                            const Field& query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                            size_t* count) {
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        context->io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        context->io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        context->io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        context->io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                context->io_ctx, r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_query(const IndexQueryContextPtr& context,
                                        const Field& query_value, InvertedIndexQueryType query_type,
                                        std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                        std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_timer);
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        context->io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        context->io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        context->io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        context->io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                context->io_ctx, r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::try_query(const IndexQueryContextPtr& context,
                                 const std::string& column_name, const Field& query_value,
                                 InvertedIndexQueryType query_type, size_t* count) {
    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(context, r);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        RETURN_IF_ERROR(
                encode_bkd_field_ascending(_type, query_value, _value_key_coder, &query_str));

        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        std::shared_ptr<roaring::Roaring> bit_map;
        if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
            *count = bit_map->cardinality();
            return Status::OK();
        }

        return invoke_bkd_try_query(context, query_value, query_type, r, count);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }

    VLOG_DEBUG << "BKD index try search column: " << column_name << " result: " << *count;
    return Status::OK();
}

Status BkdIndexReader::query(const IndexQueryContextPtr& context, const std::string& column_name,
                             const Field& query_value, InvertedIndexQueryType query_type,
                             std::shared_ptr<roaring::Roaring>& bit_map,
                             const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);

    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(context, r);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        RETURN_IF_ERROR(
                encode_bkd_field_ascending(_type, query_value, _value_key_coder, &query_str));

        auto index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map)) {
            return Status::OK();
        }

        RETURN_IF_ERROR(invoke_bkd_query(context, query_value, query_type, r, bit_map));
        bit_map->runOptimize();
        cache->insert(cache_key, bit_map, &cache_handler);

        VLOG_DEBUG << "BKD index search column: " << column_name
                   << " result: " << bit_map->cardinality();

        return Status::OK();
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "BKD Query CLuceneError Occurred, error msg:  " << e.what()
                   << " file_path:" << _index_file_reader->get_index_file_path(&_index_meta);
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }
}

Status BkdIndexReader::get_bkd_reader(const IndexQueryContextPtr& context,
                                      BKDIndexSearcherPtr& bkd_reader) {
    BKDIndexSearcherPtr* bkd_searcher = nullptr;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    RETURN_IF_ERROR(handle_searcher_cache(context, &inverted_index_cache_handle));
    auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
    bkd_searcher = std::get_if<BKDIndexSearcherPtr>(&searcher_variant);
    if (bkd_searcher) {
        _type = (FieldType)(*bkd_searcher)->type;
        if (!is_scalar_type(_type)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "unsupported typeinfo, type={}", (*bkd_searcher)->type);
        }
        _value_key_coder = get_key_coder(_type);
        bkd_reader = *bkd_searcher;
        if (bkd_reader->bytes_per_dim_ == 0) {
            bkd_reader->bytes_per_dim_ = cast_set<int32_t>(field_type_size(_type));
        }
        return Status::OK();
    }
    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "get bkd reader from searcher cache builder error");
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

template <InvertedIndexQueryType QT>
InvertedIndexVisitor<QT>::InvertedIndexVisitor(const void* io_ctx, lucene::util::bkd::bkd_reader* r,
                                               roaring::Roaring* h, bool only_count)
        : _io_ctx(io_ctx), _hits(h), _num_hits(0), _only_count(only_count), _reader(r) {}

template <InvertedIndexQueryType QT>
int InvertedIndexVisitor<QT>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool all_greater_than_max = true;
    bool all_within_range = true;

    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;

        auto result_max = lucene::util::FutureArrays::CompareUnsigned(
                packed_value, offset, offset + _reader->bytes_per_dim_,
                (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

        auto result_min = lucene::util::FutureArrays::CompareUnsigned(
                packed_value, offset, offset + _reader->bytes_per_dim_,
                (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);

        all_greater_than_max &= (result_max > 0);
        all_within_range &= (result_min > 0 && result_max < 0);

        if (!all_greater_than_max && !all_within_range) {
            return -1;
        }
    }

    if (all_greater_than_max) {
        return 1;
    } else if (all_within_range) {
        return 0;
    } else {
        return -1;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    // if query type is equal, query_min == query_max
    if (_reader->num_data_dims_ == 1) {
        return std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                           _reader->bytes_per_dim_);
    } else {
        // if all dim value > matched value, then return > 0, otherwise return < 0
        int return_result = 0;
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result < 0) {
                return -1;
            } else if (result > 0) {
                return_result = 1;
            }
        }
        return return_result;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_max.c_str(),
                                  _reader->bytes_per_dim_);
        if (result >= 0) {
            return 1;
        }
        return 0;
    } else {
        bool all_greater_or_equal = true;
        bool all_lesser = true;

        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

            all_greater_or_equal &=
                    (result >= 0);      // Remains true only if all results are greater or equal
            all_lesser &= (result < 0); // Remains true only if all results are lesser
        }

        // Return 1 if all values are greater or equal, 0 if all are lesser, otherwise -1
        return all_greater_or_equal ? 1 : (all_lesser ? 0 : -1);
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>::matches(uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_max.c_str(),
                                  _reader->bytes_per_dim_);
        if (result > 0) {
            return 1;
        }
        return 0;
    } else {
        bool all_greater = true;
        bool all_lesser_or_equal = true;

        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_max.c_str(), offset, offset + _reader->bytes_per_dim_);

            all_greater &= (result > 0); // Remains true only if all results are greater
            all_lesser_or_equal &=
                    (result <= 0); // Remains true only if all results are lesser or equal
        }

        // Return 1 if all values are greater or equal, 0 if all are lesser, otherwise -1
        return all_greater ? 1 : (all_lesser_or_equal ? 0 : -1);
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>::matches(
        uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                                  _reader->bytes_per_dim_);
        if (result <= 0) {
            return -1;
        }
        return 0;
    } else {
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result <= 0) {
                return -1;
            }
        }
        return 0;
    }
}

template <>
int InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>::matches(
        uint8_t* packed_value) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (_reader->num_data_dims_ == 1) {
        auto result = std::memcmp(packed_value, (const uint8_t*)query_min.c_str(),
                                  _reader->bytes_per_dim_);
        if (result < 0) {
            return -1;
        }
        return 0;
    } else {
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto result = lucene::util::FutureArrays::CompareUnsigned(
                    packed_value, offset, offset + _reader->bytes_per_dim_,
                    (const uint8_t*)query_min.c_str(), offset, offset + _reader->bytes_per_dim_);
            if (result < 0) {
                return -1;
            }
        }
        return 0;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(std::vector<char>& doc_id,
                                     std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    visit(roaring::Roaring::read(doc_id.data(), false));
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    visit(*doc_id);
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring&& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(roaring::Roaring& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(int row_id) {
    if (_only_count) {
        _num_hits++;
    } else {
        _hits->add(row_id);
    }
}

template <InvertedIndexQueryType QT>
void InvertedIndexVisitor<QT>::visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
                                     std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data()) != 0) {
        return;
    }
    int32_t doc_id = iter->docid_set->nextDoc();
    while (doc_id != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
        if (_only_count) {
            _num_hits++;
        } else {
            _hits->add(doc_id);
        }
        doc_id = iter->docid_set->nextDoc();
    }
}

template <InvertedIndexQueryType QT>
int InvertedIndexVisitor<QT>::visit(int row_id, std::vector<uint8_t>& packed_value) {
    auto result = matches(packed_value.data());
    if (result != 0) {
        return result;
    }
    if (_only_count) {
        _num_hits++;
    } else {
        _hits->add(row_id);
    }
    return 0;
}

template <>
lucene::util::bkd::relation InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>::compare(
        std::vector<uint8_t>& min_packed, std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(min_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_max.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) >= 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) >= 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template <>
lucene::util::bkd::relation
InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>::compare(
        std::vector<uint8_t>& min_packed, std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(max_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_min.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) <= 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) >= 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template <InvertedIndexQueryType QT>
lucene::util::bkd::relation InvertedIndexVisitor<QT>::compare_prefix(std::vector<uint8_t>& prefix) {
    const int32_t length = cast_set<int32_t>(prefix.size());
    const uint8_t* data = prefix.data();

    auto cmp = [&](const std::string& bound) {
        return lucene::util::FutureArrays::CompareUnsigned(
                data, 0, length, reinterpret_cast<const uint8_t*>(bound.data()), 0, length);
    };

    int32_t cmpMax = cmp(query_max);
    int32_t cmpMin = cmp(query_min);

    if (cmpMax > 0 || cmpMin < 0) {
        return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
    }
    if (cmpMin > 0 && cmpMax < 0) {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
    return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
}

template <InvertedIndexQueryType QT>
lucene::util::bkd::relation InvertedIndexVisitor<QT>::compare(std::vector<uint8_t>& min_packed,
                                                              std::vector<uint8_t>& max_packed) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    bool crosses = false;
    for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
        int offset = dim * _reader->bytes_per_dim_;
        if (lucene::util::FutureArrays::CompareUnsigned(min_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_max.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) > 0 ||
            lucene::util::FutureArrays::CompareUnsigned(max_packed.data(), offset,
                                                        offset + _reader->bytes_per_dim_,
                                                        (const uint8_t*)query_min.c_str(), offset,
                                                        offset + _reader->bytes_per_dim_) < 0) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        }
        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           min_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_min.c_str(), offset,
                           offset + _reader->bytes_per_dim_) < 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           max_packed.data(), offset, offset + _reader->bytes_per_dim_,
                           (const uint8_t*)query_max.c_str(), offset,
                           offset + _reader->bytes_per_dim_) > 0;
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>;

} // namespace doris::segment_v2
