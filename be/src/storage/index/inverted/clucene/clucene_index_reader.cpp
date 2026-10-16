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

#include "storage/index/inverted/clucene/clucene_index_reader.h"

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
#include "storage/index/index_fs_directory.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/clucene/inverted_index_searcher.h"
#include "storage/index/inverted/clucene/searcher_cache_value.h"
#include "storage/index/inverted/clucene/spi/clucene_index_source.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/common/single_flight.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/query/engine/all_query/all_query.h"
#include "storage/index/inverted/query/engine/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query/engine/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query/engine/boolean_query/listed_terms.h"
#include "storage/index/inverted/query/engine/boolean_query/operator.h"
#include "storage/index/inverted/query/engine/collect/doc_set_collector.h"
#include "storage/index/inverted/query/engine/complete_null_bitmap.h"
#include "storage/index/inverted/query/engine/expand_query/expand_query.h"
#include "storage/index/inverted/query/engine/phrase_prefix_query/phrase_prefix_query.h"
#include "storage/index/inverted/query/engine/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query/engine/phrase_query/phrase_query.h"
#include "storage/index/inverted/query/engine/term_query/term_query.h"
#include "storage/index/inverted/query/exec/docid_set_ops.h"
#include "storage/index/inverted/query/exec/roaring_docid_sink.h"
#include "storage/index/inverted/query/exec/term_pattern.h"
#include "storage/index/inverted/query/logical/match_lowering.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/storage_layout.h"
#include "storage/types.h"
#include "util/defer_op.h"
#include "util/faststring.h"

#define FINALIZE_INPUT(x) \
    if (x != nullptr) {   \
        x->close();       \
        _CLDELETE(x);     \
    }
#define FINALLY_FINALIZE_INPUT(x) \
    try {                         \
        FINALIZE_INPUT(x)         \
    } catch (...) {               \
    }

namespace {

// Convert compute-type limits to storage values so BKD sentinels use the same encoding
// as indexed values.
template <doris::FieldType FT>
void bkd_encode_min(const doris::KeyCoder* coder, std::string* out) {
    using compute_t =
            typename doris::PrimitiveTypeTraits<doris::storage_field_type_to_primitive_type(
                    FT)>::CppType;
    auto compute_v = doris::type_limit<compute_t>::min();
    auto v = doris::StorageLayout<FT>::to_storage(compute_v);
    coder->full_encode_ascending(&v, out);
}

template <doris::FieldType FT>
void bkd_encode_max(const doris::KeyCoder* coder, std::string* out) {
    using compute_t =
            typename doris::PrimitiveTypeTraits<doris::storage_field_type_to_primitive_type(
                    FT)>::CppType;
    auto compute_v = doris::type_limit<compute_t>::max();
    auto v = doris::StorageLayout<FT>::to_storage(compute_v);
    coder->full_encode_ascending(&v, out);
}

doris::Status encode_bkd_min_ascending(doris::FieldType ft, const doris::KeyCoder* coder,
                                       std::string* out) {
#define CASE(FT)                                          \
    case doris::FieldType::FT:                            \
        bkd_encode_min<doris::FieldType::FT>(coder, out); \
        return doris::Status::OK();
    switch (ft) {
        DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(CASE)
    default:
        break;
    }
#undef CASE
    return doris::Status::InternalError("unsupported BKD field type {}", static_cast<int>(ft));
}

doris::Status encode_bkd_max_ascending(doris::FieldType ft, const doris::KeyCoder* coder,
                                       std::string* out) {
#define CASE(FT)                                          \
    case doris::FieldType::FT:                            \
        bkd_encode_max<doris::FieldType::FT>(coder, out); \
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

namespace logical = index_query::logical;

namespace {

Status read_clucene_null_bitmap(InvertedIndexReader& reader, const IndexQueryContextPtr& context,
                                InvertedIndexQueryCacheHandle* cache_handle,
                                lucene::store::Directory* dir) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_query_null_bitmap_timer);
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    bool owned_dir = false;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        auto index_file_key =
                reader.get_index_file_reader()->get_index_file_cache_key(&reader.get_index_meta());
        InvertedIndexQueryCache::CacheKey cache_key {
                index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
        auto* cache = InvertedIndexQueryCache::instance();
        if (cache->lookup(cache_key, cache_handle)) {
            return Status::OK();
        }

        if (!dir) {
            // TODO: ugly code here, try to refact.
            auto st = reader.get_index_file_reader()->init(config::inverted_index_read_buffer_size,
                                                           context->io_ctx);
            if (!st.ok()) {
                LOG(WARNING) << st;
                return st;
            }
            auto directory = DORIS_TRY(reader.get_index_file_reader()->open(
                    &reader.get_index_meta(), context->io_ctx));
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

// NOLINTNEXTLINE(readability-function-cognitive-complexity) Cache lookup and opening share this flow.
Status handle_clucene_searcher_cache(InvertedIndexReader& reader,
                                     const IndexQueryContextPtr& context,
                                     InvertedIndexCacheHandle* inverted_index_cache_handle,
                                     const std::string& index_file_key = {}) {
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(
            index_file_key.empty() ? reader.get_index_file_reader()->get_index_file_cache_key(
                                             &reader.get_index_meta())
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
        auto st = reader.get_index_file_reader()->init(config::inverted_index_read_buffer_size,
                                                       context->io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return st;
        }
        auto dir = DORIS_TRY(
                reader.get_index_file_reader()->open(&reader.get_index_meta(), context->io_ctx));

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
        RETURN_IF_ERROR(
                read_clucene_null_bitmap(reader, context, &null_bitmap_cache_handle, dir.get()));
        size_t reader_size = 0;
        auto index_searcher_builder =
                DORIS_TRY(IndexSearcherBuilder::create_index_searcher_builder(reader.type()));
        RETURN_IF_ERROR(create_index_searcher(index_searcher_builder.get(), dir.get(), &searcher,
                                              reader_size));
        auto* cache_value =
                new CluceneSearcherCacheValue(std::move(searcher), reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       inverted_index_cache_handle);
        return Status::OK();
    }
}

} // namespace

Status CluceneTextIndexReader::handle_searcher_cache(const IndexQueryContextPtr& context,
                                                     InvertedIndexCacheHandle* cache_handle,
                                                     const std::string& index_file_key) {
    return handle_clucene_searcher_cache(*this, context, cache_handle, index_file_key);
}

Status CluceneTextIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                                InvertedIndexQueryCacheHandle* cache_handle) {
    return read_clucene_null_bitmap(*this, context, cache_handle, nullptr);
}

Status BkdIndexReader::read_null_bitmap(const IndexQueryContextPtr& context,
                                        InvertedIndexQueryCacheHandle* cache_handle) {
    return read_clucene_null_bitmap(*this, context, cache_handle, nullptr);
}

Status create_index_searcher(IndexSearcherBuilder* index_searcher_builder,
                             lucene::store::Directory* dir, IndexSearcherPtr* searcher,
                             size_t& reader_size) {
    auto searcher_result = DORIS_TRY(index_searcher_builder->get_index_searcher(dir));
    *searcher = searcher_result;

    // When the meta information has been read, the ioContext needs to be reset to prevent it from being used by other queries.
    auto* stream = static_cast<DorisCompoundReader*>(dir)->getDorisIndexInput();
    stream->setIoContext(nullptr);
    stream->setIndexFile(false);

    reader_size = index_searcher_builder->get_reader_size();
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
    auto variant = opened->handle.get<CluceneSearcherCacheValue>()->searcher;
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

// NOLINTNEXTLINE(readability-function-size) This method dispatches the existing query types.
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
        auto searcher_variant =
                inverted_index_cache_handle.get<CluceneSearcherCacheValue>()->searcher;
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
    RETURN_IF_ERROR(handle_clucene_searcher_cache(*this, context, &inverted_index_cache_handle));
    auto searcher_variant = inverted_index_cache_handle.get<CluceneSearcherCacheValue>()->searcher;
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
        if (all_greater_or_equal) {
            return 1;
        }
        return all_lesser ? 0 : -1;
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
        if (all_greater) {
            return 1;
        }
        return all_lesser_or_equal ? 0 : -1;
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
    const auto length = cast_set<int32_t>(prefix.size());
    const uint8_t* data = prefix.data();

    auto cmp = [&](const std::string& bound) {
        return lucene::util::FutureArrays::CompareUnsigned(
                data, 0, length, reinterpret_cast<const uint8_t*>(bound.data()), 0, length);
    };

    auto cmpMax = cmp(query_max);
    auto cmpMin = cmp(query_min);

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
