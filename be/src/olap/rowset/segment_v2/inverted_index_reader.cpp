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

#include "olap/rowset/segment_v2/inverted_index_reader.h"

#include <CLucene/analysis/AnalysisHeader.h>
#include <CLucene/analysis/Analyzers.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/IndexSearcher.h>
#include <CLucene/search/Query.h>
#include <CLucene/search/RangeQuery.h>
#include <CLucene/search/TermQuery.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/util/CLStreams.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/bkd/bkd_docid_iterator.h>
#include <CLucene/util/stringUtil.h>

#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>

#include "gutil/integral_types.h"
#include "inverted_index_query_type.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_factory.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"
#include "olap/types.h"
#include "runtime/runtime_state.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris::segment_v2 {

template <PrimitiveType PT>
Status InvertedIndexQueryParamFactory::create_query_value(
        const void* value, std::unique_ptr<InvertedIndexQueryParamFactory>& result_param) {
    using CPP_TYPE = typename PrimitiveTypeTraits<PT>::CppType;
    std::unique_ptr<InvertedIndexQueryParam<PT>> param =
            InvertedIndexQueryParam<PT>::create_unique();
    auto&& storage_val = PrimitiveTypeConvertor<PT>::to_storage_field_type(
            *reinterpret_cast<const CPP_TYPE*>(value));
    param->set_value(&storage_val);
    result_param = std::move(param);
    return Status::OK();
};

#define CREATE_QUERY_VALUE_TEMPLATE(PT)                                     \
    template Status InvertedIndexQueryParamFactory::create_query_value<PT>( \
            const void* value, std::unique_ptr<InvertedIndexQueryParamFactory>& result_param);

CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_BOOLEAN)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_TINYINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_SMALLINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_INT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_BIGINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_LARGEINT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_FLOAT)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DOUBLE)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_VARCHAR)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATE)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATEV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATETIME)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DATETIMEV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_CHAR)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMALV2)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL32)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL64)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL128I)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_DECIMAL256)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_HLL)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_STRING)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_IPV4)
CREATE_QUERY_VALUE_TEMPLATE(PrimitiveType::TYPE_IPV6)

std::unique_ptr<lucene::analysis::Analyzer> InvertedIndexReader::create_analyzer(
        InvertedIndexCtx* inverted_index_ctx) {
    std::unique_ptr<lucene::analysis::Analyzer> analyzer;
    auto analyser_type = inverted_index_ctx->parser_type;
    if (analyser_type == InvertedIndexParserType::PARSER_STANDARD ||
        analyser_type == InvertedIndexParserType::PARSER_UNICODE) {
        analyzer = std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
    } else if (analyser_type == InvertedIndexParserType::PARSER_ENGLISH) {
        analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
    } else if (analyser_type == InvertedIndexParserType::PARSER_CHINESE) {
        auto chinese_analyzer =
                std::make_unique<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        chinese_analyzer->initDict(config::inverted_index_dict_path);
        auto mode = inverted_index_ctx->parser_mode;
        if (mode == INVERTED_INDEX_PARSER_COARSE_GRANULARITY) {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
        } else {
            chinese_analyzer->setMode(lucene::analysis::AnalyzerMode::All);
        }
        analyzer = std::move(chinese_analyzer);
    } else {
        // default
        analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer<char>>();
    }
    return analyzer;
}

std::unique_ptr<lucene::util::Reader> InvertedIndexReader::create_reader(
        InvertedIndexCtx* inverted_index_ctx, const std::string& value) {
    std::unique_ptr<lucene::util::Reader> reader =
            std::make_unique<lucene::util::SStringReader<char>>();
    CharFilterMap& char_filter_map = inverted_index_ctx->char_filter_map;
    if (!char_filter_map.empty()) {
        reader = std::unique_ptr<lucene::util::Reader>(CharFilterFactory::create(
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE], reader.release(),
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN],
                char_filter_map[INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT]));
    }
    reader->init(value.data(), value.size(), true);
    return reader;
}

void InvertedIndexReader::get_analyse_result(std::vector<std::string>& analyse_result,
                                             lucene::util::Reader* reader,
                                             lucene::analysis::Analyzer* analyzer,
                                             const std::string& field_name,
                                             InvertedIndexQueryType query_type,
                                             bool drop_duplicates) {
    analyse_result.clear();

    std::wstring field_ws = StringUtil::string_to_wstring(field_name);
    std::unique_ptr<lucene::analysis::TokenStream> token_stream(
            analyzer->tokenStream(field_ws.c_str(), reader));

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if (token.termLength<char>() != 0) {
            analyse_result.emplace_back(token.termBuffer<char>(), token.termLength<char>());
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    if (drop_duplicates && (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
                            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY)) {
        std::set<std::string> unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }
}

Status InvertedIndexReader::read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                                             lucene::store::Directory* dir) {
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    bool owned_dir = false;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {
                index_file_key, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
        auto* cache = InvertedIndexQueryCache::instance();
        if (cache->lookup(cache_key, cache_handle)) {
            return Status::OK();
        }

        if (!dir) {
            // TODO: ugly code here, try to refact.
            auto directory = DORIS_TRY(_inverted_index_file_reader->open(&_index_meta));
            dir = directory.release();
            owned_dir = true;
        }

        // ownership of null_bitmap and its deletion will be transfered to cache
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        const char* null_bitmap_file_name =
                InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        if (dir->fileExists(null_bitmap_file_name)) {
            null_bitmap_in = dir->openInput(null_bitmap_file_name);
            size_t null_bitmap_size = null_bitmap_in->length();
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            cache->insert(cache_key, null_bitmap, cache_handle);
            FINALIZE_INPUT(null_bitmap_in);
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

Status InvertedIndexReader::handle_searcher_cache(
        InvertedIndexCacheHandle* inverted_index_cache_handle, OlapReaderStatistics* stats) {
    auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    if (InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                       inverted_index_cache_handle)) {
        return Status::OK();
    } else {
        // searcher cache miss
        auto mem_tracker = std::make_unique<MemTracker>("InvertedIndexSearcherCacheWithRead");
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_open_timer);
        IndexSearcherPtr searcher;

        auto dir = DORIS_TRY(_inverted_index_file_reader->open(&_index_meta));
        // try to reuse index_searcher's directory to read null_bitmap to cache
        // to avoid open directory additionally for null_bitmap
        // TODO: handle null bitmap procedure in new format.
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        static_cast<void>(read_null_bitmap(&null_bitmap_cache_handle, dir.get()));
        RETURN_IF_ERROR(create_index_searcher(dir.release(), &searcher, mem_tracker.get(), type()));
        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
                std::move(searcher), mem_tracker->consumption(), UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       inverted_index_cache_handle);
        return Status::OK();
    }
}

Status InvertedIndexReader::create_index_searcher(lucene::store::Directory* dir,
                                                  IndexSearcherPtr* searcher,
                                                  MemTracker* mem_tracker,
                                                  InvertedIndexReaderType reader_type) {
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
    auto index_searcher_builder =
            DORIS_TRY(IndexSearcherBuilder::create_index_searcher_builder(reader_type));

    auto searcher_result = DORIS_TRY(index_searcher_builder->get_index_searcher(dir));
    *searcher = searcher_result;
    if (std::string(dir->getObjectName()) == "DorisCompoundReader") {
        static_cast<DorisCompoundReader*>(dir)->getDorisIndexInput()->setIdxFileCache(false);
    }
    // NOTE: before mem_tracker hook becomes active, we caculate reader memory size by hand.
    mem_tracker->consume(index_searcher_builder->get_reader_size());
    return Status::OK();
};

Status FullTextIndexReader::new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                         std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status FullTextIndexReader::query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                  const std::string& column_name, const void* query_value,
                                  InvertedIndexQueryType query_type,
                                  std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    std::string search_str = reinterpret_cast<const StringRef*>(query_value)->to_string();
    VLOG_DEBUG << column_name << " begin to search the fulltext index from clucene, query_str ["
               << search_str << "]";

    try {
        InvertedIndexQueryInfo query_info;
        InvertedIndexQueryCache::CacheKey cache_key;
        auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);

        if (query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY) {
            cache_key = {index_file_key, column_name, query_type, search_str};
            query_info.terms.emplace_back(search_str);
        } else {
            if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
                PhraseQuery::parser_slop(search_str, query_info);
            }

            InvertedIndexCtxSPtr inverted_index_ctx = std::make_shared<InvertedIndexCtx>(
                    get_inverted_index_parser_type_from_string(
                            get_parser_string_from_properties(_index_meta.properties())),
                    get_parser_mode_string_from_properties(_index_meta.properties()),
                    get_parser_char_filter_map_from_properties(_index_meta.properties()));
            auto analyzer = create_analyzer(inverted_index_ctx.get());
            setup_analyzer_lowercase(analyzer, _index_meta.properties());
            setup_analyzer_use_stopwords(analyzer, _index_meta.properties());
            inverted_index_ctx->analyzer = analyzer.get();
            auto reader = create_reader(inverted_index_ctx.get(), search_str);
            get_analyse_result(query_info.terms, reader.get(), analyzer.get(), column_name,
                               query_type);
        }
        if (query_info.terms.empty()) {
            auto msg = fmt::format(
                    "token parser result is empty for query, "
                    "please check your query: '{}' and index parser: '{}'",
                    search_str, get_parser_string_from_properties(_index_meta.properties()));
            if (is_match_query(query_type)) {
                LOG(WARNING) << msg;
                return Status::OK();
            } else {
                return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
            }
        }

        std::unique_ptr<lucene::search::Query> query;
        query_info.field_name = StringUtil::string_to_wstring(column_name);

        if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::EQUAL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ANY_QUERY) {
            std::string str_tokens = join(query_info.terms, " ");
            if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
                str_tokens += " " + std::to_string(query_info.slop);
                str_tokens += " " + std::to_string(query_info.ordered);
            }
            cache_key = {index_file_key, column_name, query_type, str_tokens};
        }
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;

        std::shared_ptr<roaring::Roaring> term_match_bitmap = nullptr;
        auto cache_status = handle_query_cache(cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            return Status::OK();
        }
        FulltextIndexSearcherPtr* searcher_ptr = nullptr;

        InvertedIndexCacheHandle inverted_index_cache_handle;
        RETURN_IF_ERROR(handle_searcher_cache(&inverted_index_cache_handle, stats));
        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr) {
            term_match_bitmap = std::make_shared<roaring::Roaring>();
            RETURN_IF_ERROR(match_index_search(stats, runtime_state, query_type, query_info,
                                               *searcher_ptr, term_match_bitmap));
            term_match_bitmap->runOptimize();
            cache->insert(cache_key, term_match_bitmap, &cache_handler);
            bit_map = term_match_bitmap;
        }
        return Status::OK();
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}", e.what());
    }
}

Status FullTextIndexReader::match_index_search(
        OlapReaderStatistics* stats, RuntimeState* runtime_state, InvertedIndexQueryType query_type,
        const InvertedIndexQueryInfo& query_info, const FulltextIndexSearcherPtr& index_searcher,
        const std::shared_ptr<roaring::Roaring>& term_match_bitmap) {
    TQueryOptions queryOptions = runtime_state->query_options();
    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        auto query = QueryFactory::create(query_type, index_searcher, queryOptions);
        if (!query) {
            return Status::Error<ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>(
                    "query type " + query_type_to_string(query_type) + ", query is nullptr");
        }
        query->add(query_info);
        query->search(*term_match_bitmap);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    }
    return Status::OK();
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

void FullTextIndexReader::setup_analyzer_lowercase(
        std::unique_ptr<lucene::analysis::Analyzer>& analyzer,
        const std::map<string, string>& properties) {
    auto lowercase = get_parser_lowercase_from_properties(properties);
    if (lowercase == INVERTED_INDEX_PARSER_TRUE) {
        analyzer->set_lowercase(true);
    } else if (lowercase == INVERTED_INDEX_PARSER_FALSE) {
        analyzer->set_lowercase(false);
    }
}

void FullTextIndexReader::setup_analyzer_use_stopwords(
        std::unique_ptr<lucene::analysis::Analyzer>& analyzer,
        const std::map<string, string>& properties) {
    auto stop_words = get_parser_stopwords_from_properties(properties);
    if (stop_words == "none") {
        analyzer->set_stopwords(nullptr);
    } else {
        analyzer->set_stopwords(&lucene::analysis::standard95::stop_words);
    }
}

Status StringTypeInvertedIndexReader::new_iterator(
        OlapReaderStatistics* stats, RuntimeState* runtime_state,
        std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(OlapReaderStatistics* stats,
                                            RuntimeState* runtime_state,
                                            const std::string& column_name, const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    const auto* search_query = reinterpret_cast<const StringRef*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);

    // If the written value exceeds ignore_above, it will be written as null.
    // The queried value exceeds ignore_above means the written value cannot be found.
    // The query needs to be downgraded to read from the segment file.
    if (int ignore_above =
                std::stoi(get_parser_ignore_above_value_from_properties(_index_meta.properties()));
        act_len > ignore_above) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }

    std::string search_str(search_query->data, act_len);
    VLOG_DEBUG << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);
    std::wstring search_str_ws = StringUtil::string_to_wstring(search_str);
    // unique_ptr with custom deleter
    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term {
            _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
            [](lucene::index::Term* term) { _CLDECDELETE(term); }};
    std::unique_ptr<lucene::search::Query> query;

    auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);

    // try to get query bitmap result from cache and return immediately on cache hit
    InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                 search_str};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;

    auto cache_status = handle_query_cache(cache, cache_key, &cache_handler, stats, bit_map);
    if (cache_status.ok()) {
        return Status::OK();
    }

    roaring::Roaring result;
    FulltextIndexSearcherPtr* searcher_ptr = nullptr;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    RETURN_IF_ERROR(handle_searcher_cache(&inverted_index_cache_handle, stats));
    auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
    searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
    if (searcher_ptr != nullptr) {
        try {
            switch (query_type) {
            case InvertedIndexQueryType::MATCH_ANY_QUERY:
            case InvertedIndexQueryType::MATCH_ALL_QUERY:
            case InvertedIndexQueryType::EQUAL_QUERY: {
                query = std::make_unique<lucene::search::TermQuery>(term.get());
                SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                (*searcher_ptr)->_search(query.get(), [&result](DocRange* doc_range) {
                    if (doc_range->type_ == DocRangeType::kMany) {
                        result.addMany(doc_range->doc_many_size_, doc_range->doc_many->data());
                    } else {
                        result.addRange(doc_range->doc_range.first, doc_range->doc_range.second);
                    }
                });
                break;
            }
            case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
                query = std::make_unique<lucene::search::TermQuery>(term.get());
                SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                (*searcher_ptr)
                        ->_search(query.get(),
                                  [&result](const int32_t docid, const float_t /*score*/) {
                                      // docid equal to rowid in segment
                                      result.add(docid);
                                  });
                break;
            }

            case InvertedIndexQueryType::LESS_THAN_QUERY:
            case InvertedIndexQueryType::LESS_EQUAL_QUERY:
            case InvertedIndexQueryType::GREATER_THAN_QUERY:
            case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
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

                SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                (*searcher_ptr)
                        ->_search(query.get(),
                                  [&result](const int32_t docid, const float_t /*score*/) {
                                      result.add(docid);
                                  });
                break;
            }
            default:
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                        "invalid query type when query untokenized inverted index");
            }
        } catch (const CLuceneError& e) {
            if (is_range_query(query_type) && e.number() == CL_ERR_TooManyClauses) {
                return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                        "range query term exceeds limits, try to downgrade from inverted index, "
                        "column "
                        "name:{}, search_str:{}",
                        column_name, search_str);
            } else {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occured, error msg: {}, column name: {}, search_str: {}",
                        e.what(), column_name, search_str);
            }
        }

        // add to cache
        std::shared_ptr<roaring::Roaring> term_match_bitmap =
                std::make_shared<roaring::Roaring>(result);
        term_match_bitmap->runOptimize();
        cache->insert(cache_key, term_match_bitmap, &cache_handler);

        bit_map = term_match_bitmap;
    }
    return Status::OK();
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

Status BkdIndexReader::new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                    std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

template <InvertedIndexQueryType QT>
Status BkdIndexReader::construct_bkd_query_value(const void* query_value,
                                                 std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                                 InvertedIndexVisitor<QT>* visitor) {
    std::vector<char> tmp(r->bytes_per_dim_);
    if constexpr (QT == InvertedIndexQueryType::EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_max);
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_min);
    } else if constexpr (QT == InvertedIndexQueryType::LESS_THAN_QUERY ||
                         QT == InvertedIndexQueryType::LESS_EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_max);
        _type_info->set_to_min(tmp.data());
        _value_key_coder->full_encode_ascending(tmp.data(), &visitor->query_min);
    } else if constexpr (QT == InvertedIndexQueryType::GREATER_THAN_QUERY ||
                         QT == InvertedIndexQueryType::GREATER_EQUAL_QUERY) {
        _value_key_coder->full_encode_ascending(query_value, &visitor->query_min);
        _type_info->set_to_max(tmp.data());
        _value_key_coder->full_encode_ascending(tmp.data(), &visitor->query_max);
    } else {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "invalid query type when query bkd index");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_try_query(const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                            uint32_t* count) {
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                r.get(), nullptr, true);
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        *count = r->estimate_point_count(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::invoke_bkd_query(const void* query_value, InvertedIndexQueryType query_type,
                                        std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                        std::shared_ptr<roaring::Roaring>& bit_map) {
    switch (query_type) {
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>>(
                        r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>>(
                        r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>>(
                        r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        auto visitor =
                std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>>(
                        r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        auto visitor = std::make_unique<InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>>(
                r.get(), bit_map.get());
        RETURN_IF_ERROR(construct_bkd_query_value(query_value, r, visitor.get()));
        r->intersect(visitor.get());
        break;
    }
    default:
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>("Invalid query type");
    }
    return Status::OK();
}

Status BkdIndexReader::try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                 const void* query_value, InvertedIndexQueryType query_type,
                                 uint32_t* count) {
    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(r, stats);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _inverted_index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        _value_key_coder->full_encode_ascending(query_value, &query_str);

        auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        std::shared_ptr<roaring::Roaring> bit_map;
        auto cache_status = handle_query_cache(cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            *count = bit_map->cardinality();
            return Status::OK();
        }

        return invoke_bkd_try_query(query_value, query_type, r, count);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }

    VLOG_DEBUG << "BKD index try search column: " << column_name << " result: " << *count;
    return Status::OK();
}

Status BkdIndexReader::query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                             const std::string& column_name, const void* query_value,
                             InvertedIndexQueryType query_type,
                             std::shared_ptr<roaring::Roaring>& bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    try {
        std::shared_ptr<lucene::util::bkd::bkd_reader> r;
        auto st = get_bkd_reader(r, stats);
        if (!st.ok()) {
            LOG(WARNING) << "get bkd reader for  "
                         << _inverted_index_file_reader->get_index_file_path(&_index_meta)
                         << " failed: " << st;
            return st;
        }
        std::string query_str;
        _value_key_coder->full_encode_ascending(query_value, &query_str);

        auto index_file_key = _inverted_index_file_reader->get_index_file_cache_key(&_index_meta);
        InvertedIndexQueryCache::CacheKey cache_key {index_file_key, column_name, query_type,
                                                     query_str};
        auto* cache = InvertedIndexQueryCache::instance();
        InvertedIndexQueryCacheHandle cache_handler;
        auto cache_status = handle_query_cache(cache, cache_key, &cache_handler, stats, bit_map);
        if (cache_status.ok()) {
            return Status::OK();
        }

        RETURN_IF_ERROR(invoke_bkd_query(query_value, query_type, r, bit_map));
        bit_map->runOptimize();
        cache->insert(cache_key, bit_map, &cache_handler);

        VLOG_DEBUG << "BKD index search column: " << column_name
                   << " result: " << bit_map->cardinality();

        return Status::OK();
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "BKD Query CLuceneError Occurred, error msg:  " << e.what() << " file_path:"
                   << _inverted_index_file_reader->get_index_file_path(&_index_meta);
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }
}

Status BkdIndexReader::get_bkd_reader(BKDIndexSearcherPtr& bkd_reader,
                                      OlapReaderStatistics* stats) {
    BKDIndexSearcherPtr* bkd_searcher = nullptr;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    RETURN_IF_ERROR(handle_searcher_cache(&inverted_index_cache_handle, stats));
    auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
    bkd_searcher = std::get_if<BKDIndexSearcherPtr>(&searcher_variant);
    if (bkd_searcher) {
        _type_info = get_scalar_type_info((FieldType)(*bkd_searcher)->type);
        if (_type_info == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "unsupported typeinfo, type={}", (*bkd_searcher)->type);
        }
        _value_key_coder = get_key_coder(_type_info->type());
        bkd_reader = *bkd_searcher;
        if (bkd_reader->bytes_per_dim_ == 0) {
            bkd_reader->bytes_per_dim_ = _type_info->size();
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
InvertedIndexVisitor<QT>::InvertedIndexVisitor(lucene::util::bkd::bkd_reader* r,
                                               roaring::Roaring* h, bool only_count)
        : _hits(h), _num_hits(0), _only_count(only_count), _reader(r) {}

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
    if (lucene::util::FutureArrays::CompareUnsigned(prefix.data(), 0, prefix.size(),
                                                    (const uint8_t*)query_max.c_str(), 0,
                                                    prefix.size()) > 0 ||
        lucene::util::FutureArrays::CompareUnsigned(prefix.data(), 0, prefix.size(),
                                                    (const uint8_t*)query_min.c_str(), 0,
                                                    prefix.size()) < 0) {
        return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
    }
    if (lucene::util::FutureArrays::CompareUnsigned(prefix.data(), 0, prefix.size(),
                                                    (const uint8_t*)query_min.c_str(), 0,
                                                    prefix.size()) > 0 &&
        lucene::util::FutureArrays::CompareUnsigned(prefix.data(), 0, prefix.size(),
                                                    (const uint8_t*)query_max.c_str(), 0,
                                                    prefix.size()) < 0) {
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

Status InvertedIndexIterator::read_from_inverted_index(
        const std::string& column_name, const void* query_value, InvertedIndexQueryType query_type,
        uint32_t segment_num_rows, std::shared_ptr<roaring::Roaring>& bit_map, bool skip_try) {
    if (UNLIKELY(_reader == nullptr)) {
        throw CLuceneError(CL_ERR_NullPointer, "bkd index reader is null", false);
    }
    if (!skip_try && _reader->type() == InvertedIndexReaderType::BKD) {
        if (_runtime_state != nullptr &&
            _runtime_state->query_options().inverted_index_skip_threshold > 0 &&
            _runtime_state->query_options().inverted_index_skip_threshold < 100) {
            auto query_bkd_limit_percent =
                    _runtime_state->query_options().inverted_index_skip_threshold;
            uint32_t hit_count = 0;
            RETURN_IF_ERROR(
                    try_read_from_inverted_index(column_name, query_value, query_type, &hit_count));
            if (hit_count > segment_num_rows * query_bkd_limit_percent / 100) {
                return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                        "hit count: {}, bkd inverted reached limit {}%, segment num rows:{}",
                        hit_count, query_bkd_limit_percent, segment_num_rows);
            }
        }
    }

    RETURN_IF_ERROR(
            _reader->query(_stats, _runtime_state, column_name, query_value, query_type, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name,
                                                           const void* query_value,
                                                           InvertedIndexQueryType query_type,
                                                           uint32_t* count) {
    // NOTE: only bkd index support try read now.
    if (query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::GREATER_THAN_QUERY ||
        query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY ||
        query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
        query_type == InvertedIndexQueryType::EQUAL_QUERY) {
        RETURN_IF_ERROR(_reader->try_query(_stats, column_name, query_value, query_type, count));
    }
    return Status::OK();
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return _reader->type();
}

const std::map<string, string>& InvertedIndexIterator::get_index_properties() const {
    return _reader->get_index_properties();
}

template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::LESS_EQUAL_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_THAN_QUERY>;
template class InvertedIndexVisitor<InvertedIndexQueryType::GREATER_EQUAL_QUERY>;
} // namespace doris::segment_v2
