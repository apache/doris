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
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/Term.h>
#include <CLucene/search/IndexSearcher.h>
#include <CLucene/search/PhraseQuery.h>
#include <CLucene/search/Query.h>
#include <CLucene/search/TermQuery.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/util/CLStreams.h>
#include <CLucene/util/bkd/bkd_docid_iterator.h>
#include <CLucene/util/stringUtil.h>

#include <filesystem>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/inverted_index_parser.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/disjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/range_query.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/types.h"
#include "runtime/runtime_state.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/time.h"

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

namespace doris::segment_v2 {

bool InvertedIndexReader::indexExists(io::Path& index_file_path) {
    bool exists = false;
    RETURN_IF_ERROR(_fs->exists(index_file_path, &exists));
    return exists;
}

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

std::vector<std::string> InvertedIndexReader::get_analyse_result(
        lucene::util::Reader* reader, lucene::analysis::Analyzer* analyzer,
        const std::string& field_name, InvertedIndexQueryType query_type, bool drop_duplicates) {
    std::vector<std::string> analyse_result;

    std::wstring field_ws = std::wstring(field_name.begin(), field_name.end());
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

    return analyse_result;
}

Status InvertedIndexReader::read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                                             lucene::store::Directory* dir) {
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    bool owned_dir = false;
    try {
        // try to get query bitmap result from cache and return immediately on cache hit
        io::Path path(_path);
        auto index_dir = path.parent_path();
        auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(),
                                                                            _index_meta.index_id());
        auto index_file_path = index_dir / index_file_name;
        InvertedIndexQueryCache::CacheKey cache_key {
                index_file_path, "", InvertedIndexQueryType::UNKNOWN_QUERY, "null_bitmap"};
        auto* cache = InvertedIndexQueryCache::instance();
        if (cache->lookup(cache_key, cache_handle)) {
            return Status::OK();
        }

        if (!dir) {
            dir = new DorisCompoundReader(
                    DorisCompoundDirectory::getDirectory(_fs, index_dir.c_str()),
                    index_file_name.c_str(), config::inverted_index_read_buffer_size);
            owned_dir = true;
        }

        // ownership of null_bitmap and its deletion will be transfered to cache
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        auto null_bitmap_file_name = InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        if (dir->fileExists(null_bitmap_file_name.c_str())) {
            null_bitmap_in = dir->openInput(null_bitmap_file_name.c_str());
            size_t null_bitmap_size = null_bitmap_in->length();
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            cache->insert(cache_key, null_bitmap, cache_handle);
            FINALIZE_INPUT(null_bitmap_in)
        }
        if (owned_dir) {
            FINALIZE_INPUT(dir)
        }
    } catch (CLuceneError& e) {
        FINALLY_FINALIZE_INPUT(null_bitmap_in)
        if (owned_dir) {
            FINALLY_FINALIZE_INPUT(dir)
        }
        return Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index read null bitmap error occurred, reason={}", e.what());
    }

    return Status::OK();
}

Status FullTextIndexReader::new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                         std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status FullTextIndexReader::_query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                   const std::string& column_name, std::string& search_str,
                                   InvertedIndexQueryType query_type, roaring::Roaring* bit_map) {
    LOG(INFO) << column_name << " begin to search the fulltext index from clucene, query_str ["
              << search_str << "]";

    try {
        auto analyzer = create_analyzer(_inverted_index_ctx.get());
        auto reader = create_reader(_inverted_index_ctx.get(), search_str);
        _inverted_index_ctx->analyzer = analyzer.get();
        std::vector<std::string> analyse_result =
                get_analyse_result(reader.get(), analyzer.get(), column_name, query_type);

        if (analyse_result.empty()) {
            auto msg = fmt::format(
                    "token parser result is empty for query, "
                    "please check your query: '{}' and index parser: '{}'",
                    search_str, get_parser_string_from_properties(_index_meta.properties()));
            if (query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
                query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
                query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
                LOG(WARNING) << msg;
                return Status::OK();
            }
            return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
        }

        // check index file existence
        if (!indexExists(_file_full_path)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                    "inverted index path: {} not exist.", _file_full_path.string());
        }

        auto get_index_search = [this, &stats]() {
            InvertedIndexCacheHandle inverted_index_cache_handle;
            static_cast<void>(InvertedIndexSearcherCache::instance()->get_index_searcher(
                    _fs, _file_dir.c_str(), _file_name, &inverted_index_cache_handle, stats));
            return inverted_index_cache_handle.get_index_searcher();
        };

        std::unique_ptr<lucene::search::Query> query;
        std::wstring field_ws = std::wstring(column_name.begin(), column_name.end());

        roaring::Roaring query_match_bitmap;
        bool null_bitmap_already_read = false;
        if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::EQUAL_QUERY) {
            std::string str_tokens;
            for (auto& token : analyse_result) {
                str_tokens += token;
                str_tokens += " ";
            }
            std::shared_ptr<roaring::Roaring> term_match_bitmap = nullptr;
            InvertedIndexQueryCache::CacheKey cache_key {_file_full_path, column_name, query_type,
                                                         str_tokens};
            auto* cache = InvertedIndexQueryCache::instance();
            InvertedIndexQueryCacheHandle cache_handler;
            auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, bit_map);
            if (cache_status.ok()) {
                return Status::OK();
            }
            auto index_searcher = get_index_search();
            term_match_bitmap = std::make_shared<roaring::Roaring>();
            Status res = Status::OK();
            if (query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY) {
                auto* phrase_query = new lucene::search::PhraseQuery();
                for (auto& token : analyse_result) {
                    std::wstring wtoken = StringUtil::string_to_wstring(token);
                    auto* term = _CLNEW lucene::index::Term(field_ws.c_str(), wtoken.c_str());
                    phrase_query->add(term);
                    _CLDECDELETE(term)
                }
                query.reset(phrase_query);
                res = normal_index_search(stats, query_type, index_searcher,
                                          null_bitmap_already_read, query, term_match_bitmap);
            } else {
                res = match_all_index_search(stats, runtime_state, field_ws, analyse_result,
                                             index_searcher, term_match_bitmap);
            }
            if (!res.ok()) {
                return res;
            }

            // add to cache
            term_match_bitmap->runOptimize();
            cache->insert(cache_key, term_match_bitmap, &cache_handler);
            query_match_bitmap = *term_match_bitmap;

        } else {
            bool first = true;
            for (auto token : analyse_result) {
                std::shared_ptr<roaring::Roaring> term_match_bitmap = nullptr;

                // try to get term bitmap match result from cache to avoid query index on cache hit
                auto* cache = InvertedIndexQueryCache::instance();
                // use EQUAL_QUERY type here since cache is for each term/token
                std::wstring token_ws = StringUtil::string_to_wstring(token);
                InvertedIndexQueryCache::CacheKey cache_key {
                        _file_full_path, column_name, InvertedIndexQueryType::EQUAL_QUERY, token};
                InvertedIndexQueryCacheHandle cache_handler;
                auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, bit_map);
                if (cache_status.ok()) {
                    term_match_bitmap = cache_handler.get_bitmap();
                } else {
                    auto index_searcher = get_index_search();
                    term_match_bitmap = std::make_shared<roaring::Roaring>();
                    // unique_ptr with custom deleter
                    std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term {
                            _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str()),
                            [](lucene::index::Term* term) { _CLDECDELETE(term) }};
                    query = std::make_unique<lucene::search::TermQuery>(term.get());

                    Status res =
                            normal_index_search(stats, query_type, index_searcher,
                                                null_bitmap_already_read, query, term_match_bitmap);
                    if (!res.ok()) {
                        return res;
                    }

                    // add to cache
                    term_match_bitmap->runOptimize();
                    cache->insert(cache_key, term_match_bitmap, &cache_handler);
                }

                // add to query_match_bitmap
                if (first) {
                    SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
                    query_match_bitmap = *term_match_bitmap;
                    first = false;
                    continue;
                }

                switch (query_type) {
                case InvertedIndexQueryType::MATCH_ANY_QUERY: {
                    SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_op_timer);
                    query_match_bitmap |= *term_match_bitmap;
                    break;
                }
                default: {
                    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                            "fulltext query do not support query type other than match.");
                }
                }
            }
        }

        bit_map->swap(query_match_bitmap);
        return Status::OK();
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}", e.what());
    }
}

Status FullTextIndexReader::query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                  const std::string& column_name,
                                  InvertedIndexQueryBase* query_value, roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);
    const auto& tmp = static_cast<InvertedIndexPointQueryI*>(query_value);
    auto values = tmp->get_values();
    auto query_type = tmp->get_query_type();
    auto query_bitmap = std::make_shared<roaring::Roaring>();

    for (auto it = values.begin(); it != values.end(); ++it) {
        RETURN_IF_ERROR(
                _query(stats, runtime_state, column_name, *it, query_type, query_bitmap.get()));
        if (it == values.begin()) {
            *bit_map = *query_bitmap;
        } else {
            *bit_map |= *query_bitmap;
        }
    }
    return Status::OK();
}

Status FullTextIndexReader::normal_index_search(
        OlapReaderStatistics* stats, InvertedIndexQueryType query_type,
        const IndexSearcherPtr& index_searcher, bool& null_bitmap_already_read,
        const std::unique_ptr<lucene::search::Query>& query,
        const std::shared_ptr<roaring::Roaring>& term_match_bitmap) {
    check_null_bitmap(index_searcher, null_bitmap_already_read);

    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::EQUAL_QUERY) {
            index_searcher->_search(query.get(), [&term_match_bitmap](DocRange* doc_range) {
                if (doc_range->type_ == DocRangeType::kMany) {
                    term_match_bitmap->addMany(doc_range->doc_many_size_,
                                               doc_range->doc_many->data());
                } else {
                    term_match_bitmap->addRange(doc_range->doc_range.first,
                                                doc_range->doc_range.second);
                }
            });
        } else {
            index_searcher->_search(query.get(), [&term_match_bitmap](const int32_t docid,
                                                                      const float_t /*score*/) {
                // docid equal to rowid in segment
                term_match_bitmap->add(docid);
            });
        }
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    }

    return Status::OK();
}

Status FullTextIndexReader::match_all_index_search(
        OlapReaderStatistics* stats, RuntimeState* runtime_state, const std::wstring& field_ws,
        const std::vector<std::string>& analyse_result, const IndexSearcherPtr& index_searcher,
        const std::shared_ptr<roaring::Roaring>& term_match_bitmap) {
    TQueryOptions queryOptions = runtime_state->query_options();
    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        ConjunctionQuery query(index_searcher->getReader());
        query.set_conjunction_ratio(queryOptions.inverted_index_conjunction_opt_threshold);
        query.add(field_ws, analyse_result);
        query.search(*term_match_bitmap);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    }
    return Status::OK();
}

void FullTextIndexReader::check_null_bitmap(const IndexSearcherPtr& index_searcher,
                                            bool& null_bitmap_already_read) {
    // try to reuse index_searcher's directory to read null_bitmap to cache
    // to avoid open directory additionally for null_bitmap
    if (!null_bitmap_already_read) {
        InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        static_cast<void>(read_null_bitmap(&null_bitmap_cache_handle,
                                           index_searcher->getReader()->directory()));
        null_bitmap_already_read = true;
    }
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

Status StringTypeInvertedIndexReader::new_iterator(
        OlapReaderStatistics* stats, RuntimeState* runtime_state,
        std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status StringTypeInvertedIndexReader::handle_range_query(const std::string& column_name,
                                                         OlapReaderStatistics* stats,
                                                         InvertedIndexRangeQueryI* query,
                                                         roaring::Roaring* bit_map) {
    // std::string search_str = reinterpret_cast<const StringRef*>(query_value)->to_string();
    VLOG_DEBUG << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << query->to_string();

    InvertedIndexQueryCache::CacheKey cache_key {
            _file_full_path, column_name, InvertedIndexQueryType::RANGE_QUERY, query->to_string()};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, bit_map);
    if (cache_status.ok()) {
        return Status::OK();
    }

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    static_cast<void>(InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, _file_dir, _file_name, &inverted_index_cache_handle, stats));
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();
    RangeQuery range_query(index_searcher->getReader());
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());

    RETURN_IF_ERROR(range_query.add(column_name_ws, query));
    if (range_query.get_terms_size() > config::inverted_index_max_terms) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "range query term exceeds limits, try to downgrade from inverted index, column "
                "name:{}, search_str:{}",
                column_name, query->to_string());
    }

    // try to reuse index_searcher's directory to read null_bitmap to cache
    // to avoid open directory additionally for null_bitmap
    InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
    static_cast<void>(
            read_null_bitmap(&null_bitmap_cache_handle, index_searcher->getReader()->directory()));

    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        range_query.search(result);

    } catch (const CLuceneError& e) {
        if (e.number() == CL_ERR_TooManyClauses) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "range query term exceeds limits, try to downgrade from inverted index, column "
                    "name:{}, search_str:{}",
                    column_name, query->to_string());
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}, column name: {}, search_str: {}", e.what(),
                column_name, query->to_string());
    }

    // add to cache
    std::shared_ptr<roaring::Roaring> range_query_bitmap =
            std::make_shared<roaring::Roaring>(result);
    range_query_bitmap->runOptimize();
    cache->insert(cache_key, range_query_bitmap, &cache_handler);

    bit_map->swap(result);
    return Status::OK();
}

Status StringTypeInvertedIndexReader::handle_point_query(const std::string& column_name,
                                                         OlapReaderStatistics* stats,
                                                         InvertedIndexPointQueryI* query,
                                                         roaring::Roaring* bit_map) {
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    auto values = query->get_values();

    InvertedIndexQueryCache::CacheKey cache_key {_file_full_path, column_name,
                                                 query->get_query_type(), query->to_string()};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, bit_map);
    if (cache_status.ok()) {
        return Status::OK();
    }

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    static_cast<void>(InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, _file_dir, _file_name, &inverted_index_cache_handle, stats));
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();
    DisjunctionQuery dis_query(index_searcher->getReader());
    dis_query.add(column_name_ws, values);
    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        dis_query.search(result);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occured, error msg: {}, column name: {}, search_str: {}", e.what(),
                column_name, query->to_string());
    }

    // try to reuse index_searcher's directory to read null_bitmap to cache
    // to avoid open directory additionally for null_bitmap
    InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
    static_cast<void>(
            read_null_bitmap(&null_bitmap_cache_handle, index_searcher->getReader()->directory()));

    // add to cache
    std::shared_ptr<roaring::Roaring> term_match_bitmap =
            std::make_shared<roaring::Roaring>(result);
    term_match_bitmap->runOptimize();
    cache->insert(cache_key, term_match_bitmap, &cache_handler);

    bit_map->swap(result);
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(OlapReaderStatistics* stats,
                                            RuntimeState* runtime_state,
                                            const std::string& column_name,
                                            InvertedIndexQueryBase* query_value,
                                            roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);
    // check index file existence
    if (!indexExists(_file_full_path)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "inverted index path: {} not exist.", _file_full_path.string());
    }
    if (query_value->get_query_category() == QueryCategory::RANGE_QUERY) {
        const auto& tmp = static_cast<InvertedIndexRangeQueryI*>(query_value);
        return handle_range_query(column_name, stats, tmp, bit_map);
    }
    if (query_value->get_query_category() == QueryCategory::POINT_QUERY) {
        const auto& tmp = static_cast<InvertedIndexPointQueryI*>(query_value);
        return handle_point_query(column_name, stats, tmp, bit_map);
    }
    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
            "bkd index query type not support:{}", query_value->get_query_category());
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

BkdIndexReader::BkdIndexReader(io::FileSystemSPtr fs, const std::string& path,
                               const TabletIndex* index_meta)
        : InvertedIndexReader(fs, path, index_meta), _compoundReader(nullptr) {
    if (!indexExists(_file_full_path)) {
        LOG(WARNING) << "bkd index: " << _file_full_path.string() << " not exist.";
        return;
    }
    _compoundReader = std::make_unique<DorisCompoundReader>(
            DorisCompoundDirectory::getDirectory(fs, _file_dir.c_str()), _file_name.c_str(),
            config::inverted_index_read_buffer_size);
}

Status BkdIndexReader::new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                    std::unique_ptr<InvertedIndexIterator>* iterator) {
    *iterator = InvertedIndexIterator::create_unique(stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status BkdIndexReader::try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                 InvertedIndexQueryBase* query_value, uint32_t* count) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);
    SCOPED_RAW_TIMER(&stats->inverted_index_try_query_timer);
    auto visitor = std::make_unique<InvertedIndexVisitor>(nullptr, query_value, true);
    std::shared_ptr<lucene::util::bkd::bkd_reader> r;
    RETURN_IF_ERROR(get_bkd_reader(&r));
    std::string query_str = query_value->to_string();

    auto query_type =
            (query_value->get_query_category() == QueryCategory::RANGE_QUERY)
                    ? InvertedIndexQueryType::RANGE_QUERY
                    : static_cast<InvertedIndexPointQueryI*>(query_value)->get_query_type();
    InvertedIndexQueryCache::CacheKey cache_key {_file_full_path, column_name, query_type,
                                                 query_str + "__TRY__"};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    roaring::Roaring bit_map;
    auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, &bit_map);
    if (cache_status.ok()) {
        *count = bit_map.cardinality();
        return Status::OK();
    }
    try {
        visitor->set_reader(r.get());
        *count = r->estimate_point_count(visitor.get());
        bit_map.addRange(0, *count - 1);
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }
    std::shared_ptr<roaring::Roaring> query_bitmap = std::make_shared<roaring::Roaring>(bit_map);
    query_bitmap->runOptimize();
    cache->insert(cache_key, query_bitmap, &cache_handler);
    VLOG_DEBUG << "BKD index try search column: " << column_name << " result: " << *count;
    return Status::OK();
}

Status BkdIndexReader::query(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                             const std::string& column_name, InvertedIndexQueryBase* query_value,
                             roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);
    auto visitor = std::make_unique<InvertedIndexVisitor>(bit_map, query_value);
    std::shared_ptr<lucene::util::bkd::bkd_reader> r;
    RETURN_IF_ERROR(get_bkd_reader(&r));

    std::string query_str = query_value->to_string();
    auto query_type =
            (query_value->get_query_category() == QueryCategory::RANGE_QUERY)
                    ? InvertedIndexQueryType::RANGE_QUERY
                    : static_cast<InvertedIndexPointQueryI*>(query_value)->get_query_type();

    InvertedIndexQueryCache::CacheKey cache_key {_file_full_path, column_name, query_type,
                                                 query_str};
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    auto cache_status = handle_cache(cache, cache_key, &cache_handler, stats, bit_map);
    if (cache_status.ok()) {
        return Status::OK();
    }

    try {
        visitor->set_reader(r.get());
        SCOPED_RAW_TIMER(&stats->inverted_index_bkd_intersect_timer);
        r->intersect(visitor.get());
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "BKD Query CLuceneError Occurred, error msg: {}", e.what());
    }

    std::shared_ptr<roaring::Roaring> query_bitmap = std::make_shared<roaring::Roaring>(*bit_map);
    query_bitmap->runOptimize();
    cache->insert(cache_key, query_bitmap, &cache_handler);

    VLOG_DEBUG << "BKD index search column: " << column_name
               << " result: " << bit_map->cardinality();

    return Status::OK();
}

Status BkdIndexReader::get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>* bkdReader) {
    // bkd file reader
    if (_compoundReader == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "bkd index input file not found");
    }
    CLuceneError err;
    std::unique_ptr<lucene::store::IndexInput> data_in;
    std::unique_ptr<lucene::store::IndexInput> meta_in;
    std::unique_ptr<lucene::store::IndexInput> index_in;

    if (!_compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name().c_str(), data_in,
                err) ||
        !_compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name().c_str(), meta_in,
                err) ||
        !_compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str(), index_in,
                err)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("bkd index input error: {}",
                                                                       err.what());
    }

    *bkdReader = std::make_shared<lucene::util::bkd::bkd_reader>(data_in.release());
    if (0 == (*bkdReader)->read_meta(meta_in.get())) {
        VLOG_NOTICE << "bkd index file is empty:" << _compoundReader->toString();
        return Status::EndOfFile("bkd index file is empty");
    }

    (*bkdReader)->read_index(index_in.get());

    _type_info = get_scalar_type_info((FieldType)(*bkdReader)->type);
    if (_type_info == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "unsupported typeinfo, type={}", (*bkdReader)->type);
    }
    return Status::OK();
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

InvertedIndexVisitor::InvertedIndexVisitor(roaring::Roaring* hits,
                                           InvertedIndexQueryBase* query_value, bool only_count)
        : _hits(hits),
          _num_hits(0),
          _only_count(only_count),
          _low_op(PredicateType::GT),
          _high_op(PredicateType::LT) {
    if (query_value->get_query_category() == QueryCategory::RANGE_QUERY) {
        auto* range_query = reinterpret_cast<InvertedIndexRangeQueryI*>(query_value);
        query_max = range_query->get_high_value();
        query_min = range_query->get_low_value();
        if (range_query->is_low_value_inclusive()) {
            _low_op = PredicateType::GE;
        }
        if (range_query->is_high_value_inclusive()) {
            _high_op = PredicateType::LE;
        }
    } else if (query_value->get_query_category() == QueryCategory::POINT_QUERY) {
        auto* point_query = reinterpret_cast<InvertedIndexPointQueryI*>(query_value);
        for (const std::string& v : point_query->get_values()) {
            query_points.emplace_back(v);
        }
        // =1 equals 1<= && >=1
        _low_op = PredicateType::GE;
        _high_op = PredicateType::LE;
    }
}

bool InvertedIndexVisitor::_matches(const BinaryType& packed_value, const BinaryType& qmax,
                                    const BinaryType& qmin) {
    bool minInside = (_low_op == PredicateType::GE ? packed_value >= qmin : packed_value > qmin);
    bool maxInside = (_high_op == PredicateType::LE ? packed_value <= qmax : packed_value < qmax);
    return minInside && maxInside;
}

bool InvertedIndexVisitor::matches(uint8_t* packed_value) {
    auto dim_match = [&](const BinaryType& qmax, const BinaryType& qmin) -> bool {
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            if (!_matches(BinaryType(packed_value + offset, _reader->bytes_per_dim_),
                          BinaryType(qmax._data + offset, _reader->bytes_per_dim_),
                          BinaryType(qmin._data + offset, _reader->bytes_per_dim_))) {
                return false;
            }
        }
        return true;
    };
    if (!query_points.empty()) {
        return std::ranges::any_of(query_points, [&dim_match](const BinaryType& query_point) {
            return dim_match(query_point, query_point);
        });
    }
    return dim_match(query_max, query_min);
}

void InvertedIndexVisitor::visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) {
    if (!matches(packed_value.data())) {
        return;
    }
    visit(roaring::Roaring::read(doc_id.data(), false));
}

void InvertedIndexVisitor::visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) {
    if (!matches(packed_value.data())) {
        return;
    }
    visit(*doc_id);
}

void InvertedIndexVisitor::visit(roaring::Roaring&& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

void InvertedIndexVisitor::visit(roaring::Roaring& r) {
    if (_only_count) {
        _num_hits += r.cardinality();
    } else {
        *_hits |= r;
    }
}

void InvertedIndexVisitor::visit(int row_id) {
    if (_only_count) {
        _num_hits++;
    } else {
        _hits->add(row_id);
    }
}

void InvertedIndexVisitor::visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
                                 std::vector<uint8_t>& packed_value) {
    if (!matches(packed_value.data())) {
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

void InvertedIndexVisitor::visit(int row_id, std::vector<uint8_t>& packed_value) {
    if (matches(packed_value.data())) {
        if (_only_count) {
            _num_hits++;
        } else {
            _hits->add(row_id);
        }
    }
}

lucene::util::bkd::relation InvertedIndexVisitor::_compare(const BinaryType& min_packed,
                                                           const BinaryType& max_packed,
                                                           const BinaryType& qmax,
                                                           const BinaryType& qmin) {
    bool minOutside = (_high_op == PredicateType::LE ? min_packed > qmax : min_packed >= qmax);
    bool maxOutside = (_low_op == PredicateType::GE ? max_packed < qmin : max_packed <= qmin);
    bool minInside = (_low_op == PredicateType::GE ? min_packed >= qmin : min_packed > qmin);
    bool maxInside = (_high_op == PredicateType::LE ? max_packed <= qmax : max_packed < qmax);

    if (minOutside || maxOutside) {
        return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
    }

    if (minInside && maxInside) {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }

    return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
}

lucene::util::bkd::relation InvertedIndexVisitor::compare(std::vector<uint8_t>& min_packed,
                                                          std::vector<uint8_t>& max_packed) {
    auto dim_compare = [&](const BinaryType& qmax,
                           const BinaryType& qmin) -> lucene::util::bkd::relation {
        lucene::util::bkd::relation final_relation = lucene::util::bkd::relation::CELL_INSIDE_QUERY;
        for (int dim = 0; dim < _reader->num_data_dims_; dim++) {
            int offset = dim * _reader->bytes_per_dim_;
            auto relation =
                    _compare(BinaryType(min_packed.data() + offset, _reader->bytes_per_dim_),
                             BinaryType(max_packed.data() + offset, _reader->bytes_per_dim_),
                             BinaryType(qmax._data + offset, _reader->bytes_per_dim_),
                             BinaryType(qmin._data + offset, _reader->bytes_per_dim_));
            if (relation == lucene::util::bkd::relation::CELL_OUTSIDE_QUERY) {
                return relation;
            }
            if (relation == lucene::util::bkd::relation::CELL_CROSSES_QUERY) {
                final_relation = lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }
        return final_relation;
    };
    if (!query_points.empty()) {
        lucene::util::bkd::relation final_relation =
                lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        for (auto query_point : query_points) {
            lucene::util::bkd::relation relation = dim_compare(query_point, query_point);
            if (relation == lucene::util::bkd::relation::CELL_INSIDE_QUERY) {
                return relation;
            }
            if (relation == lucene::util::bkd::relation::CELL_CROSSES_QUERY) {
                final_relation = lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }
        return final_relation;
    }
    return dim_compare(query_max, query_min);
}

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name,
                                                       InvertedIndexQueryBase* query_value,
                                                       uint32_t segment_num_rows,
                                                       roaring::Roaring* bit_map, bool skip_try) {
    if (!skip_try && _reader->type() == InvertedIndexReaderType::BKD) {
        auto query_bkd_limit_percent = config::query_bkd_inverted_index_limit_percent;
        uint32_t hit_count = 0;
        RETURN_IF_ERROR(try_read_from_inverted_index(column_name, query_value, &hit_count));
        if (hit_count > segment_num_rows * query_bkd_limit_percent / 100) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "hit count: {}, bkd inverted reached limit {}%, segment num rows:{}", hit_count,
                    query_bkd_limit_percent, segment_num_rows);
        }
    }

    RETURN_IF_ERROR(_reader->query(_stats, _runtime_state, column_name, query_value, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name,
                                                           InvertedIndexQueryBase* query_value,
                                                           uint32_t* count) {
    // NOTE: only bkd index support try read now.
    if (_reader->type() == InvertedIndexReaderType::BKD) {
        RETURN_IF_ERROR(_reader->try_query(_stats, column_name, query_value, count));
    }
    return Status::OK();
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return _reader->type();
}

const std::map<string, string>& InvertedIndexIterator::get_index_properties() const {
    return _reader->get_index_properties();
}

} // namespace doris::segment_v2
