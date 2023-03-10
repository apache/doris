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

#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/search/BooleanQuery.h>
#include <CLucene/search/PhraseQuery.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/NumericUtils.h>

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "io/fs/file_system.h"
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/time.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace segment_v2 {
using CLTermDeleter = void (*)(lucene::index::Term*);
using CLTermType = std::unique_ptr<lucene::index::Term, CLTermDeleter>;
CLTermDeleter termDeleter = [](lucene::index::Term* term) { _CLDECDELETE(term) };

bool InvertedIndexReader::indexExists(io::Path& index_file_path) {
    bool exists = false;
    RETURN_IF_ERROR(_fs->exists(index_file_path, &exists));
    return exists;
}

std::vector<std::wstring> FullTextIndexReader::get_analyse_result(
        const std::wstring& field_name, const std::string& value, InvertedIndexQueryOp query_type,
        InvertedIndexParserType analyser_type) {
    std::vector<std::wstring> analyse_result;
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;
    std::unique_ptr<lucene::util::Reader> reader;
    if (analyser_type == InvertedIndexParserType::PARSER_STANDARD) {
        analyzer = std::make_shared<lucene::analysis::standard::StandardAnalyzer>();
        reader.reset(
                (new lucene::util::StringReader(std::wstring(value.begin(), value.end()).c_str())));
    } else if (analyser_type == InvertedIndexParserType::PARSER_CHINESE) {
        auto chinese_analyzer =
                std::make_shared<lucene::analysis::LanguageBasedAnalyzer>(L"chinese", false);
        chinese_analyzer->initDict(config::inverted_index_dict_path);
        analyzer = chinese_analyzer;
        reader.reset(new lucene::util::SimpleInputStreamReader(
                new lucene::util::AStringReader(value.c_str()),
                lucene::util::SimpleInputStreamReader::UTF8));
    } else {
        // default
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<TCHAR>>();
        reader.reset(
                (new lucene::util::StringReader(std::wstring(value.begin(), value.end()).c_str())));
    }

    std::unique_ptr<lucene::analysis::TokenStream> token_stream(
            analyzer->tokenStream(field_name.c_str(), reader.get()));

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if (token.termLength<TCHAR>() != 0) {
            analyse_result.emplace_back(
                    std::wstring(token.termBuffer<TCHAR>(), token.termLength<TCHAR>()));
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    if (query_type == InvertedIndexQueryOp::MATCH_ANY_QUERY ||
        query_type == InvertedIndexQueryOp::MATCH_ALL_QUERY) {
        std::set<std::wstring> unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }

    return analyse_result;
}

Status FullTextIndexReader::new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                                         InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, stats, this);
    return Status::OK();
}

Status FullTextIndexReader::query_internal(const std::string& search_str,
                                           const std::string& column_name,
                                           InvertedIndexQueryOp query_type,
                                           InvertedIndexParserType analyser_type,
                                           OlapReaderStatistics* stats, roaring::Roaring* bit_map) {
    LOG(INFO) << column_name << " begin to search the fulltext index from clucene, query_str ["
              << search_str << "]";

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _index_id);
    auto index_file_path = index_dir / index_file_name;

    std::unique_ptr<lucene::search::Query> query;
    std::wstring field_ws = std::wstring(column_name.begin(), column_name.end());
    try {
        std::vector<std::wstring> analyse_result =
                get_analyse_result(field_ws, search_str, query_type, analyser_type);

        if (analyse_result.empty()) {
            LOG(WARNING) << "invalid input query_str: " << search_str
                         << ", please check your query sql";
            return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>();
        }

        roaring::Roaring query_match_bitmap;
        bool first = true;
        for (auto token_ws : analyse_result) {
            roaring::Roaring* term_match_bitmap = nullptr;

            // try to get term bitmap match result from cache to avoid query index on cache hit
            auto cache = InvertedIndexQueryCache::instance();
            // use EQUAL_QUERY type here since cache is for each term/token
            InvertedIndexQueryCache::CacheKey cache_key {
                    index_file_path, column_name,
                    "= " + std::string(token_ws.begin(), token_ws.end())};
            InvertedIndexQueryCacheHandle cache_handle;
            if (cache->lookup(cache_key, &cache_handle)) {
                stats->inverted_index_query_cache_hit++;
                term_match_bitmap = cache_handle.match_bitmap();
            } else {
                stats->inverted_index_query_cache_miss++;
                term_match_bitmap = new roaring::Roaring();
                // unique_ptr with custom deleter
                std::unique_ptr<lucene::index::Term, void (*)(lucene::index::Term*)> term {
                        _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str()),
                        [](lucene::index::Term* term) { _CLDECDELETE(term); }};
                query.reset(new lucene::search::TermQuery(term.get()));

                InvertedIndexCacheHandle inverted_index_cache_handle;
                InvertedIndexSearcherCache::instance()->get_index_searcher(
                        _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle,
                        stats);
                auto index_searcher = inverted_index_cache_handle.get_index_searcher();

                try {
                    SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                    index_searcher->_search(
                            query.get(),
                            [&term_match_bitmap](const int32_t docid, const float_t /*score*/) {
                                // docid equal to rowid in segment
                                term_match_bitmap->add(docid);
                            });
                } catch (const CLuceneError& e) {
                    LOG(WARNING) << "CLuceneError occured: " << e.what();
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
                }

                {
                    // add to cache
                    term_match_bitmap->runOptimize();
                    cache->insert(cache_key, term_match_bitmap, &cache_handle);
                }
            }

            // add to query_match_bitmap
            if (first) {
                SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
                query_match_bitmap = *term_match_bitmap;
                first = false;
                continue;
            }

            switch (query_type) {
            case InvertedIndexQueryOp::MATCH_ANY_QUERY: {
                SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_op_timer);
                query_match_bitmap |= *term_match_bitmap;
                break;
            }
            case InvertedIndexQueryOp::EQUAL_QUERY:
            case InvertedIndexQueryOp::MATCH_ALL_QUERY: {
                SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_op_timer);
                query_match_bitmap &= *term_match_bitmap;
                break;
            }
            case InvertedIndexQueryOp::MATCH_PHRASE_QUERY: {
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
                break;
            }
            default: {
                LOG(ERROR) << "fulltext query do not support query type other "
                              "than match.";
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
            }
            }
        }

        bit_map->swap(query_match_bitmap);
        return Status::OK();
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }
}

Status FullTextIndexReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                                  InvertedIndexQueryType* query_range,
                                  InvertedIndexParserType analyser_type,
                                  roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

    return std::visit(
            [&](auto& arg) -> Status {
                DCHECK(arg.is_point_query());
                auto query_op = arg.point_op();
                auto query_value = arg.get_fixed_value();
                if constexpr (std::is_same_v<decltype(query_value), StringRef>) {
                    std::string search_str = query_value.to_string();

                    if (is_match_query(query_op) || is_equal_query(query_op)) {
                        return query_internal(search_str, column_name, query_op, analyser_type,
                                              stats, bit_map);
                    } else {
                        LOG(WARNING) << column_name << " must use match query";
                        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
                    }
                }
                DCHECK(false);
                return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
            },
            *query_range);
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

Status StringTypeInvertedIndexReader::new_iterator(const TabletIndex* index_meta,
                                                   OlapReaderStatistics* stats,
                                                   InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, stats, this);
    return Status::OK();
}

template <PrimitiveType field_type>
std::unique_ptr<lucene::search::Query> StringTypeInvertedIndexReader::generate_query(
        InvertedIndexQuery<field_type>& query, const std::string& column_name) {
    if constexpr (field_type == PrimitiveType::TYPE_VARCHAR ||
                  field_type == PrimitiveType::TYPE_CHAR ||
                  field_type == PrimitiveType::TYPE_STRING) {
        std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());

        if (query.is_point_query()) {
            bool deleteQuery = true;
            auto term_value_set = query.get_fixed_value_set();
            //std::vector<lucene::search::BooleanClause*> bcs;
            auto bq = std::make_unique<lucene::search::BooleanQuery>();
            for (auto& term_value : term_value_set) {
                auto act_len = strnlen(term_value.data, term_value.size);
                std::string term_value_str(term_value.data, act_len);

                std::wstring search_str_ws =
                        std::wstring(term_value_str.begin(), term_value_str.end());
                CLTermType term {
                        _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str()),
                        termDeleter};
                //lucene::search::BooleanClause* bc = _CLNEW lucene::search::BooleanClause(
                //        _CLNEW lucene::search::TermQuery(term.get()), deleteQuery,
                //        lucene::search::BooleanClause::SHOULD);
                //bcs.emplace_back(bc);

                bq->add(_CLNEW lucene::search::TermQuery(term.get()), deleteQuery,
                        lucene::search::BooleanClause::SHOULD);
            }
            //int32_t minimumNumberShouldMatch = 1;
            //lucene::search::BooleanQuery* bq =
            //        _CLNEW lucene::search::BooleanQuery(minimumNumberShouldMatch, bcs);
            //return std::make_unique<lucene::search::BooleanQuery>(minimumNumberShouldMatch, bcs);
            return bq;
        } else if (query.is_range_query()) {
            if (query.has_lower_bound()) {
                std::wstring lower_value = std::wstring(query.lower_value().to_string().begin(),
                                                        query.lower_value().to_string().end());
                CLTermType lower_term {
                        _CLNEW lucene::index::Term(column_name_ws.c_str(), lower_value.c_str()),
                        termDeleter};
                bool include_lower = query.get_include_lower();
                if (query.has_upper_bound()) {
                    std::wstring upper_value = std::wstring(query.upper_value().to_string().begin(),
                                                            query.upper_value().to_string().end());
                    bool include_upper = query.get_include_upper();
                    CLTermType upper_term {
                            _CLNEW lucene::index::Term(column_name_ws.c_str(), upper_value.c_str()),
                            termDeleter};
                    return std::make_unique<lucene::search::RangeQuery>(
                            lower_term.get(), upper_term.get(), include_lower && include_upper);
                } else {
                    return std::make_unique<lucene::search::RangeQuery>(lower_term.get(), nullptr,
                                                                        include_lower);
                }
            } else if (query.has_upper_bound()) {
                std::wstring upper_value = std::wstring(query.upper_value().to_string().begin(),
                                                        query.upper_value().to_string().end());
                bool include_upper = query.get_include_upper();
                CLTermType upper_term {
                        _CLNEW lucene::index::Term(column_name_ws.c_str(), upper_value.c_str()),
                        termDeleter};
                return std::make_unique<lucene::search::RangeQuery>(nullptr, upper_term.get(),
                                                                    include_upper);
            }
        }
    }
    return nullptr;
}

Status StringTypeInvertedIndexReader::query(OlapReaderStatistics* stats,
                                            const std::string& column_name,
                                            InvertedIndexQueryType* query,
                                            InvertedIndexParserType analyser_type,
                                            roaring::Roaring* bit_map) {
    SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);
    auto query_key = std::visit([&](auto& q) -> std::string { return q.to_string(); }, *query);

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _index_id);
    auto index_file_path = index_dir / index_file_name;

    // try to get query bitmap result from cache and return immediately on cache hit
    InvertedIndexQueryCache::CacheKey cache_key {index_file_path, column_name, query_key};
    auto cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handle;
    if (cache->lookup(cache_key, &cache_handle)) {
        stats->inverted_index_query_cache_hit++;
        SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
        *bit_map = *cache_handle.match_bitmap();
        return Status::OK();
    } else {
        stats->inverted_index_query_cache_miss++;
    }

    // check index file existence
    if (!indexExists(index_file_path)) {
        LOG(WARNING) << "inverted index path: " << index_file_path.string() << " not exist.";
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>();
    }

    auto search_query = std::visit(
            [&](auto& q) -> std::unique_ptr<lucene::search::Query> {
                return std::move(generate_query(q, column_name));
            },
            *query);

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle, stats);
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();

    try {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
        index_searcher->_search(search_query.get(),
                                [&result](const int32_t docid, const float_t /*score*/) {
                                    // docid equal to rowid in segment
                                    result.add(docid);
                                });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }

    // add to cache
    roaring::Roaring* term_match_bitmap = new roaring::Roaring(result);
    term_match_bitmap->runOptimize();
    cache->insert(cache_key, term_match_bitmap, &cache_handle);

    bit_map->swap(result);
    return Status::OK();
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

BkdIndexReader::BkdIndexReader(io::FileSystemSPtr fs, const std::string& path,
                               const uint32_t uniq_id)
        : InvertedIndexReader(fs, path, uniq_id), compoundReader(nullptr) {
    io::Path io_path(_path);
    auto index_dir = io_path.parent_path();
    auto index_file_name =
            InvertedIndexDescriptor::get_index_file_name(io_path.filename(), _index_id);

    // check index file existence
    auto index_file = index_dir / index_file_name;
    if (!indexExists(index_file)) {
        LOG(WARNING) << "bkd index: " << index_file.string() << " not exist.";
        return;
    }
    compoundReader = new DorisCompoundReader(
            DorisCompoundDirectory::getDirectory(fs, index_dir.c_str()), index_file_name.c_str(),
            config::inverted_index_read_buffer_size);
}

Status BkdIndexReader::new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                                    InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, stats, this);
    return Status::OK();
}

template <PrimitiveType field_type>
Status BkdIndexReader::bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                                 std::shared_ptr<lucene::util::bkd::bkd_reader>& r,
                                 InvertedIndexVisitor<field_type>* visitor) {
    auto status = get_bkd_reader(r);
    if (!status.ok()) {
        LOG(WARNING) << "get bkd reader for column " << column_name
                     << " failed: " << status.code_as_string();
        return status;
    }
    visitor->set_reader(r.get());
    return Status::OK();
}

template <PrimitiveType field_type>
std::unique_ptr<InvertedIndexVisitor<field_type>> construct_visitor(
        InvertedIndexQuery<field_type>* query, roaring::Roaring* hits, bool only_count) {
    return std::make_unique<InvertedIndexVisitor<field_type>>(hits, query, only_count);
}

Status BkdIndexReader::try_query(OlapReaderStatistics* stats, const std::string& column_name,
                                 InvertedIndexQueryType* query,
                                 InvertedIndexParserType analyser_type, uint32_t* count) {
    return std::visit(
            [&](auto& q) -> Status {
                uint64_t start = UnixMillis();

                auto visitor = construct_visitor(&q, nullptr, true);
                std::shared_ptr<lucene::util::bkd::bkd_reader> r;
                try {
                    RETURN_IF_ERROR(bkd_query(stats, column_name, r, visitor.get()));
                    *count = r->estimate_point_count(visitor.get());
                } catch (const CLuceneError& e) {
                    LOG(WARNING) << "BKD Query CLuceneError Occurred, error msg: " << e.what();
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
                }

                LOG(INFO) << "BKD index try search time taken: " << UnixMillis() - start << "ms "
                          << " column: " << column_name << " result: " << *count;
                return Status::OK();
            },
            *query);
}

Status BkdIndexReader::query(OlapReaderStatistics* stats, const std::string& column_name,
                             InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                             roaring::Roaring* bit_map) {
    return std::visit(
            [&](auto& q) -> Status {
                SCOPED_RAW_TIMER(&stats->inverted_index_query_timer);

                io::Path path(_path);
                auto index_dir = path.parent_path();
                auto index_file_name =
                        InvertedIndexDescriptor::get_index_file_name(path.filename(), _index_id);
                auto index_file_path = index_dir / index_file_name;
                // std::string query_str {(const char *)query_value};

                // // try to get query bitmap result from cache and return immediately on cache hit
                // InvertedIndexQueryCache::CacheKey cache_key
                //     {index_file_path, column_name, query_type, std::wstring(query_str.begin(), query_str.end())};
                // auto cache = InvertedIndexQueryCache::instance();
                // InvertedIndexQueryCacheHandle cache_handle;
                // if (cache->lookup(cache_key, &cache_handle)) {
                //     stats->inverted_index_query_cache_hit++;
                //     SCOPED_RAW_TIMER(&stats->inverted_index_query_bitmap_copy_timer);
                //     *bit_map = *cache_handle.match_bitmap();
                //     return Status::OK();
                // } else {
                //     stats->inverted_index_query_cache_miss++;
                // }
                uint64_t start = UnixMillis();
                auto visitor = construct_visitor(&q, bit_map, false);

                std::shared_ptr<lucene::util::bkd::bkd_reader> r;
                try {
                    SCOPED_RAW_TIMER(&stats->inverted_index_searcher_search_timer);
                    RETURN_IF_ERROR(bkd_query(stats, column_name, r, visitor.get()));
                    r->intersect(visitor.get());
                } catch (const CLuceneError& e) {
                    LOG(WARNING) << "BKD Query CLuceneError Occurred, error msg: " << e.what();
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
                }

                // // add to cache
                // roaring::Roaring* term_match_bitmap = new roaring::Roaring(*bit_map);
                // term_match_bitmap->runOptimize();
                // cache->insert(cache_key, term_match_bitmap, &cache_handle);

                LOG(INFO) << "BKD index search time taken: " << UnixMillis() - start << "ms "
                          << " column: " << column_name << " result: " << bit_map->cardinality()
                          << " reader stats: " << r->stats.to_string();
                return Status::OK();
            },
            *query);
}

Status BkdIndexReader::get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>& bkdReader) {
    // bkd file reader
    if (compoundReader == nullptr) {
        LOG(WARNING) << "bkd index input file not found";
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>();
    }
    CLuceneError err;
    lucene::store::IndexInput* data_in;
    lucene::store::IndexInput* meta_in;
    lucene::store::IndexInput* index_in;

    if (!compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_data_file_name().c_str(), data_in,
                err) ||
        !compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_meta_file_name().c_str(), meta_in,
                err) ||
        !compoundReader->openInput(
                InvertedIndexDescriptor::get_temporary_bkd_index_file_name().c_str(), index_in,
                err)) {
        LOG(WARNING) << "bkd index input error: " << err.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>();
    }

    bkdReader = std::make_shared<lucene::util::bkd::bkd_reader>(data_in);
    if (0 == bkdReader->read_meta(meta_in)) {
        return Status::EndOfFile("bkd index file is empty");
    }

    bkdReader->read_index(index_in);

    _type_info = get_scalar_type_info((FieldType)bkdReader->type);
    if (_type_info == nullptr) {
        LOG(WARNING) << "unsupported typeinfo, type=" << bkdReader->type;
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
    }
    _value_key_coder = get_key_coder(_type_info->type());
    return Status::OK();
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

template <PrimitiveType field_type>
InvertedIndexVisitor<field_type>::InvertedIndexVisitor(roaring::Roaring* h,
                                                       InvertedIndexQuery<field_type>* q,
                                                       bool only_count)
        : hits(h), num_hits(0), only_count(only_count), query(q) {}

template <PrimitiveType field_type>
bool InvertedIndexVisitor<field_type>::point_is_in_range(uint8_t* packedValue, int offset,
                                                         InvertedIndexQuery<field_type>* q) {
    switch (q->upper_op()) {
    case InvertedIndexQueryOp::LESS_THAN_QUERY: {
        switch (q->lower_op()) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            return (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) < 0) &&
                   (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) > 0);
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            return (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) < 0) &&
                   (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) >= 0);
        }

        default: {
            DCHECK(false);
        }
        }

        break;
    }

    case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
        switch (q->lower_op()) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            return (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) <= 0) &&
                   (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) > 0);
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            return (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) <= 0) &&
                   (lucene::util::FutureArrays::CompareUnsigned(
                            packedValue, offset, offset + reader->bytes_per_dim_,
                            (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                            offset + reader->bytes_per_dim_) >= 0);
        }

        default: {
            DCHECK(false);
        }
        }
    }

    default: {
        DCHECK(false);
    }
    }

    return false;
}

template <PrimitiveType field_type>
bool InvertedIndexVisitor<field_type>::point_is_in_list(uint8_t* packedValue, int offset,
                                                        InvertedIndexQuery<field_type>* q) {
    if (q->point_op() == InvertedIndexQueryOp::EQUAL_QUERY) {
        for (auto& value : q->get_fixed_value_encoded_set()) {
            if (lucene::util::FutureArrays::CompareUnsigned(packedValue, offset,
                                                            offset + reader->bytes_per_dim_,
                                                            (const uint8_t*)value.c_str(), offset,
                                                            offset + reader->bytes_per_dim_) == 0) {
                return true;
            }
        }
        return false;
    }
    return false;
}

template <PrimitiveType field_type>
bool InvertedIndexVisitor<field_type>::intersect_point(uint8_t* packedValue, int offset,
                                                       InvertedIndexQuery<field_type>* q) {
    if (q->is_point_query()) {
        return point_is_in_list(packedValue, offset, q);
    } else if (q->is_range_query()) {
        return point_is_in_range(packedValue, offset, q);
    }
    return false;
}

template <PrimitiveType field_type>
lucene::util::bkd::relation InvertedIndexVisitor<field_type>::relation_between_point_and_range(
        uint8_t* minValue, uint8_t* maxValue, int offset, InvertedIndexQuery<field_type>* q) {
    if (q->point_op() == InvertedIndexQueryOp::EQUAL_QUERY) {
        for (auto& value : q->get_fixed_value_encoded_set()) {
            if ((lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)value.c_str(), offset, offset + reader->bytes_per_dim_,
                         minValue, offset, offset + reader->bytes_per_dim_) >= 0) &&
                (lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)value.c_str(), offset, offset + reader->bytes_per_dim_,
                         maxValue, offset, offset + reader->bytes_per_dim_) <= 0)) {
                return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }
    }
    return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
}

template <PrimitiveType field_type>
lucene::util::bkd::relation InvertedIndexVisitor<field_type>::relation_between_range_and_range(
        uint8_t* minValue, uint8_t* maxValue, int offset, InvertedIndexQuery<field_type>* q) {
    switch (q->upper_op()) {
    case InvertedIndexQueryOp::LESS_THAN_QUERY: {
        switch (q->lower_op()) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            if ((lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, minValue, offset,
                         offset + reader->bytes_per_dim_) < 0) &&
                (lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, maxValue, offset,
                         offset + reader->bytes_per_dim_) > 0)) {
                return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
            } else if ((lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, maxValue, offset,
                                offset + reader->bytes_per_dim_) >= 0) ||
                       (lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, minValue, offset,
                                offset + reader->bytes_per_dim_) <= 0)) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            } else {
                return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            if ((lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, minValue, offset,
                         offset + reader->bytes_per_dim_) <= 0) &&
                (lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, maxValue, offset,
                         offset + reader->bytes_per_dim_) > 0)) {
                return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
            } else if ((lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, maxValue, offset,
                                offset + reader->bytes_per_dim_) > 0) ||
                       (lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, minValue, offset,
                                offset + reader->bytes_per_dim_) <= 0)) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            } else {
                return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }

        default: {
            DCHECK(false);
            break;
        }
        }
        break;
    }

    case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
        switch (q->lower_op()) {
        case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
            if ((lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, minValue, offset,
                         offset + reader->bytes_per_dim_) < 0) &&
                (lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, maxValue, offset,
                         offset + reader->bytes_per_dim_) >= 0)) {
                return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
            } else if ((lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, maxValue, offset,
                                offset + reader->bytes_per_dim_) >= 0) ||
                       (lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, minValue, offset,
                                offset + reader->bytes_per_dim_) < 0)) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            } else {
                return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }

        case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
            if ((lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, minValue, offset,
                         offset + reader->bytes_per_dim_) <= 0) &&
                (lucene::util::FutureArrays::CompareUnsigned(
                         (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                         offset + reader->bytes_per_dim_, maxValue, offset,
                         offset + reader->bytes_per_dim_) >= 0)) {
                return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
            } else if ((lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->lower_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, maxValue, offset,
                                offset + reader->bytes_per_dim_) > 0) ||
                       (lucene::util::FutureArrays::CompareUnsigned(
                                (const uint8_t*)q->upper_value_encoded().c_str(), offset,
                                offset + reader->bytes_per_dim_, minValue, offset,
                                offset + reader->bytes_per_dim_) < 0)) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            } else {
                return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
            }
        }
        default: {
            DCHECK(false);
            break;
        }
        }
        break;
    }
    default: {
        DCHECK(false);
        break;
    }
    }

    return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
}

template <PrimitiveType field_type>
lucene::util::bkd::relation InvertedIndexVisitor<field_type>::intersect_range(
        uint8_t* minValue, uint8_t* maxValue, int offset, InvertedIndexQuery<field_type>* q) {
    if (q->is_point_query()) {
        return relation_between_point_and_range(minValue, maxValue, offset, q);
    } else if (q->is_range_query()) {
        return relation_between_range_and_range(minValue, maxValue, offset, q);
    }
    return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
}

template <PrimitiveType field_type>
bool InvertedIndexVisitor<field_type>::matches(uint8_t* packedValue) {
    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;
        auto match = intersect_point(packedValue, offset, query);
        if (!match) {
            return false;
        }
    }
    return true;
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(std::vector<char>& docID,
                                             std::vector<uint8_t>& packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    visit(roaring::Roaring::read(docID.data(), false));
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(Roaring* docID, std::vector<uint8_t>& packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    visit(*docID);
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(roaring::Roaring&& r) {
    if (only_count) {
        num_hits += r.cardinality();
    } else {
        *hits |= r;
    }
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(roaring::Roaring& r) {
    if (only_count) {
        num_hits += r.cardinality();
    } else {
        *hits |= r;
    }
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(int rowID) {
    if (only_count) {
        num_hits++;
    } else {
        hits->add(rowID);
    }
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
                                             std::vector<uint8_t>& packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    int32_t docID = iter->docid_set->nextDoc();
    while (docID != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
        if (only_count) {
            num_hits++;
        } else {
            hits->add(docID);
        }
        docID = iter->docid_set->nextDoc();
    }
}

template <PrimitiveType field_type>
void InvertedIndexVisitor<field_type>::visit(int rowID, std::vector<uint8_t>& packedValue) {
    if (matches(packedValue.data())) {
        if (only_count) {
            num_hits++;
        } else {
            hits->add(rowID);
        }
    }
}

template <PrimitiveType field_type>
lucene::util::bkd::relation InvertedIndexVisitor<field_type>::compare(
        std::vector<uint8_t>& minPacked, std::vector<uint8_t>& maxPacked) {
    //bool crosses = false;

    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;
        auto relation = intersect_range(minPacked.data(), maxPacked.data(), offset, query);

        if (relation == lucene::util::bkd::relation::CELL_OUTSIDE_QUERY ||
            relation == lucene::util::bkd::relation::CELL_CROSSES_QUERY) {
            return relation;
        }
    }
    return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
}

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name,
                                                       InvertedIndexQueryType* query,
                                                       uint32_t segment_num_rows,
                                                       roaring::Roaring* bit_map, bool skip_try) {
    if (!skip_try && _reader->type() == InvertedIndexReaderType::BKD) {
        auto query_bkd_limit_percent = config::query_bkd_inverted_index_limit_percent;
        uint32_t hit_count = 0;
        RETURN_IF_ERROR(try_read_from_inverted_index(column_name, query, &hit_count));
        if (hit_count > segment_num_rows * query_bkd_limit_percent / 100) {
            LOG(INFO) << "hit count: " << hit_count << "for bkd inverted reached limit "
                      << query_bkd_limit_percent << "%, segment num rows: " << segment_num_rows;
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_HIT_LIMIT>();
        }
    }

    RETURN_IF_ERROR(_reader->query(_stats, column_name, query, _analyser_type, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name,
                                                           InvertedIndexQueryType* query,
                                                           uint32_t* count) {
    // NOTE: only bkd index support try read now.
    if (get_inverted_index_reader_type() == InvertedIndexReaderType::BKD) {
        RETURN_IF_ERROR(_reader->try_query(_stats, column_name, query, _analyser_type, count));
    }
    return Status::OK();
}

InvertedIndexParserType InvertedIndexIterator::get_inverted_index_analyser_type() const {
    return _analyser_type;
}

InvertedIndexReaderType InvertedIndexIterator::get_inverted_index_reader_type() const {
    return _reader->type();
}

} // namespace segment_v2
} // namespace doris
