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

#include <CLucene/search/BooleanQuery.h>
#include <CLucene/search/PhraseQuery.h>

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "io/fs/file_system.h"
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/string_value.h"
#include "util/time.h"

namespace doris {
namespace segment_v2 {

bool InvertedIndexReader::_is_match_query(InvertedIndexQueryType query_type) {
    return (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY);
}

bool InvertedIndexReader::indexExists(io::Path& index_file_path) {
    bool exists = false;
    RETURN_IF_ERROR(_fs->exists(index_file_path, &exists));
    return exists;
}

std::vector<std::string> FullTextIndexReader::get_analyse_result(
        const std::wstring& field_name, const std::wstring& value,
        InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type) {
    std::vector<std::string> analyse_result;
    std::shared_ptr<lucene::analysis::Analyzer> analyzer;
    if (analyser_type == InvertedIndexParserType::PARSER_STANDARD) {
        analyzer = std::make_shared<lucene::analysis::standard::StandardAnalyzer>();
    } else {
        // default
        analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<TCHAR>>();
    }

    std::unique_ptr<lucene::util::StringReader> reader(
            new lucene::util::StringReader(value.c_str()));
    std::unique_ptr<lucene::analysis::TokenStream> token_stream(
            analyzer->tokenStream(field_name.c_str(), reader.get()));

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        std::string tk =
                lucene::util::Misc::toString(token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        analyse_result.emplace_back(tk);
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
        query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
        std::set<std::string> unrepeated_result(analyse_result.begin(), analyse_result.end());
        analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    }

    return analyse_result;
}

Status FullTextIndexReader::new_iterator(const TabletIndex* index_meta,
                                         InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

Status FullTextIndexReader::query(const std::string& column_name, const void* query_value,
                                  InvertedIndexQueryType query_type,
                                  InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) {
    std::string search_str = reinterpret_cast<const StringValue*>(query_value)->to_string();
    LOG(INFO) << column_name
              << " begin to load the fulltext index from clucene, query_str=" << search_str;
    std::unique_ptr<lucene::search::Query> query;
    std::wstring field_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());
    try {
        std::vector<std::string> analyse_result =
                get_analyse_result(field_ws, search_str_ws, query_type, analyser_type);

        if (analyse_result.empty()) {
            LOG(WARNING) << "invalid input query_str: " << search_str
                         << ", please check your query sql";
            return Status::Error<ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>();
        }

        switch (query_type) {
        case InvertedIndexQueryType::MATCH_ANY_QUERY: {
            query.reset(_CLNEW lucene::search::BooleanQuery());
            for (auto token : analyse_result) {
                std::wstring token_ws = std::wstring(token.begin(), token.end());
                lucene::index::Term* term =
                        _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
                static_cast<lucene::search::BooleanQuery*>(query.get())
                        ->add(_CLNEW lucene::search::TermQuery(term), true,
                              lucene::search::BooleanClause::SHOULD);
                _CLDECDELETE(term);
            }
            break;
        }
        case InvertedIndexQueryType::MATCH_ALL_QUERY: {
            query.reset(_CLNEW lucene::search::BooleanQuery());
            for (auto token : analyse_result) {
                std::wstring token_ws = std::wstring(token.begin(), token.end());
                lucene::index::Term* term =
                        _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
                static_cast<lucene::search::BooleanQuery*>(query.get())
                        ->add(_CLNEW lucene::search::TermQuery(term), true,
                              lucene::search::BooleanClause::MUST);
                _CLDECDELETE(term);
            }
            break;
        }
        case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
            LOG(WARNING) << "match phrase of fulltext is not supported";
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
        }
        default:
            LOG(ERROR) << "fulltext query do not support query type other than match, column: " << column_name;
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
        }

    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _index_id);

    // check index file existence
    auto index_file_path = index_dir / index_file_name;
    if (!indexExists(index_file_path)) {
        LOG(WARNING) << "inverted index path: " << index_file_path.string() << " not exist.";
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>();
    }

    std::shared_ptr<lucene::search::IndexSearcher> index_searcher = nullptr;
    {
        DorisCompoundReader* directory = new DorisCompoundReader(
                DorisCompoundDirectory::getDirectory(_fs, index_dir.c_str()), index_file_name.c_str());
        index_searcher = std::make_shared<lucene::search::IndexSearcher>(directory, true);
        _CLDECDELETE(directory)
    }

    roaring::Roaring result;
    try {
        index_searcher->_search(query.get(),
                                [&result](const int32_t docid, const float_t /*score*/) {
                                    // docid equal to rowid in segment
                                    result.add(docid);
                                });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }
    bit_map->swap(result);
    return Status::OK();
}

InvertedIndexReaderType FullTextIndexReader::type() {
    return InvertedIndexReaderType::FULLTEXT;
}

Status StringTypeInvertedIndexReader::new_iterator(const TabletIndex* index_meta,
                                                   InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

Status StringTypeInvertedIndexReader::query(const std::string& column_name, const void* query_value,
                                            InvertedIndexQueryType query_type,
                                            InvertedIndexParserType analyser_type,
                                            roaring::Roaring* bit_map) {
    const StringValue* search_query = reinterpret_cast<const StringValue*>(query_value);
    auto act_len = strnlen(search_query->ptr, search_query->len);
    std::string search_str(search_query->ptr, act_len);
    LOG(INFO) << "begin to query the inverted index from clucene"
              << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());
    lucene::index::Term* term =
            new lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str());
    std::unique_ptr<lucene::search::Query> query;

    io::Path path(_path);
    auto index_dir = path.parent_path();
    auto index_file_name = InvertedIndexDescriptor::get_index_file_name(path.filename(), _index_id);

    // check index file existence
    auto index_file_path = index_dir / index_file_name;
    if (!indexExists(index_file_path)) {
        LOG(WARNING) << "inverted index path: " << index_file_path.string() << " not exist.";
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>();
    }

    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        query.reset(new lucene::search::TermQuery(term));
        break;
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, false));
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, true));
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, false));
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, true));
        break;
    }
    default:
        LOG(ERROR) << "invalid query type when query untokenized inverted index";
        if (_is_match_query(query_type)) {
            LOG(WARNING) << column_name << " is untokenized inverted index"
                    << ", please use equal query instead of match query";
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
        }
        LOG(WARNING) << "invalid query type when query untokenized inverted index";
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
    }

    std::shared_ptr<lucene::search::IndexSearcher> index_searcher = nullptr;
    {
        DorisCompoundReader* directory = new DorisCompoundReader(
                DorisCompoundDirectory::getDirectory(_fs, index_dir.c_str()), index_file_name.c_str());
        index_searcher = std::make_shared<lucene::search::IndexSearcher>(directory, true);
        _CLDECDELETE(directory)
    }

    roaring::Roaring result;
    try {
        index_searcher->_search(query.get(),
                                [&result](const int32_t docid, const float_t /*score*/) {
                                    // docid equal to rowid in segment
                                    result.add(docid);
                                });
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "CLuceneError occured, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }

    bit_map->swap(result);
    return Status::OK();
}

InvertedIndexReaderType StringTypeInvertedIndexReader::type() {
    return InvertedIndexReaderType::STRING_TYPE;
}

Status InvertedIndexIterator::read_from_inverted_index(
        const std::string& column_name, const void* query_value, InvertedIndexQueryType query_type,
        uint32_t segment_num_rows, roaring::Roaring* bit_map) {
    RETURN_IF_ERROR(_reader->query(column_name, query_value, query_type, _analyser_type, bit_map));
    return Status::OK();
}

Status InvertedIndexIterator::try_read_from_inverted_index(const std::string& column_name,
                                                           const void* query_value,
                                                           InvertedIndexQueryType query_type,
                                                           uint32_t* count) {
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
