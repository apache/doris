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

std::vector<std::wstring> FullTextIndexReader::get_analyse_result(
        const std::wstring& field_name, const std::string& value, InvertedIndexQueryType query_type,
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

    if (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
        query_type == InvertedIndexQueryType::MATCH_ALL_QUERY) {
        std::set<std::wstring> unrepeated_result(analyse_result.begin(), analyse_result.end());
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
                                  InvertedIndexParserType analyser_type,
                                  roaring::Roaring* bit_map) {
    std::string search_str = reinterpret_cast<const StringRef*>(query_value)->to_string();
    VLOG_DEBUG << column_name
               << " begin to load the fulltext index from clucene, query_str=" << search_str;
    std::unique_ptr<lucene::search::Query> query;
    std::wstring field_ws = std::wstring(column_name.begin(), column_name.end());
    try {
        std::vector<std::wstring> analyse_result =
                get_analyse_result(field_ws, search_str, query_type, analyser_type);

        if (analyse_result.empty()) {
            LOG(WARNING) << "invalid input query_str: " << search_str
                         << ", please check your query sql";
            return Status::Error<ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>();
        }

        switch (query_type) {
        case InvertedIndexQueryType::MATCH_ANY_QUERY: {
            query.reset(_CLNEW lucene::search::BooleanQuery());
            for (auto token_ws : analyse_result) {
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
            for (auto token_ws : analyse_result) {
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
            LOG(ERROR) << "fulltext query do not support query type other than match, column: "
                       << column_name;
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

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle);
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();

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
    const StringRef* search_query = reinterpret_cast<const StringRef*>(query_value);
    auto act_len = strnlen(search_query->data, search_query->size);
    std::string search_str(search_query->data, act_len);
    VLOG_DEBUG << "begin to query the inverted index from clucene"
               << ", column_name: " << column_name << ", search_str: " << search_str;
    std::wstring column_name_ws = std::wstring(column_name.begin(), column_name.end());
    std::wstring search_str_ws = std::wstring(search_str.begin(), search_str.end());
    lucene::index::Term* term =
            _CLNEW lucene::index::Term(column_name_ws.c_str(), search_str_ws.c_str());
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
        _CLDECDELETE(term);
        break;
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, false));
        _CLDECDELETE(term);
        break;
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(nullptr, term, true));
        _CLDECDELETE(term);
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, false));
        _CLDECDELETE(term);
        break;
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        query.reset(new lucene::search::RangeQuery(term, nullptr, true));
        _CLDECDELETE(term);
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

    roaring::Roaring result;
    InvertedIndexCacheHandle inverted_index_cache_handle;
    InvertedIndexSearcherCache::instance()->get_index_searcher(
            _fs, index_dir.c_str(), index_file_name, &inverted_index_cache_handle);
    auto index_searcher = inverted_index_cache_handle.get_index_searcher();

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
            DorisCompoundDirectory::getDirectory(fs, index_dir.c_str()), index_file_name.c_str());
}

Status BkdIndexReader::new_iterator(const TabletIndex* index_meta,
                                    InvertedIndexIterator** iterator) {
    *iterator = new InvertedIndexIterator(index_meta, this);
    return Status::OK();
}

Status BkdIndexReader::bkd_query(const std::string& column_name, const void* query_value,
                                 InvertedIndexQueryType query_type,
                                 std::shared_ptr<lucene::util::bkd::bkd_reader>&& r,
                                 InvertedIndexVisitor* visitor) {
    lucene::util::bkd::bkd_reader* tmp_reader;
    auto status = get_bkd_reader(tmp_reader);
    if (!status.ok()) {
        LOG(WARNING) << "get bkd reader for column " << column_name
                     << " failed: " << status.code_as_string();
        return status;
    }
    r.reset(tmp_reader);
    char tmp[r->bytes_per_dim_];
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &visitor->queryMax);
        _value_key_coder->full_encode_ascending(query_value, &visitor->queryMin);
        break;
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY:
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &visitor->queryMax);
        _type_info->set_to_min(tmp);
        _value_key_coder->full_encode_ascending(tmp, &visitor->queryMin);
        break;
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY:
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        _value_key_coder->full_encode_ascending(query_value, &visitor->queryMin);
        _type_info->set_to_max(tmp);
        _value_key_coder->full_encode_ascending(tmp, &visitor->queryMax);
        break;
    }
    default:
        LOG(ERROR) << "invalid query type when query bkd index";
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
    }
    visitor->set_reader(r.get());
    return Status::OK();
}

Status BkdIndexReader::query(const std::string& column_name, const void* query_value,
                             InvertedIndexQueryType query_type,
                             InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) {
    uint64_t start = UnixMillis();
    auto visitor = std::make_unique<InvertedIndexVisitor>(bit_map, query_type);
    std::shared_ptr<lucene::util::bkd::bkd_reader> r;
    try {
        RETURN_IF_ERROR(
                bkd_query(column_name, query_value, query_type, std::move(r), visitor.get()));
        r->intersect(visitor.get());
    } catch (const CLuceneError& e) {
        LOG(WARNING) << "BKD Query CLuceneError Occurred, error msg: " << e.what();
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>();
    }

    LOG(INFO) << "BKD index search time taken: " << UnixMillis() - start << "ms "
              << " column: " << column_name << " result: " << bit_map->cardinality()
              << " reader stats: " << r->stats.to_string();
    return Status::OK();
}

Status BkdIndexReader::get_bkd_reader(lucene::util::bkd::bkd_reader*& bkdReader) {
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

    bkdReader = new lucene::util::bkd::bkd_reader(data_in);
    if (0 == bkdReader->read_meta(meta_in)) {
        return Status::EndOfFile("bkd index file is empty");
    }

    bkdReader->read_index(index_in);

    _type_info = get_scalar_type_info((FieldType)bkdReader->type);
    if (_type_info == nullptr) {
        auto type = bkdReader->type;
        delete bkdReader;
        LOG(WARNING) << "unsupported typeinfo, type=" << type;
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>();
    }
    _value_key_coder = get_key_coder(_type_info->type());
    return Status::OK();
}

InvertedIndexReaderType BkdIndexReader::type() {
    return InvertedIndexReaderType::BKD;
}

InvertedIndexVisitor::InvertedIndexVisitor(roaring::Roaring* h, InvertedIndexQueryType query_type,
                                           bool only_count)
        : hits(h), num_hits(0), only_count(only_count), query_type(query_type) {}

bool InvertedIndexVisitor::matches(uint8_t* packedValue) {
    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;
        if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMax.c_str(), offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else if (query_type == InvertedIndexQueryType::GREATER_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMin.c_str(), offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMin.c_str(), offset,
                        offset + reader->bytes_per_dim_) < 0) {
                // Doc's value is too low, in this dimension
                return false;
            }
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMax.c_str(), offset,
                        offset + reader->bytes_per_dim_) > 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        }
    }
    return true;
}

void InvertedIndexVisitor::visit(std::vector<char>& docID, std::vector<uint8_t>& packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    visit(roaring::Roaring::read(docID.data(), false));
}

void InvertedIndexVisitor::visit(Roaring* docID, std::vector<uint8_t>& packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    visit(*docID);
}

void InvertedIndexVisitor::visit(roaring::Roaring&& r) {
    if (only_count) {
        num_hits += r.cardinality();
    } else {
        *hits |= r;
    }
}

void InvertedIndexVisitor::visit(roaring::Roaring& r) {
    if (only_count) {
        num_hits += r.cardinality();
    } else {
        *hits |= r;
    }
}

void InvertedIndexVisitor::visit(int rowID) {
    if (only_count) {
        num_hits++;
    } else {
        hits->add(rowID);
    }
}

void InvertedIndexVisitor::visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
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

void InvertedIndexVisitor::visit(int rowID, std::vector<uint8_t>& packedValue) {
    if (matches(packedValue.data())) {
        if (only_count) {
            num_hits++;
        } else {
            hits->add(rowID);
        }
    }
}

lucene::util::bkd::relation InvertedIndexVisitor::compare(std::vector<uint8_t>& minPacked,
                                                          std::vector<uint8_t>& maxPacked) {
    bool crosses = false;

    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;

        if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMax.c_str(), offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else if (query_type == InvertedIndexQueryType::GREATER_THAN_QUERY) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMin.c_str(), offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMax.c_str(), offset,
                        offset + reader->bytes_per_dim_) > 0 ||
                lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_,
                        (const uint8_t*)queryMin.c_str(), offset,
                        offset + reader->bytes_per_dim_) < 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        }
        if (query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
            query_type == InvertedIndexQueryType::GREATER_THAN_QUERY) {
            crosses |= lucene::util::FutureArrays::CompareUnsigned(
                               minPacked.data(), offset, offset + reader->bytes_per_dim_,
                               (const uint8_t*)queryMin.c_str(), offset,
                               offset + reader->bytes_per_dim_) <= 0 ||
                       lucene::util::FutureArrays::CompareUnsigned(
                               maxPacked.data(), offset, offset + reader->bytes_per_dim_,
                               (const uint8_t*)queryMax.c_str(), offset,
                               offset + reader->bytes_per_dim_) >= 0;
        } else {
            crosses |= lucene::util::FutureArrays::CompareUnsigned(
                               minPacked.data(), offset, offset + reader->bytes_per_dim_,
                               (const uint8_t*)queryMin.c_str(), offset,
                               offset + reader->bytes_per_dim_) < 0 ||
                       lucene::util::FutureArrays::CompareUnsigned(
                               maxPacked.data(), offset, offset + reader->bytes_per_dim_,
                               (const uint8_t*)queryMax.c_str(), offset,
                               offset + reader->bytes_per_dim_) > 0;
        }
    }
    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

Status InvertedIndexIterator::read_from_inverted_index(const std::string& column_name,
                                                       const void* query_value,
                                                       InvertedIndexQueryType query_type,
                                                       uint32_t segment_num_rows,
                                                       roaring::Roaring* bit_map) {
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
