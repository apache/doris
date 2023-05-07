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

#include <CLucene/util/bkd/bkd_reader.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/tablet_schema.h"

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util {
namespace bkd {
class bkd_docid_set_iterator;
} // namespace bkd
} // namespace util
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
};

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_ANY_QUERY = 5,
    MATCH_ALL_QUERY = 6,
    MATCH_PHRASE_QUERY = 7,
};

class InvertedIndexReader {
public:
    explicit InvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const uint32_t index_id)
            : _fs(std::move(fs)), _path(path), _index_id(index_id) {}
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                                InvertedIndexIterator** iterator) = 0;
    virtual Status query(OlapReaderStatistics* stats, const std::string& column_name,
                         const void* query_value, InvertedIndexQueryType query_type,
                         InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             const void* query_value, InvertedIndexQueryType query_type,
                             InvertedIndexParserType analyser_type, uint32_t* count) = 0;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(io::Path& index_file_path);

    uint32_t get_index_id() const { return _index_id; }

protected:
    bool _is_match_query(InvertedIndexQueryType query_type);
    friend class InvertedIndexIterator;
    io::FileSystemSPtr _fs;
    std::string _path;
    uint32_t _index_id;
};

class FullTextIndexReader : public InvertedIndexReader {
public:
    explicit FullTextIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                 const int64_t uniq_id)
            : InvertedIndexReader(std::move(fs), path, uniq_id) {}
    ~FullTextIndexReader() override = default;

    Status new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                        InvertedIndexIterator** iterator) override;
    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     InvertedIndexParserType analyser_type, uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }

    InvertedIndexReaderType type() override;
    std::vector<std::wstring> get_analyse_result(const std::wstring& field_name,
                                                 const std::string& value,
                                                 InvertedIndexQueryType query_type,
                                                 InvertedIndexParserType analyser_type);
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
public:
    explicit StringTypeInvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                           const int64_t uniq_id)
            : InvertedIndexReader(std::move(fs), path, uniq_id) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                        InvertedIndexIterator** iterator) override;
    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     InvertedIndexParserType analyser_type, uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }
    InvertedIndexReaderType type() override;
};

class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    roaring::Roaring* _hits;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader;
    InvertedIndexQueryType _query_type;

public:
    std::string query_min;
    std::string query_max;

public:
    InvertedIndexVisitor(roaring::Roaring* hits, InvertedIndexQueryType query_type,
                         bool only_count = false);
    virtual ~InvertedIndexVisitor() = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return _reader; }

    void visit(int row_id) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;
    bool matches(uint8_t* packed_value);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    uint32_t get_num_hits() const { return _num_hits; }
};

class BkdIndexReader : public InvertedIndexReader {
public:
    explicit BkdIndexReader(io::FileSystemSPtr fs, const std::string& path, const uint32_t uniq_id);
    ~BkdIndexReader() override {
        if (_compoundReader != nullptr) {
            _compoundReader->close();
            delete _compoundReader;
            _compoundReader = nullptr;
        }
    }

    Status new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                        InvertedIndexIterator** iterator) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 InvertedIndexParserType analyser_type, roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     InvertedIndexParserType analyser_type, uint32_t* count) override;
    Status bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     std::shared_ptr<lucene::util::bkd::bkd_reader>& r,
                     InvertedIndexVisitor* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>& reader);

private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
    DorisCompoundReader* _compoundReader;
};

class InvertedIndexIterator {
public:
    InvertedIndexIterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                          InvertedIndexReader* reader)
            : _index_meta(index_meta), _stats(stats), _reader(reader) {
        // TODO xk maybe change interface to use index
        _analyser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->properties()));
    }

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows,
                                    roaring::Roaring* bit_map, bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, uint32_t* count);

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr) {
        return _reader->read_null_bitmap(cache_handle, dir);
    }

    InvertedIndexParserType get_inverted_index_analyser_type() const;

    InvertedIndexReaderType get_inverted_index_reader_type() const;

private:
    const TabletIndex* _index_meta;
    OlapReaderStatistics* _stats;
    InvertedIndexReader* _reader;
    InvertedIndexParserType _analyser_type;
};

} // namespace segment_v2
} // namespace doris
