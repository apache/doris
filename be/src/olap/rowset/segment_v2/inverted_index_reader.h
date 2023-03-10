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

#include <CLucene.h>
#include <CLucene/util/BitSet.h>
#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/bkd/bkd_reader.h>

#include <roaring/roaring.hh>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "io/fs/file_system.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_query.h"
#include "olap/tablet_schema.h"
#include "runtime/type_limit.h"
#include "util/date_func.h"

namespace doris {
class KeyCoder;
class TypeInfo;

namespace segment_v2 {

class InvertedIndexIterator;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
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
                         InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                         roaring::Roaring* bit_map) = 0;
    virtual Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                             InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                             uint32_t* count) = 0;

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(io::Path& index_file_path);

    uint32_t get_index_id() const { return _index_id; }

protected:
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
    Status query_internal(const std::string& query, const std::string& column_name,
                          InvertedIndexQueryOp query_type, InvertedIndexParserType analyser_type,
                          OlapReaderStatistics* stats, roaring::Roaring* bit_map);
    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }

    InvertedIndexReaderType type() override;
    std::vector<std::wstring> get_analyse_result(const std::wstring& field_name,
                                                 const std::string& value,
                                                 InvertedIndexQueryOp query_type,
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
    template <PrimitiveType field_type>
    std::unique_ptr<lucene::search::Query> generate_query(InvertedIndexQuery<field_type>& query,
                                                          const std::string& column_name);
    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }
    InvertedIndexReaderType type() override;
};

template <PrimitiveType field_type>
class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    roaring::Roaring* hits;
    uint32_t num_hits;
    bool only_count;
    lucene::util::bkd::bkd_reader* reader;
    InvertedIndexQuery<field_type>* query;

public:
    InvertedIndexVisitor(roaring::Roaring* hits, InvertedIndexQuery<field_type>* q,
                         bool only_count = false);
    virtual ~InvertedIndexVisitor() = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return reader; }

    void visit(int rowID) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* docID, std::vector<uint8_t>& packedValue) override;
    void visit(std::vector<char>& docID, std::vector<uint8_t>& packedValue) override;
    void visit(int rowID, std::vector<uint8_t>& packedValue) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packedValue) override;
    bool matches(uint8_t* packedValue);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& minPacked,
                                        std::vector<uint8_t>& maxPacked) override;
    uint32_t get_num_hits() const { return num_hits; }

    bool intersect_point(uint8_t* packedValue, int offset, InvertedIndexQuery<field_type>* q);
    bool point_is_in_list(uint8_t* packedValue, int offset, InvertedIndexQuery<field_type>* q);
    bool point_is_in_range(uint8_t* packedValue, int offset, InvertedIndexQuery<field_type>* q);
    lucene::util::bkd::relation intersect_range(uint8_t* minValue, uint8_t* maxValue, int offset,
                                                InvertedIndexQuery<field_type>* q);
    lucene::util::bkd::relation relation_between_point_and_range(uint8_t* minValue,
                                                                 uint8_t* maxValue, int offset,
                                                                 InvertedIndexQuery<field_type>* q);
    lucene::util::bkd::relation relation_between_range_and_range(uint8_t* minValue,
                                                                 uint8_t* maxValue, int offset,
                                                                 InvertedIndexQuery<field_type>* q);
};

class BkdIndexReader : public InvertedIndexReader {
public:
    explicit BkdIndexReader(io::FileSystemSPtr fs, const std::string& path, const uint32_t uniq_id);
    ~BkdIndexReader() override {
        if (compoundReader != nullptr) {
            compoundReader->close();
            delete compoundReader;
            compoundReader = nullptr;
        }
    }

    Status new_iterator(const TabletIndex* index_meta, OlapReaderStatistics* stats,
                        InvertedIndexIterator** iterator) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name,
                 InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(OlapReaderStatistics* stats, const std::string& column_name,
                     InvertedIndexQueryType* query, InvertedIndexParserType analyser_type,
                     uint32_t* count) override;
    template <PrimitiveType field_type>
    Status bkd_query(OlapReaderStatistics* stats, const std::string& column_name,
                     std::shared_ptr<lucene::util::bkd::bkd_reader>& r,
                     InvertedIndexVisitor<field_type>* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(std::shared_ptr<lucene::util::bkd::bkd_reader>& reader);

private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
    DorisCompoundReader* compoundReader;
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

    //template <FieldType field_type>
    Status read_from_inverted_index(const std::string& column_name, InvertedIndexQueryType* query,
                                    uint32_t segment_num_rows, roaring::Roaring* bit_map,
                                    bool skip_try = false);
    Status try_read_from_inverted_index(const std::string& column_name,
                                        InvertedIndexQueryType* query, uint32_t* count);

    InvertedIndexParserType get_inverted_index_analyser_type() const;

    InvertedIndexReaderType get_inverted_index_reader_type() const;

    /*template <PrimitiveType Type, typename VT>
    Status add_range(InvertedIndexQueryRange<Type>* range, VT value, InvertedIndexQueryOp op);*/

private:
    const TabletIndex* _index_meta;
    OlapReaderStatistics* _stats;
    InvertedIndexReader* _reader;
    InvertedIndexParserType _analyser_type;
};

} // namespace segment_v2
} // namespace doris
