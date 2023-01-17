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

#include <roaring/roaring.hh>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "io/fs/file_system.h"
#include "olap/inverted_index_parser.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/tablet_schema.h"

namespace doris {

namespace segment_v2 {

class InvertedIndexIterator;

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
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
            : _fs(std::move(fs)), _path(path), _index_id(index_id) {};
    virtual ~InvertedIndexReader() = default;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(const TabletIndex* index_meta,
                                InvertedIndexIterator** iterator) = 0;
    virtual Status query(const std::string& column_name, const void* query_value,
                         InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                         roaring::Roaring* bit_map) = 0;
    virtual Status try_query(const std::string& column_name, const void* query_value,
                             InvertedIndexQueryType query_type,
                             InvertedIndexParserType analyser_type, uint32_t* count) = 0;

    virtual InvertedIndexReaderType type() = 0;
    bool indexExists(io::Path& index_file_path);

    uint32_t get_index_id() { return _index_id; }

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
            : InvertedIndexReader(std::move(fs), path, uniq_id) {};
    ~FullTextIndexReader() override = default;

    Status new_iterator(const TabletIndex* index_meta, InvertedIndexIterator** iterator) override;
    Status query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(const std::string& column_name, const void* query_value,
                     InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }

    InvertedIndexReaderType type() override;
    std::vector<std::string> get_analyse_result(const std::wstring& field_name,
                                                const std::wstring& value,
                                                InvertedIndexQueryType query_type,
                                                InvertedIndexParserType analyser_type);
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
public:
    explicit StringTypeInvertedIndexReader(io::FileSystemSPtr fs, const std::string& path,
                                           const int64_t uniq_id)
            : InvertedIndexReader(std::move(fs), path, uniq_id) {};
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(const TabletIndex* index_meta, InvertedIndexIterator** iterator) override;
    Status query(const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                 roaring::Roaring* bit_map) override;
    Status try_query(const std::string& column_name, const void* query_value,
                     InvertedIndexQueryType query_type, InvertedIndexParserType analyser_type,
                     uint32_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>();
    }
    InvertedIndexReaderType type() override;
};

class InvertedIndexIterator {
public:
    InvertedIndexIterator(const TabletIndex* index_meta, InvertedIndexReader* reader)
            : _index_meta(index_meta), _reader(reader) {
        _analyser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_index_meta->properties()));
    }

    Status read_from_inverted_index(const std::string& column_name, const void* query_value,
                                    InvertedIndexQueryType query_type, uint32_t segment_num_rows,
                                    roaring::Roaring* bit_map);
    Status try_read_from_inverted_index(const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, uint32_t* count);

    InvertedIndexParserType get_inverted_index_analyser_type() const;

    InvertedIndexReaderType get_inverted_index_reader_type() const;

private:
    const TabletIndex* _index_meta;
    InvertedIndexReader* _reader;
    InvertedIndexParserType _analyser_type;
};

} // namespace segment_v2
} // namespace doris
