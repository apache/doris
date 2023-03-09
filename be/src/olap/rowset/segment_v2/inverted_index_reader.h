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

#include <charconv>
#include <roaring/roaring.hh>
#include <variant>

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

enum class InvertedIndexQueryOp {
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

inline std::string inverted_index_query_op_to_string(InvertedIndexQueryOp op) {
    switch (op) {
    case InvertedIndexQueryOp::EQUAL_QUERY:
        return "=";
    case InvertedIndexQueryOp::LESS_EQUAL_QUERY:
        return "<=";
    case InvertedIndexQueryOp::LESS_THAN_QUERY:
        return "<<";
    case InvertedIndexQueryOp::GREATER_THAN_QUERY:
        return ">>";
    case InvertedIndexQueryOp::GREATER_EQUAL_QUERY:
        return ">=";
    case InvertedIndexQueryOp::MATCH_ANY_QUERY:
        return "match_any";
    case InvertedIndexQueryOp::MATCH_ALL_QUERY:
        return "match_all";
    case InvertedIndexQueryOp::MATCH_PHRASE_QUERY:
        return "match_phrase";
    default:
        return "";
    }
}

inline bool is_match_query(InvertedIndexQueryOp query_op) {
    return (query_op == InvertedIndexQueryOp::MATCH_ANY_QUERY ||
            query_op == InvertedIndexQueryOp::MATCH_ALL_QUERY ||
            query_op == InvertedIndexQueryOp::MATCH_PHRASE_QUERY);
}

inline bool is_equal_query(InvertedIndexQueryOp query_op) {
    return (query_op == InvertedIndexQueryOp::EQUAL_QUERY);
}

inline bool is_range_query(InvertedIndexQueryOp query_op) {
    return (query_op == InvertedIndexQueryOp::LESS_EQUAL_QUERY ||
            query_op == InvertedIndexQueryOp::LESS_THAN_QUERY ||
            query_op == InvertedIndexQueryOp::GREATER_THAN_QUERY ||
            query_op == InvertedIndexQueryOp::GREATER_EQUAL_QUERY);
}

template <PrimitiveType Type>
//template <FieldType field_type>
class InvertedIndexQuery {
    using CppType = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;

public:
    /*class QueryEncodedValue {
    public:
        QueryEncodedValue() = default;
        QueryEncodedValue(const uint8_t* encoded_value) : _encoded_value(encoded_value) {}
        QueryEncodedValue Operator = (const std::string& encoded_value) {
            _encoded_value = (const uint8_t*)encoded_value.c_str();
        }
        ~QueryEncodedValue() = default;

        const uint8_t* encoded_value() const { return _encoded_value; }

        bool operator<(const QueryEncodedValue& value) const { return cmp(value) < 0; }

        bool operator<=(const QueryEncodedValue& value) const { return cmp(value) <= 0; }

        bool operator>(const QueryEncodedValue& value) const { return cmp(value) > 0; }

        bool operator>=(const QueryEncodedValue& value) const { return cmp(value) >= 0; }

        bool operator==(const QueryEncodedValue& value) const { return cmp(value) == 0; }

        bool operator!=(const QueryEncodedValue& value) const { return cmp(value) != 0; }

        int32_t cmp(const QueryEncodedValue& other) const {
            return lucene::util::FutureArrays::CompareUnsigned(
                    (const uint8_t*)encoded_value().c_str(), 0, 0 + sizeof(CppType),
                    (const uint8_t*)other.encoded_value().c_str(), 0, 0 + sizeof(CppType));
        }

    private:
        const uint8_t* _encoded_value;
    };*/

    InvertedIndexQuery(const TypeInfo* type_info)
            : _low_value(TYPE_MIN),
              _high_value(TYPE_MAX),
              _low_op(InvertedIndexQueryOp::GREATER_THAN_QUERY),
              _high_op(InvertedIndexQueryOp::LESS_THAN_QUERY),
              _type_info(type_info) {
        //FieldTypeTraits<field_type>::set_to_max(&_high_value);
        //FieldTypeTraits<field_type>::set_to_min(&_low_value);
        _value_key_coder = get_key_coder(_type_info->type());
        _value_key_coder->full_encode_ascending(&TYPE_MAX, &_high_value_encoded);
        _value_key_coder->full_encode_ascending(&TYPE_MIN, &_low_value_encoded);
    }

    // if op is match, we just need to add match_value to fixed_values_str, no need to add to fixed_values.
    // because fulltext match only uses fixed_values_str.
    Status add_match_value(InvertedIndexQueryOp op, const std::string& match_value) {
        DCHECK(op == InvertedIndexQueryOp::MATCH_ANY_QUERY ||
               op == InvertedIndexQueryOp::MATCH_ALL_QUERY ||
               op == InvertedIndexQueryOp::MATCH_PHRASE_QUERY);
        _fixed_values_str.insert(match_value);
        _high_op = _low_op = op;
        _high_value = TYPE_MIN;
        _low_value = TYPE_MAX;
        return Status::OK();
    }

    Status add_fixed_value(InvertedIndexQueryOp op, const CppType& value,
                           const std::string& value_str);
    Status add_fixed_value(InvertedIndexQueryOp op, const CppType& value) {
        _fixed_values.insert(value);
        std::string tmp;
        _value_key_coder->full_encode_ascending(&value, &tmp);
        _fixed_values_encoded.insert(tmp);

        _high_value = value;
        _low_value = value;
        _high_op = op;
        _low_op = op;

        return Status::OK();
    }

    Status from_string(const std::string& str_value, CppType& value, int precision, int scale);
    Status add_value(InvertedIndexQueryOp op, const CppType& value) {
        if (is_match_query(op) || is_equal_query(op)) {
            return add_fixed_value(op, value);
        }
        if (_high_value > _low_value) {
            switch (op) {
            case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
                if (value >= _low_value) {
                    _low_value = value;
                    _low_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                    _low_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
                if (value > _low_value) {
                    _low_value = value;
                    _low_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                    _low_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::LESS_THAN_QUERY: {
                if (value <= _high_value) {
                    _high_value = value;
                    _high_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                    _high_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
                if (value < _high_value) {
                    _high_value = value;
                    _high_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                    _high_op = op;
                }
                break;
            }

            default: {
                return Status::InternalError(
                        "Add value failed! Unsupported InvertedIndexQueryOp {}", op);
            }
            }
        }

        return Status::OK();
    }
    //NOTE: value_str intentionally copy here
    Status add_value_str(InvertedIndexQueryOp op, std::string value_str, int precision, int scale) {
        CppType value;
        from_string(value_str, value, precision, scale);

        if (is_match_query(op) || is_equal_query(op)) {
            return add_fixed_value(op, value, value_str);
        }
        if (_high_value > _low_value) {
            switch (op) {
            case InvertedIndexQueryOp::GREATER_THAN_QUERY: {
                if (value >= _low_value) {
                    _low_value = value;
                    _low_value_str = value_str;
                    // NOTE:full_encode_ascending will append encoded data to the end of buffer.
                    // so we need to clear buffer first.
                    _low_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                    _low_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::GREATER_EQUAL_QUERY: {
                if (value > _low_value) {
                    _low_value = value;
                    _low_value_str = value_str;
                    _low_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
                    _low_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::LESS_THAN_QUERY: {
                if (value <= _high_value) {
                    _high_value = value;
                    _high_value_str = value_str;
                    _high_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                    _high_op = op;
                }
                break;
            }

            case InvertedIndexQueryOp::LESS_EQUAL_QUERY: {
                if (value < _high_value) {
                    _high_value = value;
                    _high_value_str = value_str;
                    _high_value_encoded.clear();
                    _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
                    _high_op = op;
                }
                break;
            }

            default: {
                return Status::InternalError(
                        "Add value string fail! Unsupported InvertedIndexQueryOp {}", op);
            }
            }
        }

        return Status::OK();
    }
    bool is_point_query() const { return _fixed_values.size() != 0; }
    bool is_range_query() const { return _high_value > _low_value; }
    CppType& lower_value() { return _low_value; }
    CppType& upper_value() { return _high_value; }
    std::string& lower_value_string() { return _low_value_str; }
    std::string& upper_value_string() { return _high_value_str; }
    std::string& lower_value_encoded() { return _low_value_encoded; }
    std::string& upper_value_encoded() { return _high_value_encoded; }
    InvertedIndexQueryOp lower_op() const { return _low_op; }
    InvertedIndexQueryOp upper_op() const { return _high_op; }
    bool get_include_lower() {
        return _low_op == InvertedIndexQueryOp::GREATER_EQUAL_QUERY ||
               _low_op == InvertedIndexQueryOp::EQUAL_QUERY;
    }
    bool get_include_upper() {
        return _high_op == InvertedIndexQueryOp::LESS_EQUAL_QUERY ||
               _high_op == InvertedIndexQueryOp::EQUAL_QUERY;
    }
    bool has_upper_bound() { return _high_value != TYPE_MAX; }
    bool has_lower_bound() { return _low_value != TYPE_MIN; }
    InvertedIndexQueryOp point_op() const {
        DCHECK(_low_op == _high_op);
        return _low_op;
    }
    size_t get_fixed_value_size() { return _fixed_values.size(); }
    const std::set<CppType>& get_fixed_value_set() { return _fixed_values; }
    const std::set<std::string>& get_fixed_value_string_set() { return _fixed_values_str; }
    const std::set<std::string>& get_fixed_value_encoded_set() { return _fixed_values_encoded; }

    CppType get_fixed_value() {
        DCHECK(get_fixed_value_size() > 0);
        return *_fixed_values.begin();
    }
    std::string get_fixed_value_string() {
        DCHECK(get_fixed_value_size() > 0);
        return *_fixed_values_str.begin();
    }
    std::string get_fixed_value_encoded() {
        DCHECK(get_fixed_value_size() > 0);
        return *_fixed_values_encoded.begin();
    }
    std::string to_string() {
        //convert query range to string
        std::stringstream ss;
        if (is_point_query()) {
            ss << inverted_index_query_op_to_string(point_op()) << " ";
            for (auto& value : get_fixed_value_string_set()) {
                ss << value << ",";
            }
            //ss << inverted_index_query_op_to_string(point_op()) << " " << get_fixed_value_string();
        } else if (is_range_query()) {
            if (has_lower_bound()) {
                ss << lower_value_string() << " " << inverted_index_query_op_to_string(lower_op())
                   << " ";
            }
            if (has_upper_bound()) {
                ss << upper_value_string() << " " << inverted_index_query_op_to_string(upper_op())
                   << " ";
            }
        }
        return ss.str();
    }

private:
    const static CppType TYPE_MIN; // Column type's min value
    const static CppType TYPE_MAX; // Column type's max value

    CppType _low_value;
    CppType _high_value;
    std::string _low_value_str;
    std::string _high_value_str;
    std::string _low_value_encoded;
    std::string _high_value_encoded;

    InvertedIndexQueryOp _low_op;
    InvertedIndexQueryOp _high_op;

    std::set<CppType> _fixed_values;
    std::set<std::string> _fixed_values_str;
    std::set<std::string> _fixed_values_encoded;

    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
};

// Define the supported data types for inverted index queries
using InvertedIndexQueryType =
        std::variant<InvertedIndexQuery<TYPE_BOOLEAN>, InvertedIndexQuery<TYPE_DATE>,
                     InvertedIndexQuery<TYPE_DATETIME>, InvertedIndexQuery<TYPE_DATETIMEV2>,
                     InvertedIndexQuery<TYPE_DATEV2>, InvertedIndexQuery<TYPE_TINYINT>,
                     InvertedIndexQuery<TYPE_SMALLINT>, InvertedIndexQuery<TYPE_INT>,
                     InvertedIndexQuery<TYPE_LARGEINT>, InvertedIndexQuery<TYPE_DECIMAL32>,
                     InvertedIndexQuery<TYPE_DECIMAL64>, InvertedIndexQuery<TYPE_DECIMAL128I>,
                     InvertedIndexQuery<TYPE_DECIMALV2>, InvertedIndexQuery<TYPE_DOUBLE>,
                     InvertedIndexQuery<TYPE_FLOAT>, InvertedIndexQuery<TYPE_BIGINT>,
                     InvertedIndexQuery<TYPE_CHAR>, InvertedIndexQuery<TYPE_VARCHAR>,
                     InvertedIndexQuery<TYPE_STRING>>;
/*using InvertedIndexQueryRangeType =
        std::variant<InvertedIndexQueryRange<OLAP_FIELD_TYPE_BOOL>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DATE>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DATETIME>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DATETIMEV2>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DATEV2>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_TINYINT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_SMALLINT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_INT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_UNSIGNED_INT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_LARGEINT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DECIMAL>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DECIMAL32>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DECIMAL64>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DECIMAL128I>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_DOUBLE>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_FLOAT>,
                     InvertedIndexQueryRange<OLAP_FIELD_TYPE_BIGINT>>;*/

template <PrimitiveType field_type>
const typename InvertedIndexQuery<field_type>::CppType InvertedIndexQuery<field_type>::TYPE_MIN =
        type_limit<typename InvertedIndexQuery<field_type>::CppType>::min();
template <PrimitiveType field_type>
const typename InvertedIndexQuery<field_type>::CppType InvertedIndexQuery<field_type>::TYPE_MAX =
        type_limit<typename InvertedIndexQuery<field_type>::CppType>::max();

class InvertedIndexQueryRangeTypeFactory {
public:
    // Create an inverted index query range for the specified column description
    //template <FieldType field_type>
    template <PrimitiveType field_type>
    static InvertedIndexQueryType* create_inverted_index_query_range(const TypeInfo* type_info) {
        return new InvertedIndexQueryType(InvertedIndexQuery<field_type>(type_info));
    }

    // Create an inverted index query range for the specified column description
    // Returns an error status if the column type is not supported
    static Status create_inverted_index_query(const TypeInfo* type_info,
                                              InvertedIndexQueryType** query_range) {
        switch (type_info->type()) {
        case OLAP_FIELD_TYPE_DATETIME:
            *query_range = create_inverted_index_query_range<TYPE_DATETIME>(type_info);
            break;
        case OLAP_FIELD_TYPE_DATE:
            *query_range = create_inverted_index_query_range<TYPE_DATE>(type_info);
            break;
        case OLAP_FIELD_TYPE_DATETIMEV2:
            *query_range = create_inverted_index_query_range<TYPE_DATETIMEV2>(type_info);
            break;
        case OLAP_FIELD_TYPE_DATEV2:
            *query_range = create_inverted_index_query_range<TYPE_DATEV2>(type_info);
            break;
        case OLAP_FIELD_TYPE_TINYINT:
            *query_range = create_inverted_index_query_range<TYPE_TINYINT>(type_info);
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            *query_range = create_inverted_index_query_range<TYPE_SMALLINT>(type_info);
            break;
        case OLAP_FIELD_TYPE_INT:
            *query_range = create_inverted_index_query_range<TYPE_INT>(type_info);
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            *query_range = create_inverted_index_query_range<TYPE_LARGEINT>(type_info);
            break;
        case OLAP_FIELD_TYPE_DECIMAL32:
            *query_range = create_inverted_index_query_range<TYPE_DECIMAL32>(type_info);
            break;
        case OLAP_FIELD_TYPE_DECIMAL64:
            *query_range = create_inverted_index_query_range<TYPE_DECIMAL64>(type_info);
            break;
        case OLAP_FIELD_TYPE_DECIMAL128I:
            *query_range = create_inverted_index_query_range<TYPE_DECIMAL128I>(type_info);
            break;
        case OLAP_FIELD_TYPE_DOUBLE:
            *query_range = create_inverted_index_query_range<TYPE_DOUBLE>(type_info);
            break;
        case OLAP_FIELD_TYPE_FLOAT:
            *query_range = create_inverted_index_query_range<TYPE_FLOAT>(type_info);
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            *query_range = create_inverted_index_query_range<TYPE_BIGINT>(type_info);
            break;
        case OLAP_FIELD_TYPE_BOOL:
            *query_range = create_inverted_index_query_range<TYPE_BOOLEAN>(type_info);
            break;
        case OLAP_FIELD_TYPE_CHAR:
            *query_range = create_inverted_index_query_range<TYPE_CHAR>(type_info);
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            *query_range = create_inverted_index_query_range<TYPE_VARCHAR>(type_info);
            break;
        case OLAP_FIELD_TYPE_STRING:
            *query_range = create_inverted_index_query_range<TYPE_STRING>(type_info);
            break;
        default:
            return Status::NotSupported("Unsupported column type for inverted index {}",
                                        type_info->type());
        }
        return Status::OK();
    }
};

/*template <FieldType field_type>
static Status add_range(InvertedIndexQueryRange<field_type>& query_range, const std::string& value,
                        InvertedIndexQueryOp query_op) {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    CppType tmp;
    FieldTypeTraits<field_type>::from_string(&tmp, value, 0, 0);
    query_range.add_range(query_op, tmp);
    return Status::OK();
}*/

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
    //InvertedIndexQueryOp query_type;

    //public:
    //    std::string queryMin;
    //    std::string queryMax;

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
