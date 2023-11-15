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

#include <CLucene/util/FutureArrays.h>
#include <CLucene/util/bkd/bkd_reader.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/tablet_schema.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util::bkd {
class bkd_docid_set_iterator;
} // namespace util::bkd
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;
class RuntimeState;
enum class PredicateType;

namespace segment_v2 {

enum class QueryCategory { POINT_QUERY, RANGE_QUERY };

class InvertedIndexQueryBase {
public:
    virtual std::string to_string() = 0;
    virtual ~InvertedIndexQueryBase() = default;
    virtual QueryCategory get_query_category() = 0;
    [[nodiscard]] virtual PredicateType get_predicate_type() const = 0;
    template <PredicateType PT>
    static Status create_and_add_value_from_field_type(
            const TypeInfo* type_info, char* value, InvertedIndexQueryType t,
            std::unique_ptr<InvertedIndexQueryBase>& result);
};

template <PrimitiveType Type, PredicateType PT>
struct Helper;

class InvertedIndexPointQueryI : public InvertedIndexQueryBase {
public:
    InvertedIndexPointQueryI() = default;
    ~InvertedIndexPointQueryI() override = default;
    QueryCategory get_query_category() override { return QueryCategory::POINT_QUERY; }
    std::string to_string() override {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexPointQueryI");
        __builtin_unreachable();
    }
    [[nodiscard]] PredicateType get_predicate_type() const override {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexPointQueryI");
        __builtin_unreachable();
    }
    [[nodiscard]] virtual const std::vector<std::string>& get_values() const {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexPointQueryI");
        __builtin_unreachable();
    };
    [[nodiscard]] virtual InvertedIndexQueryType get_query_type() const {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexPointQueryI");
        __builtin_unreachable();
    };
};

template <PrimitiveType Type, PredicateType PT>
class InvertedIndexPointQuery : public InvertedIndexPointQueryI {
public:
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    InvertedIndexPointQuery(const TypeInfo* type_info);

    Status add_value(const T& value, InvertedIndexQueryType t);
    std::string to_string() override;
    [[nodiscard]] const std::vector<std::string>& get_values() const override {
        return _values_encoded;
    };
    [[nodiscard]] PredicateType get_predicate_type() const override { return PT; };
    [[nodiscard]] InvertedIndexQueryType get_query_type() const override { return _type; };

private:
    std::vector<std::string> _values_encoded;
    const KeyCoder* _value_key_coder {};
    const TypeInfo* _type_info {};
    std::vector<const T*> _values;
    InvertedIndexQueryType _type;
};

template <PrimitiveType Type, PredicateType PT>
struct Helper {
    static Status create_and_add_value(const TypeInfo* type_info, char* value,
                                       InvertedIndexQueryType t,
                                       std::unique_ptr<InvertedIndexQueryBase>& result);
};

class InvertedIndexRangeQueryI : public InvertedIndexQueryBase {
public:
    InvertedIndexRangeQueryI() = default;
    ~InvertedIndexRangeQueryI() override = default;
    [[nodiscard]] virtual const std::string& get_low_value() const = 0;
    [[nodiscard]] virtual const std::string& get_high_value() const = 0;
    virtual bool low_value_is_null() = 0;
    virtual bool high_value_is_null() = 0;
    QueryCategory get_query_category() override { return QueryCategory::RANGE_QUERY; }
    std::string to_string() override {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexRangeQueryI");
        __builtin_unreachable();
    };
    [[nodiscard]] PredicateType get_predicate_type() const override {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexRangeQueryI");
        __builtin_unreachable();
    };
    [[nodiscard]] virtual bool is_low_value_inclusive() const {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexRangeQueryI");
        __builtin_unreachable();
    }
    [[nodiscard]] virtual bool is_high_value_inclusive() const {
        LOG_FATAL("Execution reached an undefined behavior code path in InvertedIndexRangeQueryI");
        __builtin_unreachable();
    }
};

class BinaryType {
    ENABLE_FACTORY_CREATOR(BinaryType);

public:
    const uint8_t* _data {};
    size_t _size {};

    BinaryType() = default;
    BinaryType(const uint8_t* data, size_t size) : _data(data), _size(size) {}
    BinaryType(const std::string& str)
            : _data(reinterpret_cast<const uint8_t*>(str.data())), _size(str.size()) {}

    int operator<(const BinaryType& other) const {
        return lucene::util::FutureArrays::CompareUnsigned(_data, 0, _size, other._data, 0,
                                                           other._size) < 0;
    }

    int operator<=(const BinaryType& other) const {
        return lucene::util::FutureArrays::CompareUnsigned(_data, 0, _size, other._data, 0,
                                                           other._size) <= 0;
    }

    int operator>(const BinaryType& other) const {
        return lucene::util::FutureArrays::CompareUnsigned(_data, 0, _size, other._data, 0,
                                                           other._size) > 0;
    }

    int operator>=(const BinaryType& other) const {
        return lucene::util::FutureArrays::CompareUnsigned(_data, 0, _size, other._data, 0,
                                                           other._size) >= 0;
    }
};

template <PrimitiveType Type, PredicateType PT>
class InvertedIndexRangeQuery : public InvertedIndexRangeQueryI {
public:
    using T = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    InvertedIndexRangeQuery(const TypeInfo* type_info);
    Status add_value(const T& value, InvertedIndexQueryType t);
    [[nodiscard]] const std::string& get_low_value() const override { return _low_value_encoded; };
    [[nodiscard]] const std::string& get_high_value() const override {
        return _high_value_encoded;
    };
    bool low_value_is_null() override { return _low_value == nullptr; };
    bool high_value_is_null() override { return _high_value == nullptr; };
    std::string to_string() override;
    [[nodiscard]] PredicateType get_predicate_type() const override { return PT; };
    [[nodiscard]] bool is_low_value_inclusive() const override { return _inclusive_low; }
    [[nodiscard]] bool is_high_value_inclusive() const override { return _inclusive_high; }

private:
    const T* _low_value {};
    const T* _high_value {};
    std::string _low_value_encoded {};
    std::string _high_value_encoded {};

    const KeyCoder* _value_key_coder {};
    const TypeInfo* _type_info {};
    bool _inclusive_high {};
    bool _inclusive_low {};
};
} // namespace segment_v2
} // namespace doris