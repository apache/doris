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

#include <charconv>

#include "exprs/bloomfilter_predicate.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/null_predicate.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
#include "util/string_util.h"

namespace doris {

template <typename ConditionType>
class PredicateCreator {
public:
    virtual ColumnPredicate* create(const TabletColumn& column, int index,
                                    const ConditionType& conditions, bool opposite,
                                    MemPool* pool) = 0;
    virtual ~PredicateCreator() = default;
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class IntegerPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return new InListPredicateBase<Type, PT>(index, conditions, convert, opposite);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, convert(conditions), opposite);
        }
    }

private:
    static CppType convert(const std::string& condition) {
        CppType value = 0;
        std::from_chars(condition.data(), condition.data() + condition.size(), value);
        return value;
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class DecimalPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return new InListPredicateBase<Type, PT>(index, conditions, convert, opposite, &column);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, convert(column, conditions),
                                                         opposite);
        }
    }

private:
    static CppType convert(const TabletColumn& column, const std::string& condition) {
        StringParser::ParseResult result = StringParser::ParseResult::PARSE_SUCCESS;
        // return CppType value cast from int128_t
        return StringParser::string_to_decimal<int128_t>(
                condition.data(), condition.size(), column.precision(), column.frac(), &result);
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class StringPredicateCreator : public PredicateCreator<ConditionType> {
public:
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return new InListPredicateBase<Type, PT>(index, conditions, convert, opposite, &column,
                                                     pool);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, convert(column, conditions, pool),
                                                         opposite);
        }
    }

private:
    static StringValue convert(const TabletColumn& column, const std::string& condition,
                               MemPool* pool) {
        size_t length = condition.length();
        if constexpr (Type == TYPE_CHAR) {
            length = std::max(static_cast<size_t>(column.length()), length);
        }

        char* buffer = reinterpret_cast<char*>(pool->allocate(length));
        memset(buffer, 0, length);
        memory_copy(buffer, condition.data(), condition.length());

        return StringValue(buffer, length);
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
struct CustomPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;
    CustomPredicateCreator(const std::function<CppType(const std::string& condition)>& convert)
            : _convert(convert) {};

    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, MemPool* pool) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return new InListPredicateBase<Type, PT>(index, conditions, _convert, opposite);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, _convert(conditions), opposite);
        }
    }

private:
    std::function<CppType(const std::string& condition)> _convert;
};

template <PredicateType PT, typename ConditionType>
inline std::unique_ptr<PredicateCreator<ConditionType>> get_creator(const FieldType& type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_TINYINT, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_SMALLINT, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_INT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_INT, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_BIGINT, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_LARGEINT, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        return std::make_unique<CustomPredicateCreator<TYPE_DECIMALV2, PT, ConditionType>>(
                [](const std::string& condition) {
                    decimal12_t value = {0, 0};
                    value.from_string(condition);
                    return value;
                });
    }
    case OLAP_FIELD_TYPE_DECIMAL32: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL32, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL64: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL64, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DECIMAL128: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL128, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<StringPredicateCreator<TYPE_CHAR, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING: {
        return std::make_unique<StringPredicateCreator<TYPE_STRING, PT, ConditionType>>();
    }
    case OLAP_FIELD_TYPE_DATE: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATE, PT, ConditionType>>(
                timestamp_from_date);
    }
    case OLAP_FIELD_TYPE_DATEV2: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATEV2, PT, ConditionType>>(
                timestamp_from_date_v2);
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATETIME, PT, ConditionType>>(
                timestamp_from_datetime);
    }
    case OLAP_FIELD_TYPE_DATETIMEV2: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATETIMEV2, PT, ConditionType>>(
                timestamp_from_datetime_v2);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        return std::make_unique<CustomPredicateCreator<TYPE_BOOLEAN, PT, ConditionType>>(
                [](const std::string& condition) {
                    int32_t ivalue = 0;
                    auto result = std::from_chars(condition.data(),
                                                  condition.data() + condition.size(), ivalue);
                    if (result.ec == std::errc()) {
                        return bool(ivalue);
                    }

                    StringParser::ParseResult parse_result;
                    bool value = StringParser::string_to_bool(condition.data(), condition.size(),
                                                              &parse_result);
                    return value;
                });
    }
    default:
        return nullptr;
    }
}

template <PredicateType PT, typename ConditionType>
inline ColumnPredicate* create_predicate(const TabletColumn& column, int index,
                                         const ConditionType& conditions, bool opposite,
                                         MemPool* pool) {
    return get_creator<PT, ConditionType>(column.type())
            ->create(column, index, conditions, opposite, pool);
}

template <PredicateType PT>
inline ColumnPredicate* create_comparison_predicate(const TabletColumn& column, int index,
                                                    const std::string& condition, bool opposite,
                                                    MemPool* pool) {
    static_assert(PredicateTypeTraits::is_comparison(PT));
    return create_predicate<PT, std::string>(column, index, condition, opposite, pool);
}

template <PredicateType PT>
inline ColumnPredicate* create_list_predicate(const TabletColumn& column, int index,
                                              const std::vector<std::string>& conditions,
                                              bool opposite, MemPool* pool) {
    static_assert(PredicateTypeTraits::is_list(PT));
    return create_predicate<PT, std::vector<std::string>>(column, index, conditions, opposite,
                                                          pool);
}

// This method is called in reader and in deletehandler.
// When it is called by delete handler, then it should use the delete predicate's tablet schema
// to parse the conditions.
inline ColumnPredicate* parse_to_predicate(TabletSchemaSPtr tablet_schema,
                                           const TCondition& condition, MemPool* mem_pool,
                                           bool opposite = false) {
    int32_t col_unique_id = condition.column_unique_id;
    // TODO: not equal and not in predicate is not pushed down
    const TabletColumn& column = tablet_schema->column_by_uid(col_unique_id);
    uint32_t index = tablet_schema->field_index(col_unique_id);

    if (to_lower(condition.condition_op) == "is") {
        return new NullPredicate(index, to_lower(condition.condition_values[0]) == "null",
                                 opposite);
    }

    if ((condition.condition_op == "*=" || condition.condition_op == "!*=") &&
        condition.condition_values.size() > 1) {
        decltype(create_list_predicate<PredicateType::UNKNOWN>)* create = nullptr;

        if (condition.condition_op == "*=") {
            create = create_list_predicate<PredicateType::IN_LIST>;
        } else {
            create = create_list_predicate<PredicateType::NOT_IN_LIST>;
        }
        return create(column, index, condition.condition_values, opposite, mem_pool);
    }

    decltype(create_comparison_predicate<PredicateType::UNKNOWN>)* create = nullptr;
    if (condition.condition_op == "*=" || condition.condition_op == "=") {
        create = create_comparison_predicate<PredicateType::EQ>;
    } else if (condition.condition_op == "!*=" || condition.condition_op == "!=") {
        create = create_comparison_predicate<PredicateType::NE>;
    } else if (condition.condition_op == "<<") {
        create = create_comparison_predicate<PredicateType::LT>;
    } else if (condition.condition_op == "<=") {
        create = create_comparison_predicate<PredicateType::LE>;
    } else if (condition.condition_op == ">>") {
        create = create_comparison_predicate<PredicateType::GT>;
    } else if (condition.condition_op == ">=") {
        create = create_comparison_predicate<PredicateType::GE>;
    }
    return create(column, index, condition.condition_values[0], opposite, mem_pool);
}

#define APPLY_FOR_PRIMTYPE(M) \
    M(TYPE_TINYINT)           \
    M(TYPE_SMALLINT)          \
    M(TYPE_INT)               \
    M(TYPE_BIGINT)            \
    M(TYPE_LARGEINT)          \
    M(TYPE_FLOAT)             \
    M(TYPE_DOUBLE)            \
    M(TYPE_CHAR)              \
    M(TYPE_DATE)              \
    M(TYPE_DATETIME)          \
    M(TYPE_DATEV2)            \
    M(TYPE_DATETIMEV2)        \
    M(TYPE_VARCHAR)           \
    M(TYPE_STRING)            \
    M(TYPE_DECIMAL32)         \
    M(TYPE_DECIMAL64)         \
    M(TYPE_DECIMAL128)

template <PrimitiveType PT>
inline ColumnPredicate* create_olap_column_predicate(
        uint32_t column_id, const std::shared_ptr<BloomFilterFuncBase>& filter, int be_exec_version,
        const TabletColumn* column = nullptr) {
    std::shared_ptr<BloomFilterFuncBase> filter_olap;
    filter_olap.reset(create_bloom_filter(PT));
    filter_olap->light_copy(filter.get());
    return new BloomFilterColumnPredicate<PT>(column_id, filter, be_exec_version);
}

template <PrimitiveType PT>
inline ColumnPredicate* create_olap_column_predicate(uint32_t column_id,
                                                     const std::shared_ptr<HybridSetBase>& filter,
                                                     int be_exec_version,
                                                     const TabletColumn* column = nullptr) {
    return new InListPredicateBase<PT, PredicateType::IN_LIST>(column_id, filter, column->length());
}

template <typename T>
inline ColumnPredicate* create_column_predicate(uint32_t column_id,
                                                const std::shared_ptr<T>& filter, FieldType type,
                                                int be_exec_version,
                                                const TabletColumn* column = nullptr) {
    switch (type) {
#define M(NAME)                                                                                \
    case OLAP_FIELD_##NAME: {                                                                  \
        return create_olap_column_predicate<NAME>(column_id, filter, be_exec_version, column); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case OLAP_FIELD_TYPE_DECIMAL: {
        return create_olap_column_predicate<TYPE_DECIMALV2>(column_id, filter, be_exec_version,
                                                            column);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        return create_olap_column_predicate<TYPE_BOOLEAN>(column_id, filter, be_exec_version,
                                                          column);
    }
    default:
        return nullptr;
    }
}

} //namespace doris
