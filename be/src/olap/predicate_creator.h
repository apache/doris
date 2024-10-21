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
#include <type_traits>

#include "exec/olap_utils.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/match_predicate.h"
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
                                    vectorized::Arena* arena) = 0;
    virtual ~PredicateCreator() = default;
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class IntegerPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PrimitiveTypeTraits<Type>::CppType;
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, vectorized::Arena* arena) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return create_in_list_predicate<Type, PT, ConditionType, decltype(convert)>(
                    index, conditions, convert, opposite);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, convert(conditions), opposite);
        }
    }

private:
    static CppType convert(const std::string& condition) {
        CppType value = 0;
        // because std::from_chars can't compile on macOS
        if constexpr (std::is_same_v<CppType, double>) {
            value = std::stod(condition, nullptr);
        } else if constexpr (std::is_same_v<CppType, float>) {
            value = std::stof(condition, nullptr);
        } else {
            std::from_chars(condition.data(), condition.data() + condition.size(), value);
        }
        return value;
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class DecimalPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PrimitiveTypeTraits<Type>::CppType;
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, vectorized::Arena* arena) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return create_in_list_predicate<Type, PT, ConditionType, decltype(convert)>(
                    index, conditions, convert, opposite, &column);
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
        return CppType(StringParser::string_to_decimal<Type>(
                condition.data(), condition.size(), column.precision(), column.frac(), &result));
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
class StringPredicateCreator : public PredicateCreator<ConditionType> {
public:
    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, vectorized::Arena* arena) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return create_in_list_predicate<Type, PT, ConditionType, decltype(convert)>(
                    index, conditions, convert, opposite, &column, arena);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, convert(column, conditions, arena),
                                                         opposite);
        }
    }

private:
    static StringRef convert(const TabletColumn& column, const std::string& condition,
                             vectorized::Arena* arena) {
        size_t length = condition.length();
        if constexpr (Type == TYPE_CHAR) {
            length = std::max(static_cast<size_t>(column.length()), length);
        }

        char* buffer = arena->alloc(length);
        memset(buffer, 0, length);
        memcpy(buffer, condition.data(), condition.length());

        return {buffer, length};
    }
};

template <PrimitiveType Type, PredicateType PT, typename ConditionType>
struct CustomPredicateCreator : public PredicateCreator<ConditionType> {
public:
    using CppType = typename PrimitiveTypeTraits<Type>::CppType;
    CustomPredicateCreator(const std::function<CppType(const std::string& condition)>& convert)
            : _convert(convert) {}

    ColumnPredicate* create(const TabletColumn& column, int index, const ConditionType& conditions,
                            bool opposite, vectorized::Arena* arena) override {
        if constexpr (PredicateTypeTraits::is_list(PT)) {
            return create_in_list_predicate<Type, PT, ConditionType, decltype(_convert)>(
                    index, conditions, _convert, opposite);
        } else {
            static_assert(PredicateTypeTraits::is_comparison(PT));
            return new ComparisonPredicateBase<Type, PT>(index, _convert(conditions), opposite);
        }
    }

private:
    std::function<CppType(const std::string& condition)> _convert;
};

template <PredicateType PT, typename ConditionType>
std::unique_ptr<PredicateCreator<ConditionType>> get_creator(const FieldType& type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_TINYINT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_SMALLINT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_INT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_BIGINT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_LARGEINT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return std::make_unique<IntegerPredicateCreator<TYPE_FLOAT, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return std::make_unique<IntegerPredicateCreator<TYPE_DOUBLE, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return std::make_unique<CustomPredicateCreator<TYPE_DECIMALV2, PT, ConditionType>>(
                [](const std::string& condition) {
                    decimal12_t value = {0, 0};
                    static_cast<void>(value.from_string(condition));
                    // Decimal12t is storage type, we need convert to compute type here to
                    // do comparisons
                    return DecimalV2Value(value.integer, value.fraction);
                });
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL32, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL64, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL128I, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        return std::make_unique<DecimalPredicateCreator<TYPE_DECIMAL256, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<StringPredicateCreator<TYPE_CHAR, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        return std::make_unique<StringPredicateCreator<TYPE_STRING, PT, ConditionType>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATE, PT, ConditionType>>(
                timestamp_from_date);
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATEV2, PT, ConditionType>>(
                timestamp_from_date_v2);
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATETIME, PT, ConditionType>>(
                timestamp_from_datetime);
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return std::make_unique<CustomPredicateCreator<TYPE_DATETIMEV2, PT, ConditionType>>(
                timestamp_from_datetime_v2);
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
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
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        return std::make_unique<CustomPredicateCreator<TYPE_IPV4, PT, ConditionType>>(
                [](const std::string& condition) {
                    IPv4 value;
                    bool res = IPv4Value::from_string(value, condition);
                    DCHECK(res);
                    return value;
                });
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        return std::make_unique<CustomPredicateCreator<TYPE_IPV6, PT, ConditionType>>(
                [](const std::string& condition) {
                    IPv6 value;
                    bool res = IPv6Value::from_string(value, condition);
                    DCHECK(res);
                    return value;
                });
    }
    default:
        return nullptr;
    }
}

template <PredicateType PT, typename ConditionType>
ColumnPredicate* create_predicate(const TabletColumn& column, int index,
                                  const ConditionType& conditions, bool opposite,
                                  vectorized::Arena* arena) {
    return get_creator<PT, ConditionType>(column.type())
            ->create(column, index, conditions, opposite, arena);
}

template <PredicateType PT>
ColumnPredicate* create_comparison_predicate(const TabletColumn& column, int index,
                                             const std::string& condition, bool opposite,
                                             vectorized::Arena* arena) {
    static_assert(PredicateTypeTraits::is_comparison(PT));
    return create_predicate<PT, std::string>(column, index, condition, opposite, arena);
}

template <PredicateType PT>
ColumnPredicate* create_list_predicate(const TabletColumn& column, int index,
                                       const std::vector<std::string>& conditions, bool opposite,
                                       vectorized::Arena* arena) {
    static_assert(PredicateTypeTraits::is_list(PT));
    return create_predicate<PT, std::vector<std::string>>(column, index, conditions, opposite,
                                                          arena);
}

// This method is called in reader and in deletehandler.
// The "column" parameter might represent a column resulting from the decomposition of a variant column.
inline ColumnPredicate* parse_to_predicate(const TabletColumn& column, uint32_t index,
                                           const TCondition& condition, vectorized::Arena* arena,
                                           bool opposite = false) {
    if (to_lower(condition.condition_op) == "is") {
        return new NullPredicate(index, to_lower(condition.condition_values[0]) == "null",
                                 opposite);
    } else if (is_match_condition(condition.condition_op)) {
        return new MatchPredicate(index, condition.condition_values[0],
                                  to_match_type(condition.condition_op));
    }

    if ((condition.condition_op == "*=" || condition.condition_op == "!*=") &&
        condition.condition_values.size() > 1) {
        decltype(create_list_predicate<PredicateType::UNKNOWN>)* create = nullptr;

        if (condition.condition_op == "*=") {
            create = create_list_predicate<PredicateType::IN_LIST>;
        } else {
            create = create_list_predicate<PredicateType::NOT_IN_LIST>;
        }
        return create(column, index, condition.condition_values, opposite, arena);
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
    return create(column, index, condition.condition_values[0], opposite, arena);
}

} //namespace doris
