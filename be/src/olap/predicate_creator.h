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

#include <fast_float/fast_float.h>

#include <charconv>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "exec/olap_utils.h"
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
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/cast/cast_parameters.h"
#include "vec/functions/cast/cast_to_basic_number_common.h"

namespace doris {
#include "common/compile_check_begin.h"

template <PrimitiveType TYPE, PredicateType PT>
std::shared_ptr<ColumnPredicate> create_in_list_predicate(const uint32_t cid,
                                                          const std::string col_name,
                                                          const std::shared_ptr<HybridSetBase>& set,
                                                          bool is_opposite,
                                                          size_t char_length = 0) {
    auto set_size = set->size();
    if (set_size == 1) {
        return InListPredicateBase<TYPE, PT, 1>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 2) {
        return InListPredicateBase<TYPE, PT, 2>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 3) {
        return InListPredicateBase<TYPE, PT, 3>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 4) {
        return InListPredicateBase<TYPE, PT, 4>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 5) {
        return InListPredicateBase<TYPE, PT, 5>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 6) {
        return InListPredicateBase<TYPE, PT, 6>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == 7) {
        return InListPredicateBase<TYPE, PT, 7>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else if (set_size == FIXED_CONTAINER_MAX_SIZE) {
        return InListPredicateBase<TYPE, PT, 8>::create_shared(cid, col_name, set, is_opposite,
                                                               char_length);
    } else {
        return InListPredicateBase<TYPE, PT, FIXED_CONTAINER_MAX_SIZE + 1>::create_shared(
                cid, col_name, set, is_opposite, char_length);
    }
}

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_in_list_predicate(const uint32_t cid,
                                                          const std::string col_name,
                                                          const vectorized::DataTypePtr& data_type,
                                                          const std::shared_ptr<HybridSetBase> set,
                                                          bool is_opposite) {
    switch (data_type->get_primitive_type()) {
    case TYPE_TINYINT: {
        return create_in_list_predicate<TYPE_TINYINT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_SMALLINT: {
        return create_in_list_predicate<TYPE_SMALLINT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_INT: {
        return create_in_list_predicate<TYPE_INT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_BIGINT: {
        return create_in_list_predicate<TYPE_BIGINT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_LARGEINT: {
        return create_in_list_predicate<TYPE_LARGEINT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_FLOAT: {
        return create_in_list_predicate<TYPE_FLOAT, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DOUBLE: {
        return create_in_list_predicate<TYPE_DOUBLE, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMALV2: {
        return create_in_list_predicate<TYPE_DECIMALV2, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL32: {
        return create_in_list_predicate<TYPE_DECIMAL32, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL64: {
        return create_in_list_predicate<TYPE_DECIMAL64, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL128I: {
        return create_in_list_predicate<TYPE_DECIMAL128I, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL256: {
        return create_in_list_predicate<TYPE_DECIMAL256, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_CHAR: {
        return create_in_list_predicate<TYPE_CHAR, PT>(
                cid, col_name, set, is_opposite,
                assert_cast<const vectorized::DataTypeString*>(
                        vectorized::remove_nullable(data_type).get())
                        ->len());
    }
    case TYPE_VARCHAR: {
        return create_in_list_predicate<TYPE_VARCHAR, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_STRING: {
        return create_in_list_predicate<TYPE_STRING, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DATE: {
        return create_in_list_predicate<TYPE_DATE, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DATEV2: {
        return create_in_list_predicate<TYPE_DATEV2, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DATETIME: {
        return create_in_list_predicate<TYPE_DATETIME, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_DATETIMEV2: {
        return create_in_list_predicate<TYPE_DATETIMEV2, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_TIMESTAMPTZ: {
        return create_in_list_predicate<TYPE_TIMESTAMPTZ, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_BOOLEAN: {
        return create_in_list_predicate<TYPE_BOOLEAN, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_IPV4: {
        return create_in_list_predicate<TYPE_IPV4, PT>(cid, col_name, set, is_opposite);
    }
    case TYPE_IPV6: {
        return create_in_list_predicate<TYPE_IPV6, PT>(cid, col_name, set, is_opposite);
    }
    default:
        throw Exception(Status::InternalError("Unsupported type {} for in_predicate",
                                              type_to_string(data_type->get_primitive_type())));
        return nullptr;
    }
}

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_comparison_predicate(
        const uint32_t cid, const std::string col_name, const vectorized::DataTypePtr& data_type,
        const vectorized::Field& value, bool opposite) {
    switch (data_type->get_primitive_type()) {
    case TYPE_TINYINT: {
        return ComparisonPredicateBase<TYPE_TINYINT, PT>::create_shared(cid, col_name, value,
                                                                        opposite);
    }
    case TYPE_SMALLINT: {
        return ComparisonPredicateBase<TYPE_SMALLINT, PT>::create_shared(cid, col_name, value,
                                                                         opposite);
    }
    case TYPE_INT: {
        return ComparisonPredicateBase<TYPE_INT, PT>::create_shared(cid, col_name, value, opposite);
    }
    case TYPE_BIGINT: {
        return ComparisonPredicateBase<TYPE_BIGINT, PT>::create_shared(cid, col_name, value,
                                                                       opposite);
    }
    case TYPE_LARGEINT: {
        return ComparisonPredicateBase<TYPE_LARGEINT, PT>::create_shared(cid, col_name, value,
                                                                         opposite);
    }
    case TYPE_FLOAT: {
        return ComparisonPredicateBase<TYPE_FLOAT, PT>::create_shared(cid, col_name, value,
                                                                      opposite);
    }
    case TYPE_DOUBLE: {
        return ComparisonPredicateBase<TYPE_DOUBLE, PT>::create_shared(cid, col_name, value,
                                                                       opposite);
    }
    case TYPE_DECIMALV2: {
        return ComparisonPredicateBase<TYPE_DECIMALV2, PT>::create_shared(cid, col_name, value,
                                                                          opposite);
    }
    case TYPE_DECIMAL32: {
        return ComparisonPredicateBase<TYPE_DECIMAL32, PT>::create_shared(cid, col_name, value,
                                                                          opposite);
    }
    case TYPE_DECIMAL64: {
        return ComparisonPredicateBase<TYPE_DECIMAL64, PT>::create_shared(cid, col_name, value,
                                                                          opposite);
    }
    case TYPE_DECIMAL128I: {
        return ComparisonPredicateBase<TYPE_DECIMAL128I, PT>::create_shared(cid, col_name, value,
                                                                            opposite);
    }
    case TYPE_DECIMAL256: {
        return ComparisonPredicateBase<TYPE_DECIMAL256, PT>::create_shared(cid, col_name, value,
                                                                           opposite);
    }
    case TYPE_CHAR: {
        auto target =
                std::max(cast_set<size_t>(assert_cast<const vectorized::DataTypeString*>(
                                                  vectorized::remove_nullable(data_type).get())
                                                  ->len()),
                         value.template get<TYPE_CHAR>().size());
        if (target > value.template get<TYPE_CHAR>().size()) {
            std::string tmp(target, '\0');
            memcpy(tmp.data(), value.template get<TYPE_CHAR>().data(),
                   value.template get<TYPE_CHAR>().size());
            return ComparisonPredicateBase<TYPE_CHAR, PT>::create_shared(
                    cid, col_name, vectorized::Field::create_field<TYPE_CHAR>(std::move(tmp)),
                    opposite);
        } else {
            return ComparisonPredicateBase<TYPE_CHAR, PT>::create_shared(
                    cid, col_name,
                    vectorized::Field::create_field<TYPE_CHAR>(value.template get<TYPE_CHAR>()),
                    opposite);
        }
    }
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        return ComparisonPredicateBase<TYPE_STRING, PT>::create_shared(cid, col_name, value,
                                                                       opposite);
    }
    case TYPE_DATE: {
        return ComparisonPredicateBase<TYPE_DATE, PT>::create_shared(cid, col_name, value,
                                                                     opposite);
    }
    case TYPE_DATEV2: {
        return ComparisonPredicateBase<TYPE_DATEV2, PT>::create_shared(cid, col_name, value,
                                                                       opposite);
    }
    case TYPE_DATETIME: {
        return ComparisonPredicateBase<TYPE_DATETIME, PT>::create_shared(cid, col_name, value,
                                                                         opposite);
    }
    case TYPE_DATETIMEV2: {
        return ComparisonPredicateBase<TYPE_DATETIMEV2, PT>::create_shared(cid, col_name, value,
                                                                           opposite);
    }
    case TYPE_TIMESTAMPTZ: {
        return ComparisonPredicateBase<TYPE_TIMESTAMPTZ, PT>::create_shared(cid, col_name, value,
                                                                            opposite);
    }
    case TYPE_BOOLEAN: {
        return ComparisonPredicateBase<TYPE_BOOLEAN, PT>::create_shared(cid, col_name, value,
                                                                        opposite);
    }
    case TYPE_IPV4: {
        return ComparisonPredicateBase<TYPE_IPV4, PT>::create_shared(cid, col_name, value,
                                                                     opposite);
    }
    case TYPE_IPV6: {
        return ComparisonPredicateBase<TYPE_IPV6, PT>::create_shared(cid, col_name, value,
                                                                     opposite);
    }
    default:
        throw Exception(Status::InternalError("Unsupported type {} for comparison_predicate",
                                              type_to_string(data_type->get_primitive_type())));
        return nullptr;
    }
}

template <PrimitiveType TYPE>
std::shared_ptr<HybridSetBase> build_set() {
    return std::make_shared<std::conditional_t<
            is_string_type(TYPE), StringSet<DynamicContainer<std::string>>,
            HybridSet<TYPE, DynamicContainer<typename PrimitiveTypeTraits<TYPE>::CppType>,
                      vectorized::PredicateColumnType<PredicateEvaluateType<TYPE>>>>>(false);
}

std::shared_ptr<ColumnPredicate> create_bloom_filter_predicate(
        const uint32_t cid, const std::string col_name, const vectorized::DataTypePtr& data_type,
        const std::shared_ptr<BloomFilterFuncBase>& filter);

std::shared_ptr<ColumnPredicate> create_bitmap_filter_predicate(
        const uint32_t cid, const std::string col_name, const vectorized::DataTypePtr& data_type,
        const std::shared_ptr<BitmapFilterFuncBase>& filter);
#include "common/compile_check_end.h"
} //namespace doris
