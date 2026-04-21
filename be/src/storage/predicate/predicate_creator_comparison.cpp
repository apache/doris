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

#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/data_type_string.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_comparison_predicate(const uint32_t cid,
                                                             const std::string col_name,
                                                             const DataTypePtr& data_type,
                                                             const Field& value, bool opposite) {
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
        auto target = std::max(cast_set<size_t>(assert_cast<const DataTypeString*>(
                                                        remove_nullable(data_type).get())
                                                        ->len()),
                               value.template get<TYPE_CHAR>().size());
        if (target > value.template get<TYPE_CHAR>().size()) {
            std::string tmp(target, '\0');
            memcpy(tmp.data(), value.template get<TYPE_CHAR>().data(),
                   value.template get<TYPE_CHAR>().size());
            return ComparisonPredicateBase<TYPE_CHAR, PT>::create_shared(
                    cid, col_name, Field::create_field<TYPE_CHAR>(std::move(tmp)), opposite);
        } else {
            return ComparisonPredicateBase<TYPE_CHAR, PT>::create_shared(
                    cid, col_name, Field::create_field<TYPE_CHAR>(value.template get<TYPE_CHAR>()),
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

template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::EQ>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);
template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::NE>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);
template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::LT>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);
template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::GT>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);
template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::LE>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);
template std::shared_ptr<ColumnPredicate> create_comparison_predicate<PredicateType::GE>(
        const uint32_t, const std::string, const DataTypePtr&, const Field&, bool);

} // namespace doris
