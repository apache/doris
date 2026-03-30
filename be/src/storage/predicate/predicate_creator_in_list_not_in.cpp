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
#include "storage/predicate/in_list_predicate.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {

template <PrimitiveType TYPE, PredicateType PT>
static std::shared_ptr<ColumnPredicate> create_in_list_predicate_impl(
        const uint32_t cid, const std::string col_name, const std::shared_ptr<HybridSetBase>& set,
        bool is_opposite, size_t char_length = 0) {
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

template <>
std::shared_ptr<ColumnPredicate> create_in_list_predicate<PredicateType::NOT_IN_LIST>(
        const uint32_t cid, const std::string col_name, const DataTypePtr& data_type,
        const std::shared_ptr<HybridSetBase> set, bool is_opposite) {
    switch (data_type->get_primitive_type()) {
    case TYPE_TINYINT: {
        return create_in_list_predicate_impl<TYPE_TINYINT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_SMALLINT: {
        return create_in_list_predicate_impl<TYPE_SMALLINT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_INT: {
        return create_in_list_predicate_impl<TYPE_INT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_BIGINT: {
        return create_in_list_predicate_impl<TYPE_BIGINT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_LARGEINT: {
        return create_in_list_predicate_impl<TYPE_LARGEINT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_FLOAT: {
        return create_in_list_predicate_impl<TYPE_FLOAT, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DOUBLE: {
        return create_in_list_predicate_impl<TYPE_DOUBLE, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMALV2: {
        return create_in_list_predicate_impl<TYPE_DECIMALV2, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL32: {
        return create_in_list_predicate_impl<TYPE_DECIMAL32, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL64: {
        return create_in_list_predicate_impl<TYPE_DECIMAL64, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL128I: {
        return create_in_list_predicate_impl<TYPE_DECIMAL128I, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DECIMAL256: {
        return create_in_list_predicate_impl<TYPE_DECIMAL256, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_CHAR: {
        return create_in_list_predicate_impl<TYPE_CHAR, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite,
                assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len());
    }
    case TYPE_VARCHAR: {
        return create_in_list_predicate_impl<TYPE_VARCHAR, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_STRING: {
        return create_in_list_predicate_impl<TYPE_STRING, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DATE: {
        return create_in_list_predicate_impl<TYPE_DATE, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DATEV2: {
        return create_in_list_predicate_impl<TYPE_DATEV2, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DATETIME: {
        return create_in_list_predicate_impl<TYPE_DATETIME, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_DATETIMEV2: {
        return create_in_list_predicate_impl<TYPE_DATETIMEV2, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_TIMESTAMPTZ: {
        return create_in_list_predicate_impl<TYPE_TIMESTAMPTZ, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_BOOLEAN: {
        return create_in_list_predicate_impl<TYPE_BOOLEAN, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_IPV4: {
        return create_in_list_predicate_impl<TYPE_IPV4, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    case TYPE_IPV6: {
        return create_in_list_predicate_impl<TYPE_IPV6, PredicateType::NOT_IN_LIST>(
                cid, col_name, set, is_opposite);
    }
    default:
        throw Exception(Status::InternalError("Unsupported type {} for in_predicate",
                                              type_to_string(data_type->get_primitive_type())));
        return nullptr;
    }
}

} // namespace doris
