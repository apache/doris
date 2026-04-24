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

// This header contains the shared implementation of create_in_list_predicate<PT>.
// It is included by predicate_creator_in_list_in.cpp and
// predicate_creator_in_list_not_in.cpp, which each provide one explicit
// instantiation so the two translation units can be compiled in parallel.

#pragma once

#include "common/exception.h"
#include "common/status.h"
#include "core/call_on_type_index.h"
#include "core/data_type/data_type_string.h"
#include "storage/predicate/in_list_predicate.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {

template <PrimitiveType TYPE, PredicateType PT>
static std::shared_ptr<ColumnPredicate> create_in_list_predicate_impl(
        const uint32_t cid, const std::string col_name, const std::shared_ptr<HybridSetBase>& set,
        bool is_opposite, size_t char_length = 0) {
    // Only string types construct their own HybridSetType in the constructor (to convert
    // from DynamicContainer to FixedContainer<std::string, N>), so N dispatch is only needed
    // for them. All other types directly share the caller's hybrid_set.
    if constexpr (!is_string_type(TYPE)) {
        return InListPredicateBase<TYPE, PT, FIXED_CONTAINER_MAX_SIZE + 1>::create_shared(
                cid, col_name, set, is_opposite, char_length);
    } else {
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
}

template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_in_list_predicate(const uint32_t cid,
                                                          const std::string col_name,
                                                          const DataTypePtr& data_type,
                                                          const std::shared_ptr<HybridSetBase> set,
                                                          bool is_opposite) {
    auto primitive_type = data_type->get_primitive_type();
    std::shared_ptr<ColumnPredicate> result;

    // Dispatch all scalar (non-string) types uniformly.  dispatch_switch_scalar
    // covers INT / FLOAT / DECIMAL / DATETIME / IP groups without collapsing the
    // three distinct string PrimitiveTypes, so the string cases below can each
    // keep their own template argument (TYPE_CHAR needs char_length too).
    bool matched = dispatch_switch_scalar(primitive_type, [&](auto type_tag) {
        constexpr PrimitiveType TYPE = decltype(type_tag)::PType;
        result = create_in_list_predicate_impl<TYPE, PT>(cid, col_name, set, is_opposite);
        return true;
    });
    if (matched) {
        return result;
    }

    // String types: CHAR carries a fixed char_length; VARCHAR and STRING are
    // distinct predicate template arguments and cannot be collapsed.
    switch (primitive_type) {
    case TYPE_CHAR:
        return create_in_list_predicate_impl<TYPE_CHAR, PT>(
                cid, col_name, set, is_opposite,
                assert_cast<const DataTypeString*>(remove_nullable(data_type).get())->len());
    case TYPE_VARCHAR:
        return create_in_list_predicate_impl<TYPE_VARCHAR, PT>(cid, col_name, set, is_opposite);
    case TYPE_STRING:
        return create_in_list_predicate_impl<TYPE_STRING, PT>(cid, col_name, set, is_opposite);
    default:
        throw Exception(Status::InternalError("Unsupported type {} for in_predicate",
                                              type_to_string(primitive_type)));
        return nullptr;
    }
}

} // namespace doris
