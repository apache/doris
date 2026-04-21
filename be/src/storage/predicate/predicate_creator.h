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

#include <memory>
#include <string>

#include "core/data_type/data_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/hybrid_set.h"
#include "storage/predicate/column_predicate.h"

namespace doris {

class BloomFilterFuncBase;
class BitmapFilterFuncBase;

// Defined in predicate_creator.cpp with explicit instantiations.
template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_in_list_predicate(const uint32_t cid,
                                                          const std::string col_name,
                                                          const DataTypePtr& data_type,
                                                          const std::shared_ptr<HybridSetBase> set,
                                                          bool is_opposite);

// Defined in predicate_creator.cpp with explicit instantiations.
template <PredicateType PT>
std::shared_ptr<ColumnPredicate> create_comparison_predicate(const uint32_t cid,
                                                             const std::string col_name,
                                                             const DataTypePtr& data_type,
                                                             const Field& value, bool opposite);

template <PrimitiveType TYPE>
std::shared_ptr<HybridSetBase> build_set() {
    return std::make_shared<std::conditional_t<
            is_string_type(TYPE), StringSet<DynamicContainer<std::string>>,
            HybridSet<TYPE, DynamicContainer<typename PrimitiveTypeTraits<TYPE>::CppType>,
                      PredicateColumnType<PredicateEvaluateType<TYPE>>>>>(false);
}

std::shared_ptr<ColumnPredicate> create_bloom_filter_predicate(
        const uint32_t cid, const std::string col_name, const DataTypePtr& data_type,
        const std::shared_ptr<BloomFilterFuncBase>& filter);

std::shared_ptr<ColumnPredicate> create_bitmap_filter_predicate(
        const uint32_t cid, const std::string col_name, const DataTypePtr& data_type,
        const std::shared_ptr<BitmapFilterFuncBase>& filter);
} //namespace doris
