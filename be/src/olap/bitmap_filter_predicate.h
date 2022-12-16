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

#include <cstdint>

#include "exprs/bitmapfilter_predicate.h"
#include "exprs/cast_functions.h"
#include "exprs/runtime_filter.h"
#include "olap/column_predicate.h"
#include "olap/wrapper_field.h"
#include "util/bitmap_value.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

// only use in runtime filter and segment v2
template <PrimitiveType T>
class BitmapFilterColumnPredicate : public ColumnPredicate {
public:
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
    using SpecificFilter = BitmapFilterFunc<T>;

    BitmapFilterColumnPredicate(uint32_t column_id,
                                const std::shared_ptr<BitmapFilterFuncBase>& filter, int)
            : ColumnPredicate(column_id),
              _filter(filter),
              _specific_filter(static_cast<SpecificFilter*>(_filter.get())) {}
    ~BitmapFilterColumnPredicate() override = default;

    PredicateType type() const override { return PredicateType::BITMAP_FILTER; }

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,
                     bool* flags) const override {};
    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                      bool* flags) const override {};

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override {
        if (_specific_filter->is_not_in()) {
            return true;
        }

        CppType max_value;
        if (statistic.second->is_null()) {
            // no non-null values
            return false;
        } else {
            max_value = *reinterpret_cast<const CppType*>(statistic.second->cell_ptr());
        }

        CppType min_value =
                statistic.first->is_null() /* contains null values */
                        ? 0
                        : *reinterpret_cast<const CppType*>(statistic.first->cell_ptr());
        return _specific_filter->contains_any(min_value, max_value);
    }

    Status evaluate(BitmapIndexIterator*, uint32_t, roaring::Roaring*) const override {
        return Status::OK();
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override;

private:
    template <bool is_nullable>
    uint16_t evaluate(const vectorized::IColumn& column, const uint8_t* null_map, uint16_t* sel,
                      uint16_t size) const {
        if constexpr (is_nullable) {
            DCHECK(null_map);
        }

        uint16_t new_size = 0;
        new_size = _specific_filter->find_fixed_len_olap_engine(
                (char*)reinterpret_cast<
                        const vectorized::PredicateColumnType<PredicateEvaluateType<T>>*>(&column)
                        ->get_data()
                        .data(),
                null_map, sel, size);
        return new_size;
    }

    std::string _debug_string() const override {
        return "BitmapFilterColumnPredicate(" + type_to_string(T) + ")";
    }

    std::shared_ptr<BitmapFilterFuncBase> _filter;
    SpecificFilter* _specific_filter; // owned by _filter
};

template <PrimitiveType T>
void BitmapFilterColumnPredicate<T>::evaluate(ColumnBlock*, uint16_t*, uint16_t*) const {}

template <PrimitiveType T>
uint16_t BitmapFilterColumnPredicate<T>::evaluate(const vectorized::IColumn& column, uint16_t* sel,
                                                  uint16_t size) const {
    uint16_t new_size = 0;
    if (column.is_nullable()) {
        auto* nullable_col = reinterpret_cast<const vectorized::ColumnNullable*>(&column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        new_size =
                evaluate<true>(nullable_col->get_nested_column(), null_map_data.data(), sel, size);
    } else {
        new_size = evaluate<false>(column, nullptr, sel, size);
    }
    return new_size;
}
} //namespace doris
