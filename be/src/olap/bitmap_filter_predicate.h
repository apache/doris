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
#include "olap/column_predicate.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/predicate_column.h"

namespace doris {
template <PrimitiveType T>
class BitmapFilterColumnPredicate final : public ColumnPredicate {
public:
    ENABLE_FACTORY_CREATOR(BitmapFilterColumnPredicate);
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
    using SpecificFilter = BitmapFilterFunc<T>;

    BitmapFilterColumnPredicate(uint32_t column_id, std::string col_name,
                                const std::shared_ptr<BitmapFilterFuncBase>& filter)
            : ColumnPredicate(column_id, col_name, T),
              _filter(filter),
              _specific_filter(assert_cast<SpecificFilter*>(_filter.get())) {}
    ~BitmapFilterColumnPredicate() override = default;
    BitmapFilterColumnPredicate(const BitmapFilterColumnPredicate& other, uint32_t col_id)
            : ColumnPredicate(other, col_id),
              _filter(other._filter),
              _specific_filter(assert_cast<SpecificFilter*>(_filter.get())) {}
    BitmapFilterColumnPredicate(const BitmapFilterColumnPredicate& other) = delete;
    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        return BitmapFilterColumnPredicate<T>::create_shared(*this, col_id);
    }
    std::string debug_string() const override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "BitmapFilterColumnPredicate({})",
                       ColumnPredicate::debug_string());
        return fmt::to_string(debug_string_buffer);
    }

    PredicateType type() const override { return PredicateType::BITMAP_FILTER; }

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override {
        if (_specific_filter->is_not_in()) {
            return true;
        }

        CppType max_value;
        if (!zone_map.has_not_null) {
            // no non-null values
            return false;
        } else {
            max_value = CppType(zone_map.max_value.template get<T>());
        }

        CppType min_value = zone_map.has_null /* contains null values */
                                    ? CppType(0)
                                    : CppType(zone_map.min_value.template get<T>());
        return _specific_filter->contains_any(min_value, max_value);
    }

    using ColumnPredicate::evaluate;

private:
    bool _can_ignore() const override { return false; }

    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override;

    template <bool is_nullable>
    uint16_t evaluate(const vectorized::IColumn& column, const uint8_t* null_map, uint16_t* sel,
                      uint16_t size) const {
        if constexpr (is_nullable) {
            DCHECK(null_map);
        }

        uint16_t new_size = 0;
        new_size = _specific_filter->find_fixed_len_olap_engine(
                (char*)assert_cast<
                        const vectorized::PredicateColumnType<PredicateEvaluateType<T>>*>(&column)
                        ->get_data()
                        .data(),
                null_map, sel, size);
        return new_size;
    }

    std::shared_ptr<BitmapFilterFuncBase> _filter;
    SpecificFilter* _specific_filter; // owned by _filter

    bool is_runtime_filter() const override { return true; }
};

template <PrimitiveType T>
uint16_t BitmapFilterColumnPredicate<T>::_evaluate_inner(const vectorized::IColumn& column,
                                                         uint16_t* sel, uint16_t size) const {
    uint16_t new_size = 0;
    if (column.is_nullable()) {
        const auto* nullable_col = assert_cast<const vectorized::ColumnNullable*>(&column);
        const auto& null_map_data = nullable_col->get_null_map_column().get_data();
        new_size =
                evaluate<true>(nullable_col->get_nested_column(), null_map_data.data(), sel, size);
    } else {
        new_size = evaluate<false>(column, nullptr, sel, size);
    }
    update_filter_info(size - new_size, size, 0);
    return new_size;
}
} //namespace doris
