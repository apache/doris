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

#include <stdint.h>

#include "exprs/bloomfilter_predicate.h"
#include "exprs/runtime_filter.h"
#include "olap/column_predicate.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

// only use in runtime filter and segment v2
template <PrimitiveType T>
class BloomFilterColumnPredicate : public ColumnPredicate {
public:
    using SpecificFilter = BloomFilterFunc<T>;

    BloomFilterColumnPredicate(uint32_t column_id,
                               const std::shared_ptr<BloomFilterFuncBase>& filter)
            : ColumnPredicate(column_id),
              _filter(filter),
              _specific_filter(static_cast<SpecificFilter*>(_filter.get())) {}
    ~BloomFilterColumnPredicate() override = default;

    PredicateType type() const override { return PredicateType::BF; }

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,
                     bool* flags) const override {};
    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                      bool* flags) const override {};

    Status evaluate(BitmapIndexIterator* iterators, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
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
        if (column.is_column_dictionary()) {
            auto* dict_col = reinterpret_cast<const vectorized::ColumnDictI32*>(&column);
            for (uint16_t i = 0; i < size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                if constexpr (is_nullable) {
                    new_size += !null_map[idx] &&
                                _specific_filter->find_uint32_t(dict_col->get_hash_value(idx));
                } else {
                    new_size += _specific_filter->find_uint32_t(dict_col->get_hash_value(idx));
                }
            }
        } else if (IRuntimeFilter::enable_use_batch(T)) {
            new_size = _specific_filter->find_fixed_len_olap_engine(
                    (char*)reinterpret_cast<const vectorized::PredicateColumnType<T>*>(&column)
                            ->get_data()
                            .data(),
                    null_map, sel, size);
        } else {
            uint24_t tmp_uint24_value;
            auto get_cell_value = [&tmp_uint24_value](auto& data) {
                if constexpr (std::is_same_v<std::decay_t<decltype(data)>, uint32_t> &&
                              T == PrimitiveType::TYPE_DATE) {
                    memcpy((char*)(&tmp_uint24_value), (char*)(&data), sizeof(uint24_t));
                    return (const char*)&tmp_uint24_value;
                } else {
                    return (const char*)&data;
                }
            };

            auto pred_col_data =
                    reinterpret_cast<const vectorized::PredicateColumnType<T>*>(&column)
                            ->get_data()
                            .data();
            for (uint16_t i = 0; i < size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;

                if constexpr (is_nullable) {
                    new_size += !null_map[idx] && _specific_filter->find_olap_engine(
                                                          get_cell_value(pred_col_data[idx]));
                } else {
                    new_size +=
                            _specific_filter->find_olap_engine(get_cell_value(pred_col_data[idx]));
                }
            }
        }
        return new_size;
    }

    std::shared_ptr<BloomFilterFuncBase> _filter;
    SpecificFilter* _specific_filter; // owned by _filter
    mutable uint64_t _evaluated_rows = 1;
    mutable uint64_t _passed_rows = 0;
    mutable bool _always_true = false;
    mutable bool _has_calculate_filter = false;
};

template <PrimitiveType T>
void BloomFilterColumnPredicate<T>::evaluate(ColumnBlock* block, uint16_t* sel,
                                             uint16_t* size) const {
    uint16_t new_size = 0;
    if (block->is_nullable()) {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size +=
                    (!block->cell(idx).is_null() && _specific_filter->find_olap_engine(cell_value));
        }
    } else {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size += _specific_filter->find_olap_engine(cell_value);
        }
    }
    *size = new_size;
}

template <PrimitiveType T>
uint16_t BloomFilterColumnPredicate<T>::evaluate(const vectorized::IColumn& column, uint16_t* sel,
                                                 uint16_t size) const {
    uint16_t new_size = 0;
    if (_always_true) {
        return size;
    }
    if (column.is_nullable()) {
        auto* nullable_col = reinterpret_cast<const vectorized::ColumnNullable*>(&column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        new_size =
                evaluate<true>(nullable_col->get_nested_column(), null_map_data.data(), sel, size);
    } else {
        new_size = evaluate<false>(column, nullptr, sel, size);
    }
    // If the pass rate is very high, for example > 50%, then the bloomfilter is useless.
    // Some bloomfilter is useless, for example ssb 4.3, it consumes a lot of cpu but it is
    // useless.
    _evaluated_rows += size;
    _passed_rows += new_size;
    vectorized::VRuntimeFilterWrapper::calculate_filter(
            _evaluated_rows - _passed_rows, _evaluated_rows, _has_calculate_filter, _always_true);
    return new_size;
}

class BloomFilterColumnPredicateFactory {
public:
    static ColumnPredicate* create_column_predicate(
            uint32_t column_id, const std::shared_ptr<BloomFilterFuncBase>& filter, FieldType type);
};

} //namespace doris
