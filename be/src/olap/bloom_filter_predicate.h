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

#include <roaring/roaring.hh>

#include "exprs/bloomfilter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/utils/util.hpp"
#include "vec/columns/column_dictionary.h"

namespace doris {

class VectorizedRowBatch;

// only use in runtime filter and segment v2
template <PrimitiveType T>
class BloomFilterColumnPredicate : public ColumnPredicate {
public:
    using SpecificFilter = BloomFilterFunc<T, CurrentBloomFilterAdaptor>;

    BloomFilterColumnPredicate(uint32_t column_id,
                               const std::shared_ptr<IBloomFilterFuncBase>& filter)
            : ColumnPredicate(column_id),
              _filter(filter),
              _specific_filter(static_cast<SpecificFilter*>(_filter.get())) {}
    ~BloomFilterColumnPredicate() override = default;

    PredicateType type() const override { return PredicateType::BF; }

    void evaluate(VectorizedRowBatch* batch) const override;

    void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;

    void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,
                     bool* flags) const override {};
    void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                      bool* flags) const override {};

    Status evaluate(const Schema& schema, const vector<BitmapIndexIterator*>& iterators,
                    uint32_t num_rows, roaring::Roaring* roaring) const override {
        return Status::OK();
    }

    void evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const override;

private:
    std::shared_ptr<IBloomFilterFuncBase> _filter;
    SpecificFilter* _specific_filter; // owned by _filter
};

// bloom filter column predicate do not support in segment v1
template <PrimitiveType T>
void BloomFilterColumnPredicate<T>::evaluate(VectorizedRowBatch* batch) const {
    uint16_t n = batch->size();
    uint16_t* sel = batch->selected();
    if (!batch->selected_in_use()) {
        for (uint16_t i = 0; i != n; ++i) {
            sel[i] = i;
        }
    }
}

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
void BloomFilterColumnPredicate<T>::evaluate(vectorized::IColumn& column, uint16_t* sel,
                                             uint16_t* size) const {
    uint16_t new_size = 0;
    using FT = typename PredicatePrimitiveTypeTraits<T>::PredicateFieldType;

    if (column.is_nullable()) {
        auto* nullable_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        // deal ColumnDict
        if (nullable_col->get_nested_column().is_column_dictionary()) {
            auto* dict_col = vectorized::check_and_get_column<vectorized::ColumnDictI32>(
                    nullable_col->get_nested_column());
            const_cast<vectorized::ColumnDictI32*>(dict_col)->generate_hash_values();
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                new_size += (!null_map_data[idx]) &&
                            _specific_filter->find_uint32_t(dict_col->get_hash_value(idx));
            }
        } else {
            auto* pred_col = vectorized::check_and_get_column<vectorized::PredicateColumnType<FT>>(
                    nullable_col->get_nested_column());
            auto& pred_col_data = pred_col->get_data();
            for (uint16_t i = 0; i < *size; i++) {
                uint16_t idx = sel[i];
                sel[new_size] = idx;
                const auto* cell_value = reinterpret_cast<const void*>(&(pred_col_data[idx]));
                new_size += (!null_map_data[idx]) && _specific_filter->find_olap_engine(cell_value);
            }
        }
    } else if (column.is_column_dictionary()) {
        auto* dict_col = vectorized::check_and_get_column<vectorized::ColumnDictI32>(column);
        const_cast<vectorized::ColumnDictI32*>(dict_col)->generate_hash_values();
        for (uint16_t i = 0; i < *size; i++) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            new_size += _specific_filter->find_uint32_t(dict_col->get_hash_value(idx));
        }
    } else {
        auto* pred_col =
                vectorized::check_and_get_column<vectorized::PredicateColumnType<FT>>(column);
        auto& pred_col_data = pred_col->get_data();
        for (uint16_t i = 0; i < *size; i++) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(&(pred_col_data[idx]));
            new_size += _specific_filter->find_olap_engine(cell_value);
        }
    }
    *size = new_size;
}

class BloomFilterColumnPredicateFactory {
public:
    static ColumnPredicate* create_column_predicate(
            uint32_t column_id, const std::shared_ptr<IBloomFilterFuncBase>& filter,
            FieldType type);
};

} //namespace doris
