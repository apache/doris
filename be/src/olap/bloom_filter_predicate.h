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

#include "exprs/bloom_filter_func.h"
#include "exprs/runtime_filter.h"
#include "olap/column_predicate.h"
#include "runtime/primitive_type.h"
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
                               const std::shared_ptr<BloomFilterFuncBase>& filter,
                               int be_exec_version)
            : ColumnPredicate(column_id),
              _filter(filter),
              _specific_filter(reinterpret_cast<SpecificFilter*>(_filter.get())),
              _be_exec_version(be_exec_version) {}
    ~BloomFilterColumnPredicate() override = default;

    PredicateType type() const override { return PredicateType::BF; }

    Status evaluate(BitmapIndexIterator* iterators, uint32_t num_rows,
                    roaring::Roaring* roaring) const override {
        return Status::OK();
    }

    uint16_t evaluate(const vectorized::IColumn& column, uint16_t* sel,
                      uint16_t size) const override;

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return input_type == T || (is_string_type(input_type) && is_string_type(T));
    }

private:
    template <bool is_nullable>
    uint16_t evaluate(const vectorized::IColumn& column, const uint8_t* null_map, uint16_t* sel,
                      uint16_t size) const {
        if constexpr (is_nullable) {
            DCHECK(null_map);
        }

        uint16_t new_size = 0;
        if (column.is_column_dictionary()) {
            const auto* dict_col = assert_cast<const vectorized::ColumnDictI32*>(&column);
            new_size = _specific_filter->template find_dict_olap_engine<is_nullable>(
                    dict_col, null_map, sel, size);
        } else {
            const auto& data =
                    assert_cast<const vectorized::PredicateColumnType<PredicateEvaluateType<T>>*>(
                            &column)
                            ->get_data();
            new_size = _specific_filter->find_fixed_len_olap_engine((char*)data.data(), null_map,
                                                                    sel, size, data.size() != size);
        }
        return new_size;
    }

    std::string _debug_string() const override {
        std::string info = "BloomFilterColumnPredicate(" + type_to_string(T) + ")";
        return info;
    }

    int get_filter_id() const override {
        int filter_id = _filter->get_filter_id();
        DCHECK(filter_id != -1);
        return filter_id;
    }
    bool is_filter() const override { return true; }

    std::shared_ptr<BloomFilterFuncBase> _filter;
    SpecificFilter* _specific_filter; // owned by _filter
    mutable bool _always_true = false;
    mutable bool _has_calculate_filter = false;
    int _be_exec_version;
};

template <PrimitiveType T>
uint16_t BloomFilterColumnPredicate<T>::evaluate(const vectorized::IColumn& column, uint16_t* sel,
                                                 uint16_t size) const {
    uint16_t new_size = 0;
    if (_always_true) {
        return size;
    }
    if (column.is_nullable()) {
        const auto* nullable_col = reinterpret_cast<const vectorized::ColumnNullable*>(&column);
        const auto& null_map_data = nullable_col->get_null_map_column().get_data();
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

} //namespace doris
