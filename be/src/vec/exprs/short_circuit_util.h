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
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"

namespace doris::vectorized {

// Scalar version stores a reference to the actual data type array for convenient subsequent operations.
template <PrimitiveType PType>
struct ColumnView {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ArrayType = typename ColumnType::Container;

    const ArrayType& data;
    const NullMap* null_map;
    const bool is_const;
    const size_t count;

    static ColumnView create(const ColumnPtr& column_ptr) {
        const auto& [from_data_column, is_const] = unpack_if_const(column_ptr);
        const NullMap* null_map = nullptr;
        const ArrayType* data = nullptr;
        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(from_data_column.get())) {
            null_map = &nullable_column->get_null_map_data();
            const auto& nested_from_column = nullable_column->get_nested_column();
            data = &(assert_cast<const ColumnType&>(nested_from_column).get_data());
        } else {
            data = &(assert_cast<const ColumnType&>(*from_data_column).get_data());
        }

        return ColumnView {.data = *data,
                           .null_map = null_map,
                           .is_const = is_const,
                           .count = column_ptr->size()};
    }

    template <typename NullFunc, typename Func>
    void for_each(NullFunc& null_func, Func& func) const {
        if (this->null_map != nullptr) {
            const auto& null_map_data = *(this->null_map);
            auto update = [&](size_t i) {
                if (null_map_data[i]) {
                    null_func(i);
                } else {
                    func(i, data[i]);
                }
            };
            if (is_const) {
                for_each_dispatch_const<true>(update);
            } else {
                for_each_dispatch_const<false>(update);
            }
        } else {
            // non-nullable condition column
            auto update = [&](size_t i) { func(i, data[i]); };
            if (is_const) {
                for_each_dispatch_const<true>(update);
            } else {
                for_each_dispatch_const<false>(update);
            }
        };
    }

    template <bool is_const_column, typename Func>
    void for_each_dispatch_const(Func& func) const {
        if constexpr (is_const_column) {
            for (size_t i = 0; i < count; ++i) {
                func(0);
            }
        } else {
            for (size_t i = 0; i < count; ++i) {
                func(i);
            }
        }
    }

    bool is_null_at(size_t idx) const {
        if (null_map != nullptr) {
            return (*null_map)[is_const ? 0 : idx];
        }
        return false;
    }

    auto value_at(size_t idx) const { return data[is_const ? 0 : idx]; }
};

template <PrimitiveType TypeA, PrimitiveType TypeB, typename NullFunc, typename Func>
void execute_binary(ColumnView<TypeA>& col_a, ColumnView<TypeB>& col_b, NullFunc null_func,
                    Func func) {
    DCHECK(col_a.count == col_b.count);
    size_t count = col_a.count;
    for (size_t i = 0; i < count; ++i) {
        bool is_null_a = col_a.is_null_at(i);
        bool is_null_b = col_b.is_null_at(i);
        if (is_null_a || is_null_b) {
            null_func(i, is_null_a, is_null_b);
        } else {
            func(i, col_a.value_at(i), col_b.value_at(i));
        }
    }
}

template <PrimitiveType TypeA, PrimitiveType TypeB, PrimitiveType TypeC, typename NullFunc,
          typename Func>
void execute_ternary(ColumnView<TypeA>& col_a, ColumnView<TypeB>& col_b, ColumnView<TypeC>& col_c,
                     NullFunc null_func, Func func) {
    DCHECK(col_a.count == col_b.count && col_a.count == col_c.count);
    size_t count = col_a.count;
    for (size_t i = 0; i < count; ++i) {
        bool is_null_a = col_a.is_null_at(i);
        bool is_null_b = col_b.is_null_at(i);
        bool is_null_c = col_c.is_null_at(i);
        if (is_null_a || is_null_b || is_null_c) {
            null_func(i, is_null_a, is_null_b, is_null_c);
        } else {
            func(i, col_a.value_at(i), col_b.value_at(i), col_c.value_at(i));
        }
    }
}

} // namespace doris::vectorized