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
#include <tuple>
#include <variant>

#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"

namespace doris {

// Utility tools for convenient column execution

// ColumnElementView is used to distinguish between scalar columns and string columns
template <PrimitiveType PType>
struct ColumnElementView {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ElementType = typename ColumnType::value_type;
    const typename ColumnType::Container& data;
    ElementType get_element(size_t idx) const { return data[idx]; }

    ColumnElementView(const IColumn& column)
            : data(assert_cast<const ColumnType&>(column).get_data()) {}
};

template <>
struct ColumnElementView<TYPE_STRING> {
    using ColumnType = ColumnString;
    using ElementType = StringRef;
    const ColumnString& string_column;
    ColumnElementView(const IColumn& column)
            : string_column(assert_cast<const ColumnString&>(column)) {}
    StringRef get_element(size_t idx) const { return string_column.get_data_at(idx); }
};

// ColumnView is used to handle the nullable and const properties of a column.
// For example, a regular ColumnInt32 may appear in the following 4 cases:
// 1. ColumnInt32
// 2. Const(ColumnInt32)
// 3. Nullable(ColumnInt32)
// 4. Const(Nullable(ColumnInt32)) (although this case is rare, it can still occur; many of our previous code did not consider this)
// You can use is_null_at and value_at to get the data at the corresponding position
//
// ====== Performance Guide: When to Use ColumnView ======
//
// 1. Expensive per-element operations (e.g. geo functions, complex string ops):
//    Use ColumnView freely — its overhead is negligible relative to the work.
//
// 2. Cheap per-element operations that the compiler can inline (e.g. simple arithmetic):
//
//    a) Inputs are NOT nullable (e.g. the function framework already strips nullable):
//       Safe to use. The compiler optimizes the is_const branch into code equivalent
//       to hand-written direct array access (verified via assembly and benchmarks).
//
//    b) Inputs involve nullable columns:
//       - Unary operations: safe to use, the compiler still optimizes effectively.
//       - Binary / ternary operations: the combined is_null_at checks across multiple
//         columns inhibit compiler vectorization and branch optimization, causing
//         significant regression (~1.4x for binary, ~1.8x for ternary in benchmarks).
//         In this case, hand-written column access is recommended for best performance.
//
// In summary, ColumnView is designed to eliminate the combinatorial explosion of
// handling 4 column forms. It is suitable for the vast majority of use cases.
// Only the specific combination of "cheap computation + nullable + multi-column"
// requires weighing whether to hand-write the access code.
// ====== End of Performance Guide ======

template <PrimitiveType PType>
struct ColumnView {
    const ColumnElementView<PType> data;
    const NullMap* null_map;
    const bool is_const;
    const size_t count;

    static ColumnView create(const ColumnPtr& column_ptr) {
        const auto& [from_data_column, is_const] = unpack_if_const(column_ptr);
        const NullMap* null_map = nullptr;
        const IColumn* data = nullptr;
        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(from_data_column.get())) {
            null_map = &nullable_column->get_null_map_data();
            data = nullable_column->get_nested_column_ptr().get();
        } else {
            data = from_data_column.get();
        }

        return ColumnView {.data = ColumnElementView<PType>(*data),
                           .null_map = null_map,
                           .is_const = is_const,
                           .count = column_ptr->size()};
    }

    bool is_null_at(size_t idx) const {
        if (null_map != nullptr) {
            return (*null_map)[is_const ? 0 : idx];
        }
        return false;
    }

    auto value_at(size_t idx) const { return data.get_element(is_const ? 0 : idx); }

    size_t size() const { return count; }
};
} // namespace doris