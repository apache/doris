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

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"

namespace doris::vectorized {

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

// CompileTimeColumnView builds upon ColumnView by using template parameters to distinguish
// const and nullable properties at compile time, so there is no need to check again during expression execution

template <PrimitiveType PType, bool is_const, bool is_nullable>
struct CompileTimeColumnView;

template <PrimitiveType PType>
struct CompileTimeColumnView<PType, false, false> {
    const ColumnElementView<PType>& data;

    auto value_at(size_t idx) const { return data.get_element(idx); }

    bool is_null_at(size_t idx) const { return false; }
};

template <PrimitiveType PType>
struct CompileTimeColumnView<PType, true, false> {
    const ColumnElementView<PType>& data;

    auto value_at(size_t idx) const { return data.get_element(0); }

    bool is_null_at(size_t idx) const { return false; }
};

template <PrimitiveType PType>
struct CompileTimeColumnView<PType, false, true> {
    const ColumnElementView<PType>& data;
    const NullMap& null_map;

    auto value_at(size_t idx) const { return data.get_element(idx); }

    bool is_null_at(size_t idx) const { return null_map[idx]; }
};

template <PrimitiveType PType>
struct CompileTimeColumnView<PType, true, true> {
    const ColumnElementView<PType>& data;
    const NullMap& null_map;

    auto value_at(size_t idx) const { return data.get_element(0); }

    bool is_null_at(size_t idx) const { return null_map[0]; }
};

template <PrimitiveType PType>
using CompileTimeColumnViewVariant = std::variant<
        CompileTimeColumnView<PType, false, false>, CompileTimeColumnView<PType, true, false>,
        CompileTimeColumnView<PType, false, true>, CompileTimeColumnView<PType, true, true>>;

template <PrimitiveType PType>
CompileTimeColumnViewVariant<PType> create_compile_time_column_view(
        const ColumnView<PType>& col_view) {
    if (col_view.null_map == nullptr) {
        if (col_view.is_const) {
            return CompileTimeColumnView<PType, true, false> {col_view.data};
        } else {
            return CompileTimeColumnView<PType, false, false> {col_view.data};
        }
    } else {
        if (col_view.is_const) {
            return CompileTimeColumnView<PType, true, true> {col_view.data, *(col_view.null_map)};
        } else {
            return CompileTimeColumnView<PType, false, true> {col_view.data, *(col_view.null_map)};
        }
    }
}

template <PrimitiveType PType>
using CompileTimeColumnViewVariantOnlyConst =
        std::variant<CompileTimeColumnView<PType, false, false>,
                     CompileTimeColumnView<PType, true, false>>;

template <PrimitiveType PType>
CompileTimeColumnViewVariantOnlyConst<PType> create_compile_time_column_view_only_const(
        const ColumnView<PType>& col_view) {
    DCHECK(col_view.null_map == nullptr) << "Only support non-nullable column";
    if (col_view.is_const) {
        return CompileTimeColumnView<PType, true, false> {col_view.data};
    } else {
        return CompileTimeColumnView<PType, false, false> {col_view.data};
    }
}

// Utility tool for convenient column execution
// Can be used in functions/expressions
// If you don't want compile-time expansion, use execute_xxx_runtime functions
// If you want compile-time expansion, use execute_xxx_compile_time functions
// If the function has a native implementation that doesn't use use_default_implementation_for_nulls logic,
//   use execute_xxx_compile_time functions (because the column may be nullable)
// If the function uses use_default_implementation_for_nulls logic,
//   use execute_xxx_compile_time_only_const functions (because the column cannot be nullable)

struct ExecuteColumn {
    // unary
    template <PrimitiveType TypeA, typename NullFunc, typename Func>
    static void execute_unary_runtime(const ColumnPtr& col_a, NullFunc& null_func, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        execute_unary_impl(col_a_view, col_a_view.count, null_func, func);
    }

    template <PrimitiveType TypeA, typename NullFunc, typename Func>
    static void execute_unary_compile_time(const ColumnPtr& col_a, NullFunc& null_func,
                                           Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        std::visit([&](auto&& a) { execute_unary_impl(a, col_a_view.count, null_func, func); },
                   create_compile_time_column_view(col_a_view));
    }

    template <PrimitiveType TypeA, typename Func>
    static void execute_unary_compile_time_only_const(const ColumnPtr& col_a, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        std::visit([&](auto&& a) { execute_unary_impl(a, col_a_view.count, not_null_func, func); },
                   create_compile_time_column_view_only_const(col_a_view));
    }

    // binary

    template <PrimitiveType TypeA, PrimitiveType TypeB, typename NullFunc, typename Func>
    static void execute_binary_runtime(const ColumnPtr& col_a, const ColumnPtr& col_b,
                                       NullFunc& null_func, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        DCHECK(col_a_view.count == col_b_view.count);
        execute_binary_impl(col_a_view, col_b_view, col_a_view.count, null_func, func);
    }

    template <PrimitiveType TypeA, PrimitiveType TypeB, typename NullFunc, typename Func>
    static void execute_binary_compile_time(const ColumnPtr& col_a, const ColumnPtr& col_b,
                                            NullFunc& null_func, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        DCHECK(col_a_view.count == col_b_view.count);
        size_t count = col_a_view.count;
        std::visit([&](auto&& a, auto&& b) { execute_binary_impl(a, b, count, null_func, func); },
                   create_compile_time_column_view(col_a_view),
                   create_compile_time_column_view(col_b_view));
    }

    template <PrimitiveType TypeA, PrimitiveType TypeB, typename Func>
    static void execute_binary_compile_time_only_const(const ColumnPtr& col_a,
                                                       const ColumnPtr& col_b, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        DCHECK(col_a_view.count == col_b_view.count);
        size_t count = col_a_view.count;
        std::visit(
                [&](auto&& a, auto&& b) { execute_binary_impl(a, b, count, not_null_func, func); },
                create_compile_time_column_view_only_const(col_a_view),
                create_compile_time_column_view_only_const(col_b_view));
    }

    // ternary

    template <PrimitiveType TypeA, PrimitiveType TypeB, PrimitiveType TypeC, typename NullFunc,
              typename Func>
    static void execute_ternary_runtime(const ColumnPtr& col_a, const ColumnPtr& col_b,
                                        const ColumnPtr& col_c, NullFunc& null_func, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        ColumnView<TypeC> col_c_view = ColumnView<TypeC>::create(col_c);
        DCHECK(col_a_view.count == col_b_view.count && col_a_view.count == col_c_view.count);
        size_t count = col_a_view.count;
        execute_ternary_impl(col_a_view, col_b_view, col_c_view, count, null_func, func);
    }

    template <PrimitiveType TypeA, PrimitiveType TypeB, PrimitiveType TypeC, typename NullFunc,
              typename Func>
    static void execute_ternary_compile_time(const ColumnPtr& col_a, const ColumnPtr& col_b,
                                             const ColumnPtr& col_c, NullFunc& null_func,
                                             Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        ColumnView<TypeC> col_c_view = ColumnView<TypeC>::create(col_c);
        DCHECK(col_a_view.count == col_b_view.count && col_a_view.count == col_c_view.count);
        size_t count = col_a_view.count;
        std::visit([&](auto&& a, auto&& b,
                       auto&& c) { execute_ternary_impl(a, b, c, count, null_func, func); },
                   create_compile_time_column_view(col_a_view),
                   create_compile_time_column_view(col_b_view),
                   create_compile_time_column_view(col_c_view));
    }

    template <PrimitiveType TypeA, PrimitiveType TypeB, PrimitiveType TypeC, typename Func>
    static void execute_ternary_compile_time_only_const(const ColumnPtr& col_a,
                                                        const ColumnPtr& col_b,
                                                        const ColumnPtr& col_c, Func& func) {
        ColumnView<TypeA> col_a_view = ColumnView<TypeA>::create(col_a);
        ColumnView<TypeB> col_b_view = ColumnView<TypeB>::create(col_b);
        ColumnView<TypeC> col_c_view = ColumnView<TypeC>::create(col_c);
        DCHECK(col_a_view.count == col_b_view.count && col_a_view.count == col_c_view.count);
        size_t count = col_a_view.count;

        std::visit([&](auto&& a, auto&& b,
                       auto&& c) { execute_ternary_impl(a, b, c, count, not_null_func, func); },
                   create_compile_time_column_view_only_const(col_a_view),
                   create_compile_time_column_view_only_const(col_b_view),
                   create_compile_time_column_view_only_const(col_c_view));
    }

private:
    static void not_null_func(size_t i) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "no need to call null func for not nullable column");
    }

    template <typename NullFunc, typename Func>
    static void execute_unary_impl(auto&& a, size_t count, NullFunc& null_func, Func& func) {
        for (size_t i = 0; i < count; ++i) {
            bool is_null_a = a.is_null_at(i);
            if (is_null_a) {
                null_func(i);
            } else {
                func(i, a.value_at(i));
            }
        }
    }

    template <typename NullFunc, typename Func>
    static void execute_binary_impl(auto&& a, auto&& b, size_t count, NullFunc& null_func,
                                    Func& func) {
        for (size_t i = 0; i < count; ++i) {
            bool is_null_a = a.is_null_at(i);
            bool is_null_b = b.is_null_at(i);
            if (is_null_a || is_null_b) {
                null_func(i);
            } else {
                func(i, a.value_at(i), b.value_at(i));
            }
        }
    }

    template <typename NullFunc, typename Func>
    static void execute_ternary_impl(auto&& a, auto&& b, auto&& c, size_t count,
                                     NullFunc& null_func, Func& func) {
        for (size_t i = 0; i < count; ++i) {
            bool is_null_a = a.is_null_at(i);
            bool is_null_b = b.is_null_at(i);
            bool is_null_c = c.is_null_at(i);
            if (is_null_a || is_null_b || is_null_c) {
                null_func(i);
            } else {
                func(i, a.value_at(i), b.value_at(i), c.value_at(i));
            }
        }
    }
};

} // namespace doris::vectorized