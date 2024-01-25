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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsLogical.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "common/status.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

/** Logical functions AND, OR and NOT support three-valued (or ternary) logic
  * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
  *
  * Functions NOT rely on "default implementation for NULLs":
  *   - if any of the arguments is of Nullable type, the return value type is Nullable
  *   - if any of the arguments is NULL, the return value is NULL
  *
  * Functions AND and OR provide their own special implementations for ternary logic
  */

namespace doris::vectorized {
namespace FunctionsLogicalDetail {
namespace Ternary {
using ResultType = UInt8;

static constexpr UInt8 False = 0;
static constexpr UInt8 True = -1;
static constexpr UInt8 Null = 1;

template <typename T>
ResultType make_value(T value) {
    return value != 0 ? Ternary::True : Ternary::False;
}

template <typename T>
ResultType make_value(T value, bool is_null) {
    if (is_null) return Ternary::Null;
    return make_value<T>(value);
}
} // namespace Ternary

struct AndImpl {
    using ResultType = UInt8;

    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }
    static inline constexpr ResultType apply_null(UInt8 a, UInt8 l_null, UInt8 b, UInt8 r_null) {
        // (<> && false) is false, (true && NULL) is NULL
        return (l_null & r_null) | (r_null & (l_null ^ a)) | (l_null & (r_null ^ b));
    }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

struct OrImpl {
    using ResultType = UInt8;

    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static inline constexpr ResultType apply_null(UInt8 a, UInt8 l_null, UInt8 b, UInt8 r_null) {
        // (<> || true) is true, (false || NULL) is NULL
        return (l_null & r_null) | (r_null & (r_null ^ a)) | (l_null & (l_null ^ b));
    }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

template <typename A>
struct NotImpl {
    using ResultType = UInt8;

    static inline ResultType apply(A a) { return !a; }
};

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionAnyArityLogical>(); }

public:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override {
        return !Impl::special_implementation_for_nulls();
    }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override;
};

template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionUnaryLogical>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override;
};

} // namespace FunctionsLogicalDetail

struct NameAnd {
    static constexpr auto name = "and";
};
struct NameOr {
    static constexpr auto name = "or";
};
struct NameNot {
    static constexpr auto name = "not";
};

using FunctionAnd =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionNot =
        FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;
} // namespace doris::vectorized
