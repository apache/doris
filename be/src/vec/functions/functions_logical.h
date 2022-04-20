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

#include <type_traits>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

#ifdef DORIS_ENABLE_JIT
#include "vec/data_types/native.h"
#include "vec/data_types/data_type_number.h"
#endif

/** Logical functions AND, OR, XOR and NOT support three-valued (or ternary) logic
  * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
  *
  * Functions XOR and NOT rely on "default implementation for NULLs":
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
inline ResultType make_value(T value) {
    return value != 0 ? Ternary::True : Ternary::False;
}

template <typename T>
inline ResultType make_value(T value, bool is_null) {
    if (is_null) return Ternary::Null;
    return make_value<T>(value);
}
} // namespace Ternary

struct AndImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return true; }
    static inline constexpr bool is_saturated_value(UInt8 a) { return a == Ternary::False; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

struct OrImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return true; }
    static inline constexpr bool is_saturated_value(UInt8 a) { return a == Ternary::True; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

struct XorImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return false; }
    static inline constexpr bool is_saturated_value(bool) { return false; }
    /** Considering that CH uses UInt8 for representation of boolean values this function
      * returns 255 as "true" but the current implementation of logical functions suggests that
      * any nonzero value is "true" as well. Also the current code provides no guarantee
      * for "true" to be represented with the value of 1.
      */
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) {
        return (a != b) ? Ternary::True : Ternary::False;
    }
    static inline constexpr bool special_implementation_for_nulls() { return false; }

#if DORIS_ENABLE_JIT
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * b) {
        return builder.CreateXor(a, b);
    }
#endif
};

template <typename A>
struct NotImpl {
    using ResultType = UInt8;

    static inline ResultType apply(A a) { return !a; }

#if DORIS_ENABLE_JIT
    static inline llvm::Value * apply(llvm::IRBuilder<> & builder, llvm::Value * a, llvm::Value * b) {
        return builder.CreateNot(a);
    }
#endif
};

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    //    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }
    static FunctionPtr create() { return std::make_shared<FunctionAnyArityLogical>(); }

public:
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_nulls() const override {
        return !Impl::special_implementation_for_nulls();
    }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override;

#ifdef DORIS_ENABLE_JIT
    bool is_compilable_impl(const DataTypes &) const override { return use_default_implementation_for_nulls(); }

    Status compile_impl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values, llvm::Value** result) const override {
        assert(!types.empty() && !values.empty());

        auto& b = static_cast<llvm::IRBuilder<> &>(builder);
        if constexpr (!Impl::is_saturable()) {
            auto* value = native_bool_cast(b, types[0], values[0]);
            for (size_t i = 1; i < types.size(); ++i)
                value = Impl::apply(b, value, native_bool_cast(b, types[i], values[i]));
            *result = b.CreateSelect(value, b.getInt8(1), b.getInt8(0));
            return Status::OK();
        }
        constexpr bool break_on_true = Impl::is_saturated_value(true);
        auto* next = b.GetInsertBlock();
        auto* stop = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
        b.SetInsertPoint(stop);
        auto* phi = b.CreatePHI(b.getInt8Ty(), values.size());
        for (size_t i = 0; i < types.size(); ++i) {
            b.SetInsertPoint(next);
            auto* value = values[i];
            auto* truth = native_bool_cast(b, types[i], value);
            if (!types[i]->equals(DataTypeUInt8{}))
                value = b.CreateSelect(truth, b.getInt8(1), b.getInt8(0));
            phi->addIncoming(value, b.GetInsertBlock());
            if (i + 1 < types.size()) {
                next = llvm::BasicBlock::Create(next->getContext(), "", next->getParent());
                b.CreateCondBr(truth, break_on_true ? stop : next, break_on_true ? next : stop);
            }
        }
        b.CreateBr(stop);
        b.SetInsertPoint(stop);
        *result = phi;
        return Status::OK();
    }
#endif

};

template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionUnaryLogical>(); }

public:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override;
};

} // namespace FunctionsLogicalDetail

struct NameAnd {
    static constexpr auto name = "and";
};
struct NameOr {
    static constexpr auto name = "or";
};
struct NameXor {
    static constexpr auto name = "xor";
};
struct NameNot {
    static constexpr auto name = "not";
};

using FunctionAnd =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionXor =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::XorImpl, NameXor>;
using FunctionNot =
        FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;
} // namespace doris::vectorized
