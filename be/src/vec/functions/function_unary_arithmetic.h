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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionUnaryArithmetic.h
// and modified by Doris

#pragma once

#include "runtime/define_primitive_type.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <PrimitiveType A, typename Op>
struct UnaryOperationImpl {
    static constexpr PrimitiveType ResultType = Op::ResultType;
    using ColVecA = typename PrimitiveTypeTraits<A>::ColumnType;
    using ColVecC = typename PrimitiveTypeTraits<ResultType>::ColumnType;
    using ArrayA = typename ColVecA::Container;
    using ArrayC = typename ColVecC::Container;

    static void NO_INLINE vector(const ArrayA& a, ArrayC& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) c[i] = Op::apply(a[i]);
    }
};

template <typename>
struct AbsImpl;
template <typename>
struct NegativeImpl;
template <typename>
struct PositiveImpl;

/// Used to indicate undefined operation
struct InvalidType;

template <typename Op, typename Name, PrimitiveType param_type>
class FunctionUnaryArithmetic : public IFunction {
    using ResultDataType = typename PrimitiveTypeTraits<param_type>::DataType;
    static constexpr bool allow_decimal = std::string_view(Name::name) == "abs" ||
                                          std::string_view(Name::name) == "negative" ||
                                          std::string_view(Name::name) == "positive";
    static constexpr bool not_variadic =
            std::string_view(Name::name) == "degree" || std::string_view(Name::name) == "radian" ||
            std::string_view(Name::name) == "sign" || std::string_view(Name::name) == "signbit";

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        if (const auto* data_type = typeid_cast<const ResultDataType*>(type)) {
            return f(*data_type);
        }
        return false;
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionUnaryArithmetic>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override {
        if constexpr (not_variadic) {
            return false;
        }
        return true;
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (not_variadic) {
            return {};
        }
        return {std::make_shared<ResultDataType>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr result;
        bool valid = cast_type(arguments[0].get(), [&](const auto& type) {
            using DataType = std::decay_t<decltype(type)>;

            if constexpr (IsDataTypeDecimal<DataType>) {
                if constexpr (!allow_decimal) return false;
                result = std::make_shared<DataType>(type.get_precision(), type.get_scale());
            } else {
                result = std::make_shared<typename PrimitiveTypeTraits<Op::ResultType>::DataType>();
            }
            return true;
        });
        if (!valid) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Illegal type {} of argument of function {}",
                                   arguments[0]->get_name(), get_name());
        }
        return result;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        bool valid =
                cast_type(block.get_by_position(arguments[0]).type.get(), [&](const auto& type) {
                    using DataType = std::decay_t<decltype(type)>;

                    if constexpr (IsDataTypeDecimal<DataType>) {
                        if constexpr (allow_decimal) {
                            if (auto col = check_and_get_column<ColumnDecimal<DataType::PType>>(
                                        block.get_by_position(arguments[0]).column.get())) {
                                auto col_res =
                                        PrimitiveTypeTraits<Op::ResultType>::ColumnType::create(
                                                0, type.get_scale());
                                auto& vec_res = col_res->get_data();
                                vec_res.resize(col->get_data().size());
                                UnaryOperationImpl<DataType::PType, Op>::vector(col->get_data(),
                                                                                vec_res);
                                block.replace_by_position(result, std::move(col_res));
                                return true;
                            }
                        }
                    } else {
                        if (auto col = check_and_get_column<ColumnVector<DataType::PType>>(
                                    block.get_by_position(arguments[0]).column.get())) {
                            auto col_res =
                                    PrimitiveTypeTraits<Op::ResultType>::ColumnType::create();
                            auto& vec_res = col_res->get_data();
                            vec_res.resize(col->get_data().size());
                            UnaryOperationImpl<DataType::PType, Op>::vector(col->get_data(),
                                                                            vec_res);
                            block.replace_by_position(result, std::move(col_res));
                            return true;
                        }
                    }

                    return false;
                });
        if (!valid) {
            return Status::RuntimeError("{}'s argument does not match the expected data type",
                                        get_name());
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
