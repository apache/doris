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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToSomething.h
// and modified by Doris

#pragma once

#include "common/status.h"
#include "util/binary_cast.hpp"
#include "vec/data_types/data_type_date.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <PrimitiveType FromPType, PrimitiveType ToPType, typename Transform>
struct Transformer {
    using FromType = typename PrimitiveTypeTraits<FromPType>::ColumnItemType;
    using ToType = typename PrimitiveTypeTraits<ToPType>::ColumnItemType;
    using CppType = typename PrimitiveTypeTraits<FromPType>::CppType;
    static void vector(const PaddedPODArray<FromType>& vec_from, PaddedPODArray<ToType>& vec_to) {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i) {
            //FIXME: seems external table still generates invalid date/datetime. but where?
            if (!binary_cast<FromType, CppType>(vec_from[i]).is_valid_date()) [[unlikely]] {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Operation {} meets invalid data: {}",
                                Transform::name, vec_from[i]);
            }
            auto res = Transform::execute(vec_from[i]);
            using RESULT_TYPE = std::decay_t<decltype(res)>;
            vec_to[i] = cast_set<ToType, RESULT_TYPE, false>(res);
        }
    }
};

template <PrimitiveType FromPType, PrimitiveType ToPType, template <PrimitiveType> typename Impl>
struct TransformerYear {
    using FromType = typename PrimitiveTypeTraits<FromPType>::ColumnItemType;
    using ToType = typename PrimitiveTypeTraits<ToPType>::ColumnItemType;

    static void vector(const PaddedPODArray<FromType>& vec_from, PaddedPODArray<ToType>& vec_to) {
        size_t size = vec_from.size();
        vec_to.resize(size);

        auto* __restrict to_ptr = vec_to.data();
        auto* __restrict from_ptr = vec_from.data();

        for (size_t i = 0; i < size; ++i) {
            to_ptr[i] = Impl<FromPType>::execute(from_ptr[i]);
        }
    }
};

template <PrimitiveType FromType, PrimitiveType ToType>
struct Transformer<FromType, ToType, ToYearImpl<FromType>>
        : public TransformerYear<FromType, ToType, ToYearImpl> {};

template <PrimitiveType FromType, PrimitiveType ToType>
struct Transformer<FromType, ToType, ToYearOfWeekImpl<FromType>>
        : public TransformerYear<FromType, ToType, ToYearOfWeekImpl> {};

// used in time_of/to_time functions
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (arguments.size() == 1) {
            if (!is_date_or_datetime(arguments[0].type->get_primitive_type()) &&
                !is_date_v2_or_datetime_v2(arguments[0].type->get_primitive_type())) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal type {} of argument of function {}. Should be a "
                                       "date or a date with time",
                                       arguments[0].type->get_name(), get_name());
            }
        } else if (arguments.size() == 2) {
            if (!is_date_or_datetime(arguments[0].type->get_primitive_type()) &&
                !is_date_v2_or_datetime_v2(arguments[0].type->get_primitive_type())) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal type {} of argument of function {}. Should be a "
                                       "date or a date with time",
                                       arguments[0].type->get_name(), get_name());
            }
            if (!is_string_type(arguments[1].type->get_primitive_type())) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Function {} supports 1 or 2 arguments. The 1st argument must be of type "
                        "Date or DateTime. The 2nd argument (optional) must be a constant string "
                        "with timezone name",
                        get_name());
            }
            if (arguments[0].type->get_primitive_type() == TYPE_DATE &&
                std::is_same_v<ToDataType, DataTypeDate>) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "The timezone argument of function {} is allowed only when the 1st "
                        "argument has the type DateTime",
                        get_name());
            }
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Number of arguments for function {} doesn't match: passed {}, "
                                   "should be 1 or 2",
                                   get_name(), arguments.size());
        }

        return std::make_shared<typename PrimitiveTypeTraits<ToDataType::PType>::DataType>();
    }

    // random value in nested for null row may be not valid date/datetime, which would cause false positive of
    // out of range error.
    bool need_replace_null_data_to_default() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        static constexpr PrimitiveType FromType = Transform::OpArgType;
        static constexpr PrimitiveType ToType = ToDataType::PType;
        using Op = Transformer<FromType, ToType, Transform>;

        const auto* sources = assert_cast<const ColumnVector<FromType>*>(
                block.get_by_position(arguments[0]).column.get());
        auto col_to = ColumnVector<ToType>::create();
        Op::vector(sources->get_data(), col_to->get_data());
        block.replace_by_position(result, std::move(col_to));

        return Status::OK();
    }
};

} // namespace doris::vectorized
