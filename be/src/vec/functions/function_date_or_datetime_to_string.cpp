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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToString.cpp
// and modified by Doris

#include <cstddef>
#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h" // IWYU pragma: keep
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
class DataTypeString;

template <typename Transform>
class FunctionDateOrDateTimeToString : public IFunction {
public:
    using NativeType = PrimitiveTypeTraits<Transform::OpArgType>::ColumnItemType;
    using DateType = PrimitiveTypeTraits<Transform::OpArgType>::CppType;
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;
    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(TYPE_STRING);
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Transform::get_variadic_argument_types();
        }
        return {};
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        const auto* sources =
                check_and_get_column<ColumnVector<Transform::OpArgType>>(source_col.get());
        auto col_res = ColumnString::create();

        // Support all input of datetime is valind to make sure not null return
        if (sources) {
            vector(context, sources->get_data(), col_res->get_chars(), col_res->get_offsets());
            block.replace_by_position(result, std::move(col_res));
        } else {
            return Status::InternalError("Illegal column {} of first argument of function {}",
                                         block.get_by_position(arguments[0]).column->get_name(),
                                         name);
        }
        return Status::OK();
    }

private:
    static void vector(FunctionContext* context, const PaddedPODArray<NativeType>& ts,
                       ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        const auto len = ts.size();
        res_data.resize(len * Transform::max_size);
        res_offsets.resize(len);

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            const auto& t = ts[i];
            const auto date_time_value = binary_cast<NativeType, DateType>(t);
            res_offsets[i] =
                    cast_set<UInt32>(Transform::execute(date_time_value, res_data, offset));
            DCHECK(date_time_value.is_valid_date());
        }
        res_data.resize(res_offsets[res_offsets.size() - 1]);
    }
};

using FunctionDayName = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATETIME>>;
using FunctionDayNameV2 = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATEV2>>;
using FunctionMonthName = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATETIME>>;
using FunctionMonthNameV2 = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATEV2>>;

using FunctionDateTimeV2DayName = FunctionDateOrDateTimeToString<DayNameImpl<TYPE_DATETIMEV2>>;
using FunctionDateTimeV2MonthName = FunctionDateOrDateTimeToString<MonthNameImpl<TYPE_DATETIMEV2>>;

using FunctionDateIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATEV2>>;
using FunctionDateTimeIso8601 = FunctionDateOrDateTimeToString<ToIso8601Impl<TYPE_DATETIMEV2>>;

void register_function_date_time_to_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDayName>();
    factory.register_function<FunctionMonthName>();
    factory.register_function<FunctionDayNameV2>();
    factory.register_function<FunctionMonthNameV2>();
    factory.register_function<FunctionDateTimeV2DayName>();
    factory.register_function<FunctionDateTimeV2MonthName>();
    factory.register_function<FunctionDateIso8601>();
    factory.register_function<FunctionDateTimeIso8601>();
}

} // namespace doris::vectorized
