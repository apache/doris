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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <mysql/mysql.h>

#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "olap/hll.h"
#include "olap/olap_common.h"
#include "testutil/any_type.h"
#include "testutil/function_utils.h"
#include "testutil/test_util.h"
#include "udf/udf.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/core/block.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class DataTypeJsonb;
class TableFunction;

// for an input row with only one column, should use {AnyType(xxx)} to represent it because TestArray is same with
// InputCell. just {} will be treated as copy-constructor rather than initializer list.
using TestArray = std::vector<AnyType>;
//TODO: replace Map, Struct with AnyType combinations too

using InputCell = std::vector<AnyType>;
using InputDataSet = std::vector<InputCell>;
using Expect = AnyType;
using Row = std::pair<InputCell, Expect>;
using DataSet = std::vector<Row>;
// to represent Array<Int64>: {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_BIGINT}
using InputTypeSet = std::vector<AnyType>;

struct Nullable {
    PrimitiveType tp;
};

struct Notnull {
    PrimitiveType tp;
};
// Consted already defined in types.h
struct ConstedNotnull {
    PrimitiveType tp;
};

namespace ut_type {
using BOOLEAN = uint8_t;
using TINYINT = int8_t;
using SMALLINT = int16_t;
using INT = int32_t;
using BIGINT = int64_t;
using LARGEINT = int128_t;

using VARCHAR = std::string;
using CHAR = std::string;
using STRING = std::string;

using DOUBLE = double;
using FLOAT = float;

using IPV4 = uint32_t;
using IPV6 = uint128_t;

//ATTN: keep same with `insert_cell`. not applicable for DataTypeNullable
//TODO: make default_value constexpr when we upgrade to clang++17
template <typename DataType>
struct ut_input_type {};
template <PrimitiveType NativeType>
struct ut_input_type<DataTypeNumber<NativeType>> {
    using type = DataTypeNumber<NativeType>::FieldType;
    inline static type default_value = 123;
};
template <PrimitiveType DecimalType>
struct ut_input_type<DataTypeDecimal<DecimalType>> {
    using type = DataTypeDecimal<DecimalType>::FieldType;
    inline static type default_value = type {123};
};
template <>
struct ut_input_type<DataTypeString> {
    using type = std::string;
    inline static type default_value = "test_default";
};
template <>
struct ut_input_type<DataTypeDate> {
    using type = std::string;
    inline static type default_value = "1970-01-01";
};
template <>
struct ut_input_type<DataTypeDateTime> {
    using type = std::string;
    inline static type default_value = "1970-01-01";
};
template <>
struct ut_input_type<DataTypeDateV2> {
    using type = std::string;
    inline static type default_value = "1970-01-01";
};
template <>
struct ut_input_type<DataTypeDateTimeV2> {
    using type = std::string;
    inline static type default_value = "1970-01-01";
};
template <>
struct ut_input_type<DataTypeTimeV2> {
    using type = std::string;
    inline static type default_value = "01:02:03";
};
template <>
struct ut_input_type<DataTypeJsonb> {
    using type = std::string;
};
template <>
struct ut_input_type<DataTypeBitMap> {
    using type = BitmapValue*;
};
template <>
struct ut_input_type<DataTypeHLL> {
    using type = HyperLogLog*;
};
template <>
struct ut_input_type<DataTypeIPv4> {
    using type = IPV4;
    inline static type default_value = 0;
};
template <>
struct ut_input_type<DataTypeIPv6> {
    using type = IPV6;
    inline static type default_value = 0;
};
template <>
struct ut_input_type<DataTypeArray> {
    using type = TestArray;
};
template <>
struct ut_input_type<DataTypeStruct> {
    using type = InputCell;
};

// for cast tests, the target type need a placeholder column with some legal value.
template <typename DataType>
inline static ut_input_type<DataType>::type ut_input_type_default_v =
        ut_input_type<DataType>::default_value;

// cell constructors. could also use from_int_frac if you'd like
inline auto DECIMALV2 = Decimal128V2::double_to_decimalv2;
inline auto DECIMAL32 = [](int32_t x, int32_t y, int scale) {
    return Decimal32::from_int_frac(x, y, scale);
};
inline auto DECIMAL64 = [](int64_t x, int64_t y, int scale) {
    return Decimal64::from_int_frac(x, y, scale);
};
inline auto DECIMAL128V2 = [](int128_t x, int128_t y, int scale) {
    return Decimal128V2::from_int_frac(x, y, 9);
};
inline auto DECIMAL128V3 = [](int128_t x, int128_t y, int scale) {
    return Decimal128V3::from_int_frac(x, y, scale);
};
inline auto DECIMAL256 = [](wide::Int256 x, wide::Int256 y, int scale) {
    return Decimal256::from_int_frac(x, y, scale);
};

using DATETIME = std::string;

struct UTDataTypeDesc {
    DataTypePtr data_type;
    std::string col_name;
    bool is_const = false;
    bool is_nullable = true; // ATTN: default is true
};
using UTDataTypeDescs = std::vector<UTDataTypeDesc>;
} // namespace ut_type

bool parse_ut_data_type(const std::vector<AnyType>& input_types, ut_type::UTDataTypeDescs& descs);

bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell,
                 bool datetime_is_string_format = true);

void check_vec_table_function(TableFunction* fn, const InputTypeSet& input_types,
                              const InputDataSet& input_set, const InputTypeSet& output_types,
                              const InputDataSet& output_set, bool test_get_value_func = false);

template <typename ReturnType>
DataTypePtr get_return_type_descriptor(int scale, int precision) {
    if constexpr (std::is_same_v<ReturnType, DataTypeUInt8>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_BOOLEAN,
                                                            false);
    } else if constexpr (std::is_same_v<ReturnType, DataTypeInt8>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_TINYINT,
                                                            false);
    } else if constexpr (std::is_same_v<ReturnType, DataTypeInt32>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_INT, false);
    } else if constexpr (std::is_same_v<ReturnType, DataTypeInt64>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_BIGINT,
                                                            false);
    } else if constexpr (std::is_same_v<ReturnType, DataTypeFloat64>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DOUBLE,
                                                            false);
    } else if constexpr (std::is_same_v<ReturnType, DataTypeTimeV2>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_TIMEV2,
                                                            false, precision, scale);
    } else if constexpr (std::is_same_v<ReturnType, DateTime>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DATETIME,
                                                            false);
    } else if (std::is_same_v<ReturnType, DateV2>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DATEV2,
                                                            false);
    } else if (std::is_same_v<ReturnType, DateTimeV2>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DATETIMEV2,
                                                            false, precision, scale);
    } else if (std::is_same_v<ReturnType, DataTypeDecimalV2>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DECIMALV2,
                                                            false, precision, scale);
    } else if (std::is_same_v<ReturnType, DataTypeDecimal32>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DECIMAL32,
                                                            false, precision, scale);
    } else if (std::is_same_v<ReturnType, DataTypeDecimal64>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DECIMAL64,
                                                            false, precision, scale);
    } else if (std::is_same_v<ReturnType, DataTypeDecimal128>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DECIMAL128I,
                                                            false, precision, scale);
    } else if (std::is_same_v<ReturnType, DataTypeDecimal256>) {
        return DataTypeFactory::instance().create_data_type(doris::PrimitiveType::TYPE_DECIMAL256,
                                                            false, precision, scale);
    } else {
        return std::make_shared<DataTypeNothing>();
    }
}

struct Consted {
    PrimitiveType tp;
};

/**
 * Null values are represented by Null()
 * The type of the constant column is represented as follows: Consted {PrimitiveType::TYPE_VARCHAR}
 * A DataSet with a constant column can only have one row of data
 * About scales and precisions:
    When you need scale in and scale out(like, DatetimeV2 to DatetimeV2), you need:
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 3}}; // input scale
        ...
        check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 3); // output scale
     IF YOU FORGET TO SET THE SCALE, THE MICROSECOND WILL NOT BE TESTED. we can't force to check it because Field doesn't
     keep the scale. So if the scale doesn't match, 
    And for Decimal input or output, you need:
        {{...}, DECIMAL64(1653395696, 789, 3)} // an output example
     because every Decimal type already set its precision. so scale in enough.
    For Decimal output, you need sepecific output's scale and precision:
        check_function<DataTypeDecimal<Decimal64>, true>(func_name, input_types, data_set, 6, 9);
*/
// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
template <typename ResultType, bool ResultNullable = false, bool datetime_is_string_format = true>
Status check_function(const std::string& func_name, const InputTypeSet& input_types,
                      const DataSet& data_set, int result_scale = -1, int result_precision = -1,
                      bool expect_execute_fail = false, bool expect_result_ne = false,
                      bool is_strict_mode = false) {
    TestCaseInfo::arg_size = static_cast<int>(input_types.size());
    TestCaseInfo::func_call_index++;
    // 1.0 create data type
    ut_type::UTDataTypeDescs descs;
    // desc get type's precision and scale here. FIXME: replace by DataTypePtr inputs directly.
    EXPECT_TRUE(parse_ut_data_type(input_types, descs));

    // 1.1 insert data and create block
    auto row_size = data_set.size();
    Block block;
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        auto column = desc.data_type->create_column();
        column->reserve(row_size);

        // for function cast, the second column is const but there's many rows in block. so we only insert one row
        // for the second column.
        for (int j = 0; j < ((func_name == "CAST" && i == 1) ? 1 : row_size); j++) {
            // null dealed in insert_cell
            EXPECT_TRUE(insert_cell(column, desc.data_type, data_set[j].first[i],
                                    datetime_is_string_format));
        }

        if (desc.is_const) {
            column = ColumnConst::create(std::move(column), row_size);
        }
        block.insert({std::move(column), desc.data_type, desc.col_name});
    }

    // 1.2 prepare args for function call
    ColumnNumbers arguments;
    std::vector<DataTypePtr> arg_types;
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_col_ptrs;
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols;
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        arguments.push_back(static_cast<unsigned int>(i));
        arg_types.push_back(desc.data_type);
        if (desc.is_const) {
            constant_col_ptrs.push_back(
                    std::make_shared<ColumnPtrWrapper>(block.get_by_position(i).column));
            constant_cols.push_back(constant_col_ptrs.back());
        } else {
            constant_cols.push_back(nullptr);
        }
    }

    // 2. execute function
    auto return_type = [&]() {
        if constexpr (IsDataTypeDecimal<ResultType>) { // decimal
            return ResultNullable ? make_nullable(std::make_shared<ResultType>(result_precision,
                                                                               result_scale))
                                  : std::make_shared<ResultType>(result_precision, result_scale);
        } else if constexpr (IsDataTypeDateTimeV2<ResultType> ||
                             IsTimeV2Type<ResultType>) { // datetimev2 or timev2
            UInt32 real_scale = 0;
            if (result_scale != -1) {
                real_scale = static_cast<UInt32>(result_scale);
            }
            return ResultNullable ? make_nullable(std::make_shared<ResultType>(real_scale))
                                  : std::make_shared<ResultType>(real_scale);
        } else {
            return ResultNullable ? make_nullable(std::make_shared<ResultType>())
                                  : std::make_shared<ResultType>();
        }
    }();
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    assert(func.get() != nullptr);

    // this may be useless now. for some type like array, it's wrong. TODO: need more details explainations
    auto fn_ctx_return = get_return_type_descriptor<ResultType>(std::max(0, result_scale),
                                                                std::max(0, result_precision));

    FunctionUtils fn_utils(fn_ctx_return, arg_types, is_strict_mode);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->set_constant_cols(constant_cols);
    static_cast<void>(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    static_cast<void>(func->open(fn_ctx, FunctionContext::THREAD_LOCAL));

    block.insert({nullptr, return_type, "result"});

    auto result = block.columns() - 1;
    auto st = func->execute(fn_ctx, block, arguments, result, row_size);
    if (expect_execute_fail) {
        EXPECT_NE(Status::OK(), st);
        return st;
    } else {
        EXPECT_EQ(Status::OK(), st);
    }

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));

    // 3.0. create expected result column in block
    DataTypePtr result_type_ptr;
    if constexpr (IsDataTypeDecimal<ResultType>) { // decimal
        result_type_ptr = ResultNullable
                                  ? make_nullable(std::make_shared<ResultType>(result_precision,
                                                                               result_scale))
                                  : std::make_shared<ResultType>(result_precision, result_scale);
    } else if constexpr (IsDataTypeDateTimeV2<ResultType> ||
                         IsTimeV2Type<ResultType>) { // datetimev2
        UInt32 real_scale = 0;
        if (result_scale != -1) {
            real_scale = static_cast<UInt32>(result_scale);
        }
        result_type_ptr = ResultNullable ? make_nullable(std::make_shared<ResultType>(real_scale))
                                         : std::make_shared<ResultType>(real_scale);
    } else {
        result_type_ptr = ResultNullable ? make_nullable(std::make_shared<ResultType>())
                                         : std::make_shared<ResultType>();
    }
    MutableColumnPtr expected_col_ptr = result_type_ptr->create_column();
    for (int i = 0; i < row_size; i++) {
        EXPECT_TRUE(insert_cell(expected_col_ptr, result_type_ptr, data_set[i].second,
                                datetime_is_string_format));
    }

    // 3.1. check the result of function
    ColumnPtr column = block.get_columns()[result];
    EXPECT_TRUE(column);
    if (const auto* column_str = check_and_get_column<ColumnString>(column.get());
        column_str && !expect_result_ne) {
        column_str->sanity_check();
    }

    for (int i = 0; i < row_size; ++i) {
        TestCaseInfo::error_line_number = i; // for failure report

        if (expect_result_ne) {
            EXPECT_NE(0, column->compare_at(i, i, *expected_col_ptr, 1))
                    << ", function result: "
                    << block.get_data_types()[result]->to_string(*column, i)
                    << ", expected result: " << result_type_ptr->to_string(*expected_col_ptr, i);
        } else {
            auto comp_res = column->compare_at(i, i, *expected_col_ptr, 1);
            EXPECT_EQ(0, comp_res)
                    << ", function " << func_name << ". input row:\n"
                    << block.dump_data(i, 1)
                    << "result: " << block.get_data_types()[result]->to_string(*column, i)
                    << ", expected result: " << result_type_ptr->to_string(*expected_col_ptr, i);
        }
    }

    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// Each parameter may be decorated with 'const', but each invocation of 'check_function' can only handle one state of the parameters.
// If there are 'n' parameters, it would require manually calling 'check_function' 2^n times, whereas through this function, only one
// invocation is needed.
template <typename ReturnType, bool nullable = false>
void check_function_all_arg_comb(const std::string& func_name, const InputTypeSet& base_types,
                                 const DataSet& data_set) {
    TestCaseInfo::func_call_index++;
    size_t arg_cnt = base_types.size();
    // Consider each parameter as a bit, if the j-th bit is 1, the j-th parameter is const; otherwise, it is not.
    for (int i = 0; i < (1 << arg_cnt); i++) {
        InputTypeSet input_types {};
        for (int j = 0; j < arg_cnt; j++) {
            bool is_const = (1 << j) & i;
            auto base_type_idx = any_cast<PrimitiveType>(base_types[j]);
            if (is_const) { // wrap in consted
                if (base_types[j].type() == &typeid(Notnull)) {
                    input_types.emplace_back(ConstedNotnull {base_type_idx},
                                             base_types[j].scale_or(-1),
                                             base_types[j].precision_or(-1));
                } else {
                    input_types.emplace_back(Consted {base_type_idx}, base_types[j].scale_or(-1),
                                             base_types[j].precision_or(-1));
                }
            } else {
                input_types.emplace_back(base_types[j]);
            }
        }

        TestCaseInfo::arg_const_info = i, TestCaseInfo::error_line_number = -1;
        // exists parameter are const
        if (i != 0) {
            for (const auto& line : data_set) {
                DataSet tmp_set {line};
                // check_function_all_arg_comb is ONE call. adding here and minuing in check_function to make it consistent.
                TestCaseInfo::func_call_index--;
                static_cast<void>(
                        check_function<ReturnType, nullable>(func_name, input_types, tmp_set));
            }
        } else {
            TestCaseInfo::func_call_index--;
            static_cast<void>(
                    check_function<ReturnType, nullable>(func_name, input_types, data_set));
        }
    }
}
} // namespace doris::vectorized
