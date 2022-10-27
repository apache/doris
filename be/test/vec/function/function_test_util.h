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

#include <gtest/gtest.h>
#include <time.h>

#include <any>
#include <iostream>
#include <string>

#include "exprs/table_function/table_function.h"
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using InputDataSet = std::vector<std::vector<std::any>>; // without result
using CellSet = std::vector<std::any>;
using Expect = std::any;
using Row = std::pair<CellSet, Expect>;
using DataSet = std::vector<Row>;
using InputTypeSet = std::vector<std::any>;

int64_t str_to_date_time(std::string datetime_str, bool data_time = true);
uint32_t str_to_date_v2(std::string datetime_str, std::string datetime_format);
uint64_t str_to_datetime_v2(std::string datetime_str, std::string datetime_format);

struct Nullable {
    TypeIndex tp;
};

struct Notnull {
    TypeIndex tp;
};

struct ConstedNotnull {
    TypeIndex tp;
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

inline auto DECIMAL = Decimal128::double_to_decimal;
inline auto DECIMALFIELD = [](double v) {
    return DecimalField<Decimal128>(Decimal128::double_to_decimal(v), 9);
};

using DATETIME = std::string;

template <typename T>
struct DataTypeTraits;

template <>
struct DataTypeTraits<DataTypeInt8> {
    using type = Int8;
};

template <>
struct DataTypeTraits<DataTypeInt16> {
    using type = Int16;
};

template <>
struct DataTypeTraits<DataTypeInt32> {
    using type = Int32;
};

template <>
struct DataTypeTraits<DataTypeInt64> {
    using type = Int64;
};

template <>
struct DataTypeTraits<DataTypeInt128> {
    using type = Int128;
};

template <>
struct DataTypeTraits<DataTypeFloat32> {
    using type = Float32;
};

template <>
struct DataTypeTraits<DataTypeFloat64> {
    using type = Float64;
};

template <typename To, typename From>
constexpr decltype(auto) convert_to(From value) {
    using ToType = typename DataTypeTraits<To>::type;
    return ToType(value);
}

template <typename T>
constexpr TypeIndex get_type_index() {
    if constexpr (std::is_same_v<T, DataTypeInt8>) {
        return TypeIndex::Int8;
    } else if constexpr (std::is_same_v<T, DataTypeInt16>) {
        return TypeIndex::Int16;
    } else if constexpr (std::is_same_v<T, DataTypeInt32>) {
        return TypeIndex::Int32;
    } else if constexpr (std::is_same_v<T, DataTypeInt64>) {
        return TypeIndex::Int64;
    } else if constexpr (std::is_same_v<T, DataTypeInt128>) {
        return TypeIndex::Int128;
    } else if constexpr (std::is_same_v<T, DataTypeFloat32>) {
        return TypeIndex::Float32;
    } else if constexpr (std::is_same_v<T, DataTypeFloat64>) {
        return TypeIndex::Float64;
    }
}

struct UTDataTypeDesc {
    DataTypePtr data_type;
    doris_udf::FunctionContext::TypeDesc type_desc;
    std::string col_name;
    bool is_const = false;
    bool is_nullable = true;
};
using UTDataTypeDescs = std::vector<UTDataTypeDesc>;

} // namespace ut_type

size_t type_index_to_data_type(const std::vector<std::any>& input_types, size_t index,
                               ut_type::UTDataTypeDesc& ut_desc, DataTypePtr& type);
bool parse_ut_data_type(const std::vector<std::any>& input_types, ut_type::UTDataTypeDescs& descs);

bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const std::any& cell);

Block* create_block_from_inputset(const InputTypeSet& input_types, const InputDataSet& input_set);

Block* process_table_function(TableFunction* fn, Block* input_block,
                              const InputTypeSet& output_types);
void check_vec_table_function(TableFunction* fn, const InputTypeSet& input_types,
                              const InputDataSet& input_set, const InputTypeSet& output_types,
                              const InputDataSet& output_set);

// Null values are represented by Null()
// The type of the constant column is represented as follows: Consted {TypeIndex::String}
// A DataSet with a constant column can only have one row of data
template <typename ReturnType, bool nullable = false>
Status check_function(const std::string& func_name, const InputTypeSet& input_types,
                      const DataSet& data_set, bool expect_fail = false) {
    // 1.0 create data type
    ut_type::UTDataTypeDescs descs;
    EXPECT_TRUE(parse_ut_data_type(input_types, descs));

    // 1.1 insert data and create block
    auto row_size = data_set.size();
    Block block;
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        auto column = desc.data_type->create_column();
        column->reserve(row_size);

        auto type_ptr = desc.data_type->is_nullable()
                                ? ((DataTypeNullable*)(desc.data_type.get()))->get_nested_type()
                                : desc.data_type;
        for (int j = 0; j < row_size; j++) {
            EXPECT_TRUE(insert_cell(column, type_ptr, data_set[j].first[i]));
        }

        if (desc.is_const) {
            column = ColumnConst::create(std::move(column), row_size);
        }
        block.insert({std::move(column), desc.data_type, desc.col_name});
    }

    // 1.2 parepare args for function call
    ColumnNumbers arguments;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_col_ptrs;
    std::vector<ColumnPtrWrapper*> constant_cols;
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        arguments.push_back(i);
        arg_types.push_back(desc.type_desc);
        if (desc.is_const) {
            constant_col_ptrs.push_back(
                    std::make_shared<ColumnPtrWrapper>(block.get_by_position(i).column));
            constant_cols.push_back(constant_col_ptrs.back().get());
        } else {
            constant_cols.push_back(nullptr);
        }
    }

    // 2. execute function
    auto return_type = nullable ? make_nullable(std::make_shared<ReturnType>())
                                : std::make_shared<ReturnType>();
    auto func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    EXPECT_TRUE(func != nullptr);

    doris_udf::FunctionContext::TypeDesc fn_ctx_return;
    if constexpr (std::is_same_v<ReturnType, DataTypeUInt8>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_BOOLEAN;
    } else if constexpr (std::is_same_v<ReturnType, DataTypeInt32>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_INT;
    } else if constexpr (std::is_same_v<ReturnType, DataTypeFloat64>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DOUBLE;
    } else if constexpr (std::is_same_v<ReturnType, DateTime>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DATETIME;
    } else if (std::is_same_v<ReturnType, DateV2>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DATEV2;
    } else if (std::is_same_v<ReturnType, DateTimeV2>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DATETIMEV2;
    } else {
        fn_ctx_return.type = doris_udf::FunctionContext::INVALID_TYPE;
    }

    FunctionUtils fn_utils(fn_ctx_return, arg_types, 0);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    fn_ctx->impl()->set_constant_cols(constant_cols);
    func->prepare(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
    func->prepare(fn_ctx, FunctionContext::THREAD_LOCAL);

    block.insert({nullptr, return_type, "result"});

    auto result = block.columns() - 1;
    auto st = func->execute(fn_ctx, block, arguments, result, row_size);
    if (expect_fail) {
        EXPECT_NE(Status::OK(), st);
        return st;
    } else {
        EXPECT_EQ(Status::OK(), st);
    }

    func->close(fn_ctx, FunctionContext::THREAD_LOCAL);
    func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL);

    // 3. check the result of function
    ColumnPtr column = block.get_columns()[result];
    EXPECT_TRUE(column != nullptr);

    for (int i = 0; i < row_size; ++i) {
        auto check_column_data = [&]() {
            if constexpr (std::is_same_v<ReturnType, DataTypeJsonb>) {
                const auto& expect_data = std::any_cast<String>(data_set[i].second);
                auto s = column->get_data_at(i);
                if (expect_data.size() == 0) {
                    // zero size result means invalid
                    EXPECT_EQ(0, s.size) << " invalid result size should be 0 at row " << i;
                } else {
                    // convert jsonb binary value to json string to compare with expected json text
                    EXPECT_EQ(expect_data, JsonbToJson::jsonb_to_json_string(s.data, s.size))
                            << " at row " << i;
                }
            } else {
                Field field;
                column->get(i, field);

                const auto& expect_data =
                        std::any_cast<typename ReturnType::FieldType>(data_set[i].second);

                if constexpr (std::is_same_v<ReturnType, DataTypeDecimal<Decimal128>>) {
                    const auto& column_data = field.get<DecimalField<Decimal128>>().get_value();
                    EXPECT_EQ(expect_data.value, column_data.value) << " at row " << i;
                } else if constexpr (std::is_same_v<ReturnType, DataTypeFloat32>) {
                    const auto& column_data = field.get<DataTypeFloat64::FieldType>();
                    EXPECT_EQ(expect_data, column_data) << " at row " << i;
                } else {
                    const auto& column_data = field.get<typename ReturnType::FieldType>();
                    EXPECT_EQ(expect_data, column_data) << " at row " << i;
                }
            }
        };

        if constexpr (nullable) {
            bool is_null = data_set[i].second.type() == typeid(Null);
            EXPECT_EQ(is_null, column->is_null_at(i)) << " at row " << i;
            if (!is_null) check_column_data();
        } else {
            check_column_data();
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
