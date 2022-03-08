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

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "util/bitmap_value.h"
#include "vec/columns/column_complex.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

using DataSet = std::vector<std::pair<std::vector<std::any>, std::any>>;
using InputTypeSet = std::vector<std::any>;

int64_t str_to_data_time(std::string datetime_str, bool data_time = true) {
    VecDateTimeValue v;
    v.from_date_str(datetime_str.c_str(), datetime_str.size());
    if (data_time) { //bool data_time only to simplifly means data_time or data to cast, just use in time-functions uint test
        v.to_datetime();
    } else {
        v.cast_to_date();
    }
    return binary_cast<VecDateTimeValue, Int64>(v);
}

namespace ut_type {
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

inline auto DECIMAL = Decimal<Int128>::double_to_decimal;

using DATETIME = std::string;
} // namespace ut_type

template <typename ColumnType, typename Column, typename NullColumn>
void insert_column_to_block(std::list<ColumnPtr>& columns, ColumnsWithTypeAndName& ctn,
                            Column&& col, NullColumn&& null_map, Block& block,
                            const std::string& col_name, int i, bool is_const, int row_size) {
    columns.emplace_back(ColumnNullable::create(std::move(col), std::move(null_map)));
    ColumnWithTypeAndName type_and_name(
            is_const ? ColumnConst::create(columns.back()->get_ptr(), row_size)
                     : columns.back()->get_ptr(),
            make_nullable(std::make_shared<ColumnType>()), col_name);
    block.insert(i, type_and_name);
    ctn.emplace_back(type_and_name);
}

// Null values are represented by Null()
// The type of the constant column is represented as follows: Consted {TypeIndex::String}
// A DataSet with a constant column can only have one row of data
template <typename ReturnType, bool nullable = false>
void check_function(const std::string& func_name, const std::vector<std::any>& input_types,
                    const DataSet& data_set) {
    size_t row_size = data_set.size();
    size_t column_size = input_types.size();

    std::list<ColumnPtr> columns;
    Block block;
    ColumnNumbers arguments;
    ColumnsWithTypeAndName ctn;
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_col_ptrs;
    std::vector<ColumnPtrWrapper*> constant_cols;
    std::vector<doris_udf::FunctionContext::TypeDesc> arg_types;
    doris_udf::FunctionContext::TypeDesc arg_type;
    // 1. build block and column type and names
    for (int i = 0; i < column_size; i++) {
        TypeIndex tp;
        bool is_const;
        if (input_types[i].type() == typeid(Consted)) {
            tp = std::any_cast<Consted>(input_types[i]).tp;
            is_const = true;
        } else {
            tp = std::any_cast<TypeIndex>(input_types[i]);
            is_const = false;
        }

        std::string col_name = "k" + std::to_string(i);

        auto null_map = ColumnUInt8::create(row_size, false);
        auto& null_map_data = null_map->get_data();

        if (tp == TypeIndex::String) {
            auto col = ColumnString::create();
            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto str = std::any_cast<ut_type::STRING>(data_set[j].first[i]);
                col->insert_data(str.c_str(), str.size());
            }
            insert_column_to_block<DataTypeString>(columns, ctn, std::move(col),
                                                   std::move(null_map), block, col_name, i,
                                                   is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_STRING;
        } else if (tp == TypeIndex::BitMap) {
            auto col = ColumnBitmap::create();
            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                BitmapValue* bitmap = std::any_cast<BitmapValue*>(data_set[j].first[i]);
                col->insert_value(*bitmap);
            }
            insert_column_to_block<DataTypeBitMap>(columns, ctn, std::move(col),
                                                   std::move(null_map), block, col_name, i,
                                                   is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_OBJECT;
        } else if (tp == TypeIndex::Int8) {
            auto col = ColumnInt8::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::TINYINT>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeInt8>(columns, ctn, std::move(col), std::move(null_map),
                                                 block, col_name, i, is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_TINYINT;
        } else if (tp == TypeIndex::Int16) {
            auto col = ColumnInt16::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::SMALLINT>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeInt16>(columns, ctn, std::move(col), std::move(null_map),
                                                  block, col_name, i, is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_SMALLINT;
        } else if (tp == TypeIndex::Int32) {
            auto col = ColumnInt32::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::INT>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeInt32>(columns, ctn, std::move(col), std::move(null_map),
                                                  block, col_name, i, is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_INT;
        } else if (tp == TypeIndex::Int64) {
            auto col = ColumnInt64::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::BIGINT>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeInt64>(columns, ctn, std::move(col), std::move(null_map),
                                                  block, col_name, i, is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_BIGINT;
        } else if (tp == TypeIndex::Int128) {
            auto col = ColumnInt128::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::LARGEINT>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeInt128>(columns, ctn, std::move(col),
                                                   std::move(null_map), block, col_name, i,
                                                   is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_LARGEINT;
        } else if (tp == TypeIndex::Float64) {
            auto col = ColumnFloat64::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<ut_type::DOUBLE>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeFloat64>(columns, ctn, std::move(col),
                                                    std::move(null_map), block, col_name, i,
                                                    is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_DOUBLE;
        } else if (tp == TypeIndex::Decimal128) {
            auto col = ColumnDecimal<Decimal128>::create(0, 9);

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto value = std::any_cast<Decimal<Int128>>(data_set[j].first[i]);
                col->insert_data(reinterpret_cast<char*>(&value), 0);
            }
            insert_column_to_block<DataTypeDecimal<Decimal128>>(columns, ctn, std::move(col),
                                                                std::move(null_map), block,
                                                                col_name, i, is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_DECIMALV2;
        } else if (tp == TypeIndex::DateTime) {
            static std::string date_time_format("%Y-%m-%d %H:%i:%s");
            auto col = ColumnInt64::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto datetime_str = std::any_cast<std::string>(data_set[j].first[i]);
                VecDateTimeValue v;
                v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                                       datetime_str.c_str(), datetime_str.size());
                v.to_datetime();
                col->insert_data(reinterpret_cast<char*>(&v), 0);
            }
            insert_column_to_block<DataTypeDateTime>(columns, ctn, std::move(col),
                                                     std::move(null_map), block, col_name, i,
                                                     is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_DATETIME;
        } else if (tp == TypeIndex::Date) {
            static std::string date_time_format("%Y-%m-%d");
            auto col = ColumnInt64::create();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_default();
                    continue;
                }
                auto datetime_str = std::any_cast<std::string>(data_set[j].first[i]);
                VecDateTimeValue v;
                v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                                       datetime_str.c_str(), datetime_str.size());
                v.cast_to_date();
                col->insert_data(reinterpret_cast<char*>(&v), 0);
            }
            insert_column_to_block<DataTypeDateTime>(columns, ctn, std::move(col),
                                                     std::move(null_map), block, col_name, i,
                                                     is_const, row_size);
            arg_type.type = doris_udf::FunctionContext::TYPE_DATE;
        } else {
            ASSERT_TRUE(false);
            arg_type.type = doris_udf::FunctionContext::INVALID_TYPE;
        }
        arguments.push_back(i);
        arg_types.push_back(arg_type);
        if (is_const) {
            const auto& column = block.get_by_position(i).column;
            std::shared_ptr<ColumnPtrWrapper> constant_col =
                    std::make_shared<ColumnPtrWrapper>(column);
            constant_col_ptrs.push_back(constant_col);
            constant_cols.push_back(constant_col.get());
        } else {
            constant_cols.push_back(nullptr);
        }
    }

    // 2. execute function
    auto return_type = nullable ? make_nullable(std::make_shared<ReturnType>())
                                : std::make_shared<ReturnType>();
    auto func = SimpleFunctionFactory::instance().get_function(func_name, ctn, return_type);
    ASSERT_TRUE(func != nullptr);

    doris_udf::FunctionContext::TypeDesc fn_ctx_return;
    if (std::is_same_v<ReturnType, DataTypeUInt8>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_BOOLEAN;
    } else if (std::is_same_v<ReturnType, DataTypeFloat64>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DOUBLE;
    } else if (std::is_same_v<ReturnType, DataTypeInt32>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_INT;
    } else if (std::is_same_v<ReturnType, DateTime>) {
        fn_ctx_return.type = doris_udf::FunctionContext::TYPE_DATETIME;
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
    func->execute(fn_ctx, block, arguments, result, row_size);

    func->close(fn_ctx, FunctionContext::THREAD_LOCAL);
    func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL);

    // 3. check the result of function
    ColumnPtr column = block.get_columns()[result];
    ASSERT_TRUE(column != nullptr);

    for (int i = 0; i < row_size; ++i) {
        auto check_column_data = [&]() {
            Field field;
            column->get(i, field);

            const auto& column_data = field.get<typename ReturnType::FieldType>();
            const auto& expect_data =
                    std::any_cast<typename ReturnType::FieldType>(data_set[i].second);

            ASSERT_EQ(column_data, expect_data);
        };

        if constexpr (nullable) {
            bool is_null = data_set[i].second.type() == typeid(Null);
            ASSERT_EQ(column->is_null_at(i), is_null);
            if (!is_null) check_column_data();
        } else {
            check_column_data();
        }
    }
}

} // namespace doris::vectorized
