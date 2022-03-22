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
                               doris_udf::FunctionContext::TypeDesc& desc,
                               DataTypePtr& type) {
    if(index < 0 || index >= input_types.size()) {
        return -1;
    }

    TypeIndex tp;
    if (input_types[index].type() == typeid(Consted)) {
        tp = std::any_cast<Consted>(input_types[index]).tp;
    } else {
        tp = std::any_cast<TypeIndex>(input_types[index]);
    }

    switch (tp) {
    case TypeIndex::String:
        desc.type = doris_udf::FunctionContext::TYPE_STRING;
        type = std::make_shared<DataTypeString>();
        return 1;
    case TypeIndex::BitMap:
        desc.type = doris_udf::FunctionContext::TYPE_OBJECT;
        type = std::make_shared<DataTypeBitMap>();
        return 1;
    case TypeIndex::Int8:
        desc.type = doris_udf::FunctionContext::TYPE_TINYINT;
        type = std::make_shared<DataTypeInt8>();
        return 1;
    case TypeIndex::Int16:
        desc.type = doris_udf::FunctionContext::TYPE_SMALLINT;
        type = std::make_shared<DataTypeInt16>();
        return 1;
    case TypeIndex::Int32:
        desc.type = doris_udf::FunctionContext::TYPE_INT;
        type = std::make_shared<DataTypeInt32>();
        return 1;
    case TypeIndex::Int64:
        desc.type = doris_udf::FunctionContext::TYPE_BIGINT;
        type = std::make_shared<DataTypeInt64>();
        return 1;
    case TypeIndex::Int128:
        desc.type = doris_udf::FunctionContext::TYPE_LARGEINT;
        type = std::make_shared<DataTypeInt128>();
        return 1;
    case TypeIndex::Float64:
        desc.type = doris_udf::FunctionContext::TYPE_DOUBLE;
        type = std::make_shared<DataTypeFloat64>();
        return 1;
    case TypeIndex::Decimal128:
        desc.type = doris_udf::FunctionContext::TYPE_DECIMALV2;
        type = std::make_shared<DataTypeDecimal<Decimal128>>();
        return 1;
    case TypeIndex::DateTime:
        desc.type = doris_udf::FunctionContext::TYPE_DATETIME;
        type = std::make_shared<DataTypeDateTime>();
        return 1;
    case TypeIndex::Date:
        desc.type = doris_udf::FunctionContext::TYPE_DATE;
        type = std::make_shared<DataTypeDateTime>();
        return 1;
    case TypeIndex::Array: {
        desc.type = doris_udf::FunctionContext::TYPE_ARRAY;
        doris_udf::FunctionContext::TypeDesc sub_desc;
        DataTypePtr sub_type = nullptr;
        size_t ret = type_index_to_data_type(input_types, index + 1, sub_desc, sub_type);
        if (ret <= 0) {
            return ret;
        }
        desc.children.push_back(doris_udf::FunctionContext::TypeDesc());
        type = std::make_shared<DataTypeArray>(std::move(sub_type));
        return ret + 1;
    }
    default:
        LOG(WARNING) << "not supported TypeIndex:" << (int)tp;
        return 0;
    }
}
bool parse_ut_data_type(const std::vector<std::any>& input_types, ut_type::UTDataTypeDescs& descs) {
    descs.clear();
    descs.reserve(input_types.size());
    for (size_t i = 0; i < input_types.size(); ) {
        ut_type::UTDataTypeDesc desc;
        if (input_types[i].type() == typeid(Consted)) {
            desc.is_const = true;
        }
        size_t res = type_index_to_data_type(input_types, i, desc.type_desc, desc.data_type);
        if (res <= 0) {
            return false;
        }
        if (desc.is_nullable) {
            desc.data_type = make_nullable(std::move(desc.data_type));
        }
        desc.col_name = "k" + std::to_string(i);
        descs.emplace_back(desc);
        i += res;
    }
    return true;
}

// Null values are represented by Null()
// The type of the constant column is represented as follows: Consted {TypeIndex::String}
// A DataSet with a constant column can only have one row of data
template <typename ReturnType, bool nullable = false>
void check_function(const std::string& func_name, const std::vector<std::any>& input_types,
                    const DataSet& data_set) {
    // 1.0 create data type
    ut_type::UTDataTypeDescs descs;
    ASSERT_TRUE(parse_ut_data_type(input_types, descs));

    // 1.1 insert data and create block
    auto row_size = data_set.size();
    Block block;
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        auto column = desc.data_type->create_column();
        column->reserve(row_size);

        auto type_ptr = desc.data_type->is_nullable() ?
             ((DataTypeNullable*)(desc.data_type.get()))->get_nested_type() : desc.data_type;
        WhichDataType type(type_ptr);

        for (int j = 0; j < row_size; j++) {
            if (data_set[j].first[i].type() == typeid(Null)) {
                column->insert_data(nullptr, 0);
                continue;
            }

            if (type.is_string()) {
                auto str = std::any_cast<ut_type::STRING>(data_set[j].first[i]);
                column->insert_data(str.c_str(), str.size());
            }  else if (type.idx == TypeIndex::BitMap) {
                BitmapValue* bitmap = std::any_cast<BitmapValue*>(data_set[j].first[i]);
                column->insert_data((char*)bitmap, sizeof(BitmapValue));
            } else if (type.is_int8()) {
                auto value = std::any_cast<ut_type::TINYINT>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_int16()) {
                auto value = std::any_cast<ut_type::SMALLINT>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_int32()) {
                auto value = std::any_cast<ut_type::INT>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_int64()) {
                auto value = std::any_cast<ut_type::BIGINT>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_int128()) {
                auto value = std::any_cast<ut_type::LARGEINT>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_float64()) {
                auto value = std::any_cast<ut_type::DOUBLE>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_float64()) {
                auto value = std::any_cast<ut_type::DOUBLE>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_decimal128()) {
                auto value = std::any_cast<Decimal<Int128>>(data_set[j].first[i]);
                column->insert_data(reinterpret_cast<char*>(&value), 0);
            } else if (type.is_date_time()) {
                static std::string date_time_format("%Y-%m-%d %H:%i:%s");
                auto datetime_str = std::any_cast<std::string>(data_set[j].first[i]);
                VecDateTimeValue v;
                v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                                       datetime_str.c_str(), datetime_str.size());
                v.to_datetime();
                column->insert_data(reinterpret_cast<char*>(&v), 0);
            } else if (type.is_date()) {
                static std::string date_time_format("%Y-%m-%d");
                auto datetime_str = std::any_cast<std::string>(data_set[j].first[i]);
                VecDateTimeValue v;
                v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                                       datetime_str.c_str(), datetime_str.size());
                v.cast_to_date();
                column->insert_data(reinterpret_cast<char*>(&v), 0);
            } else if (type.is_array()) {
                auto v = std::any_cast<Array>(data_set[j].first[i]);
                column->insert(v);
            } else {
                LOG(WARNING) << "dataset not supported for TypeIndex:" << (int)type.idx;
                ASSERT_TRUE(false);
            }
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
            constant_col_ptrs.push_back(std::make_shared<ColumnPtrWrapper>(block.get_by_position(i).column));
            constant_cols.push_back(constant_col_ptrs.back().get());
        } else {
            constant_cols.push_back(nullptr);
        }
    }

    // 2. execute function
    auto return_type = nullable ? make_nullable(std::make_shared<ReturnType>())
                                : std::make_shared<ReturnType>();
    auto func = SimpleFunctionFactory::instance().get_function(func_name, block.get_columns_with_type_and_name(), return_type);
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
