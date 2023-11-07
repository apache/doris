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

#include "vec/function/function_test_util.h"

#include <glog/logging.h>
#include <opentelemetry/common/threadlocal.h>

#include <iostream>

#include "runtime/jsonb_value.h"
#include "runtime/runtime_state.h"
#include "util/binary_cast.hpp"
#include "util/bitmap_value.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
int64_t str_to_date_time(std::string datetime_str, bool data_time) {
    VecDateTimeValue v;
    v.from_date_str(datetime_str.c_str(), datetime_str.size());
    if (data_time) { //bool data_time only to simplify means data_time or data to cast, just use in time-functions uint test
        v.to_datetime();
    } else {
        v.cast_to_date();
    }
    return binary_cast<VecDateTimeValue, Int64>(v);
}

uint32_t str_to_date_v2(std::string datetime_str, std::string datetime_format) {
    DateV2Value<DateV2ValueType> v;
    v.from_date_format_str(datetime_format.c_str(), datetime_format.size(), datetime_str.c_str(),
                           datetime_str.size());
    return binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v);
}

uint64_t str_to_datetime_v2(std::string datetime_str, std::string datetime_format) {
    DateV2Value<DateTimeV2ValueType> v;
    v.from_date_format_str(datetime_format.c_str(), datetime_format.size(), datetime_str.c_str(),
                           datetime_str.size());
    return binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v);
}

size_t type_index_to_data_type(const std::vector<AnyType>& input_types, size_t index,
                               ut_type::UTDataTypeDesc& ut_desc, DataTypePtr& type) {
    doris::TypeDescriptor& desc = ut_desc.type_desc;
    if (index >= input_types.size()) {
        return -1;
    }

    TypeIndex tp;
    if (input_types[index].type() == &typeid(Consted)) {
        tp = any_cast<Consted>(input_types[index]).tp;
    } else if (input_types[index].type() == &typeid(ConstedNotnull)) {
        tp = any_cast<ConstedNotnull>(input_types[index]).tp;
        ut_desc.is_nullable = false;
    } else if (input_types[index].type() == &typeid(Nullable)) {
        tp = any_cast<Nullable>(input_types[index]).tp;
        ut_desc.is_nullable = true;
    } else if (input_types[index].type() == &typeid(Notnull)) {
        tp = any_cast<Notnull>(input_types[index]).tp;
        ut_desc.is_nullable = false;
    } else {
        tp = any_cast<TypeIndex>(input_types[index]);
    }

    switch (tp) {
    case TypeIndex::String:
        desc.type = doris::PrimitiveType::TYPE_STRING;
        type = std::make_shared<DataTypeString>();
        return 1;
    case TypeIndex::JSONB:
        desc.type = doris::PrimitiveType::TYPE_JSONB;
        type = std::make_shared<DataTypeJsonb>();
        return 1;
    case TypeIndex::BitMap:
        desc.type = doris::PrimitiveType::TYPE_OBJECT;
        type = std::make_shared<DataTypeBitMap>();
        return 1;
    case TypeIndex::UInt8:
        desc.type = doris::PrimitiveType::TYPE_BOOLEAN;
        type = std::make_shared<DataTypeUInt8>();
        return 1;
    case TypeIndex::Int8:
        desc.type = doris::PrimitiveType::TYPE_TINYINT;
        type = std::make_shared<DataTypeInt8>();
        return 1;
    case TypeIndex::Int16:
        desc.type = doris::PrimitiveType::TYPE_SMALLINT;
        type = std::make_shared<DataTypeInt16>();
        return 1;
    case TypeIndex::Int32:
        desc.type = doris::PrimitiveType::TYPE_INT;
        type = std::make_shared<DataTypeInt32>();
        return 1;
    case TypeIndex::Int64:
        desc.type = doris::PrimitiveType::TYPE_BIGINT;
        type = std::make_shared<DataTypeInt64>();
        return 1;
    case TypeIndex::Int128:
        desc.type = doris::PrimitiveType::TYPE_LARGEINT;
        type = std::make_shared<DataTypeInt128>();
        return 1;
    case TypeIndex::Float32:
        desc.type = doris::PrimitiveType::TYPE_FLOAT;
        type = std::make_shared<DataTypeFloat32>();
        return 1;
    case TypeIndex::Float64:
        desc.type = doris::PrimitiveType::TYPE_DOUBLE;
        type = std::make_shared<DataTypeFloat64>();
        return 1;
    case TypeIndex::Decimal128:
        desc.type = doris::PrimitiveType::TYPE_DECIMALV2;
        type = std::make_shared<DataTypeDecimal<Decimal128>>();
        return 1;
    case TypeIndex::DateTime:
        desc.type = doris::PrimitiveType::TYPE_DATETIME;
        type = std::make_shared<DataTypeDateTime>();
        return 1;
    case TypeIndex::Date:
        desc.type = doris::PrimitiveType::TYPE_DATE;
        type = std::make_shared<DataTypeDate>();
        return 1;
    case TypeIndex::DateV2:
        desc.type = doris::PrimitiveType::TYPE_DATEV2;
        type = std::make_shared<DataTypeDateV2>();
        return 1;
    case TypeIndex::DateTimeV2:
        desc.type = doris::PrimitiveType::TYPE_DATETIMEV2;
        type = std::make_shared<DataTypeDateTimeV2>();
        return 1;
    case TypeIndex::Array: {
        desc.type = doris::PrimitiveType::TYPE_ARRAY;
        ut_type::UTDataTypeDesc sub_desc;
        DataTypePtr sub_type = nullptr;
        ++index;
        size_t ret = type_index_to_data_type(input_types, index, sub_desc, sub_type);
        if (ret <= 0) {
            return ret;
        }
        desc.children.push_back(sub_desc.type_desc);
        type = std::make_shared<DataTypeArray>(sub_type);
        return ret + 1;
    }
    case TypeIndex::Nullable: {
        ++index;
        size_t ret = type_index_to_data_type(input_types, index, ut_desc, type);
        if (ret <= 0) {
            return ret;
        }
        ut_desc.is_nullable = true;
        type = make_nullable(type);
        return ret + 1;
    }
    default:
        LOG(WARNING) << "not supported TypeIndex:" << (int)tp;
        return 0;
    }
}
bool parse_ut_data_type(const std::vector<AnyType>& input_types, ut_type::UTDataTypeDescs& descs) {
    descs.clear();
    descs.reserve(input_types.size());
    for (size_t i = 0; i < input_types.size();) {
        ut_type::UTDataTypeDesc desc;
        if (input_types[i].type() == &typeid(Consted) ||
            input_types[i].type() == &typeid(ConstedNotnull)) {
            desc.is_const = true;
        }
        size_t res = type_index_to_data_type(input_types, i, desc, desc.data_type);
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

template <typename Date, TypeIndex type_index = TypeIndex::Nothing>
bool insert_date_cell(MutableColumnPtr& column, const std::string& format, const AnyType& cell) {
    auto datetime_str = any_cast<std::string>(cell);
    Date v;
    auto result = v.from_date_format_str(format.c_str(), format.size(), datetime_str.c_str(),
                                         datetime_str.size());
    if constexpr (type_index == TypeIndex::Date) {
        v.cast_to_date();
    } else if constexpr (type_index == TypeIndex::DateTime) {
        v.to_datetime();
    }
    if (result) {
        column->insert_data(reinterpret_cast<char*>(&v), 0);
    } else if (column->is_nullable()) {
        column->insert_data(nullptr, 0);
    } else {
        return false;
    }
    return true;
}

bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell) {
    if (cell.type() == &typeid(Null)) {
        column->insert_data(nullptr, 0);
        return true;
    }

#define RETURN_IF_FALSE(x) \
    if (UNLIKELY(!(x))) return false

    WhichDataType type(type_ptr);
    if (type.is_string()) {
        auto str = any_cast<ut_type::STRING>(cell);
        column->insert_data(str.c_str(), str.size());
    } else if (type.is_json()) {
        auto str = any_cast<ut_type::STRING>(cell);
        JsonBinaryValue jsonb_val(str.c_str(), str.size());
        column->insert_data(jsonb_val.value(), jsonb_val.size());
    } else if (type.idx == TypeIndex::BitMap) {
        BitmapValue* bitmap = any_cast<BitmapValue*>(cell);
        column->insert_data((char*)bitmap, sizeof(BitmapValue));
    } else if (type.is_uint8()) {
        auto value = any_cast<ut_type::BOOLEAN>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int8()) {
        auto value = any_cast<ut_type::TINYINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int16()) {
        auto value = any_cast<ut_type::SMALLINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int32()) {
        auto value = any_cast<ut_type::INT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int64()) {
        auto value = any_cast<ut_type::BIGINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int128()) {
        auto value = any_cast<ut_type::LARGEINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_float32()) {
        auto value = any_cast<ut_type::FLOAT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_float64()) {
        auto value = any_cast<ut_type::DOUBLE>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal128()) {
        auto value = any_cast<Decimal<Int128>>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_date_time()) {
        static std::string date_time_format("%Y-%m-%d %H:%i:%s");
        RETURN_IF_FALSE((insert_date_cell<VecDateTimeValue, TypeIndex::DateTime>(
                column, date_time_format, cell)));
    } else if (type.is_date()) {
        static std::string date_time_format("%Y-%m-%d");
        RETURN_IF_FALSE((insert_date_cell<VecDateTimeValue, TypeIndex::Date>(
                column, date_time_format, cell)));
    } else if (type.is_date_v2()) {
        static std::string date_time_format("%Y-%m-%d");
        RETURN_IF_FALSE(
                (insert_date_cell<DateV2Value<DateV2ValueType>>(column, date_time_format, cell)));
    } else if (type.is_date_time_v2()) {
        static std::string date_time_format("%Y-%m-%d %H:%i:%s.%f");
        RETURN_IF_FALSE((insert_date_cell<DateV2Value<DateTimeV2ValueType>>(
                column, date_time_format, cell)));
    } else if (type.is_array()) {
        auto v = any_cast<Array>(cell);
        column->insert(v);
    } else {
        LOG(WARNING) << "dataset not supported for TypeIndex:" << (int)type.idx;
        return false;
    }
    return true;
}

Block* create_block_from_inputset(const InputTypeSet& input_types, const InputDataSet& input_set) {
    // 1.0 create data type
    ut_type::UTDataTypeDescs descs;
    if (!parse_ut_data_type(input_types, descs)) {
        return nullptr;
    }

    // 1.1 insert data and create block
    auto row_size = input_set.size();
    std::unique_ptr<Block> block = Block::create_unique();
    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        auto column = desc.data_type->create_column();
        column->reserve(row_size);

        auto type_ptr = desc.data_type->is_nullable()
                                ? ((DataTypeNullable*)(desc.data_type.get()))->get_nested_type()
                                : desc.data_type;
        WhichDataType type(type_ptr);

        for (int j = 0; j < row_size; j++) {
            if (!insert_cell(column, type_ptr, input_set[j][i])) {
                return nullptr;
            }
        }

        if (desc.is_const) {
            column = ColumnConst::create(std::move(column), row_size);
        }
        block->insert({std::move(column), desc.data_type, desc.col_name});
    }
    return block.release();
}

Block* process_table_function(TableFunction* fn, Block* input_block,
                              const InputTypeSet& output_types) {
    // pasrse output data types
    ut_type::UTDataTypeDescs descs;
    if (!parse_ut_data_type(output_types, descs)) {
        return nullptr;
    }
    if (descs.size() != 1) {
        LOG(WARNING) << "Now table function test only support return one column";
        return nullptr;
    }

    RuntimeState runtime_state((TQueryGlobals()));
    // process table function init
    if (fn->process_init(input_block, &runtime_state) != Status::OK()) {
        LOG(WARNING) << "TableFunction process_init failed";
        return nullptr;
    }

    // prepare output column
    vectorized::MutableColumnPtr column = descs[0].data_type->create_column();
    if (column->is_nullable()) {
        fn->set_nullable();
    }

    // process table function for all rows
    for (size_t row = 0; row < input_block->rows(); ++row) {
        if (fn->process_row(row) != Status::OK()) {
            LOG(WARNING) << "TableFunction process_row failed";
            return nullptr;
        }

        // consider outer
        if (!fn->is_outer() && fn->current_empty()) {
            continue;
        }

        do {
            fn->get_value(column);
            static_cast<void>(fn->forward());
        } while (!fn->eos());
    }

    std::unique_ptr<Block> output_block = Block::create_unique();
    output_block->insert({std::move(column), descs[0].data_type, descs[0].col_name});
    return output_block.release();
}

void check_vec_table_function(TableFunction* fn, const InputTypeSet& input_types,
                              const InputDataSet& input_set, const InputTypeSet& output_types,
                              const InputDataSet& output_set) {
    std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
    EXPECT_TRUE(input_block != nullptr);

    std::unique_ptr<Block> expect_output_block(
            create_block_from_inputset(output_types, output_set));
    EXPECT_TRUE(expect_output_block != nullptr);

    std::unique_ptr<Block> real_output_block(
            process_table_function(fn, input_block.get(), output_types));
    EXPECT_TRUE(real_output_block != nullptr);

    // compare real_output_block with expect_output_block
    EXPECT_EQ(expect_output_block->columns(), real_output_block->columns());
    EXPECT_EQ(expect_output_block->rows(), real_output_block->rows());
    for (size_t col = 0; col < expect_output_block->columns(); ++col) {
        auto left_col = expect_output_block->get_by_position(col).column;
        auto right_col = real_output_block->get_by_position(col).column;
        for (size_t row = 0; row < expect_output_block->rows(); ++row) {
            EXPECT_EQ(left_col->compare_at(row, row, *right_col, 0), 0);
        }
    }
}

} // namespace doris::vectorized
