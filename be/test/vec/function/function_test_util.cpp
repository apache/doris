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

namespace doris::vectorized {
int64_t str_to_data_time(std::string datetime_str, bool data_time) {
    VecDateTimeValue v;
    v.from_date_str(datetime_str.c_str(), datetime_str.size());
    if (data_time) { //bool data_time only to simplifly means data_time or data to cast, just use in time-functions uint test
        v.to_datetime();
    } else {
        v.cast_to_date();
    }
    return binary_cast<VecDateTimeValue, Int64>(v);
}
size_t type_index_to_data_type(const std::vector<std::any>& input_types, size_t index,
                               doris_udf::FunctionContext::TypeDesc& desc, DataTypePtr& type) {
    if (index < 0 || index >= input_types.size()) {
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
    for (size_t i = 0; i < input_types.size();) {
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
bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const std::any& cell) {
    if (cell.type() == typeid(Null)) {
        column->insert_data(nullptr, 0);
        return true;
    }

    WhichDataType type(type_ptr);
    if (type.is_string()) {
        auto str = std::any_cast<ut_type::STRING>(cell);
        column->insert_data(str.c_str(), str.size());
    } else if (type.idx == TypeIndex::BitMap) {
        BitmapValue* bitmap = std::any_cast<BitmapValue*>(cell);
        column->insert_data((char*)bitmap, sizeof(BitmapValue));
    } else if (type.is_int8()) {
        auto value = std::any_cast<ut_type::TINYINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int16()) {
        auto value = std::any_cast<ut_type::SMALLINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int32()) {
        auto value = std::any_cast<ut_type::INT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int64()) {
        auto value = std::any_cast<ut_type::BIGINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_int128()) {
        auto value = std::any_cast<ut_type::LARGEINT>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_float64()) {
        auto value = std::any_cast<ut_type::DOUBLE>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_float64()) {
        auto value = std::any_cast<ut_type::DOUBLE>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal128()) {
        auto value = std::any_cast<Decimal<Int128>>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_date_time()) {
        static std::string date_time_format("%Y-%m-%d %H:%i:%s");
        auto datetime_str = std::any_cast<std::string>(cell);
        VecDateTimeValue v;
        v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                               datetime_str.c_str(), datetime_str.size());
        v.to_datetime();
        column->insert_data(reinterpret_cast<char*>(&v), 0);
    } else if (type.is_date()) {
        static std::string date_time_format("%Y-%m-%d");
        auto datetime_str = std::any_cast<std::string>(cell);
        VecDateTimeValue v;
        v.from_date_format_str(date_time_format.c_str(), date_time_format.size(),
                               datetime_str.c_str(), datetime_str.size());
        v.cast_to_date();
        column->insert_data(reinterpret_cast<char*>(&v), 0);
    } else if (type.is_array()) {
        auto v = std::any_cast<Array>(cell);
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
    std::unique_ptr<Block> block(new Block());
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

    // process table function init
    if (fn->process_init(input_block) != Status::OK()) {
        LOG(WARNING) << "TableFunction process_init failed";
        return nullptr;
    }

    // prepare output column
    vectorized::MutableColumnPtr column = descs[0].data_type->create_column();

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

        bool tmp_eos = false;
        do {
            void* cell = nullptr;
            int64_t cell_len = 0;
            if (fn->get_value(&cell) != Status::OK() ||
                fn->get_value_length(&cell_len) != Status::OK()) {
                LOG(WARNING) << "TableFunction get_value or get_value_length failed";
                return nullptr;
            }

            // copy data from input block
            if (cell == nullptr) {
                column->insert_default();
            } else {
                column->insert_data(reinterpret_cast<char*>(cell), cell_len);
            }

            fn->forward(&tmp_eos);
        } while (!tmp_eos);
    }

    std::unique_ptr<Block> output_block(new Block());
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
