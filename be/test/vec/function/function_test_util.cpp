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
#include <gtest/gtest.h>

#include <iostream>

#include "runtime/jsonb_value.h"
#include "runtime/runtime_state.h"
#include "util/bitmap_value.h"
#include "util/datetype_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

// NOLINTBEGIN(readability-function-size)
// return consumed slots in input_types(for nested types it may greater than 1)
static size_t type_index_to_data_type(const std::vector<AnyType>& input_types, size_t index,
                                      ut_type::UTDataTypeDesc& ut_desc, DataTypePtr& type) {
    auto& desc = ut_desc.data_type;
    if (index >= input_types.size()) {
        return -1;
    }

    TypeIndex tp;
    // default is nullable
    if (input_types[index].type() == &typeid(Consted)) {
        tp = any_cast<Consted>(input_types[index]).tp;
        ut_desc.is_nullable = true;
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
        type = std::make_shared<DataTypeString>();
        desc = type;
        return 1;
    case TypeIndex::JSONB:
        type = std::make_shared<DataTypeJsonb>();
        desc = type;
        return 1;
    case TypeIndex::BitMap:
        type = std::make_shared<DataTypeBitMap>();
        desc = type;
        return 1;
    case TypeIndex::HLL:
        type = std::make_shared<DataTypeHLL>();
        desc = type;
        return 1;
    case TypeIndex::IPv4:
        type = std::make_shared<DataTypeIPv4>();
        desc = type;
        return 1;
    case TypeIndex::IPv6:
        type = std::make_shared<DataTypeIPv6>();
        desc = type;
        return 1;
    case TypeIndex::UInt8:
        type = std::make_shared<DataTypeUInt8>();
        desc = type;
        return 1;
    case TypeIndex::Int8:
        type = std::make_shared<DataTypeInt8>();
        desc = type;
        return 1;
    case TypeIndex::Int16:
        type = std::make_shared<DataTypeInt16>();
        desc = type;
        return 1;
    case TypeIndex::Int32:
        type = std::make_shared<DataTypeInt32>();
        desc = type;
        return 1;
    case TypeIndex::Int64:
        type = std::make_shared<DataTypeInt64>();
        desc = type;
        return 1;
    case TypeIndex::Int128:
        type = std::make_shared<DataTypeInt128>();
        desc = type;
        return 1;
    case TypeIndex::Float32:
        type = std::make_shared<DataTypeFloat32>();
        desc = type;
        return 1;
    case TypeIndex::Float64:
        type = std::make_shared<DataTypeFloat64>();
        desc = type;
        return 1;
    case TypeIndex::Decimal128V2:
        type = std::make_shared<DataTypeDecimal<Decimal128V2>>();
        desc = type;
        return 1;
    // for decimals in ut we set the default scale and precision. for more scales, we prefer test them in regression.
    case TypeIndex::Decimal32:
        type = std::make_shared<DataTypeDecimal<Decimal32>>(input_types[index].precision_or(9),
                                                            input_types[index].scale_or(5));
        desc = type;
        return 1;
    case TypeIndex::Decimal64:
        type = std::make_shared<DataTypeDecimal<Decimal64>>(input_types[index].precision_or(18),
                                                            input_types[index].scale_or(9));
        desc = type;
        return 1;
    case TypeIndex::Decimal128V3:
        type = std::make_shared<DataTypeDecimal<Decimal128V3>>(input_types[index].precision_or(38),
                                                               input_types[index].scale_or(20));
        desc = type;
        return 1;
    case TypeIndex::Decimal256:
        type = std::make_shared<DataTypeDecimal<Decimal256>>(input_types[index].precision_or(76),
                                                             input_types[index].scale_or(40));
        desc = type;
        return 1;
    case TypeIndex::DateTime:
        type = std::make_shared<DataTypeDateTime>();
        desc = type;
        return 1;
    case TypeIndex::Date:
        type = std::make_shared<DataTypeDate>();
        desc = type;
        return 1;
    case TypeIndex::DateV2:
        type = std::make_shared<DataTypeDateV2>();
        desc = type;
        return 1;
    case TypeIndex::DateTimeV2:
        type = std::make_shared<DataTypeDateTimeV2>(input_types[index].scale_or(0));
        desc = type;
        return 1;
    case TypeIndex::Array: {
        ut_type::UTDataTypeDesc sub_desc;
        DataTypePtr sub_type = nullptr;
        // parse next TypeIndex as inner type
        size_t ret = type_index_to_data_type(input_types, ++index, sub_desc, sub_type);
        if (ret <= 0) {
            return ret;
        }
        if (sub_desc.is_nullable) {
            sub_type = make_nullable(sub_type);
        }
        type = std::make_shared<DataTypeArray>(sub_type);
        desc = type;
        return ret + 1;
    }
    case TypeIndex::Map: {
        ut_type::UTDataTypeDesc key_desc;
        DataTypePtr key_type = nullptr;
        ut_type::UTDataTypeDesc value_desc;
        DataTypePtr value_type = nullptr;
        ++index;
        size_t ret = type_index_to_data_type(input_types, index, key_desc, key_type);
        if (ret <= 0) {
            return ret;
        }
        ++index;
        ret = type_index_to_data_type(input_types, index, value_desc, value_type);
        if (ret <= 0) {
            return ret;
        }
        if (key_desc.is_nullable) {
            key_type = make_nullable(key_type);
        }
        if (value_desc.is_nullable) {
            value_type = make_nullable(value_type);
        }
        type = std::make_shared<DataTypeMap>(key_type, value_type);
        desc = type;
        return ret + 1;
    }
    case TypeIndex::Struct: {
        ++index;
        size_t ret = 0;
        DataTypes sub_types;
        while (index < input_types.size()) {
            ut_type::UTDataTypeDesc sub_desc;
            DataTypePtr sub_type = nullptr;
            size_t inner_ret = type_index_to_data_type(input_types, index, sub_desc, sub_type);
            if (inner_ret <= 0) {
                return inner_ret;
            }
            ret += inner_ret;
            if (sub_desc.is_nullable) {
                sub_type = make_nullable(sub_type);
                sub_types.push_back(sub_type);
            }
            ++index;
        }
        type = std::make_shared<DataTypeStruct>(sub_types);
        desc = type;
        return ret + 1;
    }
    case TypeIndex::Nullable: { //TODO: use Nullable(T) to replace (Nullable, T)
        ++index;
        size_t ret = type_index_to_data_type(input_types, index, ut_desc, type);
        if (ret <= 0) {
            return ret;
        }
        ut_desc.is_nullable = true;
        type = make_nullable(type);
        desc = type;
        return ret + 1;
    }
    default:
        LOG(WARNING) << "not supported TypeIndex:" << (int)tp;
        return 0;
    }
}
// NOLINTEND(readability-function-size)

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
            std::cout << "return error, res:" << res << ", i:" << i
                      << ", input_types.size():" << input_types.size()
                      << "desc : " << desc.data_type->get_name() << std::endl;
            return false;
        }
        if (desc.is_nullable) {
            desc.data_type = make_nullable(desc.data_type);
        }
        desc.col_name = "k" + std::to_string(i);
        descs.emplace_back(desc);
        i += res;
    }
    return true;
}

template <typename DataType>
bool insert_datetime_cell(MutableColumnPtr& column, DataTypePtr date_type_ptr,
                          const AnyType& cell) {
    // accept cell of type string
    auto datetime_str = any_cast<std::string>(cell);
    date_cast::TypeToValueTypeV<DataType> date_value;

    bool result;
    if constexpr (std::is_same_v<DataType, DataTypeDateTimeV2>) {
        result = date_value.from_date_str(datetime_str.c_str(), datetime_str.size(),
                                          date_type_ptr->get_scale());
    } else {
        result = date_value.from_date_str(datetime_str.c_str(), datetime_str.size());
    }
    // deal for v1
    if constexpr (std::is_same_v<DataType, DataTypeDate>) {
        date_value.cast_to_date();
    } else if constexpr (std::is_same_v<DataType, DataTypeDateTime>) {
        date_value.to_datetime();
    }

    if (result) [[likely]] {
        column->insert_data(reinterpret_cast<char*>(&date_value), 0);
        return true;
    }
    // parse failed. insert null.
    column->insert_default();
    return false;
}

bool insert_array_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell) {
    //NOLINTNEXTLINE(modernize-use-auto)
    std::vector<AnyType> origin_input_array = any_cast<TestArray>(cell);
    DataTypePtr sub_type = assert_cast<const DataTypeArray*>(type_ptr.get())->get_nested_type();
    MutableColumnPtr sub_column = sub_type->create_column();
    for (const auto& item : origin_input_array) {
        insert_cell(sub_column, sub_type, item);
    }

    Array field_vector; // derived from std::vector<Field>
    for (int i = 0; i < sub_column->size(); ++i) {
        field_vector.push_back((*sub_column)[i]);
    }

    column->insert(field_vector);
    return true;
}

// NOLINTBEGIN(readability-function-size)
bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell) {
    if (cell.type() == &typeid(Null)) {
        assert_cast<ColumnNullable*>(column.get())->insert_default();
        return true;
    }

#define RETURN_IF_FALSE(x) \
    if (UNLIKELY(!(x))) return false

    WhichDataType type = type_ptr->get_type_id();
    if (type.is_string()) {
        auto str = any_cast<ut_type::STRING>(cell);
        column->insert_data(str.c_str(), str.size());
    } else if (type.is_json()) {
        auto str = any_cast<ut_type::STRING>(cell);
        JsonBinaryValue jsonb_val(str.c_str(), str.size());
        column->insert_data(jsonb_val.value(), jsonb_val.size());
    } else if (type.is_bitmap()) {
        auto* bitmap = any_cast<BitmapValue*>(cell);
        column->insert_data((char*)bitmap, sizeof(BitmapValue));
    } else if (type.is_hll()) {
        auto* hll = any_cast<HyperLogLog*>(cell);
        column->insert_data((char*)hll, sizeof(HyperLogLog));
    } else if (type.is_ipv4()) {
        auto value = any_cast<ut_type::IPV4>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_ipv6()) {
        auto value = any_cast<ut_type::IPV6>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
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
    } else if (type.is_decimal128v2()) {
        auto value = any_cast<Decimal128V2>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal32()) {
        auto value = any_cast<Decimal32>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal64()) {
        auto value = any_cast<Decimal64>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal128v3()) {
        auto value = any_cast<Decimal128V3>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_decimal256()) {
        auto value = any_cast<Decimal256>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_date_time()) {
        RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateTime>(column, type_ptr, cell)));
    } else if (type.is_date()) {
        RETURN_IF_FALSE((insert_datetime_cell<DataTypeDate>(column, type_ptr, cell)));
    } else if (type.is_date_v2()) {
        RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateV2>(column, type_ptr, cell)));
    } else if (type.is_date_time_v2()) {
        RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateTimeV2>(column, type_ptr, cell)));
    } else if (type.is_time_v2()) {
        auto value = any_cast<ut_type::DOUBLE>(cell);
        column->insert_data(reinterpret_cast<char*>(&value), 0);
    } else if (type.is_array()) {
        RETURN_IF_FALSE((insert_array_cell(column, type_ptr, cell)));
    } else if (type.is_struct()) {
        auto v = any_cast<InputCell>(cell);
        const auto* struct_type = assert_cast<const DataTypeStruct*>(type_ptr.get());
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto* struct_column =
                assert_cast<ColumnStruct*>(nullable_column->get_nested_column_ptr().get());
        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        nullmap_column->insert_default();
        for (size_t i = 0; i < v.size(); ++i) {
            auto& field = v[i];
            auto col = struct_column->get_column(i).get_ptr();
            RETURN_IF_FALSE(insert_cell(col, struct_type->get_element(i), field));
        }
    } else if (type.is_nullable()) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto col_type = remove_nullable(type_ptr);
        auto col = nullable_column->get_nested_column_ptr();
        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        bool ok = insert_cell(col, col_type, cell);
        nullmap_column->insert_value(ok ? 0 : 1);
    } else {
        std::cerr << "dataset not supported for TypeIndex:" << (int)type.idx;
        return false;
    }
    return true;
}
// NOLINTEND(readability-function-size)

// only for table function
static Block* create_block_from_inputset(const InputTypeSet& input_types,
                                         const InputDataSet& input_set) {
    // 1.0 create data type
    ut_type::UTDataTypeDescs descs;
    if (!parse_ut_data_type(input_types, descs)) {
        return nullptr;
    }

    // 1.1 insert data and create block
    auto row_size = input_set.size();
    std::unique_ptr<Block> block = Block::create_unique();

    auto input_set_size = input_set[0].size();
    // 1.2 calculate the input column size
    auto input_col_size = input_set_size / descs.size();

    for (size_t i = 0; i < descs.size(); ++i) {
        auto& desc = descs[i];
        for (size_t j = 0; j < input_col_size; ++j) {
            auto column = desc.data_type->create_column();
            column->reserve(row_size);

            auto type_ptr = desc.data_type->is_nullable()
                                    ? ((DataTypeNullable*)(desc.data_type.get()))->get_nested_type()
                                    : desc.data_type;
            WhichDataType type(type_ptr);

            for (int r = 0; r < row_size; r++) {
                if (!insert_cell(column, type_ptr, input_set[r][i * input_col_size + j])) {
                    return nullptr;
                }
            }

            if (desc.is_const) {
                column = ColumnConst::create(std::move(column), row_size);
            }
            block->insert({std::move(column), desc.data_type, desc.col_name});
        }
    }
    return block.release();
}

static Block* process_table_function(TableFunction* fn, Block* input_block,
                                     const InputTypeSet& output_types, bool test_get_value_func) {
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
        fn->process_row(row);

        // consider outer
        if (!fn->is_outer() && fn->current_empty()) {
            continue;
        }

        do {
            if (test_get_value_func) {
                fn->get_value(column, 10);
            } else {
                fn->get_same_many_values(column, 1);
                fn->forward();
            }
        } while (!fn->eos());
    }

    std::unique_ptr<Block> output_block = Block::create_unique();
    output_block->insert({std::move(column), descs[0].data_type, descs[0].col_name});
    return output_block.release();
}

void check_vec_table_function(TableFunction* fn, const InputTypeSet& input_types,
                              const InputDataSet& input_set, const InputTypeSet& output_types,
                              const InputDataSet& output_set, const bool test_get_value_func) {
    std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
    EXPECT_TRUE(input_block != nullptr);

    std::unique_ptr<Block> expect_output_block(
            create_block_from_inputset(output_types, output_set));
    EXPECT_TRUE(expect_output_block != nullptr);

    std::unique_ptr<Block> real_output_block(
            process_table_function(fn, input_block.get(), output_types, test_get_value_func));
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
