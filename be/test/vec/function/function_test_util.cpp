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

#include "common/status.h"
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
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/cast/cast_base.h"
#include "vec/functions/cast/cast_to_time_impl.hpp"
#include "vec/runtime/time_value.h"
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

    PrimitiveType tp;
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
        tp = any_cast<PrimitiveType>(input_types[index]);
    }

    switch (tp) {
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_STRING:
        type = std::make_shared<DataTypeString>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_JSONB:
        type = std::make_shared<DataTypeJsonb>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_BITMAP:
        type = std::make_shared<DataTypeBitMap>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_HLL:
        type = std::make_shared<DataTypeHLL>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_IPV4:
        type = std::make_shared<DataTypeIPv4>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_IPV6:
        type = std::make_shared<DataTypeIPv6>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_BOOLEAN:
        type = std::make_shared<DataTypeUInt8>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_TINYINT:
        type = std::make_shared<DataTypeInt8>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_SMALLINT:
        type = std::make_shared<DataTypeInt16>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_INT:
        type = std::make_shared<DataTypeInt32>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_BIGINT:
        type = std::make_shared<DataTypeInt64>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_LARGEINT:
        type = std::make_shared<DataTypeInt128>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_FLOAT:
        type = std::make_shared<DataTypeFloat32>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DOUBLE:
        type = std::make_shared<DataTypeFloat64>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DECIMALV2:
        type = std::make_shared<DataTypeDecimalV2>(27, 9, input_types[index].precision_or(27),
                                                   input_types[index].scale_or(9));
        desc = type;
        return 1;
    // for decimals in ut we set the default scale and precision. for more scales, we prefer test them in regression.
    case PrimitiveType::TYPE_DECIMAL32:
        type = std::make_shared<DataTypeDecimal32>(input_types[index].precision_or(9),
                                                   input_types[index].scale_or(5));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DECIMAL64:
        type = std::make_shared<DataTypeDecimal64>(input_types[index].precision_or(18),
                                                   input_types[index].scale_or(9));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DECIMAL128I:
        type = std::make_shared<DataTypeDecimal128>(input_types[index].precision_or(38),
                                                    input_types[index].scale_or(20));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DECIMAL256:
        type = std::make_shared<DataTypeDecimal256>(input_types[index].precision_or(76),
                                                    input_types[index].scale_or(40));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DATETIME:
        type = std::make_shared<DataTypeDateTime>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DATE:
        type = std::make_shared<DataTypeDate>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DATEV2:
        type = std::make_shared<DataTypeDateV2>();
        desc = type;
        return 1;
    case PrimitiveType::TYPE_DATETIMEV2:
        type = std::make_shared<DataTypeDateTimeV2>(input_types[index].scale_or(0));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_TIMEV2:
        type = std::make_shared<DataTypeTimeV2>(input_types[index].scale_or(0));
        desc = type;
        return 1;
    case PrimitiveType::TYPE_ARRAY: {
        ut_type::UTDataTypeDesc sub_desc;
        DataTypePtr sub_type = nullptr;
        // parse next type as inner type
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
    case PrimitiveType::TYPE_MAP: {
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
    case PrimitiveType::TYPE_STRUCT: {
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
    case PrimitiveType::TYPE_NULL: { // nested is next slot
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
        LOG(WARNING) << "not supported PrimitiveType:" << (int)tp;
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
                      << ", desc : " << desc.data_type->get_name() << std::endl;
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
bool insert_datetime_cell(MutableColumnPtr& column, DataTypePtr date_type_ptr, const AnyType& cell,
                          bool datetime_is_string_format) {
    bool result = true;
    date_cast::TypeToValueTypeV<DataType> date_value;
    //TODO: remove string format. only accept value input.
    if (datetime_is_string_format) {
        // accept cell of type string
        auto datetime_str = any_cast<std::string>(cell);

        if constexpr (std::is_same_v<DataType, DataTypeDateTimeV2>) {
            result = date_value.from_date_str(datetime_str.c_str(), datetime_str.size(),
                                              date_type_ptr->get_scale());
        } else {
            result = date_value.from_date_str(datetime_str.c_str(), datetime_str.size());
        }
    } else {
        date_value = any_cast<date_cast::TypeToValueTypeV<DataType>>(cell);
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

bool insert_array_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell,
                       bool datetime_is_string_format) {
    //NOLINTNEXTLINE(modernize-use-auto)
    std::vector<AnyType> origin_input_array = any_cast<TestArray>(cell);
    DataTypePtr sub_type = assert_cast<const DataTypeArray*>(type_ptr.get())->get_nested_type();
    MutableColumnPtr sub_column = sub_type->create_column();
    for (const auto& item : origin_input_array) {
        insert_cell(sub_column, sub_type, item, datetime_is_string_format);
    }

    Array field_vector; // derived from std::vector<Field>
    for (int i = 0; i < sub_column->size(); ++i) {
        field_vector.push_back((*sub_column)[i]);
    }

    column->insert(Field::create_field<TYPE_ARRAY>(field_vector));
    return true;
}

// NOLINTBEGIN(readability-function-size)
bool insert_cell(MutableColumnPtr& column, DataTypePtr type_ptr, const AnyType& cell,
                 bool datetime_is_string_format) {
    if (cell.type() == &typeid(Null)) {
        assert_cast<ColumnNullable*>(column.get())->insert_default();
        return true;
    }

#define RETURN_IF_FALSE(x) \
    if (UNLIKELY(!(x))) return false

    if (type_ptr->is_nullable()) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto col_type = remove_nullable(type_ptr);
        auto col = nullable_column->get_nested_column_ptr();
        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        bool ok = insert_cell(col, col_type, cell, datetime_is_string_format);
        nullmap_column->insert_value(ok ? 0 : 1);
    } else {
        auto type = type_ptr->get_primitive_type();
        switch (type) {
        case PrimitiveType::TYPE_STRING:
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR: {
            auto str = any_cast<ut_type::STRING>(cell);
            column->insert_data(str.c_str(), str.size());
            break;
        }
        case PrimitiveType::TYPE_JSONB: {
            auto str = any_cast<ut_type::STRING>(cell);
            JsonBinaryValue jsonb_val;
            auto st = jsonb_val.from_json_string(str);
            if (st.ok()) {
                column->insert_data(jsonb_val.value(), jsonb_val.size());
            } else {
                column->insert_default();
            }
            break;
        }
        case PrimitiveType::TYPE_BITMAP: {
            auto* bitmap = any_cast<BitmapValue*>(cell);
            column->insert_data((char*)bitmap, sizeof(BitmapValue));
            break;
        }
        case PrimitiveType::TYPE_HLL: {
            auto* hll = any_cast<HyperLogLog*>(cell);
            column->insert_data((char*)hll, sizeof(HyperLogLog));
            break;
        }
        case PrimitiveType::TYPE_IPV4: {
            auto value = any_cast<ut_type::IPV4>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_IPV6: {
            auto value = any_cast<ut_type::IPV6>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_BOOLEAN: {
            auto value = any_cast<ut_type::BOOLEAN>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_TINYINT: {
            auto value = any_cast<ut_type::TINYINT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_SMALLINT: {
            auto value = any_cast<ut_type::SMALLINT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_INT: {
            auto value = any_cast<ut_type::INT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_BIGINT: {
            auto value = any_cast<ut_type::BIGINT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_LARGEINT: {
            auto value = any_cast<ut_type::LARGEINT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_FLOAT: {
            auto value = any_cast<ut_type::FLOAT>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DOUBLE: {
            auto value = any_cast<ut_type::DOUBLE>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DECIMAL32: {
            auto value = any_cast<Decimal32>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DECIMAL64: {
            auto value = any_cast<Decimal64>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DECIMAL128I: {
            auto value = any_cast<Decimal128V3>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DECIMALV2: {
            auto value = any_cast<Decimal128V2>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DECIMAL256: {
            auto value = any_cast<Decimal256>(cell);
            column->insert_data(reinterpret_cast<char*>(&value), 0);
            break;
        }
        case PrimitiveType::TYPE_DATE: {
            RETURN_IF_FALSE((insert_datetime_cell<DataTypeDate>(column, type_ptr, cell,
                                                                datetime_is_string_format)));
            break;
        }
        case PrimitiveType::TYPE_DATEV2: {
            RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateV2>(column, type_ptr, cell,
                                                                  datetime_is_string_format)));
            break;
        }
        case PrimitiveType::TYPE_DATETIME: {
            RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateTime>(column, type_ptr, cell,
                                                                    datetime_is_string_format)));
            break;
        }
        case PrimitiveType::TYPE_DATETIMEV2: {
            RETURN_IF_FALSE((insert_datetime_cell<DataTypeDateTimeV2>(column, type_ptr, cell,
                                                                      datetime_is_string_format)));
            break;
        }
        case PrimitiveType::TYPE_TIMEV2: {
            TimeValue::TimeType time_value = 0;
            if (datetime_is_string_format) {
                auto value = any_cast<std::string>(cell);
                CastParameters params {.status = Status::OK(), .is_strict = false};
                RETURN_IF_FALSE(CastToTimeV2::from_string_strict_mode<false>(
                        StringRef {value}, time_value, nullptr, type_ptr->get_scale(), params));
            } else {
                time_value = any_cast<TimeValue::TimeType>(cell);
            }
            column->insert_data(reinterpret_cast<char*>(&time_value), 0);
            break;
        }
        case PrimitiveType::TYPE_ARRAY: {
            RETURN_IF_FALSE((insert_array_cell(column, type_ptr, cell, datetime_is_string_format)));
            break;
        }
        case PrimitiveType::TYPE_STRUCT: {
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
                RETURN_IF_FALSE(insert_cell(col, struct_type->get_element(i), field,
                                            datetime_is_string_format));
            }
            break;
        }
        default: {
            std::cerr << "dataset not supported for type:" << type_to_string(type);
            return false;
        }
        }
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
