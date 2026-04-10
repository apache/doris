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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/aggregate/aggregate_function_reader_first_last.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

class VAggReplaceTest : public testing::Test {
public:
    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_replace_reader_load(factory);
    }

    void TearDown() {}

    template <typename DataType, bool nullable>
    void check_column_basic(const IColumn* column, int64_t expect_num, size_t pos = 0) {
        //expect basic column[pos]=expect_num
        EXPECT_FALSE(column->is_null_at(pos));
        auto* unwrap_col = column;
        if constexpr (nullable) {
            auto* nullable_col = assert_cast<const ColumnNullable*>(unwrap_col);
            EXPECT_FALSE(nullable_col->is_null_at(pos));
            unwrap_col = nullable_col->get_nested_column_ptr().get();
        }
        if constexpr (std::is_same_v<DataType, DataTypeString>) {
            auto str = unwrap_col->get_data_at(pos).to_string();
            EXPECT_EQ("item" + std::to_string(expect_num), str);
        } else if constexpr (std::is_same_v<DataType, DataTypeBitMap>) {
            auto& container = assert_cast<const ColumnBitmap*>(unwrap_col)->get_data();
            auto& bitmap = container[pos];
            EXPECT_TRUE(bitmap.contains(static_cast<uint64_t>(expect_num)));
        } else if constexpr (std::is_same_v<DataType, DataTypeHLL>) {
            auto& container = assert_cast<const ColumnHLL*>(unwrap_col)->get_data();
            auto& hll = container[pos];
            auto expect = hll.estimate_cardinality();
            const_cast<HyperLogLog*>(&hll)->update(static_cast<uint64_t>(expect_num));
            auto actual = hll.estimate_cardinality();
            EXPECT_EQ(expect, actual);
        } else if constexpr (std::is_same_v<DataType, DataTypeDecimalV2>) {
            EXPECT_EQ(expect_num, (*(int128_t*)unwrap_col->get_data_at(pos).data));
        } else if constexpr (IsDataTypeDecimal<DataType>) {
            EXPECT_EQ(expect_num,
                      (*(typename DataType::FieldType::NativeType*)unwrap_col->get_data_at(pos)
                                .data));
        } else if constexpr (is_date_or_datetime(DataType::PType)) {
            EXPECT_EQ(expect_num, (*(int64_t*)unwrap_col->get_data_at(pos).data));
        } else if constexpr (DataType::PType == TYPE_DATEV2) {
            EXPECT_EQ(expect_num, (*(uint32_t*)unwrap_col->get_data_at(pos).data));
        } else if constexpr (DataType::PType == TYPE_DATETIMEV2 ||
                             DataType::PType == TYPE_TIMESTAMPTZ) {
            EXPECT_EQ(expect_num, (*(uint64_t*)unwrap_col->get_data_at(pos).data));
        } else {
            EXPECT_EQ(expect_num, unwrap_col->get_int(pos));
        }
    }

    template <typename DataType, bool nullable>
    void check_column_array(const IColumn* column, int32_t expect_num) {
        //expect array column:[[0..expect_num-1]]
        EXPECT_EQ(column->size(), 1);
        auto* unwrap_col = column;
        if constexpr (nullable) {
            auto* nullable_col = assert_cast<const ColumnNullable*>(unwrap_col);
            EXPECT_FALSE(nullable_col->is_null_at(0));
            unwrap_col = nullable_col->get_nested_column_ptr().get();
        }
        auto* array_col = assert_cast<const ColumnArray*>(unwrap_col);
        EXPECT_EQ(array_col->get_offsets()[0], expect_num);
        auto* data_col = array_col->get_data_ptr().get();
        EXPECT_EQ(data_col->size(), expect_num);
        for (size_t i = 0; i < expect_num; ++i) {
            check_column_basic<DataType, nullable>(data_col, i, i);
        }
    }

    template <typename DataType, bool nullable>
    void add_elements(MutableColumnPtr& input_col, size_t input_nums) {
        //fill column: [0..input_nums-1]
        using FieldType = typename DataType::FieldType;
        Field field;
        for (size_t i = 0; i < input_nums; ++i) {
            if constexpr (std::is_same_v<DataType, DataTypeString>) {
                auto item = std::string("item") + std::to_string(i);
                input_col->insert_data(item.c_str(), item.size());
                EXPECT_EQ(item, input_col->get_data_at(i).to_string());
            } else if constexpr (std::is_same_v<DataType, DataTypeBitMap>) {
                BitmapValue bitmap;
                bitmap.add(i);
                input_col->insert_data(reinterpret_cast<const char*>(&bitmap), sizeof(bitmap));
            } else if constexpr (std::is_same_v<DataType, DataTypeHLL>) {
                HyperLogLog hll;
                hll.update(i);
                input_col->insert_data(reinterpret_cast<const char*>(&hll), sizeof(hll));
            } else if constexpr (std::is_same_v<DataType, DataTypeDateV2>) {
                auto item = static_cast<uint32_t>(i);
                input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
            } else if constexpr (std::is_same_v<DataType, DataTypeDateTimeV2>) {
                auto item = static_cast<uint64_t>(i);
                input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
            } else if constexpr (std::is_same_v<DataType, DataTypeDateTime> ||
                                 std::is_same_v<DataType, DataTypeDate>) {
                auto item = static_cast<int64_t>(i);
                input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
            } else {
                auto item = FieldType(static_cast<uint64_t>(i));
                input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
            }
        }
        EXPECT_EQ(input_col->size(), input_nums);
    }

    template <typename DataType, bool nullable>
    void agg_replace_add_elements(MutableColumnPtr& input_col, AggregateFunctionPtr agg_function,
                                  AggregateDataPtr place, size_t input_nums) {
        //fill column: [0..input_nums-1]
        add_elements<DataType, nullable>(input_col, input_nums);
        const IColumn* column[1] = {input_col.get()};
        for (int i = 0; i < input_col->size(); i++) {
            agg_function->add(place, column, i, _agg_arena_pool);
        }
    }

    template <typename DataType, bool nullable>
    void array_add_elements(MutableColumnPtr& input_col, size_t input_nums) {
        //fill array column: [[],[0],[0,1]..[0..input_nums-1]]
        using FieldType = typename DataType::FieldType;
        Field field;
        for (int32_t i = 0; i <= input_nums; ++i) {
            doris::Array array(i);
            for (int32_t j = 0; j < i; ++j) {
                if constexpr (std::is_same_v<DataType, DataTypeString>) {
                    auto item = std::string("item") + std::to_string(j);
                    array[j] = Field::create_field<TYPE_STRING>(item);
                } else if constexpr (IsDecimalNumber<FieldType>) {
                    auto item = FieldType(static_cast<uint64_t>(j));
                    array[j] = Field::create_field<TYPE_DECIMALV2>(
                            *(typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType*)&item);
                } else if (is_date_or_datetime(DataType::PType)) {
                    auto v = static_cast<int64_t>(j);
                    array[j] = Field::create_field<DataType::PType>(
                            *(typename PrimitiveTypeTraits<DataType::PType>::CppType*)&v);
                } else if (DataType::PType == TYPE_TIMESTAMPTZ ||
                           DataType::PType == TYPE_DATETIMEV2) {
                    auto v = static_cast<uint64_t>(j);
                    array[j] = Field::create_field<DataType::PType>(
                            *(typename PrimitiveTypeTraits<DataType::PType>::CppType*)&v);
                } else if (DataType::PType == TYPE_DATEV2) {
                    auto v = static_cast<uint32_t>(j);
                    array[j] = Field::create_field<DataType::PType>(
                            *(typename PrimitiveTypeTraits<DataType::PType>::CppType*)&v);
                } else if (DataType::PType == TYPE_LARGEINT) {
                    auto v = static_cast<__int128>(j);
                    array[j] = Field::create_field<DataType::PType>(
                            *(typename PrimitiveTypeTraits<DataType::PType>::CppType*)&v);
                } else {
                    auto v = static_cast<uint64_t>(j);
                    array[j] = Field::create_field<DataType::PType>(
                            *(typename PrimitiveTypeTraits<DataType::PType>::CppType*)&v);
                }
            }
            input_col->insert(Field::create_field<TYPE_ARRAY>(array));
        }

        EXPECT_EQ(input_col->size(), input_nums + 1);
    }

    template <typename DataType, bool nullable>
    void agg_replace_array_add_elements(MutableColumnPtr& input_col,
                                        AggregateFunctionPtr agg_function, AggregateDataPtr place,
                                        size_t input_nums) {
        //fill array column: [[],[0],[0,1]..[0..input_nums-1]]
        array_add_elements<DataType, nullable>(input_col, input_nums);
        const IColumn* column[1] = {input_col.get()};
        for (size_t i = 0; i < input_col->size(); ++i) {
            agg_function->add(place, column, i, _agg_arena_pool);
        }
    }

    template <typename DataType, bool nullable, bool array>
    DataTypePtr get_data_type() {
        DataTypePtr data_type = get_basic_type<DataType>();
        if constexpr (nullable) {
            data_type = std::make_shared<DataTypeNullable>(data_type);
        }
        if constexpr (array) {
            data_type = std::make_shared<DataTypeArray>(data_type);
            if constexpr (nullable) {
                data_type = std::make_shared<DataTypeNullable>(data_type);
            }
        }
        return data_type;
    }

    template <typename DataType>
    DataTypePtr get_basic_type() {
        using FieldType = typename DataType::FieldType;
        if constexpr (IsDecimalNumber<FieldType>) {
            //decimal column get_int will return (data * scale), so let scale be 1.
            return std::make_shared<DataType>(27, 1);
        }
        return std::make_shared<DataType>();
    }

    template <typename DataType, bool nullable>
    void test_agg_replace(const std::string& fn_name, size_t input_nums, size_t expect_num) {
        DataTypePtr data_type = get_data_type<DataType, nullable, false>();
        DataTypes data_types = {data_type};
        LOG(INFO) << "test_agg_replace for " << fn_name << "(" << data_types[0]->get_name() << ")";
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get(fn_name, data_types, nullptr, nullable, -1);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);
        //EXPECT_EQ(3, 0);

        auto data_column = data_type->create_column();
        agg_replace_add_elements<DataType, nullable>(data_column, agg_function, place, input_nums);

        //EXPECT_EQ(4, 0);
        auto column_result = data_type->create_column();
        agg_function->insert_result_into(place, *column_result);
        check_column_basic<DataType, nullable>(column_result.get(), expect_num);
        agg_function->destroy(place);
    }

    template <typename DataType, bool nullable>
    void test_agg_array_replace(const std::string& fn_name, size_t input_nums, size_t expect_num) {
        DataTypePtr data_type = get_data_type<DataType, nullable, true>();
        DataTypes data_types = {data_type};
        LOG(INFO) << "test_agg_replace for " << fn_name << "(" << data_types[0]->get_name() << ")";
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get(fn_name, data_types, nullptr, nullable, -1);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        auto input_column = data_type->create_column();
        agg_replace_array_add_elements<DataType, nullable>(input_column, agg_function, place,
                                                           input_nums);

        auto column_result = data_type->create_column();
        agg_function->insert_result_into(place, *column_result);
        check_column_array<DataType, nullable>(column_result.get(), expect_num);

        agg_function->destroy(place);
    }

    template <typename DataType, typename ColumnType, bool nullable>
    void test_basic_data(int8_t input_nums) {
        DataTypePtr data_type = get_data_type<DataType, nullable, false>();

        auto data_column = data_type->create_column();
        add_elements<DataType, nullable>(data_column, input_nums);

        EXPECT_EQ(input_nums, data_column->size());
        //test Value
        {
            Value<nullable> value;
            EXPECT_TRUE(value.is_null());
            for (int64_t i = 0; i < input_nums; ++i) {
                value.set_value(data_column.get(), i);
                EXPECT_FALSE(value.is_null());
                auto to_column = data_type->create_column();
                if constexpr (nullable) {
                    auto& nullable_col = assert_cast<ColumnNullable&>(*to_column);
                    value.insert_into(nullable_col.get_nested_column());
                } else {
                    value.insert_into(*to_column);
                }

                EXPECT_EQ(1, to_column->size());
                check_column_basic<DataType, nullable>(to_column.get(), i);
            }
        }
        //test CopiedValue
        {
            CopiedValue<nullable> value;
            EXPECT_TRUE(value.is_null());
            for (int64_t i = 0; i < input_nums; ++i) {
                value.template set_value<ColumnType>(data_column.get(), i);
                EXPECT_FALSE(value.is_null());
                auto to_column = data_type->create_column();
                if constexpr (nullable) {
                    auto& nullable_col = assert_cast<ColumnNullable&>(*to_column);
                    value.template insert_into<ColumnType>(nullable_col.get_nested_column());
                } else {
                    value.template insert_into<ColumnType>(*to_column);
                }
                EXPECT_EQ(1, to_column->size());
                check_column_basic<DataType, nullable>(to_column.get(), i);
            }
        }
    }

    template <typename DataType, typename ColumnType, bool nullable>
    void test_array_data(int8_t input_nums) {
        DataTypePtr data_type = get_data_type<DataType, nullable, true>();

        auto data_column = data_type->create_column();
        array_add_elements<DataType, nullable>(data_column, input_nums);

        EXPECT_EQ(input_nums + 1, data_column->size());
        //test Value
        {
            Value<nullable> value;
            EXPECT_TRUE(value.is_null());
            for (int64_t i = 0; i <= input_nums; ++i) {
                value.set_value(data_column.get(), i);
                EXPECT_FALSE(value.is_null());
                auto to_column = data_type->create_column();
                if constexpr (nullable) {
                    auto& nullable_col = assert_cast<ColumnNullable&>(*to_column);
                    value.insert_into(nullable_col.get_nested_column());
                } else {
                    value.insert_into(*to_column);
                }
                EXPECT_EQ(1, to_column->size());
                check_column_array<DataType, nullable>(to_column.get(), i);
            }
        }
        //test CopiedValue
        {
            CopiedValue<nullable> value;
            EXPECT_TRUE(value.is_null());
            for (int64_t i = 0; i <= input_nums; ++i) {
                value.template set_value<ColumnArray>(data_column.get(), i);
                EXPECT_FALSE(value.is_null());
                auto to_column = data_type->create_column();
                if constexpr (nullable) {
                    auto& nullable_col = assert_cast<ColumnNullable&>(*to_column);
                    value.template insert_into<ColumnArray>(nullable_col.get_nested_column());
                } else {
                    value.template insert_into<ColumnArray>(*to_column);
                }
                EXPECT_EQ(1, to_column->size());
                check_column_array<DataType, nullable>(to_column.get(), i);
            }
        }
    }

private:
    Arena _agg_arena_pool;
};

TEST_F(VAggReplaceTest, test_basic_data) {
    test_basic_data<DataTypeInt8, ColumnInt8, false>(11);
    test_basic_data<DataTypeInt16, ColumnInt16, false>(11);
    test_basic_data<DataTypeInt32, ColumnInt32, false>(11);
    test_basic_data<DataTypeInt64, ColumnInt64, false>(11);
    test_basic_data<DataTypeInt128, ColumnInt128, false>(11);
    test_basic_data<DataTypeDecimalV2, ColumnDecimal128V2, false>(11);
    test_basic_data<DataTypeString, ColumnString, false>(11);
    test_basic_data<DataTypeInt128, ColumnInt128, false>(11);
    test_basic_data<DataTypeDate, ColumnDate, false>(11);
    test_basic_data<DataTypeDateTime, ColumnDateTime, false>(11);
}

TEST_F(VAggReplaceTest, test_array_data) {
    test_array_data<DataTypeInt8, ColumnArray, false>(11);
    test_array_data<DataTypeInt16, ColumnArray, false>(11);
    test_array_data<DataTypeInt32, ColumnArray, false>(11);
    test_array_data<DataTypeInt64, ColumnArray, false>(11);
    test_array_data<DataTypeInt128, ColumnArray, false>(11);
    test_array_data<DataTypeDecimalV2, ColumnArray, false>(11);
    test_array_data<DataTypeString, ColumnArray, false>(11);
    test_array_data<DataTypeInt128, ColumnArray, false>(11);
    test_array_data<DataTypeDate, ColumnArray, false>(11);
    test_array_data<DataTypeDateTime, ColumnArray, false>(11);
}

TEST_F(VAggReplaceTest, test_basic_replace_reader) {
    test_agg_replace<DataTypeInt8, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt16, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt32, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt64, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt128, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDecimalV2, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeString, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDate, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDateTime, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDateTime, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeBitMap, false>("replace_reader", 10, 0);
    test_agg_replace<DataTypeHLL, false>("replace_reader", 10, 0);

    test_agg_replace<DataTypeInt8, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt16, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt32, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt64, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeInt128, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDecimalV2, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeString, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDate, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeDateTime, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeBitMap, true>("replace_reader", 10, 0);
    test_agg_replace<DataTypeHLL, true>("replace_reader", 10, 0);
}

TEST_F(VAggReplaceTest, test_basic_replace_load) {
    test_agg_replace<DataTypeInt8, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt16, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt32, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt64, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt128, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeDecimalV2, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeString, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeDate, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeDateTime, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeBitMap, false>("replace_load", 10, 9);
    test_agg_replace<DataTypeHLL, false>("replace_load", 10, 9);

    test_agg_replace<DataTypeInt8, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt16, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt32, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt64, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeInt128, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeDecimalV2, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeString, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeDate, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeDateTime, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeBitMap, true>("replace_load", 10, 9);
    test_agg_replace<DataTypeHLL, true>("replace_load", 10, 9);
}

TEST_F(VAggReplaceTest, test_array_replace_reader) {
    test_agg_array_replace<DataTypeInt8, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt16, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt32, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt64, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt128, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDecimalV2, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeString, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDate, false>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDateTime, false>("replace_reader", 10, 0);

    test_agg_array_replace<DataTypeInt8, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt16, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt32, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt64, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeInt128, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDecimalV2, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeString, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDate, true>("replace_reader", 10, 0);
    test_agg_array_replace<DataTypeDateTime, true>("replace_reader", 10, 0);
}

TEST_F(VAggReplaceTest, test_array_replace_load) {
    test_agg_array_replace<DataTypeInt8, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt16, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt32, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt64, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt128, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDecimalV2, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeString, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDate, false>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDateTime, false>("replace_load", 10, 10);

    test_agg_array_replace<DataTypeInt8, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt16, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt32, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt64, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeInt128, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDecimalV2, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeString, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDate, true>("replace_load", 10, 10);
    test_agg_array_replace<DataTypeDateTime, true>("replace_load", 10, 10);
}

} // namespace doris
