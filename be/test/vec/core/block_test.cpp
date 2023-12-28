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

#include "vec/core/block.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <cmath>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "gen_cpp/data.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "util/bitmap_value.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

using vectorized::Int32;

void block_to_pb(
        const vectorized::Block& block, PBlock* pblock,
        segment_v2::CompressionTypePB compression_type = segment_v2::CompressionTypePB::SNAPPY) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    Status st = block.serialize(BeExecVersionManager::get_newest_version(), pblock,
                                &uncompressed_bytes, &compressed_bytes, compression_type);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
    EXPECT_EQ(compressed_bytes, pblock->column_values().size());

    const vectorized::ColumnWithTypeAndName& type_and_name =
            block.get_columns_with_type_and_name()[0];
    EXPECT_EQ(type_and_name.name, pblock->column_metas()[0].name());
}

void fill_block_with_array_int(vectorized::Block& block) {
    auto off_column = vectorized::ColumnVector<vectorized::ColumnArray::Offset64>::create();
    auto data_column = vectorized::ColumnVector<int32_t>::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_int(std::move(column_array_ptr), array_type,
                                                     "test_array_int");
    block.insert(test_array_int);
}

void fill_block_with_array_string(vectorized::Block& block) {
    auto off_column = vectorized::ColumnVector<vectorized::ColumnArray::Offset64>::create();
    auto data_column = vectorized::ColumnString::create();
    // init column array with [["abc","de"],["fg"],[], [""]];
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "de", "fg", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_string(std::move(column_array_ptr), array_type,
                                                        "test_array_string");
    block.insert(test_array_string);
}

void serialize_and_deserialize_test(segment_v2::CompressionTypePB compression_type) {
    // int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                        "test_string");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                              decimal_column.get())
                             ->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = i * pow(10, 9) + i * pow(10, 8);
            data.push_back(value);
        }
        vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                        decimal_data_type, "test_decimal");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnComplexType<BitmapValue>*)bitmap_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        vectorized::ColumnWithTypeAndName type_and_name(bitmap_column->get_ptr(), bitmap_data_type,
                                                        "test_bitmap");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // nullable string
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(),
                                                        nullable_data_type, "test_nullable");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(
                nullable_column->get_ptr(), nullable_data_type, "test_nullable_decimal");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        EXPECT_EQ(1, pblock.column_metas_size());
        EXPECT_TRUE(pblock.column_metas()[0].has_decimal_param());
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // int with 4096 batch size
    {
        auto column_vector_int32 = vectorized::ColumnVector<Int32>::create();
        auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
        auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
        for (int i = 0; i < 4096; i++) {
            mutable_nullable_vector->insert(vectorized::cast_to_nearest_field_type(i));
        }
        auto data_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(mutable_nullable_vector->get_ptr(),
                                                        data_type, "test_nullable_int32");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // array int and array string
    {
        vectorized::Block block;
        fill_block_with_array_int(block);
        fill_block_with_array_string(block);
        PBlock pblock;
        block_to_pb(block, &pblock, compression_type);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, compression_type);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
}

TEST(BlockTest, SerializeAndDeserializeBlock) {
    serialize_and_deserialize_test(segment_v2::CompressionTypePB::SNAPPY);
    serialize_and_deserialize_test(segment_v2::CompressionTypePB::LZ4);
}

TEST(BlockTest, dump_data) {
    auto vec = vectorized::ColumnVector<Int32>::create();
    auto& int32_data = vec->get_data();
    for (int i = 0; i < 1024; ++i) {
        int32_data.push_back(i);
    }
    vectorized::DataTypePtr int32_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName test_int(vec->get_ptr(), int32_type, "test_int");

    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < 1024; ++i) {
        std::string is = std::to_string(i);
        strcol->insert_data(is.c_str(), is.size());
    }
    vectorized::DataTypePtr string_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName test_string(strcol->get_ptr(), string_type, "test_string");

    vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
    auto decimal_column = decimal_data_type->create_column();
    auto& decimal_data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                                  decimal_column.get())
                                 ->get_data();
    for (int i = 0; i < 1024; ++i) {
        __int128_t value = i * pow(10, 9) + i * pow(10, 8);
        decimal_data.push_back(value);
    }
    vectorized::ColumnWithTypeAndName test_decimal(decimal_column->get_ptr(), decimal_data_type,
                                                   "test_decimal");

    auto column_vector_int32 = vectorized::ColumnVector<Int32>::create();
    auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
    auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
    for (int i = 0; i < 4096; i++) {
        mutable_nullable_vector->insert(vectorized::cast_to_nearest_field_type(i));
    }
    auto nint32_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName test_nullable_int32(mutable_nullable_vector->get_ptr(),
                                                          nint32_type, "test_nullable_int32");

    auto column_vector_date = vectorized::ColumnVector<vectorized::Int64>::create();
    auto& date_data = column_vector_date->get_data();
    for (int i = 0; i < 1024; ++i) {
        VecDateTimeValue value;
        value.from_date_int64(20210501);
        date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
    vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(), date_type,
                                                "test_date");

    auto column_vector_datetime = vectorized::ColumnVector<vectorized::Int64>::create();
    auto& datetime_data = column_vector_datetime->get_data();
    for (int i = 0; i < 1024; ++i) {
        VecDateTimeValue value;
        value.from_date_int64(20210501080910);
        datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr datetime_type(std::make_shared<vectorized::DataTypeDateTime>());
    vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(),
                                                    datetime_type, "test_datetime");

    auto column_vector_date_v2 = vectorized::ColumnVector<vectorized::UInt32>::create();
    auto& date_v2_data = column_vector_date_v2->get_data();
    for (int i = 0; i < 1024; ++i) {
        DateV2Value<DateV2ValueType> value;
        value.from_date((uint32_t)((2022 << 9) | (6 << 5) | 6));
        date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
    }
    vectorized::DataTypePtr date_v2_type(std::make_shared<vectorized::DataTypeDateV2>());
    vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(), date_v2_type,
                                                   "test_datev2");

    vectorized::Block block({test_int, test_string, test_decimal, test_nullable_int32, test_date,
                             test_datetime, test_date_v2});
    EXPECT_GT(block.dump_data().size(), 1);

    // test dump array int and array string
    vectorized::Block block1;
    fill_block_with_array_int(block1);
    fill_block_with_array_string(block1);
    // Note: here we should set 'row_num' in dump_data
    EXPECT_GT(block1.dump_data(10).size(), 1);

    vectorized::IColumn::Filter filter;
    int size = block1.rows() / 2;
    for (int i = 0; i < block1.rows(); i++) {
        filter.push_back(i % 2);
    }
    vectorized::Block::filter_block_internal(&block1, filter, block1.columns());
    EXPECT_EQ(size, block1.rows());
}

TEST(BlockTest, merge_with_shared_columns) {
    auto vec = vectorized::ColumnVector<Int32>::create();
    auto& int32_data = vec->get_data();
    for (int i = 0; i < 1024; ++i) {
        int32_data.push_back(i);
    }
    vectorized::DataTypePtr int32_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName test_k1(vec->get_ptr(), int32_type, "k1");

    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < 1024; ++i) {
        std::string is = std::to_string(i);
        strcol->insert_data(is.c_str(), is.size());
    }
    vectorized::DataTypePtr string_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName test_v1(strcol->get_ptr(), string_type, "v1");

    vectorized::ColumnWithTypeAndName test_v2(strcol->get_ptr(), string_type, "v2");

    vectorized::Block src_block({test_k1, test_v1, test_v2});

    auto vec_temp = vectorized::ColumnVector<Int32>::create();
    auto& int32_data_temp = vec_temp->get_data();
    for (int i = 0; i < 10; ++i) {
        int32_data_temp.push_back(i);
    }

    vectorized::ColumnWithTypeAndName test_k1_temp(vec_temp->get_ptr(), int32_type, "k1");

    auto strcol_temp = vectorized::ColumnString::create();
    for (int i = 0; i < 10; ++i) {
        std::string is = std::to_string(i);
        strcol_temp->insert_data(is.c_str(), is.size());
    }

    vectorized::ColumnWithTypeAndName test_v1_temp(strcol_temp->get_ptr(), string_type, "v1");
    vectorized::ColumnWithTypeAndName test_v2_temp(strcol_temp->get_ptr(), string_type, "v2");

    vectorized::Block temp_block({test_k1_temp, test_v1_temp, test_v2_temp});

    vectorized::MutableBlock mutable_block(&src_block);
    auto status = mutable_block.merge(temp_block);
    ASSERT_TRUE(status.ok());

    src_block.set_columns(std::move(mutable_block.mutable_columns()));

    for (auto& column : src_block.get_columns()) {
        EXPECT_EQ(1034, column->size());
    }
    EXPECT_EQ(1034, src_block.rows());
}

} // namespace doris
