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
#include <gtest/gtest-death-test.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
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
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
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
    // const column maybe uncompressed_bytes<compressed_bytes
    // as the serialize_bytes add some additional byets: STREAMVBYTE_PADDING=16;
    // EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
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
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
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
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
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

void serialize_and_deserialize_test_one() {
    // const int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        data.push_back(111);
        auto const_column = vectorized::ColumnConst::create(vec->get_ptr(), 1);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), data_type,
                                                        "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}

void serialize_and_deserialize_test_int() {
    // const int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        data.push_back(111);
        auto const_column = vectorized::ColumnConst::create(vec->get_ptr(), 10);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), data_type,
                                                        "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }

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
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}
void serialize_and_deserialize_test_long() {
    // const long
    {
        auto vec = vectorized::ColumnVector<int64>::create();
        auto& data = vec->get_data();
        data.push_back(111);
        auto const_column = vectorized::ColumnConst::create(vec->get_ptr(), 10);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt64>());
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), data_type,
                                                        "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }

    // long
    {
        auto vec = vectorized::ColumnVector<int64>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt64>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}
void serialize_and_deserialize_test_string() {
    // const_string
    {
        auto strcol = vectorized::ColumnString::create();
        std::string val = "doris";
        strcol->insert_data(val.c_str(), val.size());
        auto const_column = vectorized::ColumnConst::create(strcol->get_ptr(), 10);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), data_type,
                                                        "test_string");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::SNAPPY);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
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
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::SNAPPY);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}

void serialize_and_deserialize_test_nullable() {
    // nullable(const int)
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        data.push_back(111);
        auto nullable_column = vectorized::make_nullable(vec->get_ptr());
        auto const_column = vectorized::ColumnConst::create(nullable_column, 10);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        auto nullable_data_type = vectorized::make_nullable(data_type);
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), nullable_data_type,
                                                        "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }

    // nullable(int)
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(make_nullable(vec->get_ptr()),
                                                        make_nullable(data_type), "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }

    // nullable(const_string)
    {
        auto strcol = vectorized::ColumnString::create();
        std::string val = "doris";
        strcol->insert_data(val.c_str(), val.size());
        auto const_column = vectorized::ColumnConst::create(strcol->get_ptr(), 10);
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(make_nullable(const_column->get_ptr()),
                                                        make_nullable(data_type), "test_string");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::SNAPPY);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // nullable(string)
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(make_nullable(strcol->get_ptr()),
                                                        make_nullable(data_type), "test_string");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::SNAPPY);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}

void serialize_and_deserialize_test_decimal() {
    // const decimal
    {
        auto vec = vectorized::ColumnDecimal32::create(0, 3);
        vectorized::Decimal<int> value = 111234;
        vec->insert_value(value);
        auto const_column = vectorized::ColumnConst::create(vec->get_ptr(), 10);
        vectorized::DataTypePtr data_type(
                std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal32>>(6, 3));
        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), data_type,
                                                        "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }

    // decimal
    {
        auto vec = vectorized::ColumnDecimal32::create(0, 3);
        for (int i = 0; i < 1024; ++i) {
            vectorized::Decimal<int> value = 111000 + i;
            vec->insert_value(value);
        }
        vectorized::DataTypePtr data_type(
                std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal32>>(6, 3));
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(block.dump_data(), block2.dump_data());
        EXPECT_EQ(s1, s2);
    }
}

void serialize_and_deserialize_test_bitmap() {
    // const bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
        BitmapValue bv;
        for (int j = 0; j <= 2; ++j) {
            bv.add(j);
        }
        container.push_back(bv);
        auto const_column = vectorized::ColumnConst::create(bitmap_column->get_ptr(), 10);

        vectorized::ColumnWithTypeAndName type_and_name(const_column->get_ptr(), bitmap_data_type,
                                                        "test_bitmap");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();
        std::string bb1 = block.dump_data(0, 1024);
        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        std::string bb2 = block2.dump_data(0, 1024);
        EXPECT_EQ(bb1, bb2);
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }

    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
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
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::LZ4);
        std::string s1 = pblock.DebugString();
        std::string bb1 = block.dump_data(0, 1024);
        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        std::string bb2 = block2.dump_data(0, 1024);
        EXPECT_EQ(bb1, bb2);
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::LZ4);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
}

void serialize_and_deserialize_test_array() {
    // array int and array string
    {
        vectorized::Block block;
        fill_block_with_array_int(block);
        fill_block_with_array_string(block);
        PBlock pblock;
        block_to_pb(block, &pblock, segment_v2::CompressionTypePB::SNAPPY);
        std::string s1 = pblock.DebugString();
        vectorized::Block block2;
        static_cast<void>(block2.deserialize(pblock));
        PBlock pblock2;
        block_to_pb(block2, &pblock2, segment_v2::CompressionTypePB::SNAPPY);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
}

TEST(BlockTest, Constructor) {
    // Test empty block constructor
    {
        vectorized::Block block;
        EXPECT_EQ(0, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_TRUE(block.empty());
    }

    // Test constructor with regular Int32 columns
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::Block block({{col->get_ptr(), type, "col1"}, {col->get_ptr(), type, "col2"}});
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_TRUE(block.empty());
    }

    // Test constructor with const column
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        col->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(col->get_ptr(), 10);
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::Block block({{const_col->get_ptr(), type, "const_col"}});
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(10, block.rows());
    }

    // Test constructor with nullable column
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        auto nullable_col = vectorized::make_nullable(col->get_ptr());
        auto nullable_type =
                vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::Block block({{nullable_col, nullable_type, "nullable_col"}});
        EXPECT_EQ(1, block.columns());
    }

    // Test constructor with mixed column types
    {
        vectorized::ColumnsWithTypeAndName columns;

        // Regular column
        auto regular_col = vectorized::ColumnVector<Int32>::create();
        auto regular_type = std::make_shared<vectorized::DataTypeInt32>();
        columns.emplace_back(regular_col->get_ptr(), regular_type, "regular_col");

        // Const column
        auto const_base = vectorized::ColumnVector<Int32>::create();
        const_base->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(const_base->get_ptr(), 10);
        columns.emplace_back(const_col->get_ptr(), regular_type, "const_col");

        // Nullable column
        auto nullable_col = vectorized::make_nullable(regular_col->get_ptr());
        auto nullable_type = vectorized::make_nullable(regular_type);
        columns.emplace_back(nullable_col, nullable_type, "nullable_col");

        vectorized::Block block(columns);
        EXPECT_EQ(3, block.columns());
    }

    // Test constructor with empty columns
    {
        vectorized::ColumnsWithTypeAndName columns;
        vectorized::Block block(columns);
        EXPECT_EQ(0, block.columns());
        EXPECT_TRUE(block.empty());
    }

    // Test constructor with nullptr column (should handle gracefully)
    {
        auto type = std::make_shared<vectorized::DataTypeInt32>();
        vectorized::Block block({{nullptr, type, "null_col"}});
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(0, block.rows());
    }
}

TEST(BlockTest, BasicOperations) {
    // Test with empty block
    {
        vectorized::Block empty_block;
        EXPECT_NO_THROW(empty_block.clear());
        EXPECT_NO_THROW(empty_block.clear_names());
        EXPECT_NO_THROW(empty_block.reserve(0));
        EXPECT_DEATH(empty_block.erase(0), "Block is empty");
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto col2 = vectorized::ColumnVector<Int32>::create();
        auto col3 = vectorized::ColumnVector<Int32>::create();
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());

        // Test reserve with different sizes
        EXPECT_NO_THROW(block.reserve(0));
        EXPECT_NO_THROW(block.reserve(100));
        block.reserve(3);
        block.insert({col1->get_ptr(), type, "col1"});
        block.insert({col2->get_ptr(), type, "col2"});
        block.insert({col3->get_ptr(), type, "col3"});
        EXPECT_EQ(3, block.columns());

        // Test clear_names
        block.clear_names();
        EXPECT_EQ("", block.get_by_position(0).name);
        EXPECT_EQ("", block.get_by_position(1).name);
        EXPECT_EQ("", block.get_by_position(2).name);

        // Test clear
        block.clear();
        EXPECT_EQ(0, block.columns());
        EXPECT_TRUE(block.empty());

        // Test insert operations
        // Insert at end
        block.insert({col1->get_ptr(), type, "col1"});
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ("col1", block.get_by_position(0).name);

        // Insert duplicate name
        block.insert({col2->get_ptr(), type, "col1"});
        EXPECT_EQ(2, block.columns());

        // Insert at specific position
        block.insert(0, {col3->get_ptr(), type, "col0"});
        EXPECT_EQ(3, block.columns());

        // Insert at invalid position
        EXPECT_THROW(block.insert(10, {col3->get_ptr(), type, "col3"}), Exception);

        // Insert nullptr column
        EXPECT_NO_THROW(block.insert({nullptr, type, "null_col"}));

        // Test erase operations
        // Erase by position
        block.erase(0);
        EXPECT_EQ(3, block.columns());

        // Erase by name
        block.erase("col1");
        EXPECT_EQ(2, block.columns());

        // Erase set of positions
        std::set<size_t> positions = {0};
        block.erase(positions);
        EXPECT_EQ(1, block.columns());

        // Erase by invalid name
        EXPECT_THROW(block.erase("non_existent"), Exception);

        // Erase by invalid position
        EXPECT_DEATH(block.erase(10), "Position out of bound in Block::erase");

        // Erase with erase_not_in
        std::vector<int> empty_vec;
        EXPECT_NO_THROW(block.erase_not_in(empty_vec));
        EXPECT_EQ(0, block.columns());
    }

    // Test with const columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());

        // Create multiple const columns
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 10);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        auto base_col3 = vectorized::ColumnVector<Int32>::create();
        base_col3->insert_value(33);
        auto const_col3 = vectorized::ColumnConst::create(base_col3->get_ptr(), 8);
        block.insert({const_col3->get_ptr(), type, "const_col3"});

        EXPECT_EQ(3, block.columns());
        EXPECT_EQ(10, block.rows());

        // Test clear_names with const columns
        block.clear_names();
        EXPECT_EQ("", block.get_by_position(0).name);
        EXPECT_EQ("", block.get_by_position(1).name);
        EXPECT_EQ("", block.get_by_position(2).name);
        EXPECT_EQ(3, block.columns());
        EXPECT_EQ(10, block.rows());

        // Test clear with const columns
        block.clear();
        EXPECT_EQ(0, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_TRUE(block.empty());

        // Test insert operations
        // Insert at end
        block.insert({const_col1->get_ptr(), type, "const_col1"});
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(10, block.rows());
        EXPECT_EQ("const_col1", block.get_by_position(0).name);

        // Insert duplicate name
        block.insert({const_col2->get_ptr(), type, "const_col1"});
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(10, block.rows());

        // Insert at specific position
        block.insert(1, {const_col3->get_ptr(), type, "const_col3"});
        EXPECT_EQ(3, block.columns());
        EXPECT_EQ("const_col3", block.get_by_position(1).name);

        // Insert at invalid position
        EXPECT_THROW(block.insert(10, {const_col1->get_ptr(), type, "invalid"}), Exception);

        // Insert nullptr column
        EXPECT_NO_THROW(block.insert({nullptr, type, "null_col"}));

        // Test erase operations
        // Erase by position
        block.erase(0);
        EXPECT_EQ(3, block.columns());

        // Erase by name
        block.erase("const_col3");
        EXPECT_EQ(2, block.columns());

        // Erase set of positions
        std::set<size_t> positions = {0};
        block.erase(positions);
        EXPECT_EQ(1, block.columns());

        // Erase by invalid name
        EXPECT_THROW(block.erase("non_existent"), Exception);

        // Erase by invalid position
        EXPECT_DEATH(block.erase(10), "Position out of bound in Block::erase");

        // Erase with erase_not_in
        std::vector<int> empty_vec;
        EXPECT_NO_THROW(block.erase_not_in(empty_vec));
        EXPECT_EQ(0, block.columns());
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr base_type(std::make_shared<vectorized::DataTypeInt32>());
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create multiple nullable columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto nullable_col1 = vectorized::make_nullable(col1->get_ptr());
        block.insert({nullable_col1, nullable_type, "nullable_col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        auto nullable_col2 = vectorized::make_nullable(col2->get_ptr());
        block.insert({nullable_col2, nullable_type, "nullable_col2"});

        auto col3 = vectorized::ColumnVector<Int32>::create();
        auto nullable_col3 = vectorized::make_nullable(col3->get_ptr());
        block.insert({nullable_col3, nullable_type, "nullable_col3"});

        EXPECT_EQ(3, block.columns());

        // Test clear_names with nullable columns
        block.clear_names();
        EXPECT_EQ("", block.get_by_position(0).name);
        EXPECT_EQ("", block.get_by_position(1).name);
        EXPECT_EQ("", block.get_by_position(2).name);
        EXPECT_EQ(3, block.columns());

        // Test clear with nullable columns
        block.clear();
        EXPECT_EQ(0, block.columns());
        EXPECT_TRUE(block.empty());

        // Test insert operations
        // Insert at end
        block.insert({nullable_col1, nullable_type, "nullable_col1"});
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ("nullable_col1", block.get_by_position(0).name);

        // Insert duplicate name
        block.insert({nullable_col2, nullable_type, "nullable_col1"});
        EXPECT_EQ(2, block.columns());

        // Insert at specific position
        block.insert(1, {nullable_col3, nullable_type, "nullable_col3"});
        EXPECT_EQ(3, block.columns());
        EXPECT_EQ("nullable_col3", block.get_by_position(1).name);

        // Insert at invalid position
        EXPECT_THROW(block.insert(10, {nullable_col1, nullable_type, "invalid"}), Exception);

        // Insert nullptr column
        EXPECT_NO_THROW(block.insert({nullptr, nullable_type, "null_col"}));

        // Test erase operations
        // Erase by position
        block.erase(0);
        EXPECT_EQ(3, block.columns());

        // Erase by name
        block.erase("nullable_col3");
        EXPECT_EQ(2, block.columns());

        // Erase set of positions
        std::set<size_t> positions = {0};
        block.erase(positions);
        EXPECT_EQ(1, block.columns());

        // Erase by invalid name
        EXPECT_THROW(block.erase("non_existent"), Exception);

        // Erase by invalid position
        EXPECT_DEATH(block.erase(10), "Position out of bound in Block::erase");

        // Erase with erase_not_in
        std::vector<int> empty_vec;
        EXPECT_NO_THROW(block.erase_not_in(empty_vec));
        EXPECT_EQ(0, block.columns());
    }
}

TEST(BlockTest, ColumnOperations) {
    // Test with empty block
    {
        vectorized::Block empty_block;

        // Test get operations with empty block
        EXPECT_DEATH(empty_block.get_by_position(0), "");
        EXPECT_THROW(empty_block.safe_get_by_position(0), Exception);
        EXPECT_THROW(empty_block.get_by_name("non_existent"), Exception);
        EXPECT_EQ(nullptr, empty_block.try_get_by_name("non_existent"));

        // Test has
        EXPECT_FALSE(empty_block.has("non_existent"));

        // Test get_position_by_name
        EXPECT_THROW(empty_block.get_position_by_name("non_existent"), Exception);

        // Test get_names
        auto names = empty_block.get_names();
        EXPECT_EQ(0, names.size());

        // Test get_data_types
        auto types = empty_block.get_data_types();
        EXPECT_EQ(0, types.size());

        // Test replace_by_position
        auto col = vectorized::ColumnVector<Int32>::create();
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());
        EXPECT_DEATH(empty_block.replace_by_position(0, col->get_ptr()), "");

        // Test replace_by_position_if_const
        EXPECT_DEATH(empty_block.replace_by_position_if_const(0), "");

        // Test get_columns_with_type_and_name
        const auto& columns = empty_block.get_columns_with_type_and_name();
        EXPECT_EQ(0, columns.size());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto col2 = vectorized::ColumnVector<Int32>::create();
        auto col3 = vectorized::ColumnVector<Int32>::create();
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());

        // Setup test data
        block.insert({col1->get_ptr(), type, "col1"});
        block.insert({col2->get_ptr(), type, "col2"});
        block.insert({col3->get_ptr(), type, "col3"});

        // Test get_by_position
        EXPECT_EQ("col1", block.get_by_position(0).name);
        EXPECT_EQ("col2", block.get_by_position(1).name);
        EXPECT_EQ("col3", block.get_by_position(2).name);
        EXPECT_DEATH(block.get_by_position(3), "");

        // Test safe_get_by_position
        EXPECT_EQ("col1", block.safe_get_by_position(0).name);
        EXPECT_THROW(block.safe_get_by_position(10), Exception);

        // Test get_by_name
        EXPECT_EQ("col1", block.get_by_name("col1").name);
        EXPECT_THROW(block.get_by_name("non_existent"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test has
        EXPECT_TRUE(block.has("col1"));
        EXPECT_FALSE(block.has("non_existent"));

        // Test get_position_by_name
        EXPECT_EQ(0, block.get_position_by_name("col1"));
        EXPECT_EQ(1, block.get_position_by_name("col2"));
        EXPECT_THROW(block.get_position_by_name("non_existent"), Exception);

        // Test get_names
        auto names = block.get_names();
        EXPECT_EQ(3, names.size());
        EXPECT_EQ("col1", names[0]);
        EXPECT_EQ("col2", names[1]);
        EXPECT_EQ("col3", names[2]);

        // Test get_data_type
        EXPECT_EQ(type, block.get_data_type(0));
        EXPECT_EQ(type, block.get_data_type(1));
        EXPECT_EQ(type, block.get_data_type(2));

        // Test get_data_types
        auto types = block.get_data_types();
        EXPECT_EQ(3, types.size());
        for (const auto& t : types) {
            EXPECT_EQ(type, t);
        }

        // Test replace_by_position
        auto new_col = vectorized::ColumnVector<Int32>::create();
        block.replace_by_position(0, new_col->get_ptr());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_DEATH(block.replace_by_position(10, new_col->get_ptr()), "");

        // Test replace_by_position_if_const
        auto const_col = vectorized::ColumnVector<Int32>::create();
        const_col->insert_value(1);
        auto const_column = vectorized::ColumnConst::create(const_col->get_ptr(), 1);
        block.replace_by_position(2, const_column->get_ptr());

        // Verify it's const column before replacement
        EXPECT_NE(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                   block.get_by_position(2).column.get()));

        // Replace const column with full column
        block.replace_by_position_if_const(2);
        EXPECT_DEATH(block.replace_by_position_if_const(10), "");

        // Verify it's no longer const column after replacement
        EXPECT_EQ(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                   block.get_by_position(2).column.get()));

        // Test get_columns_with_type_and_name
        const auto& columns = block.get_columns_with_type_and_name();
        EXPECT_EQ(3, columns.size());
        EXPECT_EQ("col1", columns[0].name);
        EXPECT_EQ("col2", columns[1].name);
        EXPECT_EQ("col3", columns[2].name);
    }

    // Test with const columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());

        // Create and insert const columns
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 10);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        // Test get_by_position
        EXPECT_EQ("const_col1", block.get_by_position(0).name);
        EXPECT_EQ("const_col2", block.get_by_position(1).name);
        EXPECT_DEATH(block.get_by_position(2), "");

        // Test safe_get_by_position
        EXPECT_EQ("const_col1", block.safe_get_by_position(0).name);
        EXPECT_THROW(block.safe_get_by_position(10), Exception);

        // Test get_by_name
        EXPECT_EQ("const_col1", block.get_by_name("const_col1").name);
        EXPECT_THROW(block.get_by_name("non_existent"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("const_col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test has
        EXPECT_TRUE(block.has("const_col1"));
        EXPECT_FALSE(block.has("non_existent"));

        // Test get_position_by_name
        EXPECT_EQ(0, block.get_position_by_name("const_col1"));
        EXPECT_EQ(1, block.get_position_by_name("const_col2"));
        EXPECT_THROW(block.get_position_by_name("non_existent"), Exception);

        // Test get_names
        auto names = block.get_names();
        EXPECT_EQ(2, names.size());
        EXPECT_EQ("const_col1", names[0]);
        EXPECT_EQ("const_col2", names[1]);

        // Test get_data_type
        EXPECT_EQ(type, block.get_data_type(0));
        EXPECT_EQ(type, block.get_data_type(1));

        // Test get_data_types
        auto types = block.get_data_types();
        EXPECT_EQ(2, types.size());
        for (const auto& t : types) {
            EXPECT_EQ(type, t);
        }

        // Test replace_by_position
        auto new_const_col = vectorized::ColumnVector<Int32>::create();
        new_const_col->insert_value(100);
        auto new_const = vectorized::ColumnConst::create(new_const_col->get_ptr(), 10);
        block.replace_by_position(0, new_const->get_ptr());
        EXPECT_EQ(10, block.get_by_position(0).column->size());
        EXPECT_DEATH(block.replace_by_position(10, new_const->get_ptr()), "");

        // Test replace_by_position_if_const
        block.replace_by_position_if_const(0);
        EXPECT_EQ(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                   block.get_by_position(0).column.get()));
        EXPECT_DEATH(block.replace_by_position_if_const(10), "");

        // Test get_columns_with_type_and_name
        const auto& columns = block.get_columns_with_type_and_name();
        EXPECT_EQ(2, columns.size());
        EXPECT_EQ("const_col1", columns[0].name);
        EXPECT_EQ("const_col2", columns[1].name);
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr base_type(std::make_shared<vectorized::DataTypeInt32>());
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create and insert nullable columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto nullable_col1 = vectorized::make_nullable(col1->get_ptr());
        block.insert({nullable_col1, nullable_type, "nullable_col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        auto nullable_col2 = vectorized::make_nullable(col2->get_ptr());
        block.insert({nullable_col2, nullable_type, "nullable_col2"});

        // Test get_by_position
        EXPECT_EQ("nullable_col1", block.get_by_position(0).name);
        EXPECT_EQ("nullable_col2", block.get_by_position(1).name);
        EXPECT_DEATH(block.get_by_position(2), "");

        // Test safe_get_by_position
        EXPECT_EQ("nullable_col1", block.safe_get_by_position(0).name);
        EXPECT_THROW(block.safe_get_by_position(10), Exception);

        // Test get_by_name
        EXPECT_EQ("nullable_col1", block.get_by_name("nullable_col1").name);
        EXPECT_THROW(block.get_by_name("non_existent"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("nullable_col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test has
        EXPECT_TRUE(block.has("nullable_col1"));
        EXPECT_FALSE(block.has("non_existent"));

        // Test get_position_by_name
        EXPECT_EQ(0, block.get_position_by_name("nullable_col1"));
        EXPECT_EQ(1, block.get_position_by_name("nullable_col2"));
        EXPECT_THROW(block.get_position_by_name("non_existent"), Exception);

        // Test get_names
        auto names = block.get_names();
        EXPECT_EQ(2, names.size());
        EXPECT_EQ("nullable_col1", names[0]);
        EXPECT_EQ("nullable_col2", names[1]);

        // Test get_data_type
        EXPECT_EQ(nullable_type, block.get_data_type(0));
        EXPECT_EQ(nullable_type, block.get_data_type(1));

        // Test get_data_types
        auto types = block.get_data_types();
        EXPECT_EQ(2, types.size());
        for (const auto& t : types) {
            EXPECT_EQ(nullable_type, t);
        }

        // Test replace_by_position
        auto new_col = vectorized::ColumnVector<Int32>::create();
        auto new_nullable = vectorized::make_nullable(new_col->get_ptr());
        block.replace_by_position(0, new_nullable);
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_DEATH(block.replace_by_position(10, new_nullable), "");

        // Test replace_by_position_if_const
        block.replace_by_position_if_const(0);
        EXPECT_NE(nullptr, typeid_cast<const vectorized::ColumnNullable*>(
                                   block.get_by_position(0).column.get()));
        EXPECT_DEATH(block.replace_by_position_if_const(10), "");

        // Test get_columns_with_type_and_name
        const auto& columns = block.get_columns_with_type_and_name();
        EXPECT_EQ(2, columns.size());
        EXPECT_EQ("nullable_col1", columns[0].name);
        EXPECT_EQ("nullable_col2", columns[1].name);
    }
}

TEST(BlockTest, SortColumns) {
    // Test sort_columns with empty block
    {
        vectorized::Block empty_block;
        auto sorted_empty = empty_block.sort_columns();
        EXPECT_EQ(0, sorted_empty.columns());
        EXPECT_EQ(0, sorted_empty.rows());
    }

    // Test sort_columns with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Insert columns in random order
        auto col_c = vectorized::ColumnVector<Int32>::create();
        col_c->insert_value(1);
        block.insert({col_c->get_ptr(), type, "c"});

        auto col_a = vectorized::ColumnVector<Int32>::create();
        col_a->insert_value(2);
        block.insert({col_a->get_ptr(), type, "a"});

        auto col_b = vectorized::ColumnVector<Int32>::create();
        col_b->insert_value(3);
        block.insert({col_b->get_ptr(), type, "b"});

        // Sort and verify
        auto sorted_block = block.sort_columns();
        auto sorted_names = sorted_block.get_names();
        EXPECT_EQ("c", sorted_names[0]);
        EXPECT_EQ("b", sorted_names[1]);
        EXPECT_EQ("a", sorted_names[2]);

        // Verify data is preserved
        EXPECT_EQ(1, sorted_block.get_by_position(0).column->get_int(0));
        EXPECT_EQ(3, sorted_block.get_by_position(1).column->get_int(0));
        EXPECT_EQ(2, sorted_block.get_by_position(2).column->get_int(0));
    }

    // Test sort_columns with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create and insert const columns in random order
        auto base_c = vectorized::ColumnVector<Int32>::create();
        base_c->insert_value(42);
        auto const_c = vectorized::ColumnConst::create(base_c->get_ptr(), 10);
        block.insert({const_c->get_ptr(), type, "c"});

        auto base_a = vectorized::ColumnVector<Int32>::create();
        base_a->insert_value(24);
        auto const_a = vectorized::ColumnConst::create(base_a->get_ptr(), 10);
        block.insert({const_a->get_ptr(), type, "a"});

        auto base_b = vectorized::ColumnVector<Int32>::create();
        base_b->insert_value(33);
        auto const_b = vectorized::ColumnConst::create(base_b->get_ptr(), 10);
        block.insert({const_b->get_ptr(), type, "b"});

        // Sort and verify
        auto sorted_block = block.sort_columns();
        auto sorted_names = sorted_block.get_names();
        EXPECT_EQ("c", sorted_names[0]);
        EXPECT_EQ("b", sorted_names[1]);
        EXPECT_EQ("a", sorted_names[2]);

        // Verify const values are preserved
        EXPECT_EQ(42, sorted_block.get_by_position(0).column->get_int(0));
        EXPECT_EQ(33, sorted_block.get_by_position(1).column->get_int(0));
        EXPECT_EQ(24, sorted_block.get_by_position(2).column->get_int(0));

        // Verify columns remain const
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_NE(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                       sorted_block.get_by_position(i).column.get()));
        }
    }

    // Test sort_columns with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create and insert nullable columns in random order
        auto col_c = vectorized::ColumnVector<Int32>::create();
        col_c->insert_value(1);
        auto nullable_c = vectorized::make_nullable(col_c->get_ptr());
        block.insert({nullable_c, nullable_type, "c"});

        auto col_a = vectorized::ColumnVector<Int32>::create();
        col_a->insert_value(2);
        auto nullable_a = vectorized::make_nullable(col_a->get_ptr());
        block.insert({nullable_a, nullable_type, "a"});

        auto col_b = vectorized::ColumnVector<Int32>::create();
        col_b->insert_value(3);
        auto nullable_b = vectorized::make_nullable(col_b->get_ptr());
        block.insert({nullable_b, nullable_type, "b"});

        // Sort and verify
        auto sorted_block = block.sort_columns();
        auto sorted_names = sorted_block.get_names();
        EXPECT_EQ("c", sorted_names[0]);
        EXPECT_EQ("b", sorted_names[1]);
        EXPECT_EQ("a", sorted_names[2]);

        // Verify nullable status is preserved
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_TRUE(sorted_block.get_by_position(i).type->is_nullable());
        }
    }

    // Test sort_columns with mixed column types
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Insert regular column
        auto regular_col = vectorized::ColumnVector<Int32>::create();
        regular_col->insert_value(1);
        block.insert({regular_col->get_ptr(), base_type, "c"});

        // Insert const column
        auto const_base = vectorized::ColumnVector<Int32>::create();
        const_base->insert_value(2);
        auto const_col = vectorized::ColumnConst::create(const_base->get_ptr(), 1);
        block.insert({const_col->get_ptr(), base_type, "a"});

        // Insert nullable column
        auto nullable_base = vectorized::ColumnVector<Int32>::create();
        nullable_base->insert_value(3);
        auto nullable_col = vectorized::make_nullable(nullable_base->get_ptr());
        block.insert({nullable_col, nullable_type, "b"});

        // Sort and verify
        auto sorted_block = block.sort_columns();
        auto sorted_names = sorted_block.get_names();
        EXPECT_EQ("c", sorted_names[0]);
        EXPECT_EQ("b", sorted_names[1]);
        EXPECT_EQ("a", sorted_names[2]);

        // Verify column types are preserved
        EXPECT_EQ(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                   sorted_block.get_by_position(0).column.get()));
        EXPECT_TRUE(sorted_block.get_by_position(1).type->is_nullable());
        EXPECT_NE(nullptr, typeid_cast<const vectorized::ColumnConst*>(
                                   sorted_block.get_by_position(2).column.get()));
    }
}

TEST(BlockTest, RowOperations) {
    // Test empty block
    {
        vectorized::Block empty_block;
        EXPECT_EQ(0, empty_block.rows());
        EXPECT_EQ(0, empty_block.columns());
        EXPECT_TRUE(empty_block.empty());
        EXPECT_TRUE(empty_block.is_empty_column());

        // Test row operations on empty block
        EXPECT_NO_THROW(empty_block.set_num_rows(0));
        int64_t offset = 0;
        EXPECT_NO_THROW(empty_block.skip_num_rows(offset));
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto col2 = vectorized::ColumnString::create();
        vectorized::DataTypePtr type1(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::DataTypePtr type2(std::make_shared<vectorized::DataTypeString>());

        for (int i = 0; i < 100; ++i) {
            col1->insert_value(i);
            col2->insert_data(std::to_string(i).c_str(), std::to_string(i).length());
        }

        block.insert({col1->get_ptr(), type1, "col1"});
        block.insert({col2->get_ptr(), type2, "col2"});

        // Test basic properties
        EXPECT_EQ(100, block.rows());
        EXPECT_EQ(2, block.columns());
        EXPECT_FALSE(block.empty());
        EXPECT_FALSE(block.is_empty_column());

        // Test row operations
        block.set_num_rows(50);
        EXPECT_EQ(50, block.rows());

        int64_t offset = 20;
        block.skip_num_rows(offset);
        EXPECT_EQ(30, block.rows());
    }

    // Test with const columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());

        // Create and insert const columns
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 100);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 100);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        // Test basic properties
        EXPECT_EQ(100, block.rows());
        EXPECT_EQ(2, block.columns());
        EXPECT_FALSE(block.empty());
        EXPECT_FALSE(block.is_empty_column());

        // Test row operations
        block.set_num_rows(50);
        EXPECT_EQ(50, block.rows());

        int64_t offset = 20;
        block.skip_num_rows(offset);
        EXPECT_EQ(30, block.rows());
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        vectorized::DataTypePtr base_type(std::make_shared<vectorized::DataTypeInt32>());
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create and insert nullable columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 100; ++i) {
            col1->insert_value(i);
        }
        auto nullable_col1 = vectorized::make_nullable(col1->get_ptr());
        block.insert({nullable_col1, nullable_type, "nullable_col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 100; ++i) {
            col2->insert_value(i * 2);
        }
        auto nullable_col2 = vectorized::make_nullable(col2->get_ptr());
        block.insert({nullable_col2, nullable_type, "nullable_col2"});

        // Test basic properties
        EXPECT_EQ(100, block.rows());
        EXPECT_EQ(2, block.columns());
        EXPECT_FALSE(block.empty());
        EXPECT_FALSE(block.is_empty_column());

        // Test row operations
        block.set_num_rows(50);
        EXPECT_EQ(50, block.rows());

        int64_t offset = 20;
        block.skip_num_rows(offset);
        EXPECT_EQ(30, block.rows());
    }

    // Test with mixed column types
    {
        vectorized::Block block;
        vectorized::DataTypePtr type(std::make_shared<vectorized::DataTypeInt32>());
        auto nullable_type = vectorized::make_nullable(type);

        // Insert regular column
        auto regular_col = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 100; ++i) {
            regular_col->insert_value(i);
        }
        block.insert({regular_col->get_ptr(), type, "regular"});

        // Insert const column
        auto base_col = vectorized::ColumnVector<Int32>::create();
        base_col->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 100);
        block.insert({const_col->get_ptr(), type, "const"});

        // Insert nullable column
        auto nullable_base = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 100; ++i) {
            nullable_base->insert_value(i * 2);
        }
        auto nullable_col = vectorized::make_nullable(nullable_base->get_ptr());
        block.insert({nullable_col, nullable_type, "nullable"});

        // Test basic properties
        EXPECT_EQ(100, block.rows());
        EXPECT_EQ(3, block.columns());
        EXPECT_FALSE(block.empty());
        EXPECT_FALSE(block.is_empty_column());

        // Test row operations
        block.set_num_rows(50);
        EXPECT_EQ(50, block.rows());

        int64_t offset = 20;
        block.skip_num_rows(offset);
        EXPECT_EQ(30, block.rows());
    }
}

TEST(BlockTest, MemoryAndSize) {
    // Test empty block
    {
        vectorized::Block empty_block;
        EXPECT_EQ(0, empty_block.bytes());
        EXPECT_EQ(0, empty_block.allocated_bytes());
        EXPECT_EQ("column bytes: []", empty_block.columns_bytes());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add first column (Int32)
        auto col1 = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 1000; ++i) {
            col1->insert_value(i);
        }
        block.insert({col1->get_ptr(), type, "col1"});

        // Test with single column
        size_t bytes_one_col = block.bytes();
        size_t allocated_bytes_one_col = block.allocated_bytes();
        EXPECT_GT(bytes_one_col, 0);
        EXPECT_GT(allocated_bytes_one_col, 0);
        EXPECT_GE(allocated_bytes_one_col, bytes_one_col);

        // Add second column (String)
        auto col2 = vectorized::ColumnString::create();
        auto string_type = std::make_shared<vectorized::DataTypeString>();
        for (int i = 0; i < 1000; ++i) {
            std::string val = "test" + std::to_string(i);
            col2->insert_data(val.c_str(), val.length());
        }
        block.insert({col2->get_ptr(), string_type, "col2"});

        // Test with two columns
        size_t bytes_two_cols = block.bytes();
        EXPECT_GT(bytes_two_cols, bytes_one_col);

        // Test after erasing first column
        block.erase(0);
        EXPECT_EQ(block.bytes(), col2->byte_size());

        // Test after clearing
        block.clear();
        EXPECT_EQ(0, block.bytes());
        EXPECT_EQ(0, block.allocated_bytes());
        EXPECT_EQ("column bytes: []", block.columns_bytes());
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add first const column
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 1000);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        // Test with single const column
        size_t bytes_one_col = block.bytes();
        size_t allocated_bytes_one_col = block.allocated_bytes();
        EXPECT_GT(bytes_one_col, 0);
        EXPECT_GT(allocated_bytes_one_col, 0);
        EXPECT_GE(allocated_bytes_one_col, bytes_one_col);

        // Add second const column
        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 1000);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        // Test with two const columns
        size_t bytes_two_cols = block.bytes();
        EXPECT_GT(bytes_two_cols, bytes_one_col);

        // Test columns_bytes output
        std::string bytes_info = block.columns_bytes();
        EXPECT_TRUE(bytes_info.find("column bytes") != std::string::npos);
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Add first nullable column
        auto col1 = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 1000; ++i) {
            col1->insert_value(i);
        }
        auto nullable_col1 = vectorized::make_nullable(col1->get_ptr());
        block.insert({nullable_col1, nullable_type, "nullable_col1"});

        // Test with single nullable column
        size_t bytes_one_col = block.bytes();
        size_t allocated_bytes_one_col = block.allocated_bytes();
        EXPECT_GT(bytes_one_col, 0);
        EXPECT_GT(allocated_bytes_one_col, 0);
        EXPECT_GE(allocated_bytes_one_col, bytes_one_col);

        // Add second nullable column
        auto col2 = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 1000; ++i) {
            col2->insert_value(i * 2);
        }
        auto nullable_col2 = vectorized::make_nullable(col2->get_ptr());
        block.insert({nullable_col2, nullable_type, "nullable_col2"});

        // Test with two nullable columns
        size_t bytes_two_cols = block.bytes();
        EXPECT_GT(bytes_two_cols, bytes_one_col);

        // Test columns_bytes output
        std::string bytes_info = block.columns_bytes();
        EXPECT_TRUE(bytes_info.find("column bytes") != std::string::npos);
    }

    // Test with nullptr columns
    {
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Test with single nullptr column
        vectorized::Block block_with_null;
        block_with_null.insert({nullptr, type, "null_col"});
        EXPECT_THROW(block_with_null.bytes(), Exception);
        EXPECT_THROW(block_with_null.columns_bytes(), Exception);
        EXPECT_EQ(0, block_with_null.allocated_bytes());

        // Test with multiple nullptr columns
        vectorized::Block multi_null_block;
        multi_null_block.insert({nullptr, type, "null_col1"});
        multi_null_block.insert({nullptr, type, "null_col2"});
        EXPECT_THROW(multi_null_block.bytes(), Exception);
        EXPECT_THROW(multi_null_block.columns_bytes(), Exception);
        EXPECT_EQ(0, multi_null_block.allocated_bytes());
    }

    // Test with mixed column types
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Add regular column
        auto regular_col = vectorized::ColumnVector<Int32>::create();
        regular_col->insert_value(1);
        block.insert({regular_col->get_ptr(), base_type, "regular"});

        // Add const column
        auto const_base = vectorized::ColumnVector<Int32>::create();
        const_base->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(const_base->get_ptr(), 1);
        block.insert({const_col->get_ptr(), base_type, "const"});

        // Add nullable column
        auto nullable_base = vectorized::ColumnVector<Int32>::create();
        nullable_base->insert_value(100);
        auto nullable_col = vectorized::make_nullable(nullable_base->get_ptr());
        block.insert({nullable_col, nullable_type, "nullable"});

        // Test memory operations
        EXPECT_GT(block.bytes(), 0);
        EXPECT_GT(block.allocated_bytes(), 0);
        EXPECT_GE(block.allocated_bytes(), block.bytes());

        // Test columns_bytes output
        std::string bytes_info = block.columns_bytes();
        EXPECT_TRUE(bytes_info.find("column bytes") != std::string::npos);
    }
}

TEST(BlockTest, DumpMethods) {
    // Test empty block
    {
        vectorized::Block empty_block;
        EXPECT_EQ("", empty_block.dump_names());
        EXPECT_EQ("", empty_block.dump_types());
        EXPECT_TRUE(empty_block.dump_structure().empty());
        EXPECT_FALSE(empty_block.dump_data().empty());
    }

    // Test with regular columns
    {
        vectorized::Block block;

        // Add Int32 column
        auto col1 = vectorized::ColumnVector<Int32>::create();
        vectorized::DataTypePtr type1(std::make_shared<vectorized::DataTypeInt32>());
        col1->insert_value(123);
        col1->insert_value(456);
        block.insert({col1->get_ptr(), type1, "col1"});

        // Test single column dumps
        EXPECT_EQ("col1", block.dump_names());
        EXPECT_EQ("Int32", block.dump_types());
        EXPECT_FALSE(block.dump_structure().empty());

        // Add String column
        auto col2 = vectorized::ColumnString::create();
        vectorized::DataTypePtr type2(std::make_shared<vectorized::DataTypeString>());
        col2->insert_data("hello", 5);
        col2->insert_data("world", 5);
        block.insert({col2->get_ptr(), type2, "col2"});

        // Test multiple columns dumps
        EXPECT_EQ("col1, col2", block.dump_names());
        EXPECT_EQ("Int32, String", block.dump_types());

        // Test dump_data variations
        std::string full_data = block.dump_data();
        EXPECT_FALSE(full_data.empty());
        EXPECT_TRUE(full_data.find("col1(Int32)") != std::string::npos);
        EXPECT_TRUE(full_data.find("col2(String)") != std::string::npos);
        EXPECT_TRUE(full_data.find("123") != std::string::npos);
        EXPECT_TRUE(full_data.find("hello") != std::string::npos);

        std::string offset_data = block.dump_data(1);
        EXPECT_TRUE(offset_data.find("456") != std::string::npos);
        EXPECT_FALSE(offset_data.find("123") != std::string::npos);

        std::string limited_data = block.dump_data(0, 1);
        EXPECT_TRUE(limited_data.find("123") != std::string::npos);
        EXPECT_FALSE(limited_data.find("456") != std::string::npos);

        // Test dump_one_line
        EXPECT_EQ("123 hello", block.dump_one_line(0, 2));
        EXPECT_EQ("456 world", block.dump_one_line(1, 2));
        EXPECT_EQ("123", block.dump_one_line(0, 1));

        // Test dump_column
        std::string int_dump = vectorized::Block::dump_column(col1->get_ptr(), type1);
        EXPECT_TRUE(int_dump.find("123") != std::string::npos);
        EXPECT_TRUE(int_dump.find("456") != std::string::npos);

        std::string str_dump = vectorized::Block::dump_column(col2->get_ptr(), type2);
        EXPECT_TRUE(str_dump.find("hello") != std::string::npos);
        EXPECT_TRUE(str_dump.find("world") != std::string::npos);
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create and insert const columns
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 2);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 2);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        // Test basic dumps
        EXPECT_EQ("const_col1, const_col2", block.dump_names());
        EXPECT_EQ("Int32, Int32", block.dump_types());
        EXPECT_FALSE(block.dump_structure().empty());

        // Test dump_data variations
        std::string full_data = block.dump_data();
        EXPECT_TRUE(full_data.find("42") != std::string::npos);
        EXPECT_TRUE(full_data.find("24") != std::string::npos);

        std::string offset_data = block.dump_data(1);
        EXPECT_TRUE(offset_data.find("42") != std::string::npos);
        EXPECT_TRUE(offset_data.find("24") != std::string::npos);

        std::string limited_data = block.dump_data(0, 1);
        EXPECT_TRUE(limited_data.find("42") != std::string::npos);
        EXPECT_TRUE(limited_data.find("24") != std::string::npos);

        // Test dump_one_line
        EXPECT_EQ("42 24", block.dump_one_line(0, 2));
        EXPECT_EQ("42 24", block.dump_one_line(1, 2));
        EXPECT_EQ("42", block.dump_one_line(0, 1));

        // Test dump_column
        std::string const_dump1 = vectorized::Block::dump_column(const_col1->get_ptr(), type);
        EXPECT_TRUE(const_dump1.find("42") != std::string::npos);

        std::string const_dump2 = vectorized::Block::dump_column(const_col2->get_ptr(), type);
        EXPECT_TRUE(const_dump2.find("24") != std::string::npos);
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create and insert nullable columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        col1->insert_value(123);
        col1->insert_value(456);
        auto null_map1 = vectorized::ColumnUInt8::create();
        null_map1->insert_value(0); // Not null
        null_map1->insert_value(1); // Null
        auto nullable_col1 =
                vectorized::ColumnNullable::create(col1->get_ptr(), null_map1->get_ptr());
        block.insert({nullable_col1->get_ptr(), nullable_type, "nullable_col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        col2->insert_value(789);
        col2->insert_value(321);
        auto null_map2 = vectorized::ColumnUInt8::create();
        null_map2->insert_value(1); // Null
        null_map2->insert_value(0); // Not null
        auto nullable_col2 =
                vectorized::ColumnNullable::create(col2->get_ptr(), null_map2->get_ptr());
        block.insert({nullable_col2->get_ptr(), nullable_type, "nullable_col2"});

        // Test basic dumps
        EXPECT_EQ("nullable_col1, nullable_col2", block.dump_names());
        EXPECT_EQ("Nullable(Int32), Nullable(Int32)", block.dump_types());
        EXPECT_FALSE(block.dump_structure().empty());

        // Test dump_data variations
        std::string full_data = block.dump_data();
        EXPECT_TRUE(full_data.find("123") != std::string::npos);
        EXPECT_TRUE(full_data.find("NULL") != std::string::npos);

        std::string offset_data = block.dump_data(1);
        EXPECT_TRUE(offset_data.find("321") != std::string::npos);
        EXPECT_FALSE(offset_data.find("789") != std::string::npos);

        std::string limited_data = block.dump_data(0, 1);
        EXPECT_TRUE(limited_data.find("123") != std::string::npos);
        EXPECT_TRUE(limited_data.find("NULL") != std::string::npos);

        // Test dump_one_line
        EXPECT_EQ("123 NULL", block.dump_one_line(0, 2));
        EXPECT_EQ("NULL 321", block.dump_one_line(1, 2));
        EXPECT_EQ("123", block.dump_one_line(0, 1));

        // Test dump_column
        std::string nullable_dump1 =
                vectorized::Block::dump_column(nullable_col1->get_ptr(), nullable_type);
        EXPECT_TRUE(nullable_dump1.find("123") != std::string::npos);
        EXPECT_TRUE(nullable_dump1.find("NULL") != std::string::npos);

        std::string nullable_dump2 =
                vectorized::Block::dump_column(nullable_col2->get_ptr(), nullable_type);
        EXPECT_TRUE(nullable_dump2.find("321") != std::string::npos);
        EXPECT_TRUE(nullable_dump2.find("NULL") != std::string::npos);
    }

    // Test with mixed column types
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Add regular column
        auto regular_col = vectorized::ColumnVector<Int32>::create();
        regular_col->insert_value(1);
        regular_col->insert_value(2);
        block.insert({regular_col->get_ptr(), base_type, "regular"});

        // Add const column
        auto const_base = vectorized::ColumnVector<Int32>::create();
        const_base->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(const_base->get_ptr(), 2);
        block.insert({const_col->get_ptr(), base_type, "const"});

        // Add nullable column
        auto nullable_base = vectorized::ColumnVector<Int32>::create();
        nullable_base->insert_value(3);
        nullable_base->insert_value(4);
        auto null_map = vectorized::ColumnUInt8::create();
        null_map->insert_value(0);
        null_map->insert_value(1);
        auto nullable_col =
                vectorized::ColumnNullable::create(nullable_base->get_ptr(), null_map->get_ptr());
        block.insert({nullable_col->get_ptr(), nullable_type, "nullable"});

        // Test basic dumps
        EXPECT_EQ("regular, const, nullable", block.dump_names());
        EXPECT_EQ("Int32, Int32, Nullable(Int32)", block.dump_types());
        EXPECT_FALSE(block.dump_structure().empty());

        // Test dump_data variations
        std::string full_data = block.dump_data();
        EXPECT_TRUE(full_data.find('1') != std::string::npos);
        EXPECT_TRUE(full_data.find('4') != std::string::npos);
        EXPECT_TRUE(full_data.find('3') != std::string::npos);

        // Test dump_one_line
        EXPECT_EQ("1 42 3", block.dump_one_line(0, 3));
        EXPECT_EQ("2 42 NULL", block.dump_one_line(1, 3));

        // Test dump_column for each type
        std::string regular_dump =
                vectorized::Block::dump_column(regular_col->get_ptr(), base_type);
        EXPECT_TRUE(regular_dump.find('1') != std::string::npos);
        EXPECT_TRUE(regular_dump.find('2') != std::string::npos);

        std::string const_dump = vectorized::Block::dump_column(const_col->get_ptr(), base_type);
        EXPECT_TRUE(const_dump.find("42") != std::string::npos);

        std::string nullable_dump =
                vectorized::Block::dump_column(nullable_col->get_ptr(), nullable_type);
        EXPECT_TRUE(nullable_dump.find('3') != std::string::npos);
        EXPECT_TRUE(nullable_dump.find("NULL") != std::string::npos);
    }

    // Test with empty columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add empty regular column
        auto empty_regular = vectorized::ColumnVector<Int32>::create();
        block.insert({empty_regular->get_ptr(), type, "empty_regular"});

        // Add empty const column
        auto empty_const_base = vectorized::ColumnVector<Int32>::create();
        empty_const_base->insert_value(0);
        auto empty_const = vectorized::ColumnConst::create(empty_const_base->get_ptr(), 0);
        block.insert({empty_const->get_ptr(), type, "empty_const"});

        // Test basic dumps
        EXPECT_EQ("empty_regular, empty_const", block.dump_names());
        EXPECT_EQ("Int32, Int32", block.dump_types());
        EXPECT_FALSE(block.dump_structure().empty());

        // Test dump_data
        std::string data = block.dump_data();
        EXPECT_FALSE(data.empty());

        // Test dump_one_line
        EXPECT_EQ("0 0", block.dump_one_line(0, 2));

        // Test dump_column
        std::string empty_regular_dump =
                vectorized::Block::dump_column(empty_regular->get_ptr(), type);
        EXPECT_FALSE(empty_regular_dump.empty());

        std::string empty_const_dump = vectorized::Block::dump_column(empty_const->get_ptr(), type);
        EXPECT_FALSE(empty_const_dump.empty());
    }
}

TEST(BlockTest, CloneOperations) {
    // Test with empty block
    {
        vectorized::Block empty_block;

        // Test clone_empty
        auto cloned_empty = empty_block.clone_empty();
        EXPECT_EQ(0, cloned_empty.columns());
        EXPECT_EQ(0, cloned_empty.rows());

        // Test get_columns and get_columns_and_convert
        auto columns = empty_block.get_columns();
        auto converted_columns = empty_block.get_columns_and_convert();
        EXPECT_EQ(0, columns.size());
        EXPECT_EQ(0, converted_columns.size());

        // Test clone_empty_columns
        auto empty_columns = empty_block.clone_empty_columns();
        EXPECT_EQ(0, empty_columns.size());

        // Test mutate_columns
        auto mutable_cols = empty_block.mutate_columns();
        EXPECT_EQ(0, mutable_cols.size());

        // Test set_columns
        vectorized::Block new_block = empty_block.clone_empty();
        new_block.set_columns(columns);
        EXPECT_EQ(0, new_block.rows());
        EXPECT_EQ(0, new_block.columns());

        // Test clone_with_columns
        auto cloned_with_cols = empty_block.clone_with_columns(columns);
        EXPECT_EQ(0, cloned_with_cols.rows());
        EXPECT_EQ(0, cloned_with_cols.columns());

        // Test clone_without_columns
        std::vector<int> column_offset;
        auto partial_block = empty_block.clone_without_columns(&column_offset);
        EXPECT_EQ(0, partial_block.columns());

        // Test copy_block with different combinations
        std::vector<int> empty_columns_indices;
        auto copy = empty_block.copy_block(empty_columns_indices);
        EXPECT_EQ(0, copy.columns());
        EXPECT_EQ(0, copy.rows());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create and insert regular columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        auto col2 = vectorized::ColumnVector<Int32>::create();
        col1->insert_value(1);
        col2->insert_value(2);
        block.insert({col1->get_ptr(), type, "col1"});
        block.insert({col2->get_ptr(), type, "col2"});

        // Test clone_empty
        auto empty_block = block.clone_empty();
        EXPECT_EQ(block.columns(), empty_block.columns());
        EXPECT_EQ(0, empty_block.rows());

        // Test get_columns and get_columns_and_convert
        auto columns = block.get_columns();
        auto converted_columns = block.get_columns_and_convert();
        EXPECT_EQ(2, columns.size());
        EXPECT_EQ(2, converted_columns.size());

        // Test clone_empty_columns
        auto empty_columns = block.clone_empty_columns();
        EXPECT_EQ(2, empty_columns.size());
        EXPECT_EQ(0, empty_columns[0]->size());
        EXPECT_EQ(0, empty_columns[1]->size());

        // Test mutate_columns
        auto mutable_cols = block.mutate_columns();
        EXPECT_EQ(2, mutable_cols.size());

        // Test set_columns with const columns
        vectorized::Block new_block = block.clone_empty();
        new_block.set_columns(columns);
        EXPECT_EQ(block.rows(), new_block.rows());
        EXPECT_EQ(block.columns(), new_block.columns());
        EXPECT_EQ("col1", new_block.get_by_position(0).name);
        EXPECT_EQ("col2", new_block.get_by_position(1).name);
        EXPECT_EQ(1, assert_cast<const vectorized::ColumnVector<Int32>*>(
                             new_block.get_by_position(0).column.get())
                             ->get_data()[0]);
        EXPECT_EQ(2, assert_cast<const vectorized::ColumnVector<Int32>*>(
                             new_block.get_by_position(1).column.get())
                             ->get_data()[0]);

        // Test clone_with_columns
        auto cloned_with_cols = block.clone_with_columns(columns);
        EXPECT_EQ(block.rows(), cloned_with_cols.rows());
        EXPECT_EQ(block.columns(), cloned_with_cols.columns());
        EXPECT_EQ(1, assert_cast<const vectorized::ColumnVector<Int32>*>(
                             cloned_with_cols.get_by_position(0).column.get())
                             ->get_data()[0]);
        EXPECT_EQ(2, assert_cast<const vectorized::ColumnVector<Int32>*>(
                             cloned_with_cols.get_by_position(1).column.get())
                             ->get_data()[0]);

        // Test clone_without_columns
        std::vector<int> column_offset = {0};
        auto partial_block = block.clone_without_columns(&column_offset);
        EXPECT_EQ(1, partial_block.columns());
        EXPECT_EQ("col1", partial_block.get_by_position(0).name);
        EXPECT_EQ(nullptr, partial_block.get_by_position(0).column.get());

        // Test copy_block with different combinations
        std::vector<int> single_column = {0};
        auto single_copy = block.copy_block(single_column);
        EXPECT_EQ(1, single_copy.columns());
        EXPECT_EQ("col1", single_copy.get_by_position(0).name);

        std::vector<int> multiple_columns = {0, 1};
        auto multi_copy = block.copy_block(multiple_columns);
        EXPECT_EQ(2, multi_copy.columns());
        EXPECT_EQ("col1", multi_copy.get_by_position(0).name);
        EXPECT_EQ("col2", multi_copy.get_by_position(1).name);

        std::vector<int> reordered_columns = {1, 0};
        auto reordered_copy = block.copy_block(reordered_columns);
        EXPECT_EQ(2, reordered_copy.columns());
        EXPECT_EQ("col2", reordered_copy.get_by_position(0).name);
        EXPECT_EQ("col1", reordered_copy.get_by_position(1).name);

        std::vector<int> duplicate_columns = {0, 0};
        auto duplicate_copy = block.copy_block(duplicate_columns);
        EXPECT_EQ(2, duplicate_copy.columns());
        EXPECT_EQ("col1", duplicate_copy.get_by_position(0).name);
        EXPECT_EQ("col1", duplicate_copy.get_by_position(1).name);
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create and insert const columns
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(42);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 1);
        block.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(24);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 1);
        block.insert({const_col2->get_ptr(), type, "const_col2"});

        // Test all clone operations
        auto empty_block = block.clone_empty();
        EXPECT_EQ(block.columns(), empty_block.columns());
        EXPECT_EQ(0, empty_block.rows());

        auto columns = block.get_columns();
        auto converted_columns = block.get_columns_and_convert();
        EXPECT_EQ(2, columns.size());
        EXPECT_EQ(2, converted_columns.size());

        auto empty_columns = block.clone_empty_columns();
        EXPECT_EQ(2, empty_columns.size());
        EXPECT_EQ(0, empty_columns[0]->size());
        EXPECT_EQ(0, empty_columns[1]->size());

        auto mutable_cols = block.mutate_columns();
        EXPECT_EQ(2, mutable_cols.size());

        vectorized::Block new_block = block.clone_empty();
        new_block.set_columns(columns);
        EXPECT_EQ(block.rows(), new_block.rows());
        EXPECT_EQ(block.columns(), new_block.columns());
        EXPECT_EQ("const_col1", new_block.get_by_position(0).name);
        EXPECT_EQ("const_col2", new_block.get_by_position(1).name);

        auto cloned_with_cols = block.clone_with_columns(columns);
        EXPECT_EQ(block.rows(), cloned_with_cols.rows());
        EXPECT_EQ(block.columns(), cloned_with_cols.columns());

        std::vector<int> column_offset = {0};
        auto partial_block = block.clone_without_columns(&column_offset);
        EXPECT_EQ(1, partial_block.columns());
        EXPECT_EQ("const_col1", partial_block.get_by_position(0).name);
        EXPECT_EQ(nullptr, partial_block.get_by_position(0).column.get());

        // Test copy_block with different combinations
        std::vector<int> single_column = {0};
        auto single_copy = block.copy_block(single_column);
        EXPECT_EQ(1, single_copy.columns());
        EXPECT_EQ("const_col1", single_copy.get_by_position(0).name);

        std::vector<int> multiple_columns = {0, 1};
        auto multi_copy = block.copy_block(multiple_columns);
        EXPECT_EQ(2, multi_copy.columns());
        EXPECT_EQ("const_col1", multi_copy.get_by_position(0).name);
        EXPECT_EQ("const_col2", multi_copy.get_by_position(1).name);

        std::vector<int> reordered_columns = {1, 0};
        auto reordered_copy = block.copy_block(reordered_columns);
        EXPECT_EQ(2, reordered_copy.columns());
        EXPECT_EQ("const_col2", reordered_copy.get_by_position(0).name);
        EXPECT_EQ("const_col1", reordered_copy.get_by_position(1).name);

        std::vector<int> duplicate_columns = {0, 0};
        auto duplicate_copy = block.copy_block(duplicate_columns);
        EXPECT_EQ(2, duplicate_copy.columns());
        EXPECT_EQ("const_col1", duplicate_copy.get_by_position(0).name);
        EXPECT_EQ("const_col1", duplicate_copy.get_by_position(1).name);
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = vectorized::make_nullable(base_type);

        // Create and insert nullable columns
        auto col1 = vectorized::ColumnVector<Int32>::create();
        col1->insert_value(1);
        auto null_map1 = vectorized::ColumnUInt8::create();
        null_map1->insert_value(0); // Not null
        auto nullable_col1 =
                vectorized::ColumnNullable::create(col1->get_ptr(), null_map1->get_ptr());
        block.insert({nullable_col1->get_ptr(), nullable_type, "nullable_col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        col2->insert_value(2);
        auto null_map2 = vectorized::ColumnUInt8::create();
        null_map2->insert_value(1); // Null
        auto nullable_col2 =
                vectorized::ColumnNullable::create(col2->get_ptr(), null_map2->get_ptr());
        block.insert({nullable_col2->get_ptr(), nullable_type, "nullable_col2"});

        // Test all clone operations
        auto empty_block = block.clone_empty();
        EXPECT_EQ(block.columns(), empty_block.columns());
        EXPECT_EQ(0, empty_block.rows());

        auto columns = block.get_columns();
        auto converted_columns = block.get_columns_and_convert();
        EXPECT_EQ(2, columns.size());
        EXPECT_EQ(2, converted_columns.size());

        auto empty_columns = block.clone_empty_columns();
        EXPECT_EQ(2, empty_columns.size());
        EXPECT_EQ(0, empty_columns[0]->size());
        EXPECT_EQ(0, empty_columns[1]->size());

        auto mutable_cols = block.mutate_columns();
        EXPECT_EQ(2, mutable_cols.size());

        vectorized::Block new_block = block.clone_empty();
        new_block.set_columns(columns);
        EXPECT_EQ(block.rows(), new_block.rows());
        EXPECT_EQ(block.columns(), new_block.columns());
        EXPECT_EQ("nullable_col1", new_block.get_by_position(0).name);
        EXPECT_EQ("nullable_col2", new_block.get_by_position(1).name);

        auto cloned_with_cols = block.clone_with_columns(columns);
        EXPECT_EQ(block.rows(), cloned_with_cols.rows());
        EXPECT_EQ(block.columns(), cloned_with_cols.columns());

        std::vector<int> column_offset = {0};
        auto partial_block = block.clone_without_columns(&column_offset);
        EXPECT_EQ(1, partial_block.columns());
        EXPECT_EQ("nullable_col1", partial_block.get_by_position(0).name);
        EXPECT_EQ(nullptr, partial_block.get_by_position(0).column.get());

        // Test copy_block with different combinations
        std::vector<int> single_column = {0};
        auto single_copy = block.copy_block(single_column);
        EXPECT_EQ(1, single_copy.columns());
        EXPECT_EQ("nullable_col1", single_copy.get_by_position(0).name);

        std::vector<int> multiple_columns = {0, 1};
        auto multi_copy = block.copy_block(multiple_columns);
        EXPECT_EQ(2, multi_copy.columns());
        EXPECT_EQ("nullable_col1", multi_copy.get_by_position(0).name);
        EXPECT_EQ("nullable_col2", multi_copy.get_by_position(1).name);

        std::vector<int> reordered_columns = {1, 0};
        auto reordered_copy = block.copy_block(reordered_columns);
        EXPECT_EQ(2, reordered_copy.columns());
        EXPECT_EQ("nullable_col2", reordered_copy.get_by_position(0).name);
        EXPECT_EQ("nullable_col1", reordered_copy.get_by_position(1).name);

        std::vector<int> duplicate_columns = {0, 0};
        auto duplicate_copy = block.copy_block(duplicate_columns);
        EXPECT_EQ(2, duplicate_copy.columns());
        EXPECT_EQ("nullable_col1", duplicate_copy.get_by_position(0).name);
        EXPECT_EQ("nullable_col1", duplicate_copy.get_by_position(1).name);
    }
}

TEST(BlockTest, FilterAndSelector) {
    // Test empty block
    {
        vectorized::Block empty_block;

        // Test filter_block_internal
        vectorized::IColumn::Filter filter(0);
        EXPECT_NO_THROW(vectorized::Block::filter_block_internal(&empty_block, filter));
        EXPECT_EQ(0, empty_block.rows());
        EXPECT_EQ(0, empty_block.columns());

        // Test filter_block
        std::vector<uint32_t> columns_to_filter;
        EXPECT_DEATH(vectorized::Block::filter_block(&empty_block, columns_to_filter, 0, 0).ok(),
                     "");
        EXPECT_EQ(0, empty_block.rows());

        // Test append_to_block_by_selector
        vectorized::Block dst_block;
        vectorized::MutableBlock dst(&dst_block);
        vectorized::IColumn::Selector selector(0);
        EXPECT_TRUE(empty_block.append_to_block_by_selector(&dst, selector).ok());
        EXPECT_EQ(0, dst.rows());
    }

    // Test with regular columns
    {
        auto create_test_block = [](int size) {
            vectorized::Block test_block;
            auto test_col1 = vectorized::ColumnVector<Int32>::create();
            auto test_col2 = vectorized::ColumnVector<Int32>::create();
            auto type = std::make_shared<vectorized::DataTypeInt32>();

            for (int i = 0; i < size; ++i) {
                test_col1->insert_value(i);
                test_col2->insert_value(i * 2);
            }

            test_block.insert({test_col1->get_ptr(), type, "col1"});
            test_block.insert({test_col2->get_ptr(), type, "col2"});
            return test_block;
        };

        // Test filter_block_internal with filter only
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1); // Initialize with all 1s (keep all rows)
            filter[0] = 0;                             // Filter out first row
            filter[5] = 0;                             // Filter out sixth row

            vectorized::Block::filter_block_internal(&test_block, filter);
            EXPECT_EQ(8, test_block.rows());

            // Verify filtered data for both columns
            const auto* filtered_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(1).column.get());

            // Expected values after filtering
            std::vector<Int32> expected_col1 = {1, 2, 3, 4, 6, 7, 8, 9};
            std::vector<Int32> expected_col2 = {2, 4, 6, 8, 12, 14, 16, 18};

            for (size_t i = 0; i < expected_col1.size(); ++i) {
                EXPECT_EQ(expected_col1[i], filtered_col1->get_data()[i]);
                EXPECT_EQ(expected_col2[i], filtered_col2->get_data()[i]);
            }
        }

        // Test filter_block_internal with specific columns
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            std::vector<uint32_t> columns_to_filter = {0}; // Only filter first column

            vectorized::Block::filter_block_internal(&test_block, columns_to_filter, filter);
            EXPECT_EQ(9, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(1).column.get());
            EXPECT_EQ(1, filtered_col1->get_data()[0]); // First column filtered
            EXPECT_EQ(0, filtered_col2->get_data()[0]); // Second column unchanged
        }

        // Test filter_block_internal with column_to_keep
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;               // Filter out first row
            filter[5] = 0;               // Filter out sixth row
            uint32_t column_to_keep = 1; // Only filter first column, keep the rest columns

            vectorized::Block::filter_block_internal(&test_block, filter, column_to_keep);

            // Verify row count after filtering
            EXPECT_EQ(8, test_block.rows());
            EXPECT_EQ(2, test_block.columns());

            // Verify filtered data for both columns
            const auto* filtered_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(1).column.get());

            // Expected values after filtering
            std::vector<Int32> expected_col1 = {1, 2, 3, 4, 6, 7, 8, 9};
            std::vector<Int32> expected_col2 = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};

            // Verify each value in filtered columns
            for (size_t i = 0; i < expected_col1.size(); ++i) {
                EXPECT_EQ(expected_col1[i], filtered_col1->get_data()[i]);
            }
            for (size_t i = 0; i < expected_col2.size(); ++i) {
                EXPECT_EQ(expected_col2[i], filtered_col2->get_data()[i]);
            }
        }

        // Test filter_block with nullable filter column
        {
            auto test_block = create_test_block(10);

            // Create nullable filter column
            auto nullable_filter = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 1), // all true
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 0)  // no nulls
            );
            auto filter_type = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeUInt8>());

            // Add filter column to block
            test_block.insert({nullable_filter->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(10, test_block.rows()); // All rows kept

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({nullable_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(10, test_block2.rows()); // All rows kept
        }

        // Test filter_block with const filter column
        {
            auto test_block = create_test_block(10);

            // Create const filter column (false)
            auto const_filter = vectorized::ColumnConst::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(1, 0), // false
                    10);
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            // Add filter column to block
            test_block.insert({const_filter->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(0, test_block.rows()); // All rows filtered out

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({const_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(0, test_block2.rows()); // All rows filtered out
        }

        // Test filter_block with regular filter column
        {
            auto test_block = create_test_block(10);

            // Create regular filter column
            auto filter_column = vectorized::ColumnVector<vectorized::UInt8>::create();
            for (size_t i = 0; i < 10; ++i) {
                filter_column->insert_value(i % 2); // Keep odd-indexed rows
            }
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            // Add filter column to block
            test_block.insert({filter_column->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(5, test_block.rows()); // Half rows kept

            // Verify filtered data
            const auto* filtered_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block.get_by_position(1).column.get());

            std::vector<Int32> expected_col1 = {1, 3, 5, 7, 9};
            std::vector<Int32> expected_col2 = {2, 6, 10, 14, 18};

            for (size_t i = 0; i < expected_col1.size(); ++i) {
                EXPECT_EQ(expected_col1[i], filtered_col1->get_data()[i]);
                EXPECT_EQ(expected_col2[i], filtered_col2->get_data()[i]);
            }

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({filter_column->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(5, test_block2.rows()); // Half rows kept

            // Verify filtered data
            filtered_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block2.get_by_position(0).column.get());
            filtered_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    test_block2.get_by_position(1).column.get());

            for (size_t i = 0; i < expected_col1.size(); ++i) {
                EXPECT_EQ(expected_col1[i], filtered_col1->get_data()[i]);
                EXPECT_EQ(expected_col2[i], filtered_col2->get_data()[i]);
            }
        }

        // Test append_to_block_by_selector
        {
            auto block = create_test_block(10);
            // Create destination block with proper columns
            auto type = std::make_shared<vectorized::DataTypeInt32>();
            vectorized::Block dst_block;
            dst_block.insert({type->create_column(), type, "col1"});
            dst_block.insert({type->create_column(), type, "col2"});
            vectorized::MutableBlock dst(&dst_block);

            // Create selector to select every other row
            vectorized::IColumn::Selector selector(5, 0);
            for (size_t i = 0; i < 5; ++i) {
                selector[i] = i * 2; // Select rows 0,2,4,6,8
            }

            // Perform selection
            EXPECT_TRUE(block.append_to_block_by_selector(&dst, selector).ok());
            EXPECT_EQ(5, dst.rows());

            // Verify selected data
            const vectorized::Block& result_block = dst.to_block();

            const auto* selected_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    result_block.get_by_position(0).column.get());
            const auto* selected_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    result_block.get_by_position(1).column.get());

            // Expected values after selection
            std::vector<Int32> expected_col1 = {0, 2, 4, 6, 8};
            std::vector<Int32> expected_col2 = {0, 4, 8, 12, 16};

            for (size_t i = 0; i < expected_col1.size(); ++i) {
                EXPECT_EQ(expected_col1[i], selected_col1->get_data()[i]);
                EXPECT_EQ(expected_col2[i], selected_col2->get_data()[i]);
            }
        }
    }

    // Test with const columns
    {
        auto create_test_block = [](int size) {
            vectorized::Block test_block;
            auto type = std::make_shared<vectorized::DataTypeInt32>();

            // Create const columns with fixed values
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(42);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), size);

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(24);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), size);

            test_block.insert({const_col1->get_ptr(), type, "const_col1"});
            test_block.insert({const_col2->get_ptr(), type, "const_col2"});
            return test_block;
        };

        // Test filter_block_internal with filter only
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            filter[5] = 0;

            vectorized::Block::filter_block_internal(&test_block, filter);
            EXPECT_EQ(8, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(1).column.get());

            EXPECT_EQ(42, filtered_col1->get_int(0));
            EXPECT_EQ(24, filtered_col2->get_int(0));
            EXPECT_EQ(8, filtered_col1->size());
            EXPECT_EQ(8, filtered_col2->size());
        }

        // Test filter_block_internal with specific columns
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            std::vector<uint32_t> columns_to_filter = {0};

            vectorized::Block::filter_block_internal(&test_block, columns_to_filter, filter);
            EXPECT_EQ(9, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(1).column.get());

            EXPECT_EQ(42, filtered_col1->get_int(0));
            EXPECT_EQ(24, filtered_col2->get_int(0));
            EXPECT_EQ(9, filtered_col1->size());
            EXPECT_EQ(10, filtered_col2->size()); // Second column unchanged
        }

        // Test filter_block_internal with column_to_keep
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            filter[5] = 0;
            uint32_t column_to_keep = 1;

            vectorized::Block::filter_block_internal(&test_block, filter, column_to_keep);
            EXPECT_EQ(8, test_block.rows());
            EXPECT_EQ(2, test_block.columns());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(1).column.get());

            EXPECT_EQ(42, filtered_col1->get_int(0));
            EXPECT_EQ(24, filtered_col2->get_int(0));
            EXPECT_EQ(8, filtered_col1->size());
            EXPECT_EQ(10, filtered_col2->size()); // Second column unchanged
        }

        // Test filter_block with nullable filter column
        {
            auto test_block = create_test_block(10);

            auto nullable_filter = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 1),
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 0));
            auto filter_type = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeUInt8>());

            test_block.insert({nullable_filter->get_ptr(), filter_type, "filter"});

            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(10, test_block.rows());

            auto test_block2 = create_test_block(10);
            test_block2.insert({nullable_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(10, test_block2.rows());
        }

        // Test filter_block with const filter column
        {
            auto test_block = create_test_block(10);

            auto const_filter = vectorized::ColumnConst::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(1, 0), 10);
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            test_block.insert({const_filter->get_ptr(), filter_type, "filter"});

            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(0, test_block.rows());

            auto test_block2 = create_test_block(10);
            test_block2.insert({const_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(0, test_block2.rows());
        }
        // Test filter_block with regular filter column
        {
            auto test_block = create_test_block(10);

            // Create regular filter column
            auto filter_column = vectorized::ColumnVector<vectorized::UInt8>::create();
            for (size_t i = 0; i < 10; ++i) {
                filter_column->insert_value(i % 2); // Keep odd-indexed rows
            }
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            // Add filter column to block
            test_block.insert({filter_column->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(5, test_block.rows()); // Half rows kept

            // Verify filtered data
            const auto* filtered_col1 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnConst*>(
                    test_block.get_by_position(1).column.get());

            EXPECT_EQ(42, filtered_col1->get_int(0));
            EXPECT_EQ(24, filtered_col2->get_int(0));
            EXPECT_EQ(5, filtered_col1->size());
            EXPECT_EQ(5, filtered_col2->size());

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({filter_column->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(5, test_block2.rows()); // Half rows kept

            // Verify filtered data
            filtered_col1 = assert_cast<const vectorized::ColumnConst*>(
                    test_block2.get_by_position(0).column.get());
            filtered_col2 = assert_cast<const vectorized::ColumnConst*>(
                    test_block2.get_by_position(1).column.get());

            EXPECT_EQ(42, filtered_col1->get_int(0));
            EXPECT_EQ(24, filtered_col2->get_int(0));
            EXPECT_EQ(5, filtered_col1->size());
            EXPECT_EQ(5, filtered_col2->size());
        }

        // Test append_to_block_by_selector
        {
            auto block = create_test_block(10);
            // Create destination block with proper columns
            auto type = std::make_shared<vectorized::DataTypeInt32>();
            vectorized::Block dst_block;
            dst_block.insert({type->create_column(), type, "const_col1"});
            dst_block.insert({type->create_column(), type, "const_col2"});
            vectorized::MutableBlock dst(&dst_block);

            // Create selector to select every other row
            vectorized::IColumn::Selector selector(5, 0);
            for (size_t i = 0; i < 5; ++i) {
                selector[i] = i * 2; // Select rows 0,2,4,6,8
            }

            // Perform selection
            EXPECT_TRUE(block.append_to_block_by_selector(&dst, selector).ok());
            // Skip const columns
            EXPECT_EQ(0, dst.rows());

            // Verify selected data
            const vectorized::Block& result_block = dst.to_block();

            const auto* selected_col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    result_block.get_by_position(0).column.get());
            const auto* selected_col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                    result_block.get_by_position(1).column.get());

            EXPECT_EQ(0, selected_col1->get_int(0));
            EXPECT_EQ(0, selected_col2->get_int(0));
            EXPECT_EQ(0, selected_col1->size());
            EXPECT_EQ(0, selected_col2->size());
        }
    }

    // Test with nullable columns
    {
        auto create_test_block = [](int size) {
            vectorized::Block test_block;
            auto base_type = std::make_shared<vectorized::DataTypeInt32>();
            auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

            // Create nullable columns
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());

            // Insert test data
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());

            for (int i = 0; i < size; ++i) {
                nested1->insert_value(i);
                nested2->insert_value(i * 2);
                null_map1->insert_value(i % 2);       // Even rows are not null
                null_map2->insert_value((i + 1) % 2); // Odd rows are not null
            }

            test_block.insert({col1->get_ptr(), nullable_type, "nullable_col1"});
            test_block.insert({col2->get_ptr(), nullable_type, "nullable_col2"});
            return test_block;
        };

        // Test filter_block_internal with filter only
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            filter[5] = 0;

            vectorized::Block::filter_block_internal(&test_block, filter);
            EXPECT_EQ(8, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(1).column.get());
            // Verify filtered data
            for (size_t i = 0; i < 8; ++i) {
                size_t original_row = (i < 4) ? i + 1 : i + 2;
                bool expected_null_col1 = original_row % 2;
                bool expected_null_col2 = (original_row + 1) % 2;
                EXPECT_EQ(expected_null_col1, filtered_col1->is_null_at(i));
                EXPECT_EQ(expected_null_col2, filtered_col2->is_null_at(i));

                if (!filtered_col1->is_null_at(i)) {
                    EXPECT_EQ(original_row, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                                    filtered_col1->get_nested_column_ptr().get())
                                                    ->get_data()[i]);
                }
                if (!filtered_col2->is_null_at(i)) {
                    EXPECT_EQ(original_row * 2,
                              assert_cast<const vectorized::ColumnVector<Int32>*>(
                                      filtered_col2->get_nested_column_ptr().get())
                                      ->get_data()[i]);
                }
            }
        }

        // Test filter_block_internal with specific columns
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            std::vector<uint32_t> columns_to_filter = {0};

            vectorized::Block::filter_block_internal(&test_block, columns_to_filter, filter);
            EXPECT_EQ(9, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(1).column.get());

            // Verify filtered data for col1
            for (size_t i = 0; i < 9; ++i) {
                size_t original_row = i + 1;
                EXPECT_EQ(original_row % 2, filtered_col1->is_null_at(i));
                if (!filtered_col1->is_null_at(i)) {
                    EXPECT_EQ(original_row, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                                    filtered_col1->get_nested_column_ptr().get())
                                                    ->get_data()[i]);
                }
            }

            // Verify col2 remains unchanged
            for (size_t i = 0; i < 10; ++i) {
                EXPECT_EQ((i + 1) % 2, filtered_col2->is_null_at(i));
                if (!filtered_col2->is_null_at(i)) {
                    EXPECT_EQ(i * 2, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                             filtered_col2->get_nested_column_ptr().get())
                                             ->get_data()[i]);
                }
            }
        }
        // Test filter_block_internal with column_to_keep
        {
            auto test_block = create_test_block(10);
            vectorized::IColumn::Filter filter(10, 1);
            filter[0] = 0;
            filter[5] = 0;
            uint32_t column_to_keep = 1;

            vectorized::Block::filter_block_internal(&test_block, filter, column_to_keep);
            EXPECT_EQ(8, test_block.rows());

            const auto* filtered_col1 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(1).column.get());

            // Verify filtered data for col1
            for (size_t i = 0; i < 8; ++i) {
                size_t original_row = (i < 4) ? i + 1 : i + 2;
                bool expected_null_col1 = original_row % 2;
                EXPECT_EQ(expected_null_col1, filtered_col1->is_null_at(i));
                if (!filtered_col1->is_null_at(i)) {
                    EXPECT_EQ(original_row, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                                    filtered_col1->get_nested_column_ptr().get())
                                                    ->get_data()[i]);
                }
            }

            // Verify col2 remains unchanged
            for (size_t i = 0; i < 10; ++i) {
                EXPECT_EQ((i + 1) % 2, filtered_col2->is_null_at(i));
                if (!filtered_col2->is_null_at(i)) {
                    EXPECT_EQ(i * 2, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                             filtered_col2->get_nested_column_ptr().get())
                                             ->get_data()[i]);
                }
            }
        }

        // Test filter_block with nullable filter column
        {
            auto test_block = create_test_block(10);

            // Create nullable filter column
            auto nullable_filter = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 1),
                    vectorized::ColumnVector<vectorized::UInt8>::create(10, 0));
            auto filter_type = std::make_shared<vectorized::DataTypeNullable>(
                    std::make_shared<vectorized::DataTypeUInt8>());

            test_block.insert({nullable_filter->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(10, test_block.rows()); // All rows kept

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({nullable_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(10, test_block2.rows()); // All rows kept
        }

        // Test filter_block with const filter column
        {
            auto test_block = create_test_block(10);

            // Create const filter column (false)
            auto const_filter = vectorized::ColumnConst::create(
                    vectorized::ColumnVector<vectorized::UInt8>::create(1, 0), 10);
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            test_block.insert({const_filter->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(0, test_block.rows()); // All rows filtered out

            // Test three-parameter version
            auto test_block2 = create_test_block(10);
            test_block2.insert({const_filter->get_ptr(), filter_type, "filter"});
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block2, 2, 2).ok());
            EXPECT_EQ(0, test_block2.rows()); // All rows filtered out
        }

        // Test filter_block with regular filter column
        {
            auto test_block = create_test_block(10);

            // Create regular filter column
            auto filter_column = vectorized::ColumnVector<vectorized::UInt8>::create();
            for (size_t i = 0; i < 10; ++i) {
                filter_column->insert_value(i % 2); // Keep odd-indexed rows
            }
            auto filter_type = std::make_shared<vectorized::DataTypeUInt8>();

            test_block.insert({filter_column->get_ptr(), filter_type, "filter"});

            // Test four-parameter version
            std::vector<uint32_t> columns_to_filter = {0, 1};
            EXPECT_TRUE(vectorized::Block::filter_block(&test_block, columns_to_filter, 2, 2).ok());
            EXPECT_EQ(5, test_block.rows()); // Half rows kept

            // Verify filtered data
            const auto* filtered_col1 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(0).column.get());
            const auto* filtered_col2 = assert_cast<const vectorized::ColumnNullable*>(
                    test_block.get_by_position(1).column.get());

            for (size_t i = 0; i < 5; ++i) {
                size_t original_row = i * 2 + 1;
                EXPECT_EQ(original_row % 2, filtered_col1->is_null_at(i));
                EXPECT_EQ((original_row + 1) % 2, filtered_col2->is_null_at(i));

                if (!filtered_col1->is_null_at(i)) {
                    EXPECT_EQ(original_row, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                                    filtered_col1->get_nested_column_ptr().get())
                                                    ->get_data()[i]);
                }
                if (!filtered_col2->is_null_at(i)) {
                    EXPECT_EQ(original_row * 2,
                              assert_cast<const vectorized::ColumnVector<Int32>*>(
                                      filtered_col2->get_nested_column_ptr().get())
                                      ->get_data()[i]);
                }
            }
        }
        // Test append_to_block_by_selector
        {
            auto block = create_test_block(10);

            // Create destination block with proper columns
            auto base_type = std::make_shared<vectorized::DataTypeInt32>();
            auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);
            vectorized::Block dst_block;

            // Create nullable columns for destination
            auto dst_col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto dst_col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());

            dst_block.insert({dst_col1->get_ptr(), nullable_type, "nullable_col1"});
            dst_block.insert({dst_col2->get_ptr(), nullable_type, "nullable_col2"});
            vectorized::MutableBlock dst(&dst_block);

            // Create selector to select specific rows
            vectorized::IColumn::Selector selector(5);
            for (size_t i = 0; i < 5; ++i) {
                selector[i] = i * 2; // Select rows 0,2,4,6,8
            }

            // Perform selection
            EXPECT_TRUE(block.append_to_block_by_selector(&dst, selector).ok());
            EXPECT_EQ(5, dst.rows());

            // Verify selected data
            const vectorized::Block& result_block = dst.to_block();

            const auto* selected_col1 = assert_cast<const vectorized::ColumnNullable*>(
                    result_block.get_by_position(0).column.get());
            const auto* selected_col2 = assert_cast<const vectorized::ColumnNullable*>(
                    result_block.get_by_position(1).column.get());

            // Verify data and null map for selected rows
            for (size_t i = 0; i < 5; ++i) {
                size_t original_row = i * 2;

                // Verify null flags
                EXPECT_EQ(original_row % 2, selected_col1->is_null_at(i));
                EXPECT_EQ((original_row + 1) % 2, selected_col2->is_null_at(i));

                // Verify values for non-null elements
                if (!selected_col1->is_null_at(i)) {
                    EXPECT_EQ(original_row, assert_cast<const vectorized::ColumnVector<Int32>*>(
                                                    selected_col1->get_nested_column_ptr().get())
                                                    ->get_data()[i]);
                }
                if (!selected_col2->is_null_at(i)) {
                    EXPECT_EQ(original_row * 2,
                              assert_cast<const vectorized::ColumnVector<Int32>*>(
                                      selected_col2->get_nested_column_ptr().get())
                                      ->get_data()[i]);
                }
            }
        }
    }
}

TEST(BlockTest, RowCheck) {
    // Test with empty block
    {
        vectorized::Block empty_block;

        // Test row number check
        EXPECT_NO_THROW(empty_block.check_number_of_rows());
        EXPECT_EQ(0, empty_block.rows());

        // Test clear operations
        empty_block.clear_column_data(1);
        EXPECT_EQ(0, empty_block.columns());

        empty_block.clear();
        EXPECT_EQ(0, empty_block.columns());

        // Test swap operations
        vectorized::Block other_empty_block;
        empty_block.swap(other_empty_block);
        EXPECT_EQ(0, empty_block.columns());
        EXPECT_EQ(0, other_empty_block.columns());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Test row number check with different row counts
        {
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            block.insert({std::move(col1), type, "col1"});

            auto col2 = vectorized::ColumnVector<Int32>::create();
            block.insert({std::move(col2), type, "col2"});

            EXPECT_THROW(block.check_number_of_rows(), Exception);
        }

        // Test clear operations
        {
            block.clear_column_data(1);
            EXPECT_EQ(1, block.columns());

            block.clear();
            EXPECT_EQ(0, block.columns());
        }

        // Test swap operations
        {
            vectorized::Block other_block;
            auto col = vectorized::ColumnVector<Int32>::create();
            col->insert_value(1);
            other_block.insert({std::move(col), type, "col1"});

            block.swap(other_block);
            EXPECT_EQ(1, block.columns());
            EXPECT_EQ(0, other_block.columns());
        }
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Test row number check with const columns
        {
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(42);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 5);
            block.insert({const_col1->get_ptr(), type, "const_col1"});

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(24);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
            block.insert({const_col2->get_ptr(), type, "const_col2"});

            EXPECT_NO_THROW(block.check_number_of_rows());
            EXPECT_EQ(5, block.rows());
        }

        // Test clear operations
        {
            block.clear_column_data(1);
            EXPECT_EQ(1, block.columns());

            block.clear();
            EXPECT_EQ(0, block.columns());
        }

        // Test swap operations
        {
            vectorized::Block other_block;
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(42);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            other_block.insert({const_col->get_ptr(), type, "const_col1"});

            block.swap(other_block);
            EXPECT_EQ(1, block.columns());
            EXPECT_EQ(0, other_block.columns());
        }
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Test row number check with nullable columns
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());

            // Need to cast to concrete type before calling insert_value
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            null_map1->insert_value(0);

            block.insert({col1->get_ptr(), nullable_type, "nullable_col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            block.insert({col2->get_ptr(), nullable_type, "nullable_col2"});

            EXPECT_THROW(block.check_number_of_rows(), Exception);
        }

        // Test clear operations
        {
            block.clear_column_data(1);
            EXPECT_EQ(1, block.columns());

            block.clear();
            EXPECT_EQ(0, block.columns());
        }

        // Test swap operations
        {
            vectorized::Block other_block;
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());

            // Need to cast to concrete type before calling insert_value
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());
            nested->insert_value(1);
            null_map->insert_value(0);

            other_block.insert({col->get_ptr(), nullable_type, "nullable_col1"});

            block.swap(other_block);
            EXPECT_EQ(1, block.columns());
            EXPECT_EQ(0, other_block.columns());
        }
    }
}

TEST(BlockTest, ClearColumnData) {
    // Test with empty block
    {// Test clear with column_size == -1
     {vectorized::Block block;
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(-1);
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());
}

// Test clear with column_size == 0
{
    vectorized::Block block;
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(0);
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());
}

// Test clear with column_size > 0
{
    vectorized::Block block;
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(1);
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());
}

// Test clear after insert empty column
{
    vectorized::Block block;
    auto type = std::make_shared<vectorized::DataTypeInt32>();
    auto col = vectorized::ColumnVector<Int32>::create();
    block.insert({std::move(col), type, "empty_col"});

    EXPECT_EQ(1, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(-1);
    EXPECT_EQ(1, block.columns());
    EXPECT_EQ(0, block.rows());
    EXPECT_EQ(0, block.get_by_position(0).column->size());
}

// Test clear after multiple empty columns
{
    vectorized::Block block;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    for (int i = 0; i < 3; ++i) {
        auto col = vectorized::ColumnVector<Int32>::create();
        block.insert({std::move(col), type, "empty_col" + std::to_string(i)});
    }

    EXPECT_EQ(3, block.columns());
    EXPECT_EQ(0, block.rows());

    // Test clear with different column_size values
    block.clear_column_data(2);
    EXPECT_EQ(2, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(-1);
    EXPECT_EQ(2, block.columns());
    EXPECT_EQ(0, block.rows());

    block.clear_column_data(0);
    EXPECT_EQ(0, block.columns());
    EXPECT_EQ(0, block.rows());
}
} // namespace doris

// Test with regular columns
{
    auto create_test_block = [](int num_columns) {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        for (int i = 0; i < num_columns; ++i) {
            auto col = vectorized::ColumnVector<Int32>::create();
            col->insert_value(i + 1);
            block.insert({std::move(col), type, "col" + std::to_string(i + 1)});
        }
        return block;
    };

    // Test clear with column_size == -1
    {
        auto block = create_test_block(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(1, block.rows());

        block.clear_column_data(-1);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with specific column_size
    {
        auto block = create_test_block(3);
        EXPECT_EQ(3, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with column_size larger than actual size
    {
        auto block = create_test_block(1);
        EXPECT_EQ(1, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
    }
}

// Test with const columns
{
    auto create_test_block = [](int num_columns) {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        for (int i = 0; i < num_columns; ++i) {
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(42 + i);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            block.insert({const_col->get_ptr(), type, "const_col" + std::to_string(i + 1)});
        }
        return block;
    };

    // Test clear with column_size == -1
    {
        auto block = create_test_block(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(5, block.rows());

        block.clear_column_data(-1);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with specific column_size
    {
        auto block = create_test_block(3);
        EXPECT_EQ(3, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with column_size larger than actual size
    {
        auto block = create_test_block(1);
        EXPECT_EQ(1, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
    }
}

// Test with nullable columns
{
    auto create_test_block = [](int num_columns) {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        for (int i = 0; i < num_columns; ++i) {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());

            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());

            nested->insert_value(i + 1);
            null_map->insert_value(i % 2);

            block.insert({col->get_ptr(), nullable_type, "nullable_col" + std::to_string(i + 1)});
        }
        return block;
    };

    // Test clear with column_size == -1
    {
        auto block = create_test_block(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(1, block.rows());

        block.clear_column_data(-1);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with specific column_size
    {
        auto block = create_test_block(3);
        EXPECT_EQ(3, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
        EXPECT_EQ(0, block.get_by_position(1).column->size());
    }

    // Test clear with column_size larger than actual size
    {
        auto block = create_test_block(1);
        EXPECT_EQ(1, block.columns());

        block.clear_column_data(2);
        EXPECT_EQ(1, block.columns());
        EXPECT_EQ(0, block.rows());
        EXPECT_EQ(0, block.get_by_position(0).column->size());
    }
}
}

TEST(BlockTest, IndexByName) {
    // Test with empty block
    {
        vectorized::Block block;
        block.initialize_index_by_name();

        // Test basic name operations
        EXPECT_FALSE(block.has("col1"));
        EXPECT_THROW(block.get_position_by_name("col1"), Exception);
        EXPECT_THROW(block.get_by_name("col1"), Exception);
        EXPECT_EQ(nullptr, block.try_get_by_name("col1"));
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add columns with regular values
        {
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            block.insert({std::move(col1), type, "col1"});

            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(2);
            block.insert({std::move(col2), type, "col2"});

            auto col3 = vectorized::ColumnVector<Int32>::create();
            col3->insert_value(3);
            block.insert({std::move(col3), type, "col1"}); // Duplicate name
        }

        // Test before index initialization
        EXPECT_EQ(0, block.get_position_by_name("col1")); // Returns first occurrence
        EXPECT_EQ(1, block.get_position_by_name("col2"));

        // Test after index initialization
        block.initialize_index_by_name();
        EXPECT_EQ(2, block.get_position_by_name("col1")); // Returns last occurrence
        EXPECT_EQ(1, block.get_position_by_name("col2"));

        // Test has() function
        EXPECT_TRUE(block.has("col1"));
        EXPECT_TRUE(block.has("col2"));
        EXPECT_FALSE(block.has("col3"));

        // Test get_by_name
        const auto& col1 = block.get_by_name("col1");
        EXPECT_EQ(1, col1.column->size());
        EXPECT_THROW(block.get_by_name("col3"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test after structure modification
        block.erase(2); // Remove last "col1"
        block.initialize_index_by_name();
        EXPECT_EQ(0, block.get_position_by_name("col1")); // Now first "col1" is found
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add columns with const values
        {
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(42);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 5);
            block.insert({const_col1->get_ptr(), type, "const_col1"});

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(24);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
            block.insert({const_col2->get_ptr(), type, "const_col2"});

            auto base_col3 = vectorized::ColumnVector<Int32>::create();
            base_col3->insert_value(33);
            auto const_col3 = vectorized::ColumnConst::create(base_col3->get_ptr(), 5);
            block.insert({const_col3->get_ptr(), type, "const_col1"}); // Duplicate name
        }

        // Test before index initialization
        EXPECT_EQ(0, block.get_position_by_name("const_col1")); // Returns first occurrence
        EXPECT_EQ(1, block.get_position_by_name("const_col2"));

        // Test after index initialization
        block.initialize_index_by_name();
        EXPECT_EQ(2, block.get_position_by_name("const_col1")); // Returns last occurrence
        EXPECT_EQ(1, block.get_position_by_name("const_col2"));

        // Test has() function
        EXPECT_TRUE(block.has("const_col1"));
        EXPECT_TRUE(block.has("const_col2"));
        EXPECT_FALSE(block.has("const_col3"));

        // Test get_by_name
        const auto& col1 = block.get_by_name("const_col1");
        EXPECT_EQ(5, col1.column->size());
        EXPECT_THROW(block.get_by_name("const_col3"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("const_col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test after structure modification
        block.erase(2); // Remove last "const_col1"
        block.initialize_index_by_name();
        EXPECT_EQ(0, block.get_position_by_name("const_col1")); // Now first "const_col1" is found
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Add columns with nullable values
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            null_map1->insert_value(0);
            block.insert({col1->get_ptr(), nullable_type, "nullable_col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(2);
            null_map2->insert_value(1);
            block.insert({col2->get_ptr(), nullable_type, "nullable_col2"});

            auto col3 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested3 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col3->get_nested_column_ptr().get());
            auto* null_map3 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col3->get_null_map_column_ptr().get());
            nested3->insert_value(3);
            null_map3->insert_value(0);
            block.insert({col3->get_ptr(), nullable_type, "nullable_col1"}); // Duplicate name
        }

        // Test before index initialization
        EXPECT_EQ(0, block.get_position_by_name("nullable_col1")); // Returns first occurrence
        EXPECT_EQ(1, block.get_position_by_name("nullable_col2"));

        // Test after index initialization
        block.initialize_index_by_name();
        EXPECT_EQ(2, block.get_position_by_name("nullable_col1")); // Returns last occurrence
        EXPECT_EQ(1, block.get_position_by_name("nullable_col2"));

        // Test has() function
        EXPECT_TRUE(block.has("nullable_col1"));
        EXPECT_TRUE(block.has("nullable_col2"));
        EXPECT_FALSE(block.has("nullable_col3"));

        // Test get_by_name
        const auto& col1 = block.get_by_name("nullable_col1");
        EXPECT_EQ(1, col1.column->size());
        EXPECT_THROW(block.get_by_name("nullable_col3"), Exception);

        // Test try_get_by_name
        EXPECT_NE(nullptr, block.try_get_by_name("nullable_col1"));
        EXPECT_EQ(nullptr, block.try_get_by_name("non_existent"));

        // Test after structure modification
        block.erase(2); // Remove last "nullable_col1"
        block.initialize_index_by_name();
        EXPECT_EQ(0, block.get_position_by_name(
                             "nullable_col1")); // Now first "nullable_col1" is found
    }
}

TEST(BlockTest, ColumnTransformations) {
    // Test with empty block
    {
        vectorized::Block block;
        std::vector<int> positions = {};
        block.shuffle_columns(positions);
        EXPECT_EQ(0, block.columns());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Insert columns with regular values
        {
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            block.insert({std::move(col1), type, "col1"});

            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(2);
            block.insert({std::move(col2), type, "col2"});
        }

        // Verify initial order
        EXPECT_EQ("col1", block.get_by_position(0).name);
        EXPECT_EQ("col2", block.get_by_position(1).name);

        // Test shuffle_columns
        std::vector<int> positions = {1, 0}; // change the order of columns
        block.shuffle_columns(positions);

        // Verify shuffled order
        EXPECT_EQ("col2", block.get_by_position(0).name);
        EXPECT_EQ("col1", block.get_by_position(1).name);

        // Verify column data is correctly shuffled
        const auto* col1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                block.get_by_position(1).column.get());
        const auto* col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                block.get_by_position(0).column.get());

        EXPECT_EQ(1, col1->get_data()[0]);
        EXPECT_EQ(2, col2->get_data()[0]);
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Insert columns with const values
        {
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(42);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 5);
            block.insert({const_col1->get_ptr(), type, "const_col1"});

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(24);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
            block.insert({const_col2->get_ptr(), type, "const_col2"});
        }

        // Verify initial order
        EXPECT_EQ("const_col1", block.get_by_position(0).name);
        EXPECT_EQ("const_col2", block.get_by_position(1).name);

        // Test shuffle_columns
        std::vector<int> positions = {1, 0};
        block.shuffle_columns(positions);

        // Verify shuffled order
        EXPECT_EQ("const_col2", block.get_by_position(0).name);
        EXPECT_EQ("const_col1", block.get_by_position(1).name);

        // Verify const values are preserved
        const auto* col1 =
                assert_cast<const vectorized::ColumnConst*>(block.get_by_position(1).column.get());
        const auto* col2 =
                assert_cast<const vectorized::ColumnConst*>(block.get_by_position(0).column.get());

        EXPECT_EQ(42, col1->get_int(0));
        EXPECT_EQ(24, col2->get_int(0));
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Insert columns with nullable values
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            null_map1->insert_value(0);
            block.insert({col1->get_ptr(), nullable_type, "nullable_col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(2);
            null_map2->insert_value(1);
            block.insert({col2->get_ptr(), nullable_type, "nullable_col2"});
        }

        // Verify initial order
        EXPECT_EQ("nullable_col1", block.get_by_position(0).name);
        EXPECT_EQ("nullable_col2", block.get_by_position(1).name);

        // Test shuffle_columns
        std::vector<int> positions = {1, 0};
        block.shuffle_columns(positions);

        // Verify shuffled order
        EXPECT_EQ("nullable_col2", block.get_by_position(0).name);
        EXPECT_EQ("nullable_col1", block.get_by_position(1).name);

        // Verify nullable values and null states are preserved
        const auto* col1 = assert_cast<const vectorized::ColumnNullable*>(
                block.get_by_position(1).column.get());
        const auto* col2 = assert_cast<const vectorized::ColumnNullable*>(
                block.get_by_position(0).column.get());

        EXPECT_FALSE(col1->is_null_at(0));
        EXPECT_TRUE(col2->is_null_at(0));

        const auto* nested1 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                col1->get_nested_column_ptr().get());
        const auto* nested2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                col2->get_nested_column_ptr().get());

        EXPECT_EQ(1, nested1->get_data()[0]);
        EXPECT_EQ(2, nested2->get_data()[0]);
    }
}

TEST(BlockTest, HashUpdate) {
    // Test with empty block
    {// Single empty block
     {vectorized::Block empty_block;
    SipHash hash1;
    empty_block.update_hash(hash1);
    uint64_t hash1_value = hash1.get64();

    // Same empty block should produce same hash
    SipHash hash2;
    empty_block.update_hash(hash2);
    EXPECT_EQ(hash1_value, hash2.get64());
}

// Multiple empty blocks
{
    vectorized::Block block1;
    vectorized::Block block2;

    SipHash hash1, hash2;
    block1.update_hash(hash1);
    block2.update_hash(hash2);
    EXPECT_EQ(hash1.get64(), hash2.get64());
}
}

// Test with regular columns
{// Single column with single value
 {vectorized::Block block;
auto type = std::make_shared<vectorized::DataTypeInt32>();

auto col = vectorized::ColumnVector<Int32>::create();
col->insert_value(42);
block.insert({std::move(col), type, "col1"});

SipHash hash1;
block.update_hash(hash1);
uint64_t hash1_value = hash1.get64();

// Same data should produce same hash
SipHash hash2;
block.update_hash(hash2);
EXPECT_EQ(hash1_value, hash2.get64());
}

// Multiple columns
{
    vectorized::Block block1;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    // Create first block with values [1, 2]
    {
        auto col1 = vectorized::ColumnVector<Int32>::create();
        col1->insert_value(1);
        block1.insert({std::move(col1), type, "col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        col2->insert_value(2);
        block1.insert({std::move(col2), type, "col2"});
    }

    // Create second block with values [2, 1]
    vectorized::Block block2;
    {
        auto col1 = vectorized::ColumnVector<Int32>::create();
        col1->insert_value(2);
        block2.insert({std::move(col1), type, "col1"});

        auto col2 = vectorized::ColumnVector<Int32>::create();
        col2->insert_value(1);
        block2.insert({std::move(col2), type, "col2"});
    }

    // Different order of same values should produce different hash
    SipHash hash1, hash2;
    block1.update_hash(hash1);
    block2.update_hash(hash2);
    EXPECT_NE(hash1.get64(), hash2.get64());
}

// Multiple rows
{
    vectorized::Block block1;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    // Create first block with ascending values
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 5; ++i) {
            col->insert_value(i);
        }
        block1.insert({std::move(col), type, "col1"});
    }

    // Create second block with descending values
    vectorized::Block block2;
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        for (int i = 4; i >= 0; --i) {
            col->insert_value(i);
        }
        block2.insert({std::move(col), type, "col1"});
    }

    // Different order of same values should produce different hash
    SipHash hash1, hash2;
    block1.update_hash(hash1);
    block2.update_hash(hash2);
    EXPECT_NE(hash1.get64(), hash2.get64());
}
}

// Test with const columns
{// Single column with single value
 {vectorized::Block block;
auto type = std::make_shared<vectorized::DataTypeInt32>();

auto base_col = vectorized::ColumnVector<Int32>::create();
base_col->insert_value(42);
auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
block.insert({const_col->get_ptr(), type, "const_col"});

SipHash hash1;
block.update_hash(hash1);
uint64_t hash1_value = hash1.get64();

// Same data should produce same hash
SipHash hash2;
block.update_hash(hash2);
EXPECT_EQ(hash1_value, hash2.get64());
}

// Multiple columns
{
    vectorized::Block block1;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    // Create first block with const values [1, 2]
    {
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(1);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 5);
        block1.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(2);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
        block1.insert({const_col2->get_ptr(), type, "const_col2"});
    }

    // Create second block with const values [2, 1]
    vectorized::Block block2;
    {
        auto base_col1 = vectorized::ColumnVector<Int32>::create();
        base_col1->insert_value(2);
        auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 5);
        block2.insert({const_col1->get_ptr(), type, "const_col1"});

        auto base_col2 = vectorized::ColumnVector<Int32>::create();
        base_col2->insert_value(1);
        auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 5);
        block2.insert({const_col2->get_ptr(), type, "const_col2"});
    }

    // Different order of same values should produce different hash
    SipHash hash1, hash2;
    block1.update_hash(hash1);
    block2.update_hash(hash2);
    EXPECT_NE(hash1.get64(), hash2.get64());
}

// Multiple rows (same value repeated)
{
    vectorized::Block block1;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    auto base_col = vectorized::ColumnVector<Int32>::create();
    base_col->insert_value(42);
    auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
    block1.insert({const_col->get_ptr(), type, "const_col"});

    // Create second block with same value but different row count
    vectorized::Block block2;
    auto base_col2 = vectorized::ColumnVector<Int32>::create();
    base_col2->insert_value(42);
    auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 3);
    block2.insert({const_col2->get_ptr(), type, "const_col"});

    // Different row counts should produce different hash
    SipHash hash1, hash2;
    block1.update_hash(hash1);
    block2.update_hash(hash2);
    EXPECT_NE(hash1.get64(), hash2.get64());
}
}

// Test with nullable columns
{
    // Single column with single value
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        auto col = vectorized::ColumnNullable::create(
                vectorized::ColumnVector<Int32>::create(),
                vectorized::ColumnVector<vectorized::UInt8>::create());
        auto* nested =
                assert_cast<vectorized::ColumnVector<Int32>*>(col->get_nested_column_ptr().get());
        auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                col->get_null_map_column_ptr().get());
        nested->insert_value(42);
        null_map->insert_value(0);
        block.insert({col->get_ptr(), nullable_type, "nullable_col"});

        SipHash hash1;
        block.update_hash(hash1);
        uint64_t hash1_value = hash1.get64();

        // Same data should produce same hash
        SipHash hash2;
        block.update_hash(hash2);
        EXPECT_EQ(hash1_value, hash2.get64());
    }

    // Multiple columns
    {
        vectorized::Block block1;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Create first block with values [1(not null), 2(null)]
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            null_map1->insert_value(0);
            block1.insert({col1->get_ptr(), nullable_type, "nullable_col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(2);
            null_map2->insert_value(1);
            block1.insert({col2->get_ptr(), nullable_type, "nullable_col2"});
        }

        // Create second block with values [2(null), 1(not null)]
        vectorized::Block block2;
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(2);
            null_map1->insert_value(1);
            block2.insert({col1->get_ptr(), nullable_type, "nullable_col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(1);
            null_map2->insert_value(0);
            block2.insert({col2->get_ptr(), nullable_type, "nullable_col2"});
        }

        // Different order of same values should produce different hash
        SipHash hash1, hash2;
        block1.update_hash(hash1);
        block2.update_hash(hash2);
        EXPECT_NE(hash1.get64(), hash2.get64());
    }

    // Multiple rows
    {
        vectorized::Block block1;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Create first block with ascending values and alternating null flags
        {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());

            for (int i = 0; i < 5; ++i) {
                nested->insert_value(i);
                null_map->insert_value(i % 2);
            }
            block1.insert({col->get_ptr(), nullable_type, "nullable_col"});
        }

        // Create second block with descending values and alternating null flags
        vectorized::Block block2;
        {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());

            for (int i = 4; i >= 0; --i) {
                nested->insert_value(i);
                null_map->insert_value(i % 2);
            }
            block2.insert({col->get_ptr(), nullable_type, "nullable_col"});
        }

        // Different order of same values should produce different hash
        SipHash hash1, hash2;
        block1.update_hash(hash1);
        block2.update_hash(hash2);
        EXPECT_NE(hash1.get64(), hash2.get64());
    }
}
}

TEST(BlockTest, EraseUselessColumn) {
    // Test with empty block
    {
        vectorized::Block block;
        vectorized::Block::erase_useless_column(&block, 0);
        EXPECT_EQ(0, block.columns());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Insert three columns
        for (int i = 1; i <= 3; ++i) {
            auto col = vectorized::ColumnVector<Int32>::create();
            col->insert_value(i);
            block.insert({std::move(col), type, "col" + std::to_string(i)});
        }

        EXPECT_EQ(3, block.columns());
        vectorized::Block::erase_useless_column(&block, 2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ("col1", block.get_by_position(0).name);
        EXPECT_EQ("col2", block.get_by_position(1).name);
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Insert three const columns
        for (int i = 1; i <= 3; ++i) {
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(i);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            block.insert({const_col->get_ptr(), type, "const_col" + std::to_string(i)});
        }

        EXPECT_EQ(3, block.columns());
        vectorized::Block::erase_useless_column(&block, 2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ("const_col1", block.get_by_position(0).name);
        EXPECT_EQ("const_col2", block.get_by_position(1).name);
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Insert three nullable columns
        for (int i = 1; i <= 3; ++i) {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());
            nested->insert_value(i);
            null_map->insert_value(i % 2);
            block.insert({col->get_ptr(), nullable_type, "nullable_col" + std::to_string(i)});
        }

        EXPECT_EQ(3, block.columns());
        vectorized::Block::erase_useless_column(&block, 2);
        EXPECT_EQ(2, block.columns());
        EXPECT_EQ("nullable_col1", block.get_by_position(0).name);
        EXPECT_EQ("nullable_col2", block.get_by_position(1).name);
    }
}

TEST(BlockTest, CompareAt) {
    // Test with empty blocks
    {
        vectorized::Block block1, block2;

        // Test basic compare_at
        EXPECT_EQ(0, block1.compare_at(0, 0, block2, 0));
        EXPECT_DEATH(block1.compare_at(1, 1, block2, 0), "");

        // Test compare_at with num_columns
        EXPECT_DEATH(block1.compare_at(0, 0, 1, block2, 0), "");

        // Test compare_at with specific columns
        std::vector<uint32_t> compare_cols = {0};
        EXPECT_DEATH(block1.compare_at(0, 0, &compare_cols, block2, 0), "");
    }

    // Test with regular columns
    {
        vectorized::Block block1, block2;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create first block with ascending values
        {
            // First column: [1, 2]
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            col1->insert_value(2);
            block1.insert({std::move(col1), type, "col1"});

            // Second column: [3, 4]
            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(3);
            col2->insert_value(4);
            block1.insert({std::move(col2), type, "col2"});
        }

        // Create second block with different values
        {
            // First column: [1, 3]
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            col1->insert_value(3);
            block2.insert({std::move(col1), type, "col1"});

            // Second column: [3, 5]
            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(3);
            col2->insert_value(5);
            block2.insert({std::move(col2), type, "col2"});
        }

        // Test basic compare_at
        EXPECT_EQ(0, block1.compare_at(0, 0, block2, 0)); // Equal rows
        EXPECT_LT(block1.compare_at(0, 1, block2, 1), 0); // [1,3] < [3,5]
        EXPECT_GT(block1.compare_at(1, 0, block2, 0), 0); // [2,4] > [1,3]

        // Test compare_at with num_columns
        EXPECT_EQ(0, block1.compare_at(0, 0, 1, block2, 0)); // Compare only first column

        // Test compare_at with specific columns
        std::vector<uint32_t> compare_cols = {1}; // Compare only second column
        EXPECT_EQ(0, block1.compare_at(0, 0, &compare_cols, block2, 0));
    }

    // Test with const columns
    {
        vectorized::Block block1, block2;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create first block with const columns
        {
            // First column: const(1)
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(1);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 2);
            block1.insert({const_col1->get_ptr(), type, "col1"});

            // Second column: const(2)
            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(2);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 2);
            block1.insert({const_col2->get_ptr(), type, "col2"});
        }

        // Create second block with different const values
        {
            // First column: const(1)
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(1);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 2);
            block2.insert({const_col1->get_ptr(), type, "col1"});

            // Second column: const(3)
            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(3);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 2);
            block2.insert({const_col2->get_ptr(), type, "col2"});
        }

        // Test basic compare_at
        EXPECT_EQ(-1, block1.compare_at(0, 0, block2, 0));
        EXPECT_LT(block1.compare_at(0, 1, block2, 1), 0);
        EXPECT_EQ(-1, block1.compare_at(1, 0, block2, 0));

        // Test compare_at with num_columns
        EXPECT_EQ(0, block1.compare_at(0, 0, 1, block2, 0)); // Compare only first column

        // Test compare_at with specific columns
        std::vector<uint32_t> compare_cols = {1}; // Compare only second column
        EXPECT_LT(block1.compare_at(0, 0, &compare_cols, block2, 0), 0); // const(2) < const(3)
    }

    // Test with nullable columns
    {
        vectorized::Block block1, block2;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Create first block with nullable columns
        {
            // First column: [1, null]
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            nested1->insert_value(2);
            null_map1->insert_value(0); // not null
            null_map1->insert_value(1); // null
            block1.insert({col1->get_ptr(), nullable_type, "col1"});

            // Second column: [null, 4]
            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(3);
            nested2->insert_value(4);
            null_map2->insert_value(1); // null
            null_map2->insert_value(0); // not null
            block1.insert({col2->get_ptr(), nullable_type, "col2"});
        }

        // Create second block with different nullable values
        {
            // First column: [1, 3]
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            nested1->insert_value(3);
            null_map1->insert_value(0); // not null
            null_map1->insert_value(0); // not null
            block2.insert({col1->get_ptr(), nullable_type, "col1"});

            // Second column: [3, null]
            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(3);
            nested2->insert_value(5);
            null_map2->insert_value(0); // not null
            null_map2->insert_value(1); // null
            block2.insert({col2->get_ptr(), nullable_type, "col2"});
        }

        // Test basic compare_at
        EXPECT_EQ(0, block1.compare_at(0, 0, block2, 0)); // Equal non-null values
        EXPECT_LT(block1.compare_at(0, 1, block2, 1), 0); // null < non-null
        EXPECT_GT(block1.compare_at(1, 0, block2, 0), 0); // non-null > null

        // Test compare_at with num_columns
        EXPECT_EQ(0, block1.compare_at(0, 0, 1, block2, 0)); // Compare only first column

        // Test compare_at with specific columns
        std::vector<uint32_t> compare_cols = {1}; // Compare only second column
        EXPECT_EQ(0, block1.compare_at(0, 0, &compare_cols, block2, 0)); // null > non-null
    }
}

TEST(BlockTest, CompareColumnAt) {
    // Test with empty blocks
    {
        vectorized::Block block1, block2;
        EXPECT_DEATH(block1.compare_column_at(0, 0, 0, block2, 0), ""); // 空块不应该比较列
    }

    // Test with regular columns
    {
        vectorized::Block block1, block2;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create first block with ascending values
        {
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            col1->insert_value(2);
            block1.insert({std::move(col1), type, "col1"});

            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(3);
            col2->insert_value(4);
            block1.insert({std::move(col2), type, "col2"});
        }

        // Create second block with different values
        {
            auto col1 = vectorized::ColumnVector<Int32>::create();
            col1->insert_value(1);
            col1->insert_value(3);
            block2.insert({std::move(col1), type, "col1"});

            auto col2 = vectorized::ColumnVector<Int32>::create();
            col2->insert_value(3);
            col2->insert_value(5);
            block2.insert({std::move(col2), type, "col2"});
        }

        // Test compare_column_at for each column
        EXPECT_EQ(0, block1.compare_column_at(0, 0, 0, block2, 0)); // First column, equal values
        EXPECT_LT(block1.compare_column_at(0, 1, 0, block2, 1), 0); // First column, 2 < 3
        EXPECT_EQ(0, block1.compare_column_at(0, 0, 1, block2, 0)); // Second column, equal values
        EXPECT_LT(block1.compare_column_at(0, 1, 1, block2, 1), 0); // Second column, 4 < 5
    }

    // Test with const columns
    {
        vectorized::Block block1, block2;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create first block with const columns
        {
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(1);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 2);
            block1.insert({const_col1->get_ptr(), type, "col1"});

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(2);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 2);
            block1.insert({const_col2->get_ptr(), type, "col2"});
        }

        // Create second block with different const values
        {
            auto base_col1 = vectorized::ColumnVector<Int32>::create();
            base_col1->insert_value(1);
            auto const_col1 = vectorized::ColumnConst::create(base_col1->get_ptr(), 2);
            block2.insert({const_col1->get_ptr(), type, "col1"});

            auto base_col2 = vectorized::ColumnVector<Int32>::create();
            base_col2->insert_value(3);
            auto const_col2 = vectorized::ColumnConst::create(base_col2->get_ptr(), 2);
            block2.insert({const_col2->get_ptr(), type, "col2"});
        }

        // Test compare_column_at for each column
        EXPECT_EQ(0, block1.compare_column_at(0, 0, 0, block2, 0)); // First column, equal values
        EXPECT_EQ(0, block1.compare_column_at(1, 1, 0, block2, 1)); // First column, equal values
        EXPECT_LT(block1.compare_column_at(0, 0, 1, block2, 0), 0); // Second column, 2 < 3
        EXPECT_LT(block1.compare_column_at(1, 1, 1, block2, 1), 0); // Second column, 2 < 3
    }

    // Test with nullable columns
    {
        vectorized::Block block1, block2;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Create first block with nullable columns
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            nested1->insert_value(2);
            null_map1->insert_value(0); // not null
            null_map1->insert_value(1); // null
            block1.insert({col1->get_ptr(), nullable_type, "col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(3);
            nested2->insert_value(4);
            null_map2->insert_value(1); // null
            null_map2->insert_value(0); // not null
            block1.insert({col2->get_ptr(), nullable_type, "col2"});
        }

        // Create second block with different nullable values
        {
            auto col1 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested1 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col1->get_nested_column_ptr().get());
            auto* null_map1 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col1->get_null_map_column_ptr().get());
            nested1->insert_value(1);
            nested1->insert_value(3);
            null_map1->insert_value(0); // not null
            null_map1->insert_value(0); // not null
            block2.insert({col1->get_ptr(), nullable_type, "col1"});

            auto col2 = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested2 = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col2->get_nested_column_ptr().get());
            auto* null_map2 = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col2->get_null_map_column_ptr().get());
            nested2->insert_value(3);
            nested2->insert_value(5);
            null_map2->insert_value(0); // not null
            null_map2->insert_value(1); // null
            block2.insert({col2->get_ptr(), nullable_type, "col2"});
        }

        // Test compare_column_at for each column
        EXPECT_EQ(0, block1.compare_column_at(0, 0, 0, block2,
                                              0)); // First column, equal non-null values
        EXPECT_GT(block1.compare_column_at(1, 1, 0, block2, 1), 0); // First column, null < non-null
        EXPECT_EQ(block1.compare_column_at(0, 0, 1, block2, 0),
                  0); // Second column, non-null > null
        EXPECT_LT(block1.compare_column_at(1, 1, 1, block2, 1),
                  0); // Second column, non-null < null
    }
}

TEST(BlockTest, SameBitOperations) {
    // Test with empty block
    {
        vectorized::Block block;
        std::vector<bool> same_bits = {};
        block.set_same_bit(same_bits.begin(), same_bits.end());
        EXPECT_FALSE(block.get_same_bit(0));
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create block with data
        auto col = vectorized::ColumnVector<Int32>::create();
        for (int i = 0; i < 3; ++i) {
            col->insert_value(i);
        }
        block.insert({std::move(col), type, "col1"});

        // Test set_same_bit
        std::vector<bool> same_bits = {true, false, true};
        block.set_same_bit(same_bits.begin(), same_bits.end());

        // Test get_same_bit
        EXPECT_TRUE(block.get_same_bit(0));
        EXPECT_FALSE(block.get_same_bit(1));
        EXPECT_TRUE(block.get_same_bit(2));
        EXPECT_FALSE(block.get_same_bit(3)); // Out of range

        // Test clear_same_bit
        block.clear_same_bit();
        for (int i = 0; i < 3; ++i) {
            EXPECT_FALSE(block.get_same_bit(i));
        }
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Create const column
        auto base_col = vectorized::ColumnVector<Int32>::create();
        base_col->insert_value(1);
        auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 3);
        block.insert({const_col->get_ptr(), type, "const_col"});

        // Test set_same_bit
        std::vector<bool> same_bits = {true, false, true};
        block.set_same_bit(same_bits.begin(), same_bits.end());

        // Test get_same_bit
        EXPECT_TRUE(block.get_same_bit(0));
        EXPECT_FALSE(block.get_same_bit(1));
        EXPECT_TRUE(block.get_same_bit(2));

        // Test clear_same_bit
        block.clear_same_bit();
        for (int i = 0; i < 3; ++i) {
            EXPECT_FALSE(block.get_same_bit(i));
        }
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Create nullable column
        auto col = vectorized::ColumnNullable::create(
                vectorized::ColumnVector<Int32>::create(),
                vectorized::ColumnVector<vectorized::UInt8>::create());
        for (int i = 0; i < 3; ++i) {
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());
            nested->insert_value(i);
            null_map->insert_value(i % 2);
        }
        block.insert({col->get_ptr(), nullable_type, "nullable_col"});

        // Test set_same_bit
        std::vector<bool> same_bits = {true, false, true};
        block.set_same_bit(same_bits.begin(), same_bits.end());

        // Test get_same_bit
        EXPECT_TRUE(block.get_same_bit(0));
        EXPECT_FALSE(block.get_same_bit(1));
        EXPECT_TRUE(block.get_same_bit(2));

        // Test clear_same_bit
        block.clear_same_bit();
        for (int i = 0; i < 3; ++i) {
            EXPECT_FALSE(block.get_same_bit(i));
        }
    }
}

TEST(BlockTest, CreateSameStructBlock) {
    // Test with empty block
    {// Test case 1: with default values (is_reserve = false)
     {vectorized::Block original_block;
    auto new_block = original_block.create_same_struct_block(5, false);
    EXPECT_EQ(0, new_block->columns());
    EXPECT_EQ(0, new_block->rows());
}

// Test case 2: with reserved space (is_reserve = true)
{
    vectorized::Block original_block;
    auto new_block = original_block.create_same_struct_block(5, true);
    EXPECT_EQ(0, new_block->columns());
    EXPECT_EQ(0, new_block->rows());
}
}

// Test with regular columns
{
    vectorized::Block original_block;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    // Create original block with data
    {
        auto col = vectorized::ColumnVector<Int32>::create();
        col->insert_value(1);
        original_block.insert({std::move(col), type, "col1"});
    }

    // Test case 1: with default values (is_reserve = false)
    {
        auto new_block = original_block.create_same_struct_block(5, false);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(5, new_block->rows()); // Should have 5 default values
        EXPECT_EQ("col1", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*type));

        // Verify default values are inserted
        const auto* col = assert_cast<const vectorized::ColumnVector<Int32>*>(
                new_block->get_by_position(0).column.get());
        for (size_t i = 0; i < 5; ++i) {
            EXPECT_EQ(0, col->get_data()[i]); // Default value for Int32 is 0
        }
    }

    // Test case 2: with reserved space (is_reserve = true)
    {
        auto new_block = original_block.create_same_struct_block(5, true);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(0, new_block->rows()); // Should be empty but with reserved space
        EXPECT_EQ("col1", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*type));
    }
}

// Test with const columns
{
    vectorized::Block original_block;
    auto type = std::make_shared<vectorized::DataTypeInt32>();

    // Create original block with data
    {
        auto base_col = vectorized::ColumnVector<Int32>::create();
        base_col->insert_value(42);
        auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 1);
        original_block.insert({const_col->get_ptr(), type, "const_col"});
    }

    // Test case 1: with default values (is_reserve = false)
    {
        auto new_block = original_block.create_same_struct_block(5, false);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(5, new_block->rows()); // Should have 5 default values
        EXPECT_EQ("const_col", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*type));

        // Verify default values are inserted
        const auto* col = assert_cast<const vectorized::ColumnVector<Int32>*>(
                new_block->get_by_position(0).column.get());
        for (size_t i = 0; i < 5; ++i) {
            EXPECT_EQ(0, col->get_data()[i]); // Default value for Int32 is 0
        }
    }

    // Test case 2: with reserved space (is_reserve = true)
    {
        auto new_block = original_block.create_same_struct_block(5, true);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(0, new_block->rows()); // Should be empty but with reserved space
        EXPECT_EQ("const_col", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*type));
    }
}

// Test with nullable columns
{
    vectorized::Block original_block;
    auto base_type = std::make_shared<vectorized::DataTypeInt32>();
    auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

    // Create original block with data
    {
        auto col = vectorized::ColumnNullable::create(
                vectorized::ColumnVector<Int32>::create(),
                vectorized::ColumnVector<vectorized::UInt8>::create());
        auto* nested =
                assert_cast<vectorized::ColumnVector<Int32>*>(col->get_nested_column_ptr().get());
        auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                col->get_null_map_column_ptr().get());
        nested->insert_value(1);
        null_map->insert_value(0);
        original_block.insert({col->get_ptr(), nullable_type, "nullable_col"});
    }

    // Test case 1: with default values (is_reserve = false)
    {
        auto new_block = original_block.create_same_struct_block(5, false);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(5, new_block->rows()); // Should have 5 default values
        EXPECT_EQ("nullable_col", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*nullable_type));

        // Verify default values are inserted
        const auto* col = assert_cast<const vectorized::ColumnNullable*>(
                new_block->get_by_position(0).column.get());
        const auto* nested = assert_cast<const vectorized::ColumnVector<Int32>*>(
                col->get_nested_column_ptr().get());
        const auto* null_map = assert_cast<const vectorized::ColumnVector<vectorized::UInt8>*>(
                col->get_null_map_column_ptr().get());
        for (size_t i = 0; i < 5; ++i) {
            EXPECT_EQ(0, nested->get_data()[i]);   // Default value for Int32 is 0
            EXPECT_EQ(1, null_map->get_data()[i]); // Default is null
        }
    }

    // Test case 2: with reserved space (is_reserve = true)
    {
        auto new_block = original_block.create_same_struct_block(5, true);
        EXPECT_EQ(original_block.columns(), new_block->columns());
        EXPECT_EQ(0, new_block->rows()); // Should be empty but with reserved space
        EXPECT_EQ("nullable_col", new_block->get_by_position(0).name);
        EXPECT_TRUE(new_block->get_by_position(0).type->equals(*nullable_type));
    }
}
}

TEST(BlockTest, EraseTmpColumns) {
    // Test with empty block
    {
        vectorized::Block block;
        block.erase_tmp_columns();
        EXPECT_EQ(0, block.columns());
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add regular columns
        for (int i = 1; i <= 3; ++i) {
            auto col = vectorized::ColumnVector<Int32>::create();
            col->insert_value(i);
            block.insert({std::move(col), type, "normal_col" + std::to_string(i)});
        }

        // Add temporary columns
        for (int i = 1; i <= 2; ++i) {
            auto col = vectorized::ColumnVector<Int32>::create();
            col->insert_value(i + 10);
            block.insert({std::move(col), type,
                          std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_col" +
                                  std::to_string(i)});
        }

        EXPECT_EQ(5, block.columns());
        block.erase_tmp_columns();
        EXPECT_EQ(3, block.columns());

        // Verify regular columns are kept
        for (int i = 1; i <= 3; ++i) {
            std::string col_name = "normal_col" + std::to_string(i);
            EXPECT_TRUE(block.has(col_name));
            EXPECT_EQ(col_name, block.get_by_position(i - 1).name);
        }

        // Verify temporary columns are removed
        for (int i = 1; i <= 2; ++i) {
            EXPECT_FALSE(block.has(std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_col" +
                                   std::to_string(i)));
        }
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add regular const columns
        for (int i = 1; i <= 2; ++i) {
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(i);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            block.insert({const_col->get_ptr(), type, "const_col" + std::to_string(i)});
        }

        // Add temporary const columns
        for (int i = 1; i <= 2; ++i) {
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(i + 10);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            block.insert({const_col->get_ptr(), type,
                          std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_const" +
                                  std::to_string(i)});
        }

        EXPECT_EQ(4, block.columns());
        block.erase_tmp_columns();
        EXPECT_EQ(2, block.columns());

        // Verify regular const columns are kept
        for (int i = 1; i <= 2; ++i) {
            std::string col_name = "const_col" + std::to_string(i);
            EXPECT_TRUE(block.has(col_name));
            EXPECT_EQ(col_name, block.get_by_position(i - 1).name);
        }

        // Verify temporary const columns are removed
        for (int i = 1; i <= 2; ++i) {
            EXPECT_FALSE(block.has(std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_const" +
                                   std::to_string(i)));
        }
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Add regular nullable columns
        for (int i = 1; i <= 2; ++i) {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());
            nested->insert_value(i);
            null_map->insert_value(i % 2);
            block.insert({col->get_ptr(), nullable_type, "nullable_col" + std::to_string(i)});
        }

        // Add temporary nullable columns
        for (int i = 1; i <= 2; ++i) {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());
            nested->insert_value(i + 10);
            null_map->insert_value((i + 1) % 2);
            block.insert({col->get_ptr(), nullable_type,
                          std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_null" +
                                  std::to_string(i)});
        }

        EXPECT_EQ(4, block.columns());
        block.erase_tmp_columns();
        EXPECT_EQ(2, block.columns());

        // Verify regular nullable columns are kept
        for (int i = 1; i <= 2; ++i) {
            std::string col_name = "nullable_col" + std::to_string(i);
            EXPECT_TRUE(block.has(col_name));
            EXPECT_EQ(col_name, block.get_by_position(i - 1).name);
        }

        // Verify temporary nullable columns are removed
        for (int i = 1; i <= 2; ++i) {
            EXPECT_FALSE(block.has(std::string(BeConsts::BLOCK_TEMP_COLUMN_PREFIX) + "tmp_null" +
                                   std::to_string(i)));
        }
    }
}

TEST(BlockTest, ClearColumnMemNotKeep) {
    // Test with empty block
    {
        vectorized::Block block;
        std::vector<bool> keep_flags;
        EXPECT_DEATH(block.clear_column_mem_not_keep(keep_flags, true), "");
    }

    // Test with regular columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add multiple columns with regular values
        const int num_columns = 3;
        for (int i = 0; i < num_columns; ++i) {
            auto col = vectorized::ColumnVector<Int32>::create();
            for (int j = 0; j < 5; ++j) {
                col->insert_value(i * 10 + j);
            }
            block.insert({std::move(col), type, "col" + std::to_string(i)});
        }

        std::vector<bool> keep_flags(num_columns);
        keep_flags[0] = true;
        keep_flags[1] = false;
        keep_flags[2] = true;

        EXPECT_EQ(keep_flags.size(), block.columns());

        block.clear_column_mem_not_keep(keep_flags, true);

        // Verify columns are kept but data is cleared for non-kept columns
        EXPECT_EQ(num_columns, block.columns());
        EXPECT_EQ(5, block.get_by_position(0).column->size()); // Kept
        EXPECT_EQ(0, block.get_by_position(1).column->size()); // Cleared
        EXPECT_EQ(5, block.get_by_position(2).column->size()); // Kept

        // Verify data in kept columns remains intact
        const auto* col0 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                block.get_by_position(0).column.get());
        const auto* col2 = assert_cast<const vectorized::ColumnVector<Int32>*>(
                block.get_by_position(2).column.get());

        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(i, col0->get_data()[i]);
            EXPECT_EQ(20 + i, col2->get_data()[i]);
        }
    }

    // Test with const columns
    {
        vectorized::Block block;
        auto type = std::make_shared<vectorized::DataTypeInt32>();

        // Add multiple const columns
        const int num_columns = 3;
        for (int i = 0; i < num_columns; ++i) {
            auto base_col = vectorized::ColumnVector<Int32>::create();
            base_col->insert_value(i * 10);
            auto const_col = vectorized::ColumnConst::create(base_col->get_ptr(), 5);
            block.insert({const_col->get_ptr(), type, "const_col" + std::to_string(i)});
        }

        std::vector<bool> keep_flags(num_columns);
        keep_flags[0] = true;
        keep_flags[1] = false;
        keep_flags[2] = true;

        EXPECT_EQ(keep_flags.size(), block.columns());

        block.clear_column_mem_not_keep(keep_flags, true);

        // Verify columns are kept but data is cleared for non-kept columns
        EXPECT_EQ(num_columns, block.columns());
        EXPECT_EQ(5, block.get_by_position(0).column->size()); // Kept
        EXPECT_EQ(0, block.get_by_position(1).column->size()); // Cleared
        EXPECT_EQ(5, block.get_by_position(2).column->size()); // Kept

        // Verify const values in kept columns remain intact
        const auto* col0 =
                assert_cast<const vectorized::ColumnConst*>(block.get_by_position(0).column.get());
        const auto* col2 =
                assert_cast<const vectorized::ColumnConst*>(block.get_by_position(2).column.get());

        EXPECT_EQ(0, col0->get_int(0));
        EXPECT_EQ(20, col2->get_int(0));
    }

    // Test with nullable columns
    {
        vectorized::Block block;
        auto base_type = std::make_shared<vectorized::DataTypeInt32>();
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(base_type);

        // Add multiple nullable columns
        const int num_columns = 3;
        for (int i = 0; i < num_columns; ++i) {
            auto col = vectorized::ColumnNullable::create(
                    vectorized::ColumnVector<Int32>::create(),
                    vectorized::ColumnVector<vectorized::UInt8>::create());
            auto* nested = assert_cast<vectorized::ColumnVector<Int32>*>(
                    col->get_nested_column_ptr().get());
            auto* null_map = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(
                    col->get_null_map_column_ptr().get());

            for (int j = 0; j < 5; ++j) {
                nested->insert_value(i * 10 + j);
                null_map->insert_value(j % 2); // Alternate between null and non-null
            }
            block.insert({col->get_ptr(), nullable_type, "nullable_col" + std::to_string(i)});
        }

        std::vector<bool> keep_flags(num_columns);
        keep_flags[0] = true;
        keep_flags[1] = false;
        keep_flags[2] = true;

        EXPECT_EQ(keep_flags.size(), block.columns());

        block.clear_column_mem_not_keep(keep_flags, true);

        // Verify columns are kept but data is cleared for non-kept columns
        EXPECT_EQ(num_columns, block.columns());
        EXPECT_EQ(5, block.get_by_position(0).column->size()); // Kept
        EXPECT_EQ(0, block.get_by_position(1).column->size()); // Cleared
        EXPECT_EQ(5, block.get_by_position(2).column->size()); // Kept

        // Verify data and null states in kept columns remain intact
        const auto* col0 = assert_cast<const vectorized::ColumnNullable*>(
                block.get_by_position(0).column.get());
        const auto* col2 = assert_cast<const vectorized::ColumnNullable*>(
                block.get_by_position(2).column.get());

        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(i % 2, col0->is_null_at(i));
            EXPECT_EQ(i % 2, col2->is_null_at(i));
            if (!col0->is_null_at(i)) {
                EXPECT_EQ(i, col0->get_nested_column_ptr()->get_int(i));
            }
            if (!col2->is_null_at(i)) {
                EXPECT_EQ(20 + i, col2->get_nested_column_ptr()->get_int(i));
            }
        }
    }
}

TEST(BlockTest, StringOperations) {
    using namespace std::string_literals;

    // Test with empty block
    {
        vectorized::Block block;
        std::vector<size_t> char_type_idx;
        block.shrink_char_type_column_suffix_zero(char_type_idx);
        EXPECT_EQ(0, block.columns());
    }

    // Test with regular string column
    {
        vectorized::Block block;

        // Add a string column with padding zeros
        {
            auto col = vectorized::ColumnString::create();
            std::vector<std::pair<std::string, size_t>> test_strings = {
                    {"hello\0\0\0"s, 8}, // 8 bytes, 3 trailing zeros
                    {"world\0\0"s, 7},   // 7 bytes, 2 trailing zeros
                    {"test\0"s, 5},      // 5 bytes, 1 trailing zero
                    {""s, 0}             // empty string
            };

            for (const auto& [str, size] : test_strings) {
                col->insert_data(str.c_str(), size);
            }

            auto type = std::make_shared<vectorized::DataTypeString>();
            block.insert({std::move(col), type, "str_col"});
        }

        // Add a non-string column for comparison
        {
            auto col = vectorized::ColumnVector<Int32>::create();
            std::vector<Int32> test_values = {1, 2, 3, 4};
            for (const auto& val : test_values) {
                col->insert_value(val);
            }
            auto type = std::make_shared<vectorized::DataTypeInt32>();
            block.insert({std::move(col), type, "int_col"});
        }

        // Verify initial state
        ASSERT_EQ(2, block.columns());

        // Test shrinking string column
        std::vector<size_t> char_type_idx = {0}; // Index of string column
        ASSERT_LT(char_type_idx[0], block.columns());
        block.shrink_char_type_column_suffix_zero(char_type_idx);

        // Verify string column is shrunk
        const auto* str_col =
                assert_cast<const vectorized::ColumnString*>(block.get_by_position(0).column.get());
        ASSERT_NE(nullptr, str_col);

        // Verify each string
        std::vector<std::pair<std::string, size_t>> expected_results = {
                {"hello", 5}, {"world", 5}, {"test", 4}, {"", 0}};

        ASSERT_EQ(expected_results.size(), str_col->size());
        for (size_t i = 0; i < expected_results.size(); ++i) {
            StringRef ref = str_col->get_data_at(i);
            const auto& [expected_str, expected_size] = expected_results[i];
            EXPECT_EQ(expected_size, ref.size);
            if (expected_size > 0) {
                EXPECT_EQ(0, memcmp(ref.data, expected_str.c_str(), expected_size));
            }
        }

        // Verify non-string column remains unchanged
        const auto* int_col = assert_cast<const vectorized::ColumnVector<Int32>*>(
                block.get_by_position(1).column.get());
        ASSERT_NE(nullptr, int_col);
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(i + 1, int_col->get_data()[i]);
        }
    }

    // Test with Array<String>
    {
        vectorized::Block block;

        // Create Array<String> column with padding zeros
        auto string_type = std::make_shared<vectorized::DataTypeString>();
        auto array_type = std::make_shared<vectorized::DataTypeArray>(string_type);

        // Add strings with trailing zeros
        auto string_col = vectorized::ColumnString::create();
        std::vector<std::pair<std::string, size_t>> test_strings = {
                {"hello\0\0"s, 7}, {"world\0"s, 6}, {"test\0\0\0"s, 8}, {""s, 0}};

        for (const auto& [str, size] : test_strings) {
            string_col->insert_data(str.c_str(), size);
        }

        // Create array offsets column
        auto array_offsets = vectorized::ColumnArray::ColumnOffsets::create();
        array_offsets->get_data().push_back(2); // First array: ["hello", "world"]
        array_offsets->get_data().push_back(4); // Second array: ["test", ""]

        // Create and insert array column
        auto array_col =
                vectorized::ColumnArray::create(std::move(string_col), std::move(array_offsets));
        block.insert({std::move(array_col), array_type, "array_str_col"});

        // Verify initial state
        ASSERT_EQ(1, block.columns());

        // Shrink array<string> column
        std::vector<size_t> char_type_idx = {0};
        ASSERT_LT(char_type_idx[0], block.columns());
        block.shrink_char_type_column_suffix_zero(char_type_idx);

        // Verify strings in array are shrunk
        const auto* array_col_result =
                assert_cast<const vectorized::ColumnArray*>(block.get_by_position(0).column.get());
        ASSERT_NE(nullptr, array_col_result);

        const auto* string_col_result = assert_cast<const vectorized::ColumnString*>(
                array_col_result->get_data_ptr().get());
        ASSERT_NE(nullptr, string_col_result);

        // Verify each string in the arrays
        std::vector<std::pair<std::string, size_t>> expected_results = {
                {"hello", 5}, {"world", 5}, {"test", 4}, {"", 0}};

        ASSERT_EQ(expected_results.size(), string_col_result->size());
        for (size_t i = 0; i < expected_results.size(); ++i) {
            StringRef ref = string_col_result->get_data_at(i);
            const auto& [expected_str, expected_size] = expected_results[i];
            EXPECT_EQ(expected_size, ref.size);
            if (expected_size > 0) {
                EXPECT_EQ(0, memcmp(ref.data, expected_str.c_str(), expected_size));
            }
        }

        // Verify array structure remains intact
        const auto& offsets = array_col_result->get_offsets();
        ASSERT_EQ(2, offsets.size());
        EXPECT_EQ(2, offsets[0]);
        EXPECT_EQ(4, offsets[1]);
    }
}

TEST(BlockTest, SerializeAndDeserializeBlock) {
    serialize_and_deserialize_test(segment_v2::CompressionTypePB::SNAPPY);
    serialize_and_deserialize_test(segment_v2::CompressionTypePB::LZ4);
    serialize_and_deserialize_test_string();
    serialize_and_deserialize_test_int();
    serialize_and_deserialize_test_nullable();
    serialize_and_deserialize_test_decimal();
    serialize_and_deserialize_test_bitmap();
    serialize_and_deserialize_test_array();
    serialize_and_deserialize_test_long();
    serialize_and_deserialize_test_one();
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
        __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
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
