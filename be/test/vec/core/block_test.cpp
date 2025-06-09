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

#include <concurrentqueue.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "gen_cpp/data.pb.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/sip_hash.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
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
    auto off_column = vectorized::ColumnOffset64::create();
    auto data_column = vectorized::ColumnInt32::create();
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
    auto off_column = vectorized::ColumnOffset64::create();
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
        auto vec = vectorized::ColumnInt32::create();
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
        auto& data = ((vectorized::ColumnDecimal128V2*)decimal_column.get())->get_data();
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
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
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
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
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
        auto column_vector_int32 = vectorized::ColumnInt32::create();
        auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
        auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
        for (int i = 0; i < 4096; i++) {
            mutable_nullable_vector->insert(vectorized::Field::create_field<TYPE_INT>(
                    vectorized::cast_to_nearest_field_type(i)));
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
        auto vec = vectorized::ColumnInt32::create();
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
        auto vec = vectorized::ColumnInt32::create();
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
        auto vec = vectorized::ColumnInt32::create();
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
        auto vec = vectorized::ColumnInt64::create();
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
        auto vec = vectorized::ColumnInt64::create();
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
        auto vec = vectorized::ColumnInt32::create();
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
        auto vec = vectorized::ColumnInt32::create();
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
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeDecimal32>(6, 3));
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
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeDecimal32>(6, 3));
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
    auto vec = vectorized::ColumnInt32::create();
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
    auto& decimal_data = ((vectorized::ColumnDecimal128V2*)decimal_column.get())->get_data();
    for (int i = 0; i < 1024; ++i) {
        __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
        decimal_data.push_back(value);
    }
    vectorized::ColumnWithTypeAndName test_decimal(decimal_column->get_ptr(), decimal_data_type,
                                                   "test_decimal");

    auto column_vector_int32 = vectorized::ColumnInt32::create();
    auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
    auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
    for (int i = 0; i < 4096; i++) {
        mutable_nullable_vector->insert(vectorized::Field::create_field<TYPE_INT>(
                vectorized::cast_to_nearest_field_type(i)));
    }
    auto nint32_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName test_nullable_int32(mutable_nullable_vector->get_ptr(),
                                                          nint32_type, "test_nullable_int32");

    auto column_vector_date = vectorized::ColumnDate::create();
    auto& date_data = column_vector_date->get_data();
    for (int i = 0; i < 1024; ++i) {
        VecDateTimeValue value;
        value.from_date_int64(20210501);
        date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
    vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(), date_type,
                                                "test_date");

    auto column_vector_datetime = vectorized::ColumnDateTime::create();
    auto& datetime_data = column_vector_datetime->get_data();
    for (int i = 0; i < 1024; ++i) {
        VecDateTimeValue value;
        value.from_date_int64(20210501080910);
        datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr datetime_type(std::make_shared<vectorized::DataTypeDateTime>());
    vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(),
                                                    datetime_type, "test_datetime");

    auto column_vector_date_v2 = vectorized::ColumnDateV2::create();
    auto& date_v2_data = column_vector_date_v2->get_data();
    for (int i = 0; i < 1024; ++i) {
        DateV2Value<DateV2ValueType> value;
        value.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
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
    auto vec = vectorized::ColumnInt32::create();
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

    auto vec_temp = vectorized::ColumnInt32::create();
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

namespace vectorized {
template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr);
}

TEST(BlockTest, clear_blocks) {
    {
        moodycamel::ConcurrentQueue<vectorized::Block> queue;

        RuntimeProfile::Counter counter(TUnit::BYTES);
        auto block1 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        auto block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});

        counter.update(block1.allocated_bytes());
        counter.update(block2.allocated_bytes());

        ASSERT_EQ(counter.value(), block1.allocated_bytes() + block2.allocated_bytes());

        queue.enqueue(std::move(block1));
        queue.enqueue(std::move(block2));

        vectorized::clear_blocks(queue, &counter);
        EXPECT_EQ(counter.value(), 0);

        ASSERT_FALSE(queue.try_dequeue(block1));
    }

    {
        moodycamel::ConcurrentQueue<std::unique_ptr<vectorized::Block>> queue;

        RuntimeProfile::Counter counter(TUnit::BYTES);
        auto block1 = std::make_unique<vectorized::Block>(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));
        auto block2 = std::make_unique<vectorized::Block>(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({4, 5, 6}));

        counter.update(block1->allocated_bytes());
        counter.update(block2->allocated_bytes());

        ASSERT_EQ(counter.value(), block1->allocated_bytes() + block2->allocated_bytes());

        queue.enqueue(std::move(block1));
        queue.enqueue(std::move(block2));

        vectorized::clear_blocks(queue, &counter);
        EXPECT_EQ(counter.value(), 0);

        ASSERT_FALSE(queue.try_dequeue(block1));
    }
}

TEST(BlockTest, replace_by_position) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    block.insert(0, vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
                            {"a", "b", "c"}));

    auto col = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>({4, 5, 6});
    block.replace_by_position(0, std::move(col));

    ASSERT_EQ(block.get_by_position(0).column->get_int(0), 4);
    ASSERT_EQ(block.get_by_position(0).column->get_int(1), 5);
    ASSERT_EQ(block.get_by_position(0).column->get_int(2), 6);
}

TEST(BlockTest, compare_at) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    auto ret = block.compare_at(0, 0, block, -1);
    ASSERT_EQ(ret, 0);

    ret = block.compare_at(0, 1, block, -1);
    ASSERT_LT(ret, 0);

    ret = block.compare_at(2, 1, block, -1);
    ASSERT_GT(ret, 0);

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeInt32>(
            {2, 2, 1}));
    std::vector<uint32_t> compare_columns {1, 0};

    ret = block.compare_at(0, 1, &compare_columns, block, -1);
    ASSERT_LT(ret, 0);

    ret = block.compare_at(1, 2, &compare_columns, block, -1);
    ASSERT_GT(ret, 0);
}

TEST(BlockTest, same_bit) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 1, 3});
    std::vector<bool> same_bit = {false, true, false};
    block.set_same_bit(same_bit.begin(), same_bit.end());
    ASSERT_EQ(block.get_same_bit(0), false);
    ASSERT_EQ(block.get_same_bit(1), true);
    ASSERT_EQ(block.get_same_bit(2), false);

    auto block2 = block;
    block2.set_num_rows(2);
    ASSERT_EQ(block2.rows(), 2);

    int64_t length = 3;
    block2.skip_num_rows(length);
    ASSERT_EQ(1, length);
    ASSERT_EQ(block2.rows(), 0);

    block2 = block;
    length = 1;
    block2.skip_num_rows(length);
    ASSERT_EQ(block2.rows(), 2);
    ASSERT_EQ(block2.get_same_bit(0), true);

    block2.skip_num_rows(length);
    ASSERT_EQ(block2.rows(), 1);
    ASSERT_EQ(block2.get_same_bit(0), false);

    block.clear_same_bit();

    ASSERT_EQ(block.get_same_bit(1), false);
}

TEST(BlockTest, dump) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    auto types = block.dump_types();
    ASSERT_TRUE(types.find("INT") != std::string::npos);

    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({}));
    types = block.dump_types();
    ASSERT_TRUE(types.find("String") != std::string::npos);

    block.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeFloat64>({}));
    types = block.dump_types();
    ASSERT_TRUE(types.find("DOUBLE") != std::string::npos);

    auto names = block.get_names();
    for (const auto& name : names) {
        ASSERT_EQ(name, "column");
    }

    auto dumped_names = block.dump_names();
    ASSERT_TRUE(dumped_names.find(", ") != std::string::npos);
    ASSERT_TRUE(dumped_names.find("column") != std::string::npos);

    block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
            {"a", "b", "c"}));

    auto dumped_line = block.dump_one_line(1, 2);
    ASSERT_EQ(dumped_line, "2 b");
}

TEST(BlockTest, merge_impl_ignore_overflow) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({}));
    block.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeFloat64>({}));
    auto block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});

    auto mutable_block = vectorized::MutableBlock::build_mutable_block(&block);

    auto st = mutable_block.merge_ignore_overflow(std::move(block2));
    ASSERT_FALSE(st.ok());

    block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({}));
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeFloat32>({}));

    EXPECT_ANY_THROW(st = mutable_block.merge_impl_ignore_overflow(std::move(block2)));
}

TEST(BlockTest, merge_impl) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({}));
    block.insert(
            vectorized::ColumnHelper::create_nullable_column_with_name<vectorized::DataTypeFloat64>(
                    {1.0}, {0}));

    block.get_by_position(2).column = nullptr;

    auto mutable_block = vectorized::MutableBlock::build_mutable_block(nullptr);

    auto st = mutable_block.merge(std::move(block));
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({}));

    st = mutable_block.merge_impl(std::move(block2));
    ASSERT_FALSE(st);

    block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1});
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({"a"}));
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeFloat64>({}));

    EXPECT_ANY_THROW(st = mutable_block.merge_impl(std::move(block2)));
}

TEST(BlockTest, ctor) {
    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder
            .add_slot(
                    TSlotDescriptorBuilder().type(PrimitiveType::TYPE_INT).nullable(false).build())
            .add_slot(TSlotDescriptorBuilder()
                              .type(PrimitiveType::TYPE_STRING)
                              .set_isMaterialized(false)
                              .build());
    tuple_builder.build(&builder);

    auto t_table = builder.desc_tbl();

    ObjectPool pool;
    DescriptorTbl* tbl;
    auto st = DescriptorTbl::create(&pool, t_table, &tbl);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto slots = tbl->get_tuple_descriptor(0)->slots();

    std::vector<SlotDescriptor> slot_descs;
    for (auto* slot : slots) {
        slot_descs.push_back(*slot);
    }

    vectorized::Block block(slot_descs, 1);
    ASSERT_EQ(block.columns(), 2);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_INT);
    ASSERT_TRUE(block.get_by_position(1).type->is_nullable());

    {
        auto mutable_block =
                vectorized::MutableBlock::create_unique(tbl->get_tuple_descs(), 10, false);
        ASSERT_EQ(mutable_block->columns(), 2);
        auto mutable_block2 = vectorized::MutableBlock::create_unique();
        mutable_block->swap(*mutable_block2);
        ASSERT_EQ(mutable_block->columns(), 0);
        ASSERT_EQ(mutable_block2->columns(), 2);
    }

    {
        auto mutable_block =
                vectorized::MutableBlock::create_unique(tbl->get_tuple_descs(), 10, true);
        ASSERT_EQ(mutable_block->columns(), 1);
        auto mutable_block2 = vectorized::MutableBlock::create_unique();
        mutable_block->swap(*mutable_block2);
        ASSERT_EQ(mutable_block->columns(), 0);
        ASSERT_EQ(mutable_block2->columns(), 1);
    }
}

TEST(BlockTest, insert_erase) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});

    auto column_with_name =
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({});
    EXPECT_ANY_THROW(block.insert(2, column_with_name));
    block.insert(0, column_with_name);

    ASSERT_EQ(block.columns(), 2);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_STRING);

    EXPECT_ANY_THROW(block.insert(3, std::move(column_with_name)));

    column_with_name =
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeFloat64>({});
    block.insert(0, std::move(column_with_name));
    ASSERT_EQ(block.columns(), 3);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_DOUBLE);

    std::set<size_t> positions = {0, 2};
    block.erase(positions);

    ASSERT_EQ(block.columns(), 1);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_STRING);

    block.erase_tail(0);
    ASSERT_EQ(block.columns(), 0);

    EXPECT_ANY_THROW(block.erase("column"));
    column_with_name =
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({});
    block.insert(0, column_with_name);
    EXPECT_NO_THROW(block.erase("column"));
    ASSERT_EQ(block.columns(), 0);

    EXPECT_ANY_THROW(block.safe_get_by_position(0));

    ASSERT_EQ(block.try_get_by_name("column"), nullptr);
    EXPECT_ANY_THROW(block.get_by_name("column"));
    EXPECT_ANY_THROW(block.get_position_by_name("column"));
    block.insert(0, column_with_name);

    EXPECT_NO_THROW(auto item = block.get_by_name("column"));
    ASSERT_NE(block.try_get_by_name("column"), nullptr);
    EXPECT_EQ(block.get_position_by_name("column"), 0);

    block.insert({nullptr, nullptr, BeConsts::BLOCK_TEMP_COLUMN_PREFIX});
    EXPECT_NO_THROW(auto item = block.get_by_name(BeConsts::BLOCK_TEMP_COLUMN_PREFIX));

    block.erase_tmp_columns();
    ASSERT_EQ(block.try_get_by_name(BeConsts::BLOCK_TEMP_COLUMN_PREFIX), nullptr);

    {
        // test const block
        const auto const_block = block;
        EXPECT_EQ(const_block.try_get_by_name("column2"), nullptr);
        EXPECT_ANY_THROW(const_block.get_by_name("column2"));
        EXPECT_ANY_THROW(const_block.get_position_by_name("column2"));

        EXPECT_NO_THROW(auto item = const_block.get_by_name("column"));
        ASSERT_NE(const_block.try_get_by_name("column"), nullptr);
        EXPECT_EQ(const_block.get_position_by_name("column"), 0);
    }

    block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});
    block.insert({nullptr, std::make_shared<vectorized::DataTypeString>(), "col1"});

    block.insert({nullptr, std::make_shared<vectorized::DataTypeString>(), "col2"});

    vectorized::MutableBlock mutable_block(&block);
    mutable_block.erase("col1");
    ASSERT_EQ(mutable_block.columns(), 2);

    EXPECT_ANY_THROW(mutable_block.erase("col1"));
    ASSERT_EQ(mutable_block.columns(), 2);
    mutable_block.erase("col2");
    ASSERT_EQ(mutable_block.columns(), 1);
}

TEST(BlockTest, check_number_of_rows) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({});

    block.insert({nullptr, nullptr, "null_col"});
    EXPECT_NO_THROW(block.check_number_of_rows(true));
    EXPECT_ANY_THROW(block.check_number_of_rows(false));

    block.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({"abc"}));
    EXPECT_ANY_THROW(block.check_number_of_rows(true));

    EXPECT_ANY_THROW(block.columns_bytes());

    ASSERT_GT(block.allocated_bytes(), 0);
}

TEST(BlockTest, clear_column_mem_not_keep) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
            {"abc", "efg", "hij"}));

    std::vector<bool> column_keep_flags {false, true};

    auto block2 = block;
    block2.clear_column_mem_not_keep(column_keep_flags, false);
    ASSERT_EQ(block2.get_by_position(0).column->size(), 0);
    ASSERT_EQ(block2.get_by_position(1).column->size(), 3);

    block.clear_column_mem_not_keep(column_keep_flags, true);
    ASSERT_EQ(block.get_by_position(0).column->size(), 3);
    ASSERT_EQ(block.get_by_position(1).column->size(), 3);
}

TEST(BlockTest, filter) {
    {
        // normal filter
        auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
                {"abc", "efg", "hij"}));

        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeUInt8>(
                {1, 1, 1}));
        auto st = vectorized::Block::filter_block(&block, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block.rows(), 3);

        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeUInt8>(
                {0, 1, 0}));
        st = vectorized::Block::filter_block(&block, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block.rows(), 1);

        block.insert(
                vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeUInt8>({0}));
        auto block2 = block;
        st = vectorized::Block::filter_block(&block2, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block2.rows(), 0);
    }

    {
        // nullable filter column
        auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
                {"abc", "efg", "hij"}));

        block.insert(vectorized::ColumnHelper::create_nullable_column_with_name<
                     vectorized::DataTypeUInt8>({1, 1, 1}, {0, 1, 0}));
        auto st = vectorized::Block::filter_block(&block, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block.rows(), 2);
    }

    {
        // const filter column
        auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
                {"abc", "efg", "hij"}));

        block.insert({vectorized::ColumnConst::create(vectorized::ColumnUInt8::create(1, 1), 3),
                      std::make_shared<vectorized::DataTypeUInt8>(), "filter"});
        auto st = vectorized::Block::filter_block(&block, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block.rows(), 3);

        block.insert({vectorized::ColumnConst::create(vectorized::ColumnUInt8::create(1, 0), 3),
                      std::make_shared<vectorized::DataTypeUInt8>(), "filter2"});
        st = vectorized::Block::filter_block(&block, {0, 1}, 2, 2);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(block.rows(), 0);
    }

    {
        // append_to_block_by_selector
        auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
                {"abc", "efg", "hij"}));

        vectorized::IColumn::Selector selector(2);
        selector[0] = 0;
        selector[1] = 2;

        vectorized::MutableBlock mutable_block(block.clone_empty());
        auto st = block.append_to_block_by_selector(&mutable_block, selector);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(mutable_block.rows(), 2);
        ASSERT_EQ(mutable_block.mutable_columns()[0]->get_int(0), 1);
        ASSERT_EQ(mutable_block.mutable_columns()[0]->get_int(1), 3);
    }
}

TEST(BlockTest, add_rows) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
            {"abc", "efg", "hij"}));

    auto block2 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({4});
    block2.insert(
            vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>({"lmn"}));

    vectorized::MutableBlock mutable_block(&block);
    mutable_block.add_row(&block2, 0);
    ASSERT_EQ(mutable_block.rows(), 4);

    vectorized::MutableBlock mutable_block2(&block2);
    auto st = mutable_block2.add_rows(&block, {0, 2});
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(mutable_block2.rows(), 3);
}

TEST(BlockTest, others) {
    auto block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    block.insert(vectorized::ColumnHelper::create_column_with_name<vectorized::DataTypeString>(
            {"abc", "efg", "hij"}));

    block.shrink_char_type_column_suffix_zero({1, 2});

    SipHash hash;
    block.update_hash(hash);

    auto hash_value = hash.get64();
    ASSERT_EQ(hash_value, 15311230698310402164ULL);

    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
            {"lmn", "opq", "rst"});

    block.replace_by_position(1, std::move(column));
    SipHash hash2;
    block.update_hash(hash2);
    hash_value = hash2.get64();
    ASSERT_EQ(hash_value, 16980601693179295243ULL);

    std::vector<int> result_column_ids = {0, 1};
    block.shuffle_columns(result_column_ids);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_INT);
    ASSERT_EQ(block.get_by_position(1).type->get_primitive_type(), TYPE_STRING);

    result_column_ids = {1, 0};
    block.shuffle_columns(result_column_ids);
    ASSERT_EQ(block.get_by_position(1).type->get_primitive_type(), TYPE_INT);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_STRING);

    result_column_ids = {1};
    block.shuffle_columns(result_column_ids);
    ASSERT_EQ(block.get_by_position(0).type->get_primitive_type(), TYPE_INT);
    ASSERT_EQ(block.columns(), 1);

    vectorized::MutableBlock mutable_block(&block);
    auto dumped = mutable_block.dump_data();
    ASSERT_GT(dumped.size(), 0) << "Dumped data size: " << dumped.size();

    mutable_block.clear_column_data();
    ASSERT_EQ(mutable_block.get_column_by_position(0)->size(), 0);
    ASSERT_TRUE(mutable_block.has("column"));
    ASSERT_EQ(mutable_block.get_position_by_name("column"), 0);

    auto dumped_names = mutable_block.dump_names();
    ASSERT_TRUE(dumped_names.find("column") != std::string::npos);

    block.clear_names();
    dumped_names = block.dump_names();
    ASSERT_TRUE(dumped_names.empty()) << "Dumped names: " << dumped_names;
}

} // namespace doris
