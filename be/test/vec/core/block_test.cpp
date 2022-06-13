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

#include <gtest/gtest.h>

#include <cmath>
#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/data.pb.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/exception.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris {

using vectorized::Int32;

TEST(BlockTest, RowBatchCovertToBlock) {
    SchemaScanner::ColumnDesc column_descs[] = {
            {"k1", TYPE_SMALLINT, sizeof(int16_t), true},
            {"k2", TYPE_INT, sizeof(int32_t), false},
            {"k3", TYPE_DOUBLE, sizeof(double), false},
            {"k4", TYPE_VARCHAR, sizeof(StringValue), false},
            {"k5", TYPE_DECIMALV2, sizeof(DecimalV2Value), false},
            {"k6", TYPE_LARGEINT, sizeof(__int128), false},
            {"k7", TYPE_DATETIME, sizeof(int64_t), false}};

    SchemaScanner schema_scanner(column_descs,
                                 sizeof(column_descs) / sizeof(SchemaScanner::ColumnDesc));
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);

    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());

    int16_t k1 = -100;
    int32_t k2 = 100000;
    double k3 = 7.7;

    for (int i = 0; i < 1024; ++i, k1++, k2++, k3 += 0.1) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);

        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        if (i % 5 == 0) {
            tuple->set_null(slot_desc->null_indicator_offset());
        } else {
            tuple->set_not_null(slot_desc->null_indicator_offset());
            memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, column_descs[0].size);
        }
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k3, column_descs[2].size);

        // string slot
        slot_desc = tuple_desc->slots()[3];
        auto num_str = std::to_string(k1);
        auto string_slot = tuple->get_string_slot(slot_desc->tuple_offset());
        string_slot->ptr = (char*)row_batch.tuple_data_pool()->allocate(num_str.size());
        string_slot->len = num_str.size();
        memcpy(string_slot->ptr, num_str.c_str(), num_str.size());

        slot_desc = tuple_desc->slots()[4];
        DecimalV2Value decimalv2_num(std::to_string(k3));
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &decimalv2_num, column_descs[4].size);

        slot_desc = tuple_desc->slots()[5];
        int128_t k6 = k1;
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k6, column_descs[5].size);

        slot_desc = tuple_desc->slots()[6];
        vectorized::VecDateTimeValue k7;
        std::string now_time("2020-12-02");
        k7.from_date_str(now_time.c_str(), now_time.size());
        vectorized::TimeInterval time_interval(vectorized::TimeUnit::DAY, k1, false);
        k7.date_add_interval(time_interval, vectorized::TimeUnit::DAY);
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k7, column_descs[6].size);

        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    auto block = row_batch.convert_to_vec_block();
    k1 = -100;
    k2 = 100000;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i) {
        vectorized::ColumnPtr column1 = block.get_columns()[0];
        vectorized::ColumnPtr column2 = block.get_columns()[1];
        vectorized::ColumnPtr column3 = block.get_columns()[2];
        vectorized::ColumnPtr column4 = block.get_columns()[3];
        vectorized::ColumnPtr column5 = block.get_columns()[4];
        vectorized::ColumnPtr column6 = block.get_columns()[5];
        vectorized::ColumnPtr column7 = block.get_columns()[6];

        if (i % 5 != 0) {
            ASSERT_EQ((int16_t)column1->get64(i), k1);
        } else {
            ASSERT_TRUE(column1->is_null_at(i));
        }
        ASSERT_EQ(column2->get_int(i), k2++);
        ASSERT_EQ(column3->get_float64(i), k3);
        ASSERT_STREQ(column4->get_data_at(i).data, std::to_string(k1).c_str());
        auto decimal_field =
                column5->operator[](i).get<vectorized::DecimalField<vectorized::Decimal128>>();
        DecimalV2Value decimalv2_num(std::to_string(k3));
        ASSERT_EQ(DecimalV2Value(decimal_field.get_value()), decimalv2_num);

        int128_t larget_int = k1;
        ASSERT_EQ(column6->operator[](i).get<vectorized::Int128>(), k1);

        larget_int = column7->operator[](i).get<vectorized::Int128>();
        vectorized::VecDateTimeValue k7;
        memcpy(reinterpret_cast<vectorized::Int128*>(&k7), &larget_int, column_descs[6].size);
        vectorized::VecDateTimeValue date_time_value;
        std::string now_time("2020-12-02");
        date_time_value.from_date_str(now_time.c_str(), now_time.size());
        vectorized::TimeInterval time_interval(vectorized::TimeUnit::DAY, k1, false);
        date_time_value.date_add_interval(time_interval, vectorized::TimeUnit::DAY);

        ASSERT_EQ(k7, date_time_value);

        k1++;
        k3 += 0.1;
    }
}

void block_to_pb(const vectorized::Block& block, PBlock* pblock) {
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    Status st = block.serialize(pblock, &uncompressed_bytes, &compressed_bytes);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(uncompressed_bytes >= compressed_bytes);
    EXPECT_EQ(compressed_bytes, pblock->column_values().size());

    const vectorized::ColumnWithTypeAndName& type_and_name =
            block.get_columns_with_type_and_name()[0];
    EXPECT_EQ(type_and_name.name, pblock->column_metas()[0].name());
}

TEST(BlockTest, SerializeAndDeserializeBlock) {
    config::compress_rowbatches = true;
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
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
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
        vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, "test_string");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                decimal_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = i * pow(10, 9) + i * pow(10, 8);
            data.push_back(value);
        }
        vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(), decimal_data_type,
                "test_decimal");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container = ((vectorized::ColumnComplexType<BitmapValue>*) bitmap_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);    
        }
        vectorized::ColumnWithTypeAndName type_and_name(bitmap_column->get_ptr(), bitmap_data_type, "test_bitmap");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // nullable string
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*) nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(), nullable_data_type,
                "test_nullable");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9));
        vectorized::DataTypePtr nullable_data_type(std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*) nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(), nullable_data_type,
                "test_nullable_decimal");
        vectorized::Block block({type_and_name});
        PBlock pblock;
        block_to_pb(block, &pblock);
        EXPECT_EQ(1, pblock.column_metas_size());
        EXPECT_TRUE(pblock.column_metas()[0].has_decimal_param());
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
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
        block_to_pb(block, &pblock);
        std::string s1 = pblock.DebugString();

        vectorized::Block block2(pblock);
        PBlock pblock2;
        block_to_pb(block2, &pblock2);
        std::string s2 = pblock2.DebugString();
        EXPECT_EQ(s1, s2);
    }
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

    vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9));
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
        vectorized::VecDateTimeValue value;
        value.from_date_int64(20210501);
        date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
    vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(), date_type,
                                                "test_date");

    auto column_vector_datetime = vectorized::ColumnVector<vectorized::Int64>::create();
    auto& datetime_data = column_vector_datetime->get_data();
    for (int i = 0; i < 1024; ++i) {
        vectorized::VecDateTimeValue value;
        value.from_date_int64(20210501080910);
        datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
    }
    vectorized::DataTypePtr datetime_type(std::make_shared<vectorized::DataTypeDateTime>());
    vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(), datetime_type,
                                                    "test_datetime");

    vectorized::Block block(
            {test_int, test_string, test_decimal, test_nullable_int32, test_date, test_datetime});
    EXPECT_GT(block.dump_data().size(), 1);
}
} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
