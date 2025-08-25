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

#include "vec/data_types/data_type_bitmap.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>

#include "agent/be_exec_version_manager.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric, should_align_right_in_pretty_formats
//         text_can_contain_only_valid_utf8
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         is_null_literal, is_value_represented_by_number, is_value_unambiguously_represented_in_contiguous_memory_region
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
// 4. serialize/serialize_as_stream/deserialize/deserialize_as_stream
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)

namespace doris::vectorized {

class DataTypeBitMapTest : public ::testing::TestWithParam<int> {
public:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }
    std::unique_ptr<CommonDataTypeTest> helper;
    DataTypePtr dt_bitmap =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_BITMAP, 0, 0);
    int rows_value;
};

TEST_P(DataTypeBitMapTest, MetaInfoTest) {
    auto bitmap_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_BITMAP, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_BITMAP);
    CommonDataTypeTest::DataTypeMetaInfo bitmap_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_BITMAP,
            .type_as_type_descriptor = bitmap_type_descriptor,
            .family_name = "BitMap",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_BITMAP,
            .should_align_right_in_pretty_formats = false,
            .text_can_contain_only_valid_utf8 = true,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = false,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_BITMAP>(BitmapValue::empty_bitmap()),
    };
    helper->meta_info_assert(dt_bitmap, bitmap_meta_info_to_assert);
}

TEST_P(DataTypeBitMapTest, CreateColumnTest) {
    Field default_field_bitmap = Field::create_field<TYPE_BITMAP>(BitmapValue::empty_bitmap());
    helper->create_column_assert(dt_bitmap, default_field_bitmap, 17);
}

void insert_data_bitmap(MutableColumns* bitmap_cols, DataTypePtr dt_bitmap, int rows_value,
                        std::vector<std::string>* data_strs = nullptr) {
    auto serde_bitmap = dt_bitmap->get_serde(1);
    auto column_bitmap = dt_bitmap->create_column();

    bitmap_cols->push_back(column_bitmap->get_ptr());
    DataTypeSerDeSPtrs serde = {dt_bitmap->get_serde()};
    auto& data = assert_cast<ColumnBitmap*>((*bitmap_cols)[0].get())->get_data();
    for (size_t i = 0; i != rows_value; ++i) {
        BitmapValue bitmap_value;
        for (size_t j = 0; j <= i; ++j) {
            bitmap_value.add(j);
        }
        if (data_strs) {
            data_strs->push_back(bitmap_value.to_string());
        }
        std::string memory_buffer(bitmap_value.getSizeInBytes(), '0');
        bitmap_value.write_to(memory_buffer.data());
        data.emplace_back(std::move(bitmap_value));
    }
    std::cout << "finish insert data" << std::endl;
}

// not support function: get_filed

// test to_string | to_string_batch | from_string
TEST_P(DataTypeBitMapTest, FromAndToStringTest) {
    MutableColumns bitmap_cols;
    std::vector<std::string> data_strs;
    insert_data_bitmap(&bitmap_cols, dt_bitmap, rows_value, &data_strs);

    {
        // to_string_batch | from_string
        auto col_to = ColumnString::create();
        dt_bitmap->to_string_batch(*bitmap_cols[0]->get_ptr(), *col_to);
        ASSERT_EQ(col_to->size(), bitmap_cols[0]->get_ptr()->size());
        // from_string assert col_to to assert_column and check same with mutableColumn
        auto assert_column = dt_bitmap->create_column();
        for (int i = 0; i < col_to->size(); ++i) {
            std::string s = col_to->get_data_at(i).to_string();
            StringRef rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(), dt_bitmap->from_string(rb, assert_column.get()));
            ASSERT_EQ(assert_column->operator[](i), bitmap_cols[0]->get_ptr()->operator[](i))
                    << "i: " << i << " s: " << s << " datatype: " << dt_bitmap->get_name()
                    << " assert_column: " << assert_column->get_name()
                    << " mutableColumn:" << bitmap_cols[0]->get_ptr()->get_name() << std::endl;
        }
        std::cout << "finish to_string_batch | from_string test" << std::endl;
    }

    {
        // to_string | from_string
        auto ser_col = ColumnString::create();
        ser_col->reserve(bitmap_cols[0]->get_ptr()->size());
        VectorBufferWriter buffer_writer(*ser_col.get());
        for (int i = 0; i < bitmap_cols[0]->get_ptr()->size(); ++i) {
            dt_bitmap->to_string(*bitmap_cols[0]->get_ptr(), i, buffer_writer);
            std::string res = dt_bitmap->to_string(*bitmap_cols[0]->get_ptr(), i);
            buffer_writer.commit();
            EXPECT_EQ(res, data_strs[i]);
        }
        // check ser_col to assert_column and check same with mutableColumn
        auto assert_column_1 = dt_bitmap->create_column();
        for (int i = 0; i < ser_col->size(); ++i) {
            std::string s = ser_col->get_data_at(i).to_string();
            StringRef rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(), dt_bitmap->from_string(rb, assert_column_1.get()));
            auto aaa = assert_column_1->operator[](i);
            ASSERT_EQ(assert_column_1->operator[](i), bitmap_cols[0]->get_ptr()->operator[](i));
        }
        std::cout << "finish to_string | from_string test" << std::endl;
    }
}

// serialize / deserialize
TEST_P(DataTypeBitMapTest, SerializeDeserializeTest) {
    MutableColumns bitmap_cols;
    insert_data_bitmap(&bitmap_cols, dt_bitmap, rows_value);

    auto* column = assert_cast<ColumnBitmap*>(bitmap_cols[0].get());
    auto size = dt_bitmap->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result =
            dt_bitmap->serialize(*column, buf.get(), BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = dt_bitmap->create_column();
    dt_bitmap->deserialize(buf.get(), &column2, BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnBitmap*>(column2.get());
        ASSERT_EQ(column->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    helper->serialize_deserialize_assert(bitmap_cols, {dt_bitmap});
    std::cout << "finish serialize deserialize test" << std::endl;
}

// serialize / deserialize as stream
TEST_P(DataTypeBitMapTest, SerializeDeserializeAsStreamTest) {
    MutableColumns bitmap_cols;
    insert_data_bitmap(&bitmap_cols, dt_bitmap, rows_value);

    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto* column_data = assert_cast<ColumnBitmap*>(bitmap_cols[0].get());
    auto c = dt_bitmap->create_column();
    auto* column_res = assert_cast<ColumnBitmap*>(c.get());
    column_res->resize(rows_value);
    for (size_t i = 0; i != rows_value; ++i) {
        doris::vectorized::DataTypeBitMap::serialize_as_stream(column_data->get_element(i),
                                                               buffer_writer);
        buffer_writer.commit();
        BufferReadable buffer_readable(ser_col->get_data_at(i));
        doris::vectorized::DataTypeBitMap::deserialize_as_stream(column_res->get_element(i),
                                                                 buffer_readable);
        ASSERT_EQ(column_data->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    std::cout << "finish serialize deserialize as stream test" << std::endl;
}
// sh run-be-ut.sh --run --filter=Params/DataTypeBitMapTest.*
// need rows_value to cover bitmap all type: empty/single/set/bitmap
INSTANTIATE_TEST_SUITE_P(Params, DataTypeBitMapTest, ::testing::Values(0, 1, 31, 1024));

} // namespace doris::vectorized