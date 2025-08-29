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

#include "vec/data_types/data_type_fixed_length_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>

#include "agent/be_exec_version_manager.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
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
//         is_null_literal, is_value_represented_by_number
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
// 4. serialize/serialize_as_stream/deserialize/deserialize_as_stream
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)

namespace doris::vectorized {

class DataTypeFixedLengthObjectTest : public ::testing::TestWithParam<int> {
public:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }
    std::unique_ptr<CommonDataTypeTest> helper;
    int rows_value;
    DataTypePtr datatype_fixed_length = std::make_shared<DataTypeFixedLengthObject>();
};

TEST_P(DataTypeFixedLengthObjectTest, MetaInfoTest) {
    auto bitmap_type_descriptor = std::make_shared<DataTypeFixedLengthObject>();
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_FIXEDLENGTHOBJECT);
    CommonDataTypeTest::DataTypeMetaInfo bitmap_meta_info_to_assert = {
            .type_id = PrimitiveType::
                    INVALID_TYPE, // we dont have one for this type now. but type_id is not used now.
            .type_as_type_descriptor = bitmap_type_descriptor,
            .family_name = "DataTypeFixedLengthObject",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_NONE,
            .should_align_right_in_pretty_formats = false,
            .text_can_contain_only_valid_utf8 = false,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = false,
            .pColumnMeta = col_meta.get(),
            .default_field = Field::create_field<TYPE_STRING>(String()),
    };
    helper->meta_info_assert(datatype_fixed_length, bitmap_meta_info_to_assert);
}

TEST_P(DataTypeFixedLengthObjectTest, CreateColumnTest) {
    std::string res;
    res.resize(8);
    memset(res.data(), 0, 8);
    Field default_field = Field::create_field<TYPE_STRING>(res);
    std::cout << "create_column_assert: " << datatype_fixed_length->get_name() << std::endl;
    auto column = (datatype_fixed_length)->create_column();
    ASSERT_EQ(column->size(), 0);
    auto fixed_length_column = ColumnFixedLengthObject::create(8);
    fixed_length_column->insert(default_field);
    ASSERT_EQ(fixed_length_column->size(), 1);
    auto default_const_col = ColumnFixedLengthObject::create(8);
    auto data = fixed_length_column->get_data_at(0);
    default_const_col->insert_data(data.data, data.size);
    for (int i = 0; i < 1; ++i) {
        ASSERT_EQ(fixed_length_column->operator[](i), default_const_col->operator[](i));
    }
    // get_uncompressed_serialized_bytes
    ASSERT_EQ(datatype_fixed_length->get_uncompressed_serialized_bytes(
                      *column, BeExecVersionManager::get_newest_version()),
              17);
}

void insert_data_fixed_length_data(MutableColumns* fixed_length_cols,
                                   DataTypePtr datatype_fixed_length, int rows_value,
                                   std::vector<std::string>* data_strs = nullptr) {
    auto serde_fixed_length = datatype_fixed_length->get_serde(1);
    auto column_fixed = ColumnFixedLengthObject::create(sizeof(size_t));
    column_fixed->resize(rows_value);
    fixed_length_cols->push_back(column_fixed->get_ptr());
    DataTypeSerDeSPtrs serde = {datatype_fixed_length->get_serde()};
    auto& data = assert_cast<ColumnFixedLengthObject*>((*fixed_length_cols)[0].get())->get_data();
    for (size_t i = 0; i != rows_value; ++i) {
        data[i] = i;
    }
    std::cout << "finish insert data" << std::endl;
}

// not support function: get_filed to_string | to_string_batch | from_string

// serialize / deserialize
TEST_P(DataTypeFixedLengthObjectTest, SerializeDeserializeTest) {
    MutableColumns fixed_length_cols;
    insert_data_fixed_length_data(&fixed_length_cols, datatype_fixed_length, rows_value);

    auto* column = assert_cast<ColumnFixedLengthObject*>(fixed_length_cols[0].get());
    auto size = datatype_fixed_length->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result = datatype_fixed_length->serialize(*column, buf.get(),
                                                    BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = datatype_fixed_length->create_column();
    datatype_fixed_length->deserialize(buf.get(), &column2,
                                       BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnFixedLengthObject*>(column2.get());
        ASSERT_EQ(column->get_data()[i], column_res->get_data()[i]);
    }
    helper->serialize_deserialize_assert(fixed_length_cols, {datatype_fixed_length});
    std::cout << "finish serialize deserialize test" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(Params, DataTypeFixedLengthObjectTest, ::testing::Values(0, 1, 31, 1024));

} // namespace doris::vectorized