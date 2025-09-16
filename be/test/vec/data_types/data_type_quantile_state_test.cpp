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
#include <gtest/gtest.h>

#include <iostream>

#include "agent/be_exec_version_manager.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_quantilestate.h"

// this test is gonna to be a data type test template for all DataType which should make ut test to coverage the function defined
// for example DataTypeQuantileState should test this function:
// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric,
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         is_null_literal
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to)
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)

namespace doris::vectorized {

class DataTypeQuantileStateTest : public ::testing::TestWithParam<int> {
protected:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }

public:
    std::unique_ptr<CommonDataTypeTest> helper;
    int rows_value;
    DataTypePtr datatype_quantile_state = DataTypeFactory::instance().create_data_type(
            FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, 0, 0);
};

TEST_P(DataTypeQuantileStateTest, MetaInfoTest) {
    auto quantile_state_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_QUANTILE_STATE, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_QUANTILE_STATE);
    CommonDataTypeTest::DataTypeMetaInfo quantile_state_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_QUANTILE_STATE,
            .type_as_type_descriptor = quantile_state_type_descriptor,
            .family_name = "QuantileState",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .pColumnMeta = col_meta.get(),
            .default_field = Field::create_field<TYPE_QUANTILE_STATE>(QuantileState()),
    };
    helper->meta_info_assert(datatype_quantile_state, quantile_state_meta_info_to_assert);
}

TEST_P(DataTypeQuantileStateTest, CreateColumnTest) {
    Field default_field_quantile_state = Field::create_field<TYPE_QUANTILE_STATE>(QuantileState());
    helper->create_column_assert(datatype_quantile_state, default_field_quantile_state, 17);
}

void insert_data_quantile_state(MutableColumns* quantile_state_cols,
                                DataTypePtr datetype_quantile_state, int rows_value,
                                std::vector<std::string>* data_strs = nullptr) {
    auto serde_quantile_state = datetype_quantile_state->get_serde(1);
    auto column_quantile_state = datetype_quantile_state->create_column();

    quantile_state_cols->push_back(column_quantile_state->get_ptr());
    DataTypeSerDeSPtrs serde = {datetype_quantile_state->get_serde()};
    auto& data = assert_cast<ColumnQuantileState*>((*quantile_state_cols)[0].get())->get_data();
    for (size_t i = 0; i != rows_value; ++i) {
        QuantileState quantile_state_value;
        for (size_t j = 0; j <= i; ++j) {
            quantile_state_value.add_value(j);
        }
        std::string memory_buffer(quantile_state_value.get_serialized_size(), '0');
        quantile_state_value.serialize(reinterpret_cast<uint8_t*>(memory_buffer.data()));
        data.emplace_back(std::move(quantile_state_value));
    }
    std::cout << "finish insert data" << std::endl;
}

// test to_string | to_string_batch | from_string
TEST_P(DataTypeQuantileStateTest, FromAndToStringTest) {
    MutableColumns quantile_state_cols;
    std::vector<std::string> data_strs;
    insert_data_quantile_state(&quantile_state_cols, datatype_quantile_state, rows_value,
                               &data_strs);

    {
        // to_string_batch | from_string
        auto col_to = ColumnString::create();
        datatype_quantile_state->to_string_batch(*quantile_state_cols[0]->get_ptr(), *col_to);
        ASSERT_EQ(col_to->size(), quantile_state_cols[0]->get_ptr()->size());
        std::cout << "finish to_string_batch | from_string not support test" << std::endl;
    }

    {
        // to_string | from_string
        auto ser_col = ColumnString::create();
        ser_col->reserve(quantile_state_cols[0]->get_ptr()->size());
        VectorBufferWriter buffer_writer(*ser_col.get());
        for (int i = 0; i < quantile_state_cols[0]->get_ptr()->size(); ++i) {
            datatype_quantile_state->to_string(*quantile_state_cols[0]->get_ptr(), i,
                                               buffer_writer);
            std::string res =
                    datatype_quantile_state->to_string(*quantile_state_cols[0]->get_ptr(), i);
            buffer_writer.commit();
            EXPECT_EQ(res, "QuantileState()"); // QuantileState to_string is not implemented
        }
        std::cout << "finish to_string | from_string not support test" << std::endl;
    }
}

// serialize / deserialize
TEST_P(DataTypeQuantileStateTest, SerializeDeserializeTest) {
    MutableColumns quantile_state_cols;
    insert_data_quantile_state(&quantile_state_cols, datatype_quantile_state, rows_value);

    auto* column = assert_cast<ColumnQuantileState*>(quantile_state_cols[0].get());
    auto size = datatype_quantile_state->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result = datatype_quantile_state->serialize(*column, buf.get(),
                                                      BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = datatype_quantile_state->create_column();
    datatype_quantile_state->deserialize(buf.get(), &column2,
                                         BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnQuantileState*>(column2.get());
        ASSERT_EQ(column->get_data()[i].get_serialized_size(),
                  column_res->get_data()[i].get_serialized_size());
    }
    helper->serialize_deserialize_assert(quantile_state_cols, {datatype_quantile_state});
    std::cout << "finish serialize deserialize test" << std::endl;
}

// serialize / deserialize as stream
TEST_P(DataTypeQuantileStateTest, SerializeDeserializeAsStreamTest) {
    MutableColumns quantile_state_cols;
    insert_data_quantile_state(&quantile_state_cols, datatype_quantile_state, rows_value);

    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto* column_data = assert_cast<ColumnQuantileState*>(quantile_state_cols[0].get());
    auto c = datatype_quantile_state->create_column();
    auto* column_res = assert_cast<ColumnQuantileState*>(c.get());
    column_res->resize(rows_value);
    for (size_t i = 0; i != rows_value; ++i) {
        doris::vectorized::DataTypeQuantileState::serialize_as_stream(column_data->get_element(i),
                                                                      buffer_writer);
        buffer_writer.commit();
        BufferReadable buffer_readable(ser_col->get_data_at(i));
        doris::vectorized::DataTypeQuantileState::deserialize_as_stream(column_res->get_element(i),
                                                                        buffer_readable);
        ASSERT_EQ(column_data->get_data()[i].get_serialized_size(),
                  column_res->get_data()[i].get_serialized_size());
    }
    std::cout << "finish serialize deserialize as stream test" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(Params, DataTypeQuantileStateTest, ::testing::Values(0, 1, 100, 1000));
} // namespace doris::vectorized