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

#include "vec/data_types/data_type_hll.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

// this test is gonna to be a data type test template for all DataType which should make ut test to coverage the function defined
// for example DataTypeHLL should test this function:
// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric,
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         is_null_literal
// 2. datatype creation with column : create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size), get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)

namespace doris::vectorized {

class DataTypeHLLTest : public ::testing::TestWithParam<int> {
protected:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }

public:
    std::unique_ptr<CommonDataTypeTest> helper;
    int rows_value;
    DataTypePtr dt_hll =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_HLL, 0, 0);
};

TEST_P(DataTypeHLLTest, MetaInfoTest) {
    auto hll_type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_HLL, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_HLL);
    CommonDataTypeTest::DataTypeMetaInfo hll_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_HLL,
            .type_as_type_descriptor = hll_type_descriptor,
            .family_name = "HLL",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_HLL,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .pColumnMeta = col_meta.get(),
            .default_field = Field::create_field<TYPE_HLL>(HyperLogLog::empty()),
    };
    helper->meta_info_assert(dt_hll, hll_meta_info_to_assert);
}

TEST_P(DataTypeHLLTest, CreateColumnTest) {
    Field default_field_hll = Field::create_field<TYPE_HLL>(HyperLogLog::empty());
    helper->create_column_assert(dt_hll, default_field_hll, 17);
}

void insert_data_hll(MutableColumns* hll_cols, DataTypePtr datetype_hll, int rows_value,
                     std::vector<std::string>* data_strs = nullptr) {
    auto serde_hll = datetype_hll->get_serde(1);
    auto column_hll = datetype_hll->create_column();

    hll_cols->push_back(column_hll->get_ptr());
    DataTypeSerDeSPtrs serde = {datetype_hll->get_serde()};
    auto& data = assert_cast<ColumnHLL*>((*hll_cols)[0].get())->get_data();
    for (size_t i = 0; i != rows_value; ++i) {
        HyperLogLog hll_value;
        for (size_t j = 0; j <= i; ++j) {
            hll_value.update(j);
        }
        if (data_strs) {
            data_strs->push_back(hll_value.to_string());
        }
        std::string memory_buffer(hll_value.max_serialized_size(), '0');
        hll_value.serialize(reinterpret_cast<uint8_t*>(memory_buffer.data()));
        data.emplace_back(std::move(hll_value));
    }
    std::cout << "finish insert data" << std::endl;
}

// serialize / deserialize
TEST_P(DataTypeHLLTest, SerializeDeserializeTest) {
    MutableColumns hll_cols;
    insert_data_hll(&hll_cols, dt_hll, rows_value);

    auto* column = assert_cast<ColumnHLL*>(hll_cols[0].get());
    auto size = dt_hll->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result =
            dt_hll->serialize(*column, buf.get(), BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = dt_hll->create_column();
    dt_hll->deserialize(buf.get(), &column2, BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnHLL*>(column2.get());
        ASSERT_EQ(column->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    helper->serialize_deserialize_assert(hll_cols, {dt_hll});
    std::cout << "finish serialize deserialize test" << std::endl;
}

// serialize / deserialize as stream
TEST_P(DataTypeHLLTest, SerializeDeserializeAsStreamTest) {
    MutableColumns hll_cols;
    insert_data_hll(&hll_cols, dt_hll, rows_value);

    auto ser_col = ColumnString::create();
    VectorBufferWriter buffer_writer(*ser_col.get());
    auto* column_data = assert_cast<ColumnHLL*>(hll_cols[0].get());
    auto c = dt_hll->create_column();
    auto* column_res = assert_cast<ColumnHLL*>(c.get());
    column_res->resize(rows_value);
    for (size_t i = 0; i != rows_value; ++i) {
        doris::vectorized::DataTypeHLL::serialize_as_stream(column_data->get_element(i),
                                                            buffer_writer);
        buffer_writer.commit();
        BufferReadable buffer_readable(ser_col->get_data_at(i));
        doris::vectorized::DataTypeHLL::deserialize_as_stream(column_res->get_element(i),
                                                              buffer_readable);
        ASSERT_EQ(column_data->get_data()[i].to_string(), column_res->get_data()[i].to_string());
    }
    std::cout << "finish serialize deserialize as stream test" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(Params, DataTypeHLLTest, ::testing::Values(0, 1, 10, 100));

} // namespace doris::vectorized