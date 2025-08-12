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

#include "vec/data_types/data_type_agg_state.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>

#include "agent/be_exec_version_manager.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

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

class DataTypeAggStateTest : public ::testing::TestWithParam<int> {
public:
    void SetUp() override {
        rows_value = GetParam();
        helper = std::make_unique<CommonDataTypeTest>();
    }
    std::unique_ptr<CommonDataTypeTest> helper;
    DataTypePtr sub_type = std::make_shared<DataTypeInt32>();
    DataTypes sub_types = {sub_type};
    // DataTypeAggState---> column_fixed_length_object
    DataTypePtr datatype_agg_state_count = std::make_shared<DataTypeAggState>(
            sub_types, false, "count", BeExecVersionManager::get_newest_version());
    // DataTypeAggState---> column_string
    DataTypePtr datatype_agg_state_hll_union = std::make_shared<DataTypeAggState>(
            sub_types, false, "hll_union", BeExecVersionManager::get_newest_version());
    int rows_value;
};

TEST_P(DataTypeAggStateTest, MetaInfoTest) {
    auto agg_state_type_descriptor = std::make_shared<DataTypeAggState>();
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_AGG_STATE);
    CommonDataTypeTest::DataTypeMetaInfo agg_state_meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_AGG_STATE,
            .type_as_type_descriptor = agg_state_type_descriptor,
            .family_name = "AggState",
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_AGG_STATE,
            .should_align_right_in_pretty_formats = false,
            .text_can_contain_only_valid_utf8 = false,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = size_t(-1),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = false,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_STRING>(String()),
    };
    helper->meta_info_assert(datatype_agg_state_count, agg_state_meta_info_to_assert);
}

TEST_P(DataTypeAggStateTest, CreateColumnTest) {
    std::string res;
    res.resize(8);
    memset(res.data(), 0, 8);
    Field default_field = Field::create_field<TYPE_STRING>(res);
    std::cout << "create_column_assert: " << datatype_agg_state_count->get_name() << std::endl;
    auto column = (datatype_agg_state_count)->create_column();
    ASSERT_EQ(column->size(), 0);
    column->insert_default();
    auto fixed_length_column = ColumnFixedLengthObject::create(8);
    fixed_length_column->insert(default_field);
    ASSERT_EQ(fixed_length_column->size(), 1);

    for (int i = 0; i < 1; ++i) {
        ASSERT_EQ(fixed_length_column->operator[](i), column->operator[](i));
    }
    // get_uncompressed_serialized_bytes
    ASSERT_EQ(datatype_agg_state_count->get_uncompressed_serialized_bytes(
                      *column, BeExecVersionManager::get_newest_version()),
              25);
}

void insert_data_agg_state(MutableColumns* agg_state_cols, DataTypePtr datatype_agg_state,
                           int rows_value, std::vector<std::string>* data_strs = nullptr) {
    auto column_fixed = datatype_agg_state->create_column();
    agg_state_cols->push_back(column_fixed->get_ptr());
    std::cout << "insert_data_agg_state: " << datatype_agg_state->get_name() << " "
              << column_fixed->get_name() << std::endl;
    if (column_fixed->is_column_string()) {
        ASSERT_TRUE(is_string_type(assert_cast<const DataTypeAggState*>(datatype_agg_state.get())
                                           ->get_serialized_type()
                                           ->get_primitive_type()));
        auto* column = assert_cast<ColumnString*>((*agg_state_cols)[0].get());
        for (size_t i = 0; i != rows_value; ++i) {
            auto val = std::to_string(i);
            column->insert_data(val.c_str(), val.size());
            if (data_strs) {
                data_strs->push_back(val);
            }
            // std::cout<<"insert_data_agg_state: "<<val<<" "<<val.size()<<" "<<column->get_data_at(i).to_string()<<std::endl;
        }
    } else {
        assert_cast<ColumnFixedLengthObject*>((*agg_state_cols)[0].get())->set_item_size(8);
        column_fixed->resize(rows_value);
        ASSERT_EQ(assert_cast<const DataTypeAggState*>(datatype_agg_state.get())
                          ->get_serialized_type()
                          ->get_primitive_type(),
                  TYPE_FIXED_LENGTH_OBJECT);
        auto& data = assert_cast<ColumnFixedLengthObject*>((*agg_state_cols)[0].get())->get_data();
        for (size_t i = 0; i != rows_value; ++i) {
            data[i] = i;
        }
    }
    std::cout << "finish insert data" << std::endl;
}

// // not support function: get_filed

// test to_string | to_string_batch | from_string
TEST_P(DataTypeAggStateTest, FromAndToStringTest) {
    MutableColumns agg_state_cols;
    std::vector<std::string> data_strs;
    insert_data_agg_state(&agg_state_cols, datatype_agg_state_hll_union, rows_value, &data_strs);

    {
        // to_string_batch | from_string
        auto col_to = ColumnString::create();
        datatype_agg_state_hll_union->to_string_batch(*agg_state_cols[0]->get_ptr(), *col_to);
        ASSERT_EQ(col_to->size(), agg_state_cols[0]->get_ptr()->size());
        // from_string assert col_to to assert_column and check same with mutableColumn
        auto assert_column = datatype_agg_state_hll_union->create_column();
        for (int i = 0; i < col_to->size(); ++i) {
            std::string s = col_to->get_data_at(i).to_string();
            std::cout << "s: " << s << std::endl;
            StringRef rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(),
                      datatype_agg_state_hll_union->from_string(rb, assert_column.get()));
            ASSERT_EQ(assert_column->operator[](i), agg_state_cols[0]->get_ptr()->operator[](i))
                    << "i: " << i << " s: " << s
                    << " datatype: " << datatype_agg_state_hll_union->get_name()
                    << " assert_column: " << assert_column->get_name()
                    << " mutableColumn:" << agg_state_cols[0]->get_ptr()->get_name() << std::endl;
        }
        std::cout << "finish to_string_batch | from_string test" << std::endl;
    }

    {
        // to_string | from_string
        auto ser_col = ColumnString::create();
        ser_col->reserve(agg_state_cols[0]->get_ptr()->size());
        VectorBufferWriter buffer_writer(*ser_col.get());
        for (int i = 0; i < agg_state_cols[0]->get_ptr()->size(); ++i) {
            datatype_agg_state_hll_union->to_string(*agg_state_cols[0]->get_ptr(), i,
                                                    buffer_writer);
            std::string res =
                    datatype_agg_state_hll_union->to_string(*agg_state_cols[0]->get_ptr(), i);
            buffer_writer.commit();
            EXPECT_EQ(data_strs[i], ser_col->get_data_at(i).to_string());
        }
        // check ser_col to assert_column and check same with mutableColumn
        auto assert_column_1 = datatype_agg_state_hll_union->create_column();
        for (int i = 0; i < ser_col->size(); ++i) {
            std::string s = ser_col->get_data_at(i).to_string();
            StringRef rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(),
                      datatype_agg_state_hll_union->from_string(rb, assert_column_1.get()));
            auto aaa = assert_column_1->operator[](i);
            ASSERT_EQ(assert_column_1->operator[](i), agg_state_cols[0]->get_ptr()->operator[](i));
        }
        std::cout << "finish to_string | from_string test" << std::endl;
    }
}

// // serialize / deserialize
TEST_P(DataTypeAggStateTest, SerializeDeserializeTest) {
    MutableColumns agg_state_cols;
    insert_data_agg_state(&agg_state_cols, datatype_agg_state_hll_union, rows_value);

    auto* column = assert_cast<ColumnString*>(agg_state_cols[0].get());
    auto size = datatype_agg_state_hll_union->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result = datatype_agg_state_hll_union->serialize(
            *column, buf.get(), BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = datatype_agg_state_hll_union->create_column();
    datatype_agg_state_hll_union->deserialize(buf.get(), &column2,
                                              BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnString*>(column2.get());
        ASSERT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i).to_string());
    }
    helper->serialize_deserialize_assert(agg_state_cols, {datatype_agg_state_hll_union});
    std::cout << "finish serialize deserialize test" << std::endl;
}

// // serialize / deserialize
TEST_P(DataTypeAggStateTest, SerializeDeserializeTest2) {
    MutableColumns agg_state_cols;
    insert_data_agg_state(&agg_state_cols, datatype_agg_state_count, rows_value);

    auto* column = assert_cast<ColumnFixedLengthObject*>(agg_state_cols[0].get());
    auto size = datatype_agg_state_count->get_uncompressed_serialized_bytes(
            *column, BeExecVersionManager::get_newest_version());
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
    auto* result = datatype_agg_state_count->serialize(*column, buf.get(),
                                                       BeExecVersionManager::get_newest_version());
    ASSERT_EQ(result, buf.get() + size);

    auto column2 = datatype_agg_state_count->create_column();
    datatype_agg_state_count->deserialize(buf.get(), &column2,
                                          BeExecVersionManager::get_newest_version());
    for (size_t i = 0; i != rows_value; ++i) {
        auto* column_res = assert_cast<ColumnFixedLengthObject*>(column2.get());
        ASSERT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i).to_string());
    }
    helper->serialize_deserialize_assert(agg_state_cols, {datatype_agg_state_count});
    std::cout << "finish serialize deserialize test2" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(Params, DataTypeAggStateTest, ::testing::Values(0, 1, 31));

} // namespace doris::vectorized