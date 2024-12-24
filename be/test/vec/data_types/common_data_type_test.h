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
#include <gen_cpp/data.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "olap/schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"

// this test is gonna to be a data type test template for all DataType which should make ut test to coverage the function defined
// for example DataTypeIPv4 should test this function:
// 1. datatype meta info:
//         get_type_id, get_type_as_type_descriptor, get_storage_field_type, have_subtypes, get_pdata_type (const IDataType *data_type), to_pb_column_meta (PColumnMeta *col_meta)
//         get_family_name, get_is_parametric, should_align_right_in_pretty_formats
//         text_can_contain_only_valid_utf8
//         have_maximum_size_of_value, get_maximum_size_of_value_in_memory, get_size_of_value_in_memory
//         get_precision, get_scale
//         get_field
//         is_null_literal, is_value_represented_by_number, is_value_unambiguously_represented_in_contiguous_memory_region, is_value_unambiguously_represented_in_fixed_size_contiguous_memory_region
// 2. datatype creation with column: create_column, create_column_const (size_t size, const Field &field), create_column_const_with_default_value (size_t size),  get_uncompressed_serialized_bytes (const IColumn &column, int be_exec_version)
// 3. serde related: get_serde (int nesting_level=1)
//          to_string (const IColumn &column, size_t row_num, BufferWritable &ostr), to_string (const IColumn &column, size_t row_num), to_string_batch (const IColumn &column, ColumnString &column_to), from_string (ReadBuffer &rb, IColumn *column)
//          this two function should move to DataTypeSerDe and only used in Block
//          serialize (const IColumn &column, char *buf, int be_exec_version), deserialize (const char *buf, MutableColumnPtr *column, int be_exec_version)
// 4. compare: equals (const IDataType &rhs), is_comparable

namespace doris::vectorized {

static bool gen_check_data_in_assert = true;

class CommonDataTypeTest : public ::testing::Test {
protected:
    // Helper function to load data from CSV, with index which splited by spliter and load to columns
    void load_data_from_csv(const DataTypeSerDeSPtrs serders, MutableColumns& columns,
                            const std::string& file_path, const char spliter = ';',
                            const std::set<int> idxes = {0}) {
        ASSERT_EQ(serders.size(), columns.size())
                << "serder size: " << serders.size() << " column size: " << columns.size();
        ASSERT_EQ(serders.size(), idxes.size())
                << "serder size: " << serders.size() << " idxes size: " << idxes.size();
        std::ifstream file(file_path);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                                   file_path);
        }

        std::string line;
        DataTypeSerDe::FormatOptions options;
        while (std::getline(file, line)) {
            std::stringstream lineStream(line);
            //            std::cout << "whole : " << lineStream.str() << std::endl;
            std::string value;
            int l_idx = 0;
            int c_idx = 0;
            while (std::getline(lineStream, value, spliter)) {
                if (idxes.contains(l_idx)) {
                    Slice string_slice(value.data(), value.size());
                    std::cout << string_slice << std::endl;
                    if (auto st = serders[c_idx]->deserialize_one_cell_from_json(
                                *columns[c_idx], string_slice, options);
                        !st.ok()) {
                        //                        std::cout << "error in deserialize but continue: " << st.to_string()
                        //                                  << std::endl;
                    }
                    ++c_idx;
                }
                ++l_idx;
            }
        }
    }

public:
    // we make meta info a default value, so assert should change the struct value to the right value
    struct DataTypeMetaInfo {
        TypeIndex type_id = TypeIndex::Nothing;
        TypeDescriptor* type_as_type_descriptor = nullptr;
        std::string family_name = "";
        bool has_subtypes = false;
        doris::FieldType storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_UNKNOWN;
        bool should_align_right_in_pretty_formats = false;
        bool text_can_contain_only_valid_utf8 = false;
        bool have_maximum_size_of_value = false;
        size_t size_of_value_in_memory = -1;
        size_t precision = -1;
        size_t scale = -1;
        bool is_null_literal = true;
        bool is_value_represented_by_number = false;
        PColumnMeta* pColumnMeta = nullptr;
        DataTypeSerDeSPtr serde = nullptr;
        //        bool is_value_unambiguously_represented_in_contiguous_memory_region = false;
    };
    void SetUp() override {}

    // meta info assert is simple and can be used for all DataType
    void meta_info_assert(DataTypePtr& data_type, DataTypeMetaInfo& meta_info) {
        ASSERT_NE(data_type->get_serde(1), nullptr);
        ASSERT_EQ(IDataType::get_pdata_type(data_type.get()), meta_info.pColumnMeta->type());
        ASSERT_EQ(data_type->get_type_id(), meta_info.type_id);
        ASSERT_EQ(data_type->get_type_as_type_descriptor(), *meta_info.type_as_type_descriptor);
        ASSERT_EQ(data_type->get_family_name(), meta_info.family_name);
        ASSERT_EQ(data_type->have_subtypes(), meta_info.has_subtypes);
        ASSERT_EQ(data_type->get_storage_field_type(), meta_info.storage_field_type);
        ASSERT_EQ(data_type->should_align_right_in_pretty_formats(),
                  meta_info.should_align_right_in_pretty_formats);
        ASSERT_EQ(data_type->text_can_contain_only_valid_utf8(),
                  meta_info.text_can_contain_only_valid_utf8);
        ASSERT_EQ(data_type->have_maximum_size_of_value(), meta_info.have_maximum_size_of_value);
        ASSERT_EQ(data_type->get_size_of_value_in_memory(), meta_info.size_of_value_in_memory);
        if (is_decimal(data_type)) {
            ASSERT_EQ(data_type->get_precision(), meta_info.precision);
            ASSERT_EQ(data_type->get_scale(), meta_info.scale);
        } else {
            EXPECT_ANY_THROW(EXPECT_FALSE(data_type->get_precision()));
            EXPECT_THROW(EXPECT_FALSE(data_type->get_scale()), doris::Exception);
        }
        ASSERT_EQ(data_type->is_null_literal(), meta_info.is_null_literal);
        ASSERT_EQ(data_type->is_value_represented_by_number(),
                  meta_info.is_value_represented_by_number);
        //        ASSERT_EQ(data_type->is_value_unambiguously_represented_in_contiguous_memory_region(), meta_info.is_value_unambiguously_represented_in_contiguous_memory_region);
    }

    // create column assert with default field is simple and can be used for all DataType
    void create_column_assert(DataTypePtr& data_type, Field& default_field) {
        auto column = data_type->create_column();
        ASSERT_EQ(column->size(), 0);
        ColumnPtr const_col = data_type->create_column_const(10, default_field);
        ASSERT_EQ(const_col->size(), 10);
        ColumnPtr default_const_col = data_type->create_column_const_with_default_value(10);
        ASSERT_EQ(default_const_col->size(), 10);
        for (int i = 0; i < 10; ++i) {
            ASSERT_EQ(const_col->operator[](i), default_const_col->operator[](i));
        }
        // get_uncompressed_serialized_bytes
        ASSERT_EQ(data_type->get_uncompressed_serialized_bytes(*column, 0), 4);
    }

    // get_field assert is simple and can be used for all DataType
    void get_field_assert(DataTypePtr& data_type, TExprNode& node, Field& assert_field,
                          bool assert_false = false) {
        if (assert_false) {
            EXPECT_ANY_THROW(data_type->get_field(node))
                    << "get_field_assert: "
                    << " datatype:" + data_type->get_name() << " node_type:" << node.node_type
                    << " field: " << assert_field.get_type() << std::endl;
        } else {
            Field field = data_type->get_field(node);
            ASSERT_EQ(field, assert_field)
                    << "get_field_assert: "
                    << " datatype:" + data_type->get_name() << " node_type:" << node.node_type
                    << " field: " << assert_field.get_type() << std::endl;
        }
    }

    // to_string | to_string_batch | from_string assert is simple and can be used for all DataType
    void assert_to_string_from_string_assert(MutableColumnPtr mutableColumn,
                                             DataTypePtr& data_type) {
        // to_string_batch | from_string
        auto col_to = ColumnString::create();
        data_type->to_string_batch(*mutableColumn, *col_to);
        ASSERT_EQ(col_to->size(), mutableColumn->size());
        // from_string assert col_to to assert_column and check same with mutableColumn
        auto assert_column = data_type->create_column();
        for (int i = 0; i < col_to->size(); ++i) {
            std::string s = col_to->get_data_at(i).to_string();
            ReadBuffer rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(), data_type->from_string(rb, assert_column.get()));
            ASSERT_EQ(assert_column->operator[](i), mutableColumn->operator[](i));
        }
        // to_string | from_string
        auto ser_col = ColumnString::create();
        ser_col->reserve(mutableColumn->size());
        VectorBufferWriter buffer_writer(*ser_col.get());
        for (int i = 0; i < mutableColumn->size(); ++i) {
            data_type->to_string(*mutableColumn, i, buffer_writer);
            buffer_writer.commit();
        }
        // check ser_col to assert_column and check same with mutableColumn
        auto assert_column_1 = data_type->create_column();
        for (int i = 0; i < ser_col->size(); ++i) {
            std::string s = ser_col->get_data_at(i).to_string();
            ReadBuffer rb(s.data(), s.size());
            ASSERT_EQ(Status::OK(), data_type->from_string(rb, assert_column_1.get()));
            ASSERT_EQ(assert_column_1->operator[](i), mutableColumn->operator[](i));
        }
    }

    // should all datatype is compare?
    void assert_compare_behavior(DataTypePtr l_dt, DataTypePtr& r_dt) {
        ASSERT_TRUE(l_dt->is_comparable());
        ASSERT_TRUE(r_dt->is_comparable());
        // compare
        ASSERT_FALSE(l_dt->equals(*r_dt));
    }
};

} // namespace doris::vectorized