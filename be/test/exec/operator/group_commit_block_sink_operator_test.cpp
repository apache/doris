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

#include "exec/operator/group_commit_block_sink_operator.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/vtablet_block_convertor.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/mock/mock_descriptors.h"
#include "util/defer_op.h"

namespace doris {

class GroupCommitAnnValidationTest : public testing::Test {
protected:
    RowDescriptor row_desc;
    std::vector<TExpr> expressions;
    GroupCommitBlockSinkOperatorX sink {0, row_desc, expressions};

    static ColumnWithTypeAndName array_column(const std::vector<size_t>& dimensions,
                                              const std::string& name,
                                              const std::vector<size_t>& null_positions = {}) {
        auto values = ColumnFloat32::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        size_t total = 0;
        for (auto dimension : dimensions) {
            values->insert_many_defaults(dimension);
            total += dimension;
            offsets->insert_value(total);
        }
        auto nulls = ColumnUInt8::create(total, 0);
        for (auto position : null_positions) {
            nulls->get_data()[position] = 1;
        }
        return {ColumnArray::create(ColumnNullable::create(std::move(values), std::move(nulls)),
                                    std::move(offsets)),
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()), name};
    }
};

TEST_F(GroupCommitAnnValidationTest, ValidateEveryRowAndEveryBlock) {
    sink._ann_columns = {{0, 3, "vec"}};
    Block valid {array_column({3, 3}, "vec")};
    ASSERT_TRUE(sink._validate_ann_vectors(valid).ok());
    for (size_t dimension : {2, 4}) {
        Block invalid {array_column({3, dimension}, "vec")};
        auto status = sink._validate_ann_vectors(invalid);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("row: 1"), std::string::npos);
    }
    Block sparse {array_column({0, 3, 0, 3, 0}, "vec")};
    const bool previous = config::enable_ann_index_empty_array;
    Defer restore([&] { config::enable_ann_index_empty_array = previous; });
    config::enable_ann_index_empty_array = false;
    auto status = sink._validate_ann_vectors(sparse);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("enable_ann_index_empty_array=true"), std::string::npos);
    config::enable_ann_index_empty_array = true;
    EXPECT_TRUE(sink._validate_ann_vectors(sparse).ok());
    config::enable_ann_index_empty_array = false;
    EXPECT_FALSE(sink._validate_ann_vectors(sparse).ok());
    // An empty retained batch can occur after ordinary load filtering.
    Block empty {array_column({}, "vec")};
    EXPECT_TRUE(sink._validate_ann_vectors(empty).ok());
}

TEST_F(GroupCommitAnnValidationTest, OnlyIndexedColumnsHaveFixedDimensions) {
    Block block {array_column({0, 1}, "ordinary_array"), array_column({3, 3}, "vec"),
                 array_column({2, 1}, "vec2")};
    EXPECT_TRUE(sink._validate_ann_vectors(block).ok());
    sink._ann_columns = {{1, 3, "vec"}};
    EXPECT_TRUE(sink._validate_ann_vectors(block).ok());
    sink._ann_columns.push_back({2, 2, "vec2"});
    auto status = sink._validate_ann_vectors(block);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Column: vec2"), std::string::npos);
}

TEST_F(GroupCommitAnnValidationTest, ConstantAndNullableWrappers) {
    sink._ann_columns = {{0, 3, "vec"}};
    auto column = array_column({3}, "vec");
    column.column = ColumnConst::create(column.column, 2);
    Block constant {column};
    EXPECT_TRUE(sink._validate_ann_vectors(constant).ok());

    column = array_column({3, 2}, "vec");
    column.column = make_nullable(column.column);
    column.type = make_nullable(column.type);
    Block nullable {column};
    EXPECT_FALSE(sink._validate_ann_vectors(nullable).ok());

    column = array_column({3}, "vec", {1});
    column.column = ColumnConst::create(column.column, 2);
    Block constant_null_element {column};
    auto status = sink._validate_ann_vectors(constant_null_element);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("NULL elements"), std::string::npos);
}

TEST_F(GroupCommitAnnValidationTest, RejectNullElementsOnlyInIndexedColumns) {
    const bool previous = config::enable_ann_index_empty_array;
    Defer restore([&] { config::enable_ann_index_empty_array = previous; });
    for (bool allow_empty : {false, true}) {
        config::enable_ann_index_empty_array = allow_empty;
        sink._ann_columns = {{1, 3, "vec"}, {2, 2, "vec2"}};
        Block valid {array_column({3, 3}, "ordinary_array", {1, 4}), array_column({3, 3}, "vec"),
                     array_column({2, 2}, "vec2")};
        EXPECT_TRUE(sink._validate_ann_vectors(valid).ok());
        // Check the first/last coordinate of both rows, including the second ANN index.
        for (size_t position : {0, 1, 2, 3, 4, 5}) {
            Block invalid {array_column({3, 3}, "ordinary_array"),
                           array_column({3, 3}, "vec", {position}), array_column({2, 2}, "vec2")};
            auto status = sink._validate_ann_vectors(invalid);
            ASSERT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("NULL elements. Column: vec, row: " +
                                              std::to_string(position / 3)),
                      std::string::npos);
        }
        Block invalid_second {array_column({3, 3}, "ordinary_array"), array_column({3, 3}, "vec"),
                              array_column({2, 2}, "vec2", {3})};
        auto status = sink._validate_ann_vectors(invalid_second);
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("Column: vec2, row: 1"), std::string::npos);
    }
    config::enable_ann_index_empty_array = true;
    sink._ann_columns = {{0, 3, "vec"}};
    Block sparse {array_column({0, 3, 0, 3, 0}, "vec", {4})};
    auto status = sink._validate_ann_vectors(sparse);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("NULL elements. Column: vec, row: 3"), std::string::npos);
}

TEST_F(GroupCommitAnnValidationTest, ResolveIndexMetadataByName) {
    TSlotDescriptor slot_desc;
    slot_desc.colName = "vec";
    slot_desc.nullIndicatorBit = -1;
    TTypeNode array_type;
    array_type.type = TTypeNodeType::ARRAY;
    array_type.__set_contains_nulls({true});
    TTypeNode float_type;
    float_type.type = TTypeNodeType::SCALAR;
    float_type.__isset.scalar_type = true;
    float_type.scalar_type.type = TPrimitiveType::FLOAT;
    slot_desc.slotType.types = {array_type, float_type};
    SlotDescriptor slot(slot_desc);
    MockTupleDescriptor tuple;
    tuple.Slots = {&slot};
    sink._output_tuple_desc = &tuple;

    TabletColumn column;
    column.set_name("VeC");
    column.set_unique_id(17);
    TOlapTableIndex thrift_index;
    thrift_index.__set_index_type(TIndexType::ANN);
    thrift_index.__set_index_name("ann_vec");
    thrift_index.__set_properties({{"dim", "3"}});
    TabletIndex index;
    index.init_from_thrift(thrift_index, std::vector<int32_t> {17});
    OlapTableIndexSchema schema {};
    schema.columns = {&column};
    schema.indexes = {&index};
    sink._schema = std::make_shared<OlapTableSchemaParam>();
    sink._schema->_indexes = {&schema};

    ASSERT_TRUE(sink._init_ann_validation().ok());
    ASSERT_EQ(sink._ann_columns.size(), 1);
    EXPECT_EQ(sink._ann_columns[0].position, 0);
    EXPECT_EQ(sink._ann_columns[0].dimension, 3);

    // Match the writer's std::stoi parsing, including leading signs and numeric prefixes.
    for (const auto* dimension : {"+3", "3bad"}) {
        thrift_index.__set_properties({{"dim", dimension}});
        index.init_from_thrift(thrift_index, std::vector<int32_t> {17});
        ASSERT_TRUE(sink._init_ann_validation().ok());
        ASSERT_EQ(sink._ann_columns.size(), 1);
        EXPECT_EQ(sink._ann_columns[0].dimension, 3);
    }
    thrift_index.__set_properties({});
    TabletIndex missing_dimension_index;
    missing_dimension_index.init_from_thrift(thrift_index, std::vector<int32_t> {17});
    schema.indexes = {&missing_dimension_index};
    auto status = sink._init_ann_validation();
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Missing dim property for ANN index: ann_vec"),
              std::string::npos);
    EXPECT_TRUE(sink._ann_columns.empty());

    schema.indexes = {&index};
    column.set_unique_id(18);
    EXPECT_FALSE(sink._init_ann_validation().ok());
}

} // namespace doris
