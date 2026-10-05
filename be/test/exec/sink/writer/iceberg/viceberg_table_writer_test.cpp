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

#include "exec/sink/writer/iceberg/viceberg_table_writer.h"

#include <gtest/gtest.h>

#include "common/exception.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/table/iceberg/partition_spec_parser.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"

namespace doris {

TEST(VIcebergTableWriterTest, RejectMissingPartitionSource) {
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(false, 3, "id", std::make_unique<iceberg::IntegerType>(), std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(columns));
    const std::string spec_json =
            R"({"spec-id":1,"fields":[{"name":"missing","transform":"identity",)"
            R"("source-id":1,"field-id":1000}]})";

    TIcebergTableSink iceberg_sink;
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(iceberg_sink);
    VIcebergTableWriter writer(data_sink, {}, nullptr, nullptr);
    writer._schema = schema;
    writer._partition_spec = iceberg::PartitionSpecParser::from_json(schema, spec_json);

    try {
        static_cast<void>(writer._to_iceberg_partition_columns());
        FAIL() << "missing partition source must fail writer initialization";
    } catch (const Exception& exception) {
        EXPECT_NE(exception.to_string().find("source field 1 outside writer schema"),
                  std::string::npos);
    }
}

TEST(VIcebergTableWriterTest, ResolvesNestedPartitionSource) {
    std::vector<iceberg::NestedField> children;
    children.emplace_back(true, 2, "part", std::make_unique<iceberg::IntegerType>(), std::nullopt);
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(true, 1, "payload",
                         std::make_unique<iceberg::StructType>(std::move(children)), std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(columns));
    const std::string spec_json = R"({"spec-id":1,"fields":[{"name":"part","transform":"identity",)"
                                  R"("source-id":2,"field-id":1000}]})";
    auto child_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {"part"}));
    VExprContextSPtrs output_exprs {
            VExprContext::create_shared(VSlotRef::create_shared(0, 0, -1, struct_type, "payload"))};

    TIcebergTableSink iceberg_sink;
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(iceberg_sink);
    VIcebergTableWriter writer(data_sink, output_exprs, nullptr, nullptr);
    writer._schema = schema;
    writer._partition_spec = iceberg::PartitionSpecParser::from_json(schema, spec_json);

    auto partition_columns = writer._to_iceberg_partition_columns();
    ASSERT_EQ(partition_columns.size(), 1);
    EXPECT_EQ(partition_columns[0].source_idx(), 0);
    EXPECT_EQ(partition_columns[0].child_indices(), std::vector<size_t>({0}));
    EXPECT_EQ(partition_columns[0].source_type(), TYPE_INT);

    auto child_data = ColumnInt32::create();
    child_data->insert_value(7);
    child_data->insert_value(8);
    auto child_nulls = ColumnUInt8::create(2, 0);
    auto child_column = ColumnNullable::create(std::move(child_data), std::move(child_nulls));
    Columns children_columns {std::move(child_column)};
    auto struct_column = ColumnStruct::create(std::move(children_columns));
    auto parent_nulls = ColumnUInt8::create(2, 0);
    parent_nulls->get_data()[1] = 1;
    auto parent_column = ColumnNullable::create(std::move(struct_column), std::move(parent_nulls));
    Block block;
    block.insert({std::move(parent_column), struct_type, "payload"});

    auto source = writer._nested_partition_source(block, partition_columns[0]);
    const auto* nullable_source = check_and_get_column<ColumnNullable>(source.column.get());
    ASSERT_NE(nullable_source, nullptr);
    ASSERT_EQ(nullable_source->size(), 2);
    EXPECT_EQ(nullable_source->get_null_map_data()[0], 0);
    EXPECT_EQ(nullable_source->get_null_map_data()[1], 1);
    const auto* source_data =
            check_and_get_column<ColumnInt32>(nullable_source->get_nested_column_ptr().get());
    ASSERT_NE(source_data, nullptr);
    EXPECT_EQ(source_data->get_data()[0], 7);
    EXPECT_EQ(source_data->get_data()[1], 8);
}

} // namespace doris
