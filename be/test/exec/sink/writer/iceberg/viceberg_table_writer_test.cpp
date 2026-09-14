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
#include "core/data_type/data_type_varbinary.h"
#include "exec/sink/writer/iceberg/partition_transformers.h"
#include "format/table/iceberg/partition_spec_parser.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"

namespace doris {

TEST(VIcebergTableWriterTest, BinaryIdentityStaticAndDynamicRouting) {
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(false, 1, "key", std::make_unique<iceberg::BinaryType>(), std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(columns));
    auto spec = iceberg::PartitionSpecParser::from_json(
            schema,
            R"({"spec-id":0,"fields":[{"name":"key","transform":"identity","source-id":1,"field-id":1000}]})");
    auto type = std::make_shared<DataTypeVarbinary>();
    TIcebergTableSink sink;
    sink.__set_static_partition_values({{"key", "0xdead"}});
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(sink);
    VExprContextSPtrs exprs;
    VIcebergTableWriter writer(data_sink, exprs);
    writer._schema = schema;
    writer._iceberg_partition_columns.emplace_back(
            spec->fields()[0], TYPE_VARBINARY, 0,
            std::make_unique<IdentityPartitionColumnTransform>(type));
    auto column = type->create_column();
    const std::string bytes("\xDE\xAD", 2);
    column->insert_data(bytes.data(), bytes.size());
    ColumnWithTypeAndName partition(column->get_ptr(), type, "key");
    auto value = writer._get_iceberg_partition_value(TYPE_VARBINARY, partition, 0);
    EXPECT_EQ(bytes, std::any_cast<std::string>(value));
    IcebergPartitionData data({value});
    EXPECT_EQ("key=3q0%3D", writer._partition_to_path(data));
    EXPECT_EQ(std::vector<std::string>({"0xdead"}), writer._partition_values(data));
    writer._init_static_partition_values();
    EXPECT_TRUE(writer._is_full_static_partition);
    EXPECT_EQ("key=3q0%3D", writer._static_partition_path);
    EXPECT_EQ(std::vector<std::string>({"0xdead"}), writer._static_partition_value_list);
    // Add a dynamic field to exercise hybrid routing with the same static binary value.
    iceberg::PartitionField dynamic_field(2, 1001, "id", "identity");
    writer._iceberg_partition_columns.emplace_back(
            dynamic_field, TYPE_INT, 1,
            std::make_unique<IdentityPartitionColumnTransform>(std::make_shared<DataTypeInt32>()));
    writer._init_static_partition_values();
    EXPECT_FALSE(writer._is_full_static_partition);
    IcebergPartitionData hybrid({std::any(), Int32(7)});
    EXPECT_EQ("key=3q0%3D/id=7", writer._partition_to_path(hybrid));
    EXPECT_EQ(std::vector<std::string>({"0xdead", "7"}), writer._partition_values(hybrid));
}

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
    VExprContextSPtrs output_exprs;
    VIcebergTableWriter writer(data_sink, output_exprs);
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

TEST(VIcebergTableWriterTest, UuidStaticPartitionAcceptsCanonicalText) {
    std::vector<iceberg::NestedField> columns;
    columns.emplace_back(false, 1, "key", std::make_unique<iceberg::UUIDType>(), std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(columns));
    auto spec = iceberg::PartitionSpecParser::from_json(
            schema,
            R"({"spec-id":0,"fields":[{"name":"key","transform":"identity","source-id":1,"field-id":1000}]})");
    const std::string uuid = "00112233-4455-6677-8899-aabbccddeeff";
    TIcebergTableSink sink;
    sink.__set_static_partition_values({{"key", uuid}});
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(sink);
    VExprContextSPtrs exprs;
    VIcebergTableWriter writer(data_sink, exprs);
    writer._schema = schema;
    writer._iceberg_partition_columns.emplace_back(
            spec->fields()[0], TYPE_VARBINARY, 0,
            std::make_unique<IdentityPartitionColumnTransform>(
                    std::make_shared<DataTypeVarbinary>()));
    writer._init_static_partition_values();
    EXPECT_EQ("key=" + uuid, writer._static_partition_path);
    EXPECT_EQ(std::vector<std::string>({"0x00112233445566778899aabbccddeeff"}),
              writer._static_partition_value_list);
    writer._t_sink.iceberg_table_sink.static_partition_values["key"] = "invalid";
    EXPECT_THROW(writer._init_static_partition_values(), Exception);
}

} // namespace doris
