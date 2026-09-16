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
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "common/exception.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "exec/sink/writer/iceberg/partition_transformers.h"
#include "exec/sink/writer/iceberg/vpartition_writer_base.h"
#include "format/table/iceberg/partition_spec_parser.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/types.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {
class RecordingPartitionWriter : public IPartitionWriterBase {
public:
    Status open(RuntimeState*, RuntimeProfile*, const RowDescriptor*) override {
        return Status::OK();
    }
    Status write(Block& block) override {
        rows += block.rows();
        for (size_t i = 0; i < block.rows(); ++i) {
            EXPECT_TRUE(block.get_by_position(0).column->is_null_at(i));
        }
        return Status::OK();
    }
    Status close(const Status& status) override { return status; }
    const std::string& file_name() const override { return name; }
    int file_name_index() const override { return 0; }
    size_t written_len() const override { return 0; }
    size_t rows = 0;
    const std::string name = "part";
};
} // namespace

TEST(VIcebergTableWriterTest, TimestampIdentityPreservesUtcCommitAndNull) {
    std::vector<iceberg::NestedField> fields;
    fields.emplace_back(false, 1, "event_time", std::make_unique<iceberg::TimestampType>(true),
                        std::nullopt);
    auto schema = std::make_shared<iceberg::Schema>(std::move(fields));
    auto spec = iceberg::PartitionSpecParser::from_json(
            schema,
            R"({"spec-id":0,"fields":[{"name":"event_time","transform":"identity","source-id":1,"field-id":1000}]})");
    auto type = std::make_shared<DataTypeTimeStampTz>(6);
    TIcebergTableSink sink;
    TDataSink data_sink;
    data_sink.__set_iceberg_table_sink(sink);
    VExprContextSPtrs exprs;
    VIcebergTableWriter writer(data_sink, exprs);
    RuntimeState state;
    state.set_timezone("America/New_York");
    writer._state = &state;
    writer._schema = schema;
    writer._iceberg_partition_columns.emplace_back(
            spec->fields()[0], TYPE_TIMESTAMPTZ, 0,
            std::make_unique<IdentityPartitionColumnTransform>(type));
    auto column = ColumnTimeStampTz::create();
    TimestampTzValue first;
    first.unchecked_set_time(2021, 11, 7, 5, 30, 0, 123456);
    TimestampTzValue second;
    second.unchecked_set_time(2021, 11, 7, 6, 30, 0, 123456);
    column->get_data().assign({first, second, TimestampTzValue()});
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign({0, 0, 1});
    ColumnWithTypeAndName partition(ColumnNullable::create(std::move(column), std::move(null_map)),
                                    make_nullable(type), "event_time");
    std::vector<std::string> paths;
    for (int row = 0; row < 3; ++row) {
        auto value = writer._get_iceberg_partition_value(TYPE_TIMESTAMPTZ, partition, row);
        IcebergPartitionData data({value});
        paths.push_back(writer._partition_to_path(data));
        const std::vector<std::string> expected = {"2021-11-07 05:30:00.123456+00:00",
                                                   "2021-11-07 06:30:00.123456+00:00", "null"};
        EXPECT_EQ(std::vector<std::string>({expected[row]}), writer._partition_values(data));
    }
    EXPECT_NE(paths[0], paths[1]);
    EXPECT_EQ("event_time=null", paths[2]);
    // Static literals with an offset must route to exactly the same partition as dynamic rows.
    writer._t_sink.iceberg_table_sink.__set_static_partition_values(
            {{"event_time", "2021-11-07 01:30:00.123456-05:00"}});
    writer._init_static_partition_values();
    EXPECT_EQ(paths[1], writer._static_partition_path);
    EXPECT_EQ(std::vector<std::string>({"2021-11-07 06:30:00.123456+00:00"}),
              writer._static_partition_value_list);
}

TEST(VIcebergTableWriterTest, StaticNullBinaryPartitionsKeepNullPathsAndCommitValues) {
    for (int kind = 0; kind < 3; ++kind) {
        SCOPED_TRACE(kind);
        std::unique_ptr<iceberg::Type> source_type;
        if (kind == 0) {
            source_type = std::make_unique<iceberg::BinaryType>();
        } else if (kind == 1) {
            source_type = std::make_unique<iceberg::FixedType>(16);
        } else {
            source_type = std::make_unique<iceberg::UUIDType>();
        }
        std::vector<iceberg::NestedField> fields;
        fields.emplace_back(true, 1, "key", std::move(source_type), std::nullopt);
        fields.emplace_back(true, 2, "id", std::make_unique<iceberg::IntegerType>(), std::nullopt);
        auto schema = std::make_shared<iceberg::Schema>(std::move(fields));
        auto spec = iceberg::PartitionSpecParser::from_json(
                schema,
                R"({"spec-id":0,"fields":[{"name":"key","transform":"identity","source-id":1,"field-id":1000}]})");
        // Encode the nullable wire contract independently of the generated reader's schema.
        using namespace apache::thrift::protocol;
        auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        TBinaryProtocol protocol(buffer);
        protocol.writeStructBegin("TIcebergTableSink");
        protocol.writeFieldBegin("static_partition_values", T_MAP, 15);
        protocol.writeMapBegin(T_STRING, T_STRING, 1);
        protocol.writeString(std::string("key"));
        protocol.writeString(std::string("null"));
        protocol.writeMapEnd();
        protocol.writeFieldEnd();
        protocol.writeFieldBegin("static_partition_null_keys", T_SET, 19);
        protocol.writeSetBegin(T_STRING, 1);
        protocol.writeString(std::string("key"));
        protocol.writeSetEnd();
        protocol.writeFieldEnd();
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        TIcebergTableSink sink;
        sink.read(&protocol);
        TDataSink data_sink;
        data_sink.__set_iceberg_table_sink(sink);
        VExprContextSPtrs exprs;
        VIcebergTableWriter writer(data_sink, exprs);
        writer._schema = schema;
        auto type = make_nullable(std::make_shared<DataTypeVarbinary>());
        writer._iceberg_partition_columns.emplace_back(
                spec->fields()[0], TYPE_VARBINARY, 0,
                std::make_unique<IdentityPartitionColumnTransform>(type));
        ASSERT_NO_THROW(writer._init_static_partition_values());
        EXPECT_TRUE(writer._is_full_static_partition);
        ASSERT_EQ("key=null", writer._static_partition_path);
        EXPECT_EQ(std::vector<std::string>({"null"}), writer._static_partition_value_list);
        auto recording_writer = std::make_shared<RecordingPartitionWriter>();
        writer._partitions_to_writers["key=null"] = recording_writer;
        auto null_column = type->create_column();
        null_column->insert_default();
        auto id_type = std::make_shared<DataTypeInt32>();
        auto id_column = id_type->create_column();
        id_column->insert(Field::create_field<TYPE_INT>(7));
        Block full_static({ColumnWithTypeAndName(null_column->get_ptr(), type, "key"),
                           ColumnWithTypeAndName(id_column->get_ptr(), id_type, "id")});
        ASSERT_TRUE(writer._write_prepared_block(full_static).ok());
        EXPECT_EQ(1, recording_writer->rows);
        // Partition columns retain a reference to their spec field throughout dispatch.
        iceberg::PartitionField dynamic_field(2, 1001, "id", "identity");
        writer._iceberg_partition_columns.emplace_back(
                dynamic_field, TYPE_INT, 1,
                std::make_unique<IdentityPartitionColumnTransform>(
                        std::make_shared<DataTypeInt32>()));
        ASSERT_NO_THROW(writer._init_static_partition_values());
        EXPECT_FALSE(writer._is_full_static_partition);
        IcebergPartitionData hybrid({std::any(), Int32(7)});
        ASSERT_EQ("key=null/id=7", writer._partition_to_path(hybrid));
        EXPECT_EQ(std::vector<std::string>({"null", "7"}), writer._partition_values(hybrid));
        writer._partitions_to_writers["key=null/id=7"] = recording_writer;
        Block hybrid_block({ColumnWithTypeAndName(null_column->get_ptr(), type, "key"),
                            ColumnWithTypeAndName(id_column->get_ptr(), id_type, "id")});
        ASSERT_TRUE(writer._write_prepared_block(hybrid_block).ok());
        EXPECT_EQ(2, recording_writer->rows);

        // Empty bytes and malformed non-null bytes must not be silently converted to SQL NULL.
        if (kind == 0) {
            writer._t_sink.iceberg_table_sink.__set_static_partition_null_keys({});
            writer._t_sink.iceberg_table_sink.__set_static_partition_values({{"key", "0x"}});
            ASSERT_NO_THROW(writer._init_static_partition_values());
            EXPECT_EQ("key=/id=7", writer._partition_to_path(hybrid));
            EXPECT_EQ(std::vector<std::string>({"0x", "7"}), writer._partition_values(hybrid));
            writer._t_sink.iceberg_table_sink.__set_static_partition_values({{"key", "null"}});
            EXPECT_THROW(writer._init_static_partition_values(), Exception);
        }
    }
}

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
