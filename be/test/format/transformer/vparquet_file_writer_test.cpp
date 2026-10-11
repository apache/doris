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

#include <arrow/memory_pool.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/schema.h>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg/schema_parser.h"
#include "format/transformer/viceberg_parquet_writer.h"
#include "format/transformer/vparquet_writer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/uid_util.h"

namespace doris {

class VParquetFileWriterTest : public testing::Test {
protected:
    void SetUp() override {
        _file_path = "./vparquet_transformer_" + UniqueId::gen_uid().to_string() + ".parquet";
        _previous_pool = ExecEnv::GetInstance()->_arrow_memory_pool;
        ExecEnv::GetInstance()->_arrow_memory_pool = arrow::default_memory_pool();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_arrow_memory_pool = _previous_pool;
        static_cast<void>(io::global_local_filesystem()->delete_file(_file_path));
    }

    std::string _file_path;
    arrow::MemoryPool* _previous_pool = nullptr;
};

TEST_F(VParquetFileWriterTest, RejectsFileOutputIncludingNestedTypes) {
    const auto file = std::make_shared<DataTypeFile>();
    const auto nullable_file = make_nullable(file);
    const auto string = make_nullable(std::make_shared<DataTypeString>());
    const auto array = make_nullable(std::make_shared<DataTypeArray>(nullable_file));
    const auto map = make_nullable(std::make_shared<DataTypeMap>(string, nullable_file));
    const DataTypes types {
            file,
            nullable_file,
            array,
            map,
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {file}, Strings {"file"})),
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {array, map},
                                                           Strings {"files", "by_name"})),
            make_nullable(std::make_shared<DataTypeArray>(map))};
    const std::string iceberg_json =
            R"({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"f","required":false,"type":"string"}]})";
    const auto iceberg_schema = iceberg::SchemaParser::from_json(iceberg_json);
    RuntimeState state;
    state.set_timezone("UTC");
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        // Check inferred names, explicit Parquet column names, and Iceberg schemas.
        for (int schema_mode = 0; schema_mode < 3; ++schema_mode) {
            SCOPED_TRACE(schema_mode);
            io::FileWriterPtr file_writer;
            ASSERT_TRUE(io::global_local_filesystem()->create_file(_file_path, &file_writer).ok());
            const auto contexts = MockSlotRef::create_mock_contexts(DataTypes {type});
            std::unique_ptr<VParquetWriter> transformer;
            if (schema_mode == 1) {
                TParquetSchema schema;
                schema.__set_schema_column_name("f");
                transformer = std::make_unique<VParquetWriter>(&state, file_writer.get(), contexts,
                                                               std::vector<TParquetSchema> {schema},
                                                               false, ParquetFileOptions {});
            } else if (schema_mode == 2) {
                transformer = std::make_unique<VIcebergParquetWriter>(
                        &state, file_writer.get(), contexts, std::vector<std::string> {"f"}, false,
                        ParquetFileOptions {}, &iceberg_json, *iceberg_schema);
            } else {
                transformer = std::make_unique<VParquetWriter>(&state, file_writer.get(), contexts,
                                                               std::vector<std::string> {"f"},
                                                               false, ParquetFileOptions {});
            }
            const auto status = transformer->open();
            EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
            EXPECT_NE(status.to_string().find("Parquet output does not support type"),
                      std::string::npos);
            EXPECT_EQ(transformer->written_len(), 0);
            EXPECT_TRUE(transformer->close().ok());
        }
    }
}

TEST_F(VParquetFileWriterTest, OrdinaryScalarAndNestedOutputStillWrites) {
    const auto integer = make_nullable(std::make_shared<DataTypeInt32>());
    const auto nested = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeArray>(integer)),
                       make_nullable(std::make_shared<DataTypeMap>(
                               make_nullable(std::make_shared<DataTypeString>()), integer))},
            Strings {"items", "by_name"}));
    const DataTypes types {integer, nested};
    const auto contexts = MockSlotRef::create_mock_contexts(types);
    RuntimeState state;
    state.set_timezone("UTC");
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()->create_file(_file_path, &file_writer).ok());
    VParquetWriter transformer(&state, file_writer.get(), contexts,
                               std::vector<std::string> {"i", "s"}, false, ParquetFileOptions {});
    ASSERT_TRUE(transformer.open().ok());
    Block block;
    for (size_t i = 0; i < types.size(); ++i) {
        auto column = types[i]->create_column();
        column->insert_default();
        block.insert({std::move(column), types[i], i == 0 ? "i" : "s"});
    }
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());
    const auto reader = ::parquet::ParquetFileReader::OpenFile(_file_path);
    EXPECT_EQ(reader->metadata()->num_rows(), 1);
    EXPECT_EQ(reader->metadata()->schema()->group_node()->field_count(), 2);
}

} // namespace doris
