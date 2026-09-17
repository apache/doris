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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <paimon/commit_message.h>
#include <paimon/factories/factory_creator.h>
#include <paimon/schema/schema.h>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"
#include "format/arrow/arrow_block_convertor.h"

namespace doris {

TEST(CppPaimonWriteBackendTest, ConversionPreservesMapFieldMetadata) {
    auto ids = arrow::KeyValueMetadata::Make({"paimon.field.id"}, {"3"});
    auto map = std::make_shared<arrow::MapType>(arrow::field("key", arrow::utf8(), false, ids),
                                                arrow::field("value", arrow::int32(), true, ids));
    DataTypePtr doris_map =
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                          make_nullable(std::make_shared<DataTypeInt32>()));
    for (bool nested : {false, true}) {
        DataTypePtr type =
                nested ? std::make_shared<DataTypeArray>(make_nullable(doris_map)) : doris_map;
        std::shared_ptr<arrow::DataType> target = nested ? arrow::list(map) : map;
        auto column = type->create_column();
        column->insert_default();
        Block block {{std::move(column), type, "m"}};
        auto schema = arrow::schema({arrow::field("m", target)});
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(convert_to_arrow_batch(block, schema, arrow::default_memory_pool(), &batch,
                                           cctz::utc_time_zone())
                            .ok());
        ASSERT_TRUE(validate_paimon_cpp_batch(*batch, *schema).ok());
        // The array-only C Data boundary must see the same nested layout and IDs as the schema.
        ArrowArray exported {};
        ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &exported).ok());
        auto imported = arrow::ImportRecordBatch(&exported, schema);
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        EXPECT_TRUE(imported.ValueOrDie()->Equals(*batch, /*check_metadata=*/true));
    }
}

TEST(CppPaimonWriteBackendTest, RejectsLargeOffsetsBeforeCDataExport) {
    auto check = [](const std::shared_ptr<arrow::DataType>& expected,
                    const std::shared_ptr<arrow::DataType>& actual) {
        auto array = arrow::MakeArrayOfNull(actual, 1).ValueOrDie();
        auto schema = arrow::schema({arrow::field("col", expected)});
        auto batch = arrow::RecordBatch::Make(schema, 1, {array});
        EXPECT_FALSE(validate_paimon_cpp_batch(*batch, *schema).ok());
    };
    // Small arrays exercise the offset-layout mismatch without allocating a 2 GiB input.
    check(arrow::utf8(), arrow::large_utf8());
    check(arrow::binary(), arrow::large_binary());
    check(arrow::struct_({arrow::field("value", arrow::binary(), false)}),
          arrow::struct_({arrow::field("value", arrow::large_binary(), false)}));
}

TEST(CppPaimonWriteBackendTest, VariantSchemaPreservesSdkMetadataAndLayout) {
    auto table = paimon::DataSchema::FromJson(R"({
        "id": 0, "fields": [{"id": 0, "name": "payload", "type": "VARIANT"}],
        "highestFieldId": 0, "partitionKeys": [], "primaryKeys": [], "options": {}
    })");
    ASSERT_TRUE(table.ok()) << table.status().ToString();
    auto exported = table.value()->GetArrowSchema();
    ASSERT_TRUE(exported.ok()) << exported.status().ToString();
    auto imported = arrow::ImportSchema(exported.value().get());
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    auto schema = imported.ValueOrDie();
    auto field = schema->field(0);
    ASSERT_TRUE(field->HasMetadata());
    EXPECT_EQ("paimon.type.variant", field->metadata()->Get("paimon.extension.type").ValueOrDie());
    auto type = field->type();
    ASSERT_EQ(arrow::Type::STRUCT, type->id());
    ASSERT_EQ(2, type->num_fields());
    EXPECT_EQ("value", type->field(0)->name());
    EXPECT_EQ("metadata", type->field(1)->name());
    for (const auto& child : type->fields()) {
        EXPECT_EQ(arrow::Type::BINARY, child->type()->id());
        EXPECT_FALSE(child->nullable());
        EXPECT_TRUE(child->HasMetadata());
    }
    auto array = arrow::MakeArrayOfNull(type, 0).ValueOrDie();
    auto batch = arrow::RecordBatch::Make(schema, 0, {array});
    EXPECT_TRUE(validate_paimon_cpp_batch(*batch, *schema).ok());
    auto stripped = arrow::schema({field->RemoveMetadata()});
    EXPECT_FALSE(validate_paimon_cpp_batch(*batch, *stripped).ok());
    for (auto wrong : {arrow::struct_({type->field(1), type->field(0)}),
                       arrow::struct_({type->field(0)->WithNullable(true), type->field(1)})}) {
        auto wrong_array = arrow::MakeArrayOfNull(wrong, 0).ValueOrDie();
        auto wrong_batch = arrow::RecordBatch::Make(schema, 0, {wrong_array});
        EXPECT_FALSE(validate_paimon_cpp_batch(*wrong_batch, *schema).ok());
    }
}

TEST(CppPaimonWriteBackendTest, DpcmFrame) {
    TPaimonCommitMessage message;
    ASSERT_TRUE(frame_paimon_cpp_commit(std::string("a\0b", 3), 11, &message).ok());
    EXPECT_EQ(std::string("DPCM\0\0\0\x0b\0\0\0\x03", 12) + std::string("a\0b", 3),
              message.payload);
    EXPECT_TRUE(message.__isset.payload);
    EXPECT_FALSE(frame_paimon_cpp_commit(std::string(8 * 1024 * 1024, 'x'), 11, &message).ok());
    EXPECT_FALSE(frame_paimon_cpp_commit("", -1, &message).ok());
}

TEST(CppPaimonWriteBackendTest, LinkedFormatsAndCommitVersion) {
    // Exercise link-time registration without dlopen. An SDK upgrade must revalidate the
    // native serializer against Java FE before changing this expected version.
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("parquet"));
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("orc"));
    EXPECT_NE(nullptr, paimon::FactoryCreator::GetInstance()->Create("avro"));
    EXPECT_EQ(12, paimon::CommitMessage::CurrentVersion());
}

} // namespace doris
