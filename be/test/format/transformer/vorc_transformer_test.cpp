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

#include "format/transformer/vorc_transformer.h"

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg/schema_parser.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/uid_util.h"

namespace doris {

class VOrcTransformerTest : public testing::Test {
protected:
    void SetUp() override {
        _file_path = "./vorc_transformer_" + UniqueId::gen_uid().to_string() + ".orc";
        _fs = io::global_local_filesystem();
    }

    void TearDown() override { static_cast<void>(_fs->delete_file(_file_path)); }

    std::string _file_path;
    std::shared_ptr<io::FileSystem> _fs;
};

TEST_F(VOrcTransformerTest, CollectsBoundsForTopLevelFieldAfterStruct) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto string_type = std::make_shared<DataTypeString>();
    VExprContextSPtrs output_exprs =
            MockSlotRef::create_mock_contexts(DataTypes {struct_type, string_type});

    const std::string schema_json = R"({
        "type": "struct",
        "fields": [
            {
                "id": 1,
                "name": "s",
                "required": true,
                "type": {
                    "type": "struct",
                    "fields": [
                        {"id": 2, "name": "a", "required": true, "type": "int"}
                    ]
                }
            },
            {"id": 3, "name": "b", "required": true, "type": "string"}
        ]
    })";
    std::unique_ptr<iceberg::Schema> schema = iceberg::SchemaParser::from_json(schema_json);

    io::FileWriterPtr file_writer;
    ASSERT_TRUE(_fs->create_file(_file_path, &file_writer).ok());
    RuntimeState state;
    VOrcTransformer transformer(&state, file_writer.get(), output_exprs, "", {"s", "b"}, false,
                                TFileCompressType::PLAIN, schema.get(), _fs);
    ASSERT_TRUE(transformer.open().ok());

    auto nested_column = ColumnInt32::create();
    nested_column->insert_value(-1);
    Columns struct_columns;
    struct_columns.emplace_back(std::move(nested_column));
    auto struct_column = ColumnStruct::create(std::move(struct_columns));
    auto string_column = ColumnString::create();
    string_column->insert_data("hello", 5);

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(struct_column), struct_type, "s"));
    block.insert(ColumnWithTypeAndName(std::move(string_column), string_type, "b"));
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    TIcebergColumnStats stats;
    ASSERT_TRUE(transformer.collect_file_statistics_after_close(&stats).ok());
    ASSERT_TRUE(stats.__isset.lower_bounds);
    ASSERT_TRUE(stats.__isset.upper_bounds);
    ASSERT_EQ(1, stats.lower_bounds.count(3));
    ASSERT_EQ(1, stats.upper_bounds.count(3));
    EXPECT_EQ("hello", stats.lower_bounds.at(3));
    EXPECT_EQ("hello", stats.upper_bounds.at(3));
}

} // namespace doris
