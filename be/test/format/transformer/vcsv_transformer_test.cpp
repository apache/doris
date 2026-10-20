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

#include "format/transformer/vcsv_transformer.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/consts.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "io/fs/file_writer.h"
#include "runtime/runtime_state.h"
#include "runtime/workload_management/resource_context.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/block_compression.h"
#include "util/faststring.h"

namespace doris {

class CSVTestFileWriter final : public io::FileWriter {
public:
    Status close(bool) override {
        _state = State::CLOSED;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t data_cnt) override {
        for (size_t i = 0; i < data_cnt; ++i) {
            _data.append(reinterpret_cast<const char*>(data[i].data), data[i].size);
        }
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _data.size(); }
    State state() const override { return _state; }
    const std::string& data() const { return _data; }

private:
    io::Path _path {"csv-test"};
    std::string _data;
    State _state = State::OPENED;
};

static VExprContextSPtrs make_exprs(const DataTypes& types) {
    return MockSlotRef::create_mock_contexts(types);
}

TEST(VCSVTransformerTest, QuotesSpecialCharactersAcrossBlocks) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false, true);

    auto first_column = ColumnString::create();
    const std::string value = "a,\"b\"\r\nc\\d";
    first_column->insert_data(value.data(), value.size());
    first_column->insert_default();
    Block first_block {{std::move(first_column), string_type, "value"}};

    auto second_column = ColumnString::create();
    second_column->insert_data("你好\\世界", 13);
    Block second_block {{std::move(second_column), string_type, "value"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(first_block).ok());
    ASSERT_TRUE(transformer.write(second_block).ok());
    ASSERT_EQ("\"a,\"\"b\"\"\r\nc\\d\"\n\"\"\n\"你好\\世界\"\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, GzipRoundTripPreservesExactQuotedBytes) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false, true,
                                TFileCompressType::GZ);

    auto column = ColumnString::create();
    const std::string value = "comma,\"quote\"\nand newline";
    column->insert_data(value.data(), value.size());
    Block block {{std::move(column), string_type, "value"}};
    const std::string expected = "\"comma,\"\"quote\"\"\nand newline\"\n";

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.close().ok());

    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(TFileCompressType::GZ, &codec).ok());
    ASSERT_NE(nullptr, codec);
    std::string uncompressed(expected.size(), '\0');
    Slice output(uncompressed);
    ASSERT_TRUE(codec->decompress(Slice(file_writer.data()), &output).ok());
    ASSERT_EQ(expected, uncompressed);
}

TEST(VCSVTransformerTest, EmptyBlockWritesNoBytes) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false, true);
    Block empty_block {{string_type->create_column(), string_type, "value"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(empty_block).ok());
    ASSERT_TRUE(file_writer.data().empty());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, QuotesNullableNullAndConstantWithCustomSeparators) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto nullable_string_type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({nullable_string_type, string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", "||", "\r\n", false,
                                true);

    auto nested = ColumnString::create();
    nested->insert_data("value", 5);
    nested->insert_default();
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    auto nullable = ColumnNullable::create(std::move(nested), std::move(null_map));

    auto constant_value = ColumnString::create();
    constant_value->insert_data("fixed", 5);
    auto constant = ColumnConst::create(std::move(constant_value), 2);
    Block block {{std::move(nullable), nullable_string_type, "nullable"},
                 {std::move(constant), string_type, "constant"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_EQ("\"value\"||\"fixed\"\r\n\"\\N\"||\"fixed\"\r\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, QuotesCompleteComplexValue) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto exprs = make_exprs({array_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false, true);

    auto array_column = array_type->create_column();
    Array array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)};
    array_column->insert(Field::create_field<TYPE_ARRAY>(array));
    Block block {{std::move(array_column), array_type, "array_value"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_EQ("\"[1, 2]\"\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, QuotesTypeHeaderAndPreservesBom) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false,
                                BeConsts::CSV_WITH_NAMES_AND_TYPES, "\"na\"\"me\"\n", ",", "\n",
                                true, true);

    ASSERT_TRUE(transformer.open().ok());
    const std::string bom("\xEF\xBB\xBF", 3);
    ASSERT_EQ(bom + "\"na\"\"me\"\n\"STRING\"\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, DisabledRemainsByteCompatible) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false,
                                false);
    auto column = ColumnString::create();
    column->insert_data("a,b\nc", 5);
    Block block {{std::move(column), string_type, "value"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_EQ("a,b\nc\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VCSVTransformerTest, HiveTextIgnoresCsvQuote) {
    RuntimeState state;
    CSVTestFileWriter file_writer;
    auto string_type = std::make_shared<DataTypeString>();
    auto exprs = make_exprs({string_type});
    THiveSerDeProperties hive_properties;
    hive_properties.__set_field_delim(",");
    hive_properties.__set_line_delim("\n");
    hive_properties.__set_collection_delim("\002");
    hive_properties.__set_mapkv_delim("\003");
    hive_properties.__set_null_format("\\N");
    VCSVTransformer transformer(&state, &file_writer, exprs, false, "", "", ",", "\n", false, true,
                                TFileCompressType::PLAIN, &hive_properties);
    auto column = ColumnString::create();
    column->insert_data("a,b", 3);
    Block block {{std::move(column), string_type, "value"}};

    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_EQ("a,b\n", file_writer.data());
    ASSERT_TRUE(transformer.close().ok());
}

} // namespace doris
