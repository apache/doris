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

#include "format/transformer/vjson_transformer.h"

#include <gtest/gtest.h>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "io/fs/file_writer.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "util/block_compression.h"

namespace doris {
namespace {

class JsonMemoryWriter final : public io::FileWriter {
public:
    Status appendv(const Slice* slices, size_t count) override {
        RETURN_IF_ERROR(append_status);
        for (size_t i = 0; i < count; ++i) {
            parts.emplace_back(slices[i].data, slices[i].size);
            data.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }
    Status close(bool non_block = false) override {
        _state = State::CLOSED;
        return close_status;
    }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return data.size(); }
    State state() const override { return _state; }

    std::string data;
    std::vector<std::string> parts;
    Status append_status;
    Status close_status;

private:
    io::Path _path = "json-memory";
    State _state = State::OPENED;
};

Block make_json_rows() {
    const auto integer = std::make_shared<DataTypeInt64>();
    const auto file = make_nullable(std::make_shared<DataTypeFile>());
    const auto string = std::make_shared<DataTypeString>();
    const auto boolean = std::make_shared<DataTypeBool>();
    auto ids = integer->create_column();
    ids->insert(Field::create_field<TYPE_BIGINT>(1));
    ids->insert(Field::create_field<TYPE_BIGINT>(2));
    auto files = file->create_column();
    const std::string bytes("\0\xff", 2);
    files->insert(Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>("urn:json:payload"), Field(),
                  Field::create_field<TYPE_BIGINT>(2), Field(), Field(),
                  Field::create_field<TYPE_VARBINARY>(StringView(bytes))}));
    files->insert_default();
    auto strings = string->create_column();
    strings->insert(Field::create_field<TYPE_STRING>("{\"x\":1}"));
    strings->insert(Field::create_field<TYPE_STRING>(std::string("a\0b", 3)));
    auto bools = boolean->create_column();
    bools->insert(Field::create_field<TYPE_BOOLEAN>(true));
    bools->insert(Field::create_field<TYPE_BOOLEAN>(false));
    Block block;
    block.insert({std::move(ids), integer, "id"});
    block.insert({std::move(files), file, "f"});
    block.insert({std::move(strings), string, "s"});
    block.insert({std::move(bools), boolean, "b"});
    return block;
}

std::string expected_json_rows() {
    return "{\"id\":1,\"f\":{\"uri\":\"urn:json:payload\",\"offset\":null,\"size\":2,"
           "\"content_type\":null,\"checksum\":null,\"inline\":\"AP8=\"},"
           "\"s\":\"{\\\"x\\\":1}\",\"b\":true}\n"
           "{\"id\":2,\"f\":null,\"s\":\"a\\u0000b\",\"b\":false}\n";
}

DataTypes block_types(const Block& block) {
    DataTypes types;
    for (size_t i = 0; i < block.columns(); ++i) types.push_back(block.get_by_position(i).type);
    return types;
}

TEST(VJSONTransformerTest, WritesTypedRowsAndReusesWriterAcrossBlocks) {
    RuntimeState state;
    state.set_timezone("UTC");
    JsonMemoryWriter writer;
    auto block = make_json_rows();
    const auto exprs = MockSlotRef::create_mock_contexts(block_types(block));
    VJSONTransformer transformer(&state, &writer, exprs, false, {"id", "f", "s", "b"}, "\n",
                                 TFileCompressType::PLAIN);
    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block.clone_empty()).ok());
    EXPECT_TRUE(writer.data.empty());
    ASSERT_TRUE(transformer.write(block).ok());
    EXPECT_EQ(writer.data, expected_json_rows());
    ASSERT_TRUE(transformer.write(block).ok());
    EXPECT_EQ(writer.data, expected_json_rows() + expected_json_rows());
    EXPECT_EQ(transformer.written_len(), writer.data.size());
    ASSERT_TRUE(transformer.close().ok());
    EXPECT_EQ(writer.state(), io::FileWriter::State::CLOSED);
}

TEST(VJSONTransformerTest, GzipWritesCompleteMembersForSuccessiveBlocks) {
    RuntimeState state;
    state.set_timezone("UTC");
    JsonMemoryWriter writer;
    auto block = make_json_rows();
    const auto exprs = MockSlotRef::create_mock_contexts(block_types(block));
    VJSONTransformer transformer(&state, &writer, exprs, false, {"id", "f", "s", "b"}, "\n",
                                 TFileCompressType::GZ);
    ASSERT_TRUE(transformer.open().ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_TRUE(transformer.write(block).ok());
    ASSERT_EQ(writer.parts.size(), 2);
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(TFileCompressType::GZ, &codec).ok());
    ASSERT_NE(codec, nullptr);
    for (const auto& part : writer.parts) {
        std::string text(expected_json_rows().size(), '\0');
        Slice uncompressed(text.data(), text.size());
        ASSERT_TRUE(codec->decompress(Slice(part), &uncompressed).ok());
        EXPECT_EQ(uncompressed.size, text.size());
        EXPECT_EQ(text, expected_json_rows());
    }
    EXPECT_EQ(transformer.written_len(), writer.data.size());
    ASSERT_TRUE(transformer.close().ok());
}

TEST(VJSONTransformerTest, EscapesLongColumnNamesAndHonorsLineDelimiter) {
    RuntimeState state;
    state.set_timezone("UTC");
    JsonMemoryWriter writer;
    const auto type = std::make_shared<DataTypeInt64>();
    auto values = type->create_column();
    values->insert(Field::create_field<TYPE_BIGINT>(1));
    const auto exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
    const std::string name = std::string(300, 'a') + std::string("\"\n\0", 3);
    VJSONTransformer transformer(&state, &writer, exprs, false, {name}, "\r\n",
                                 TFileCompressType::PLAIN);
    ASSERT_TRUE(transformer.open().ok());
    Block block;
    block.insert({std::move(values), type, "value"});
    ASSERT_TRUE(transformer.write(block).ok());
    EXPECT_EQ(writer.data, "{\"" + std::string(300, 'a') + "\\\"\\n\\u0000\":1}\r\n");
}

TEST(VJSONTransformerTest, RejectsMissingNamesAndOrdinaryBinarySchemas) {
    RuntimeState state;
    state.set_timezone("UTC");
    const auto binary = make_nullable(std::make_shared<DataTypeVarbinary>());
    const DataTypes rejected {
            binary, std::make_shared<DataTypeArray>(binary),
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {binary}, Strings {"b"}))};
    for (const auto& type : rejected) {
        JsonMemoryWriter writer;
        const auto exprs = MockSlotRef::create_mock_contexts(DataTypes {type});
        VJSONTransformer transformer(&state, &writer, exprs, false, {"value"}, "\n",
                                     TFileCompressType::PLAIN);
        EXPECT_FALSE(transformer.open().ok());
        EXPECT_TRUE(writer.data.empty());
    }
    JsonMemoryWriter writer;
    const auto exprs =
            MockSlotRef::create_mock_contexts(DataTypes {std::make_shared<DataTypeInt64>()});
    VJSONTransformer transformer(&state, &writer, exprs, false, {}, "\n", TFileCompressType::PLAIN);
    EXPECT_FALSE(transformer.open().ok());
    EXPECT_TRUE(writer.data.empty());
}

TEST(VJSONTransformerTest, InvalidLaterCellDoesNotAppendPartialRows) {
    RuntimeState state;
    state.set_timezone("UTC");
    JsonMemoryWriter writer;
    const auto integer = std::make_shared<DataTypeInt64>();
    const auto json = std::make_shared<DataTypeJsonb>();
    auto ids = integer->create_column();
    ids->insert(Field::create_field<TYPE_BIGINT>(1));
    auto malformed = json->create_column();
    malformed->insert_data("invalid", 7);
    Block block;
    block.insert({std::move(ids), integer, "id"});
    block.insert({std::move(malformed), json, "json"});
    const auto exprs = MockSlotRef::create_mock_contexts({integer, json});
    VJSONTransformer transformer(&state, &writer, exprs, false, {"id", "json"}, "\n",
                                 TFileCompressType::PLAIN);
    ASSERT_TRUE(transformer.open().ok());
    EXPECT_FALSE(transformer.write(block).ok());
    EXPECT_TRUE(writer.data.empty());
    EXPECT_TRUE(writer.parts.empty());
}

TEST(VJSONTransformerTest, CancellationAndFileErrorsPropagate) {
    RuntimeState state;
    state.set_timezone("UTC");
    JsonMemoryWriter writer;
    auto block = make_json_rows();
    const auto exprs = MockSlotRef::create_mock_contexts(block_types(block));
    VJSONTransformer transformer(&state, &writer, exprs, false, {"id", "f", "s", "b"}, "\n",
                                 TFileCompressType::PLAIN);
    ASSERT_TRUE(transformer.open().ok());
    state.cancel(Status::Cancelled("cancelled JSON outfile"));
    EXPECT_FALSE(transformer.write(block).ok());
    EXPECT_TRUE(writer.data.empty());
    state._exec_status.reset();
    writer.append_status = Status::IOError("failed append");
    EXPECT_FALSE(transformer.write(block).ok());
    EXPECT_TRUE(writer.data.empty());
    writer.close_status = Status::IOError("failed close");
    EXPECT_FALSE(transformer.close().ok());
}

} // namespace
} // namespace doris
