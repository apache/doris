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

#include "format_v2/native/native_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/native/native_format.h"
#include "format_v2/column_mapper.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"
#include "util/uid_util.h"

namespace doris::format::native {
namespace {

std::unique_ptr<io::FileDescription> file_description(const std::string& path) {
    auto desc = std::make_unique<io::FileDescription>();
    desc->path = path;
    desc->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
    desc->range_start_offset = 0;
    desc->range_size = desc->file_size;
    return desc;
}

Status write_file(const std::string& path, std::string_view content) {
    io::FileWriterPtr writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &writer));
    if (!content.empty()) {
        RETURN_IF_ERROR(writer->append({content.data(), content.size()}));
    }
    return writer->close();
}

std::unique_ptr<NativeReader> create_reader(const std::string& path, RuntimeState* state,
                                            RuntimeProfile* profile,
                                            std::shared_ptr<io::IOContext> io_ctx = nullptr) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto desc = file_description(path);
    return std::make_unique<NativeReader>(system_properties, desc, std::move(io_ctx), profile);
}

Block make_source_block() {
    auto id_column = ColumnInt32::create();
    id_column->insert_value(10);
    id_column->insert_value(20);

    auto name_column = ColumnString::create();
    name_column->insert_data("alice", 5);
    name_column->insert_data("bob", 3);

    Block block;
    block.insert({id_column->get_ptr(), std::make_shared<DataTypeInt32>(), "id"});
    block.insert({name_column->get_ptr(), std::make_shared<DataTypeString>(), "name"});
    return block;
}

Status write_native_file(const std::string& path, const Block& block) {
    io::FileWriterPtr writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &writer));
    RETURN_IF_ERROR(writer->append({DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC)}));

    uint8_t version_buffer[sizeof(uint32_t)];
    encode_fixed32_le(version_buffer, DORIS_NATIVE_FORMAT_VERSION);
    RETURN_IF_ERROR(writer->append({version_buffer, sizeof(version_buffer)}));

    PBlock pblock;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compressed_time = 0;
    RETURN_IF_ERROR(block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                    &uncompressed_bytes, &compressed_bytes, &compressed_time,
                                    segment_v2::CompressionTypePB::SNAPPY));

    const std::string payload = pblock.SerializeAsString();
    uint8_t len_buffer[sizeof(uint64_t)];
    encode_fixed64_le(len_buffer, payload.size());
    RETURN_IF_ERROR(writer->append({len_buffer, sizeof(len_buffer)}));
    RETURN_IF_ERROR(writer->append(payload));
    return writer->close();
}

Block make_request_block(const std::vector<ColumnDefinition>& schema,
                         const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::find_if(schema.begin(), schema.end(), [&](const auto& column) {
            return column.local_id == local_id;
        });
        DORIS_CHECK(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

int32_t nullable_int_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    return nested.get_data()[row];
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

class NullableIntGreaterThanExpr final : public VExpr {
public:
    NullableIntGreaterThanExpr(size_t block_position, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _block_position(block_position),
              _value(value) {}

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_block_position).column);
        const auto& data = assert_cast<const ColumnInt32&>(nullable.get_nested_column());

        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto source_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable.is_null_at(source_row) && data.get_element(source_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NullableIntGreaterThanExpr>(_block_position, _value);
        return Status::OK();
    }

private:
    size_t _block_position;
    int32_t _value;
    const std::string _name = "NullableIntGreaterThanExpr";
};

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto context = VExprContext::create_shared(expr);
    auto status = context->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = context->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return context;
}

} // namespace

TEST(NativeV2ReaderTest, SchemaProbeReplaysFirstBlockAndProjectsColumns) {
    const auto path = "./log/native_v2_reader_" + UniqueId::gen_uid().to_string() + ".native";
    std::filesystem::create_directories("./log");
    ASSERT_TRUE(write_native_file(path, make_source_block()).ok());

    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_test");
    auto reader = create_reader(path, &state, &profile);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_EQ(schema[1].name, "name");
    EXPECT_EQ(schema[1].local_id, 1);
    EXPECT_TRUE(schema[0].type->is_nullable());
    EXPECT_TRUE(schema[1].type->is_nullable());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {1, 0});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "alice");
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 1), "bob");
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 0), 10);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(1).column, 1), 20);

    block.clear_column_data(2);
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    ASSERT_TRUE(reader->close().ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(path));
}

TEST(NativeV2ReaderTest, AppliesConjunctsAndTracksPredicateFilteredRows) {
    const auto path =
            "./log/native_v2_reader_filter_" + UniqueId::gen_uid().to_string() + ".native";
    std::filesystem::create_directories("./log");
    ASSERT_TRUE(write_native_file(path, make_source_block()).ok());

    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_filter_test");
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(path, &state, &profile, io_ctx);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(1)).ok());
    request->conjuncts = {
            prepared_conjunct(&state, std::make_shared<NullableIntGreaterThanExpr>(0, 10))};
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0, 1});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(nullable_int_at(*block.get_by_position(0).column, 0), 20);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(1).column, 0), "bob");
    EXPECT_EQ(io_ctx->predicate_filtered_rows, 1);
    ASSERT_TRUE(reader->close().ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(path));
}

TEST(NativeV2ReaderTest, RejectsInvalidHeaderAndEmptyFile) {
    std::filesystem::create_directories("./log");
    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_bad_header_test");

    const auto bad_magic_path =
            "./log/native_v2_bad_magic_" + UniqueId::gen_uid().to_string() + ".native";
    std::string bad_magic(sizeof(DORIS_NATIVE_MAGIC) + sizeof(uint32_t), '\0');
    bad_magic.replace(0, 4, "BAD!");
    ASSERT_TRUE(write_file(bad_magic_path, bad_magic).ok());
    auto bad_magic_reader = create_reader(bad_magic_path, &state, &profile);
    EXPECT_FALSE(bad_magic_reader->init(&state).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(bad_magic_path));

    const auto empty_path = "./log/native_v2_empty_" + UniqueId::gen_uid().to_string() + ".native";
    ASSERT_TRUE(write_file(empty_path, "").ok());
    auto empty_reader = create_reader(empty_path, &state, &profile);
    EXPECT_FALSE(empty_reader->init(&state).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(empty_path));
}

TEST(NativeV2ReaderTest, RejectsUnsupportedVersionAndHeaderOnlyFile) {
    std::filesystem::create_directories("./log");
    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_header_boundary_test");

    const auto bad_version_path =
            "./log/native_v2_bad_version_" + UniqueId::gen_uid().to_string() + ".native";
    std::string bad_version;
    bad_version.append(DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC));
    uint8_t version_buffer[sizeof(uint32_t)];
    encode_fixed32_le(version_buffer, DORIS_NATIVE_FORMAT_VERSION + 1);
    bad_version.append(reinterpret_cast<const char*>(version_buffer), sizeof(version_buffer));
    ASSERT_TRUE(write_file(bad_version_path, bad_version).ok());
    auto bad_version_reader = create_reader(bad_version_path, &state, &profile);
    EXPECT_FALSE(bad_version_reader->init(&state).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(bad_version_path));

    const auto header_only_path =
            "./log/native_v2_header_only_" + UniqueId::gen_uid().to_string() + ".native";
    std::string header_only;
    header_only.append(DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC));
    encode_fixed32_le(version_buffer, DORIS_NATIVE_FORMAT_VERSION);
    header_only.append(reinterpret_cast<const char*>(version_buffer), sizeof(version_buffer));
    ASSERT_TRUE(write_file(header_only_path, header_only).ok());
    auto header_only_reader = create_reader(header_only_path, &state, &profile);
    ASSERT_TRUE(header_only_reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    EXPECT_FALSE(header_only_reader->get_schema(&schema).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(header_only_path));
}

TEST(NativeV2ReaderTest, RejectsTruncatedBlockDuringSchemaProbe) {
    const auto path = "./log/native_v2_truncated_" + UniqueId::gen_uid().to_string() + ".native";
    std::filesystem::create_directories("./log");

    std::string content;
    content.append(DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC));
    uint8_t version_buffer[sizeof(uint32_t)];
    encode_fixed32_le(version_buffer, DORIS_NATIVE_FORMAT_VERSION);
    content.append(reinterpret_cast<const char*>(version_buffer), sizeof(version_buffer));
    uint8_t len_buffer[sizeof(uint64_t)];
    encode_fixed64_le(len_buffer, 8);
    content.append(reinterpret_cast<const char*>(len_buffer), sizeof(len_buffer));
    content.append("x");
    ASSERT_TRUE(write_file(path, content).ok());

    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_truncated_test");
    auto reader = create_reader(path, &state, &profile);
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    EXPECT_FALSE(reader->get_schema(&schema).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(path));
}

TEST(NativeV2ReaderTest, RejectsZeroLengthBlockAndInvalidPBlock) {
    std::filesystem::create_directories("./log");
    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_bad_block_test");

    auto build_header = [] {
        std::string content;
        content.append(DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC));
        uint8_t version_buffer[sizeof(uint32_t)];
        encode_fixed32_le(version_buffer, DORIS_NATIVE_FORMAT_VERSION);
        content.append(reinterpret_cast<const char*>(version_buffer), sizeof(version_buffer));
        return content;
    };

    const auto zero_len_path =
            "./log/native_v2_zero_len_" + UniqueId::gen_uid().to_string() + ".native";
    auto zero_len_content = build_header();
    uint8_t len_buffer[sizeof(uint64_t)];
    encode_fixed64_le(len_buffer, 0);
    zero_len_content.append(reinterpret_cast<const char*>(len_buffer), sizeof(len_buffer));
    ASSERT_TRUE(write_file(zero_len_path, zero_len_content).ok());
    auto zero_len_reader = create_reader(zero_len_path, &state, &profile);
    ASSERT_TRUE(zero_len_reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    EXPECT_FALSE(zero_len_reader->get_schema(&schema).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(zero_len_path));

    const auto invalid_pblock_path =
            "./log/native_v2_invalid_pblock_" + UniqueId::gen_uid().to_string() + ".native";
    auto invalid_pblock_content = build_header();
    encode_fixed64_le(len_buffer, 1);
    invalid_pblock_content.append(reinterpret_cast<const char*>(len_buffer), sizeof(len_buffer));
    invalid_pblock_content.append("x");
    ASSERT_TRUE(write_file(invalid_pblock_path, invalid_pblock_content).ok());
    auto invalid_pblock_reader = create_reader(invalid_pblock_path, &state, &profile);
    ASSERT_TRUE(invalid_pblock_reader->init(&state).ok());
    schema.clear();
    EXPECT_FALSE(invalid_pblock_reader->get_schema(&schema).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(invalid_pblock_path));
}

TEST(NativeV2ReaderTest, RejectsUnknownRequestedLocalColumn) {
    const auto path =
            "./log/native_v2_unknown_column_" + UniqueId::gen_uid().to_string() + ".native";
    std::filesystem::create_directories("./log");
    ASSERT_TRUE(write_native_file(path, make_source_block()).ok());

    RuntimeState state;
    RuntimeProfile profile("native_v2_reader_unknown_column_test");
    auto reader = create_reader(path, &state, &profile);
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(42)).ok());
    ASSERT_TRUE(reader->open(request).ok());
    Block block;
    block.insert({schema[0].type->create_column(), schema[0].type, schema[0].name});
    size_t rows = 0;
    bool eof = false;
    EXPECT_FALSE(reader->get_block(&block, &rows, &eof).ok());
    static_cast<void>(io::global_local_filesystem()->delete_file(path));
}

} // namespace doris::format::native
