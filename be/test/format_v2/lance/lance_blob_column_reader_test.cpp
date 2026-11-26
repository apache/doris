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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <utility>

#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/field.h"
#include "exprs/function/simple_function_factory.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/table/lance_reader.h"
#include "runtime/lance_blob_reader.h"
#include "runtime/runtime_state.h"

namespace doris::format::lance {

class LanceBlobColumnReaderTest : public testing::Test {
protected:
    void SetUp() override {
        column.name = "image /一";
        column.type = make_nullable(std::make_shared<DataTypeFile>());
    }

    std::shared_ptr<arrow::Array> descriptors(const std::vector<uint8_t>& kinds,
                                              const std::vector<uint64_t>& sizes,
                                              const std::vector<std::string>& uris,
                                              const std::vector<uint32_t>& bases,
                                              int null_kind = -1, int null_struct = -1) {
        arrow::UInt8Builder kind;
        arrow::UInt64Builder size;
        arrow::UInt32Builder base;
        arrow::StringBuilder uri;
        arrow::BooleanBuilder validity;
        for (size_t row = 0; row < kinds.size(); ++row) {
            if (std::cmp_equal(row, null_kind)) {
                EXPECT_TRUE(kind.AppendNull().ok());
            } else {
                EXPECT_TRUE(kind.Append(kinds[row]).ok());
            }
            EXPECT_TRUE(size.Append(sizes[row]).ok());
            EXPECT_TRUE(base.Append(bases[row]).ok());
            EXPECT_TRUE(uri.Append(uris[row]).ok());
            EXPECT_TRUE(validity.Append(std::cmp_not_equal(row, null_struct)).ok());
        }
        auto kind_array = kind.Finish().ValueOrDie();
        auto size_array = size.Finish().ValueOrDie();
        auto base_array = base.Finish().ValueOrDie();
        auto uri_array = uri.Finish().ValueOrDie();
        auto valid_array =
                std::static_pointer_cast<arrow::BooleanArray>(validity.Finish().ValueOrDie());
        return arrow::StructArray::Make({kind_array, size_array, base_array, uri_array},
                                        {"kind", "size", "blob_id", "blob_uri"},
                                        valid_array->values())
                .ValueOrDie();
    }

    static void expect_logical_descriptor(const File& file, uint32_t source_id) {
        EXPECT_TRUE(file[DataTypeFile::URI_FIELD_INDEX].get<TYPE_STRING>().starts_with(
                "lance-blob://"));
        EXPECT_TRUE(file[DataTypeFile::OFFSET_FIELD_INDEX].is_null());
        EXPECT_TRUE(file[DataTypeFile::INLINE_FIELD_INDEX].is_null());
        EXPECT_EQ(file.blob_source_id, source_id);
    }

    static void expect_appended_blob_column(const IColumn& values, uint32_t source_id,
                                            int64_t first_size) {
        for (size_t row = 0; row < values.size(); ++row) {
            const auto file = values[row].get<TYPE_FILE>();
            EXPECT_EQ(file.blob_source_id, source_id);
            EXPECT_EQ(file.blob_row_id, 91 + row % 2);
            const auto expected_size = first_size + static_cast<int64_t>(row % 2);
            EXPECT_EQ(file[DataTypeFile::SIZE_FIELD_INDEX].get<TYPE_BIGINT>(), expected_size);
        }
    }

    uint32_t source = 7;
    ColumnDefinition column;
};

TEST_F(LanceBlobColumnReaderTest, BuildsLogicalReferencesWithoutPhysicalPositions) {
    // Includes an empty inline Blob and an external length marker. Input is sliced and row IDs
    // are deliberately repeated/out of order; neither physical positions nor SDK handles exist.
    auto input = descriptors({0, 0, 1, 2, 3, 3}, {99, 0, 12, 23, 0, 8},
                             {"", "", "", "", "s3://external/whole", "s3://external/range"},
                             {0, 0, 4, 5, 0, 0});
    MutableColumnPtr output;
    const auto status = convert_lance_blob_descriptors_to_file_column(
            input->Slice(1, 5), {9, 3, 9, 7, 8}, column, source, &output);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output->size(), 5);
    for (size_t row = 0; row < output->size(); ++row) {
        const auto file = (*output)[row].get<TYPE_FILE>();
        expect_logical_descriptor(file, source);
    }
    EXPECT_EQ((*output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 12);
    EXPECT_EQ((*output)[2].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 23);
    EXPECT_TRUE((*output)[3].get<TYPE_FILE>()[2].is_null());
    EXPECT_EQ((*output)[4].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 8);
    EXPECT_EQ((*output)[2].get<TYPE_FILE>().blob_row_id, 9);
}

TEST_F(LanceBlobColumnReaderTest, PreservesStructAndKindNulls) {
    auto input = descriptors({0, 3, 0}, {0, 0, 0}, {"", "file:///missing", ""}, {0, 0, 0}, 1, 0);
    MutableColumnPtr output;
    ASSERT_TRUE(
            convert_lance_blob_descriptors_to_file_column(input, {1, 2, 3}, column, source, &output)
                    .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
    EXPECT_FALSE(output->is_null_at(2));
    column.type = std::make_shared<DataTypeFile>();
    EXPECT_FALSE(
            convert_lance_blob_descriptors_to_file_column(input, {1, 2, 3}, column, source, &output)
                    .ok());
}

TEST_F(LanceBlobColumnReaderTest, RejectsLocalExternalObjects) {
    MutableColumnPtr output;
    for (const auto* uri :
         {"file:///tmp/blob", "FiLe:///tmp/blob", "/tmp/blob", "relative/blob", "memory://blob"}) {
        auto input = descriptors({3}, {0}, {uri}, {0});
        const auto status =
                convert_lance_blob_descriptors_to_file_column(input, {1}, column, source, &output);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("Local external Lance BLOB objects are not supported"),
                  std::string::npos);
    }
    auto input = descriptors({3}, {0}, {"s3://bucket/blob"}, {0});
    EXPECT_TRUE(convert_lance_blob_descriptors_to_file_column(input, {1}, column, source, &output)
                        .ok());
}

TEST_F(LanceBlobColumnReaderTest, ValidatesDescriptorAlignmentAndStorageKind) {
    auto input = descriptors({4}, {0}, {""}, {0});
    MutableColumnPtr output;
    EXPECT_FALSE(
            convert_lance_blob_descriptors_to_file_column(input, {}, column, source, &output).ok());
    EXPECT_FALSE(convert_lance_blob_descriptors_to_file_column(input, {1}, column, source, &output)
                         .ok());
    EXPECT_FALSE(
            convert_lance_blob_descriptors_to_file_column(nullptr, {1}, column, source, &output)
                    .ok());
    EXPECT_FALSE(
            convert_lance_blob_descriptors_to_file_column(input, {1}, column, 0, &output).ok());
}

TEST_F(LanceBlobColumnReaderTest, UnknownLengthReachesContentReadContextValidation) {
    auto input = descriptors({3}, {0}, {"s3://bucket/blob"}, {0});
    MutableColumnPtr output;
    ASSERT_TRUE(convert_lance_blob_descriptors_to_file_column(input, {1}, column, source, &output)
                        .ok());
    const auto return_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    Block block {{std::move(output), column.type, "blob"}, {nullptr, return_type, "content"}};
    auto function = SimpleFunctionFactory::instance().get_function(
            "fl_get_binary", {block.get_by_position(0)}, return_type);
    ASSERT_NE(function, nullptr);
    // A missing size is valid. Without a RuntimeState, content consumption must instead fail
    // at the query-context boundary, before any SDK handle or payload can be accessed.
    const auto status = function->execute(nullptr, block, {0}, 1, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("requires a query context"), std::string::npos);
}

TEST_F(LanceBlobColumnReaderTest, SdkOpenAndBlobResolutionFailuresReturnErrors) {
    TQueryOptions options;
    TQueryGlobals globals;
    globals.__set_time_zone("UTC");
    RuntimeState state(options, globals);
    LanceBlobReader reader;
    auto output = ColumnVarbinary::create(1);
    TLanceBlobResource resource;
    resource.__set_dataset_uri("./be/test/format_v2/table/lance/data/missing_blob_dataset.lance");
    resource.__set_version(3);
    resource.__set_column_name("missing_blob");
    auto status = reader._read_source_blobs(&state, resource, {{0, {0}}}, output.get());
    EXPECT_FALSE(status.ok());

    // Bypass FE's local-root rejection to exercise the real SDK error path on an existing dataset.
    resource.__set_dataset_uri("./be/test/format_v2/table/lance/data/all_types.lance");
    status = reader._read_source_blobs(&state, resource, {{0, {0}}}, output.get());
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("resolve FILE payload handles"), std::string::npos);
}

TEST_F(LanceBlobColumnReaderTest, RowIdFetchStateDoesNotNeedQueryContextToBuildDescriptors) {
    TQueryOptions options;
    options.__set_be_exec_version(SUPPORT_LANCE_BLOB_RESOURCES_VERSION);
    TQueryGlobals globals;
    globals.__set_time_zone("UTC");
    RuntimeState state(options, globals);
    ASSERT_EQ(state.get_query_ctx(), nullptr);
    RuntimeProfile profile("blob_row_id_fetch");
    TLanceScanParams lance;
    lance.__set_dataset_uri("s3://bucket/assets.lance");
    lance.__set_version(42);
    lance.__set_blob_read_source_ids({{column.name, static_cast<int32_t>(source)}});
    TFileScanRangeParams params;
    params.__set_lance_scan_params(lance);
    LanceTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {column},
                             .conjuncts = {},
                             .format = FileFormat::LANCE,
                             .scan_params = &params,
                             .io_ctx = nullptr,
                             .runtime_state = &state,
                             .scanner_profile = &profile})
                        .ok());
    auto input = descriptors({0}, {3}, {""}, {0});
    arrow::UInt64Builder row_ids;
    ASSERT_TRUE(row_ids.Append(91).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field(column.name, input->type()),
                                                         arrow::field("_rowid", arrow::uint64())}),
                                          1, {input, row_ids.Finish().ValueOrDie()});
    Block block {{column.type->create_column(), column.type, column.name}};
    size_t rows = 0;
    const auto status = reader._convert_record_batch(batch, &block, &rows);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows, 1);
    const auto result = (*block.get_by_position(0).column)[0].get<TYPE_FILE>();
    EXPECT_EQ(result.blob_source_id, source);
    EXPECT_EQ(result.blob_row_id, 91);
}

TEST_F(LanceBlobColumnReaderTest, AppendsMultipleBlobColumnsAcrossEmptyAndNonemptyBatches) {
    TQueryOptions options;
    options.__set_be_exec_version(SUPPORT_LANCE_BLOB_RESOURCES_VERSION);
    TQueryGlobals globals;
    globals.__set_time_zone("UTC");
    RuntimeState state(options, globals);
    RuntimeProfile profile("multiple_blob_columns");
    auto audio = column;
    audio.name = "audio";
    TLanceScanParams lance;
    lance.__set_dataset_uri("s3://bucket/assets.lance");
    lance.__set_version(42);
    lance.__set_blob_read_source_ids({{column.name, static_cast<int32_t>(source)},
                                      {audio.name, static_cast<int32_t>(source + 1)}});
    TFileScanRangeParams params;
    params.__set_lance_scan_params(lance);
    LanceTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {column, audio},
                             .conjuncts = {},
                             .format = FileFormat::LANCE,
                             .scan_params = &params,
                             .io_ctx = nullptr,
                             .runtime_state = &state,
                             .scanner_profile = &profile})
                        .ok());
    Block block {{column.type->create_column(), column.type, column.name},
                 {audio.type->create_column(), audio.type, audio.name}};
    auto empty = descriptors({}, {}, {}, {});
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field(audio.name, empty->type()),
                                                         arrow::field(column.name, empty->type())}),
                                          0, {empty, empty});
    size_t rows = 1;
    // Empty batches do not need _rowid, and Arrow order need not match the output Block.
    auto status = reader._convert_record_batch(batch, &block, &rows);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(rows, 0);
    EXPECT_EQ(block.rows(), 0);

    auto images = descriptors({0, 0}, {11, 12}, {"", ""}, {0, 0});
    auto sounds = descriptors({0, 0}, {22, 23}, {"", ""}, {0, 0});
    arrow::UInt64Builder row_ids;
    ASSERT_TRUE(row_ids.Append(91).ok());
    ASSERT_TRUE(row_ids.Append(92).ok());
    batch = arrow::RecordBatch::Make(arrow::schema({arrow::field(audio.name, sounds->type()),
                                                    arrow::field("_rowid", arrow::uint64()),
                                                    arrow::field(column.name, images->type())}),
                                     2, {sounds, row_ids.Finish().ValueOrDie(), images});
    for (size_t i = 0; i < 2; ++i) {
        status = reader._convert_record_batch(batch, &block, &rows);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(rows, 2);
    }
    ASSERT_EQ(block.rows(), 4);
    expect_appended_blob_column(*block.get_by_position(0).column, source, 11);
    expect_appended_blob_column(*block.get_by_position(1).column, source + 1, 22);
}

} // namespace doris::format::lance
