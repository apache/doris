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

#include "format/table/iceberg_delete_file_reader_helper.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "io/fs/file_meta_cache.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

constexpr const char* kMixedPositionDeleteFile =
        "./be/test/exec/test_data/iceberg_mixed_position_delete_parquet/"
        "mixed_encoding_position_delete.parquet";
constexpr const char* kTargetDataFilePath =
        "s3://warehouse/wh/test_db/000_target_data_file.parquet";

class CollectPositionDeleteVisitor final : public IcebergPositionDeleteVisitor {
public:
    Status visit(const std::string& file_path, int64_t pos) override {
        delete_rows[file_path].push_back(pos);
        ++total_rows;
        return Status::OK();
    }

    std::unordered_map<std::string, std::vector<int64_t>> delete_rows;
    size_t total_rows = 0;
};

TFileScanRangeParams make_local_parquet_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    return scan_params;
}

TIcebergDeleteFileDesc make_iceberg_deletion_vector(const std::string& path, int64_t offset,
                                                    int64_t size) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__set_path(path);
    delete_file.__set_content_offset(offset);
    delete_file.__set_content_size_in_bytes(size);
    return delete_file;
}

int64_t write_iceberg_deletion_vector_file(const std::string& file_path,
                                           const std::vector<uint64_t>& deleted_positions) {
    roaring::Roaring64Map rows;
    for (const auto position : deleted_positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    std::vector<char> blob(4 + 4 + bitmap_size + 4);
    rows.write(blob.data() + 8);

    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    BigEndian::Store32(blob.data(), total_length);
    constexpr char DV_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
    memcpy(blob.data() + 4, DV_MAGIC, 4);
    BigEndian::Store32(blob.data() + 8 + bitmap_size, 0);

    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    return static_cast<int64_t>(blob.size());
}

std::string temp_parquet_path(const std::string& filename) {
    return (std::filesystem::temp_directory_path() / filename).string();
}

void write_position_delete_parquet(const std::string& path,
                                   const std::vector<std::optional<std::string>>& file_paths,
                                   const std::vector<std::optional<int64_t>>& positions) {
    ASSERT_EQ(file_paths.size(), positions.size());

    arrow::StringBuilder path_builder;
    arrow::Int64Builder pos_builder;
    for (size_t i = 0; i < file_paths.size(); ++i) {
        if (file_paths[i].has_value()) {
            ASSERT_TRUE(path_builder.Append(*file_paths[i]).ok());
        } else {
            ASSERT_TRUE(path_builder.AppendNull().ok());
        }
        if (positions[i].has_value()) {
            ASSERT_TRUE(pos_builder.Append(*positions[i]).ok());
        } else {
            ASSERT_TRUE(pos_builder.AppendNull().ok());
        }
    }

    std::shared_ptr<arrow::Array> path_array;
    std::shared_ptr<arrow::Array> pos_array;
    ASSERT_TRUE(path_builder.Finish(&path_array).ok());
    ASSERT_TRUE(pos_builder.Finish(&pos_array).ok());

    auto schema = arrow::schema({arrow::field("file_path", arrow::utf8(), true),
                                 arrow::field("pos", arrow::int64(), true)});
    auto table = arrow::Table::Make(schema, {path_array, pos_array});

    std::shared_ptr<arrow::io::FileOutputStream> output;
    ASSERT_TRUE(arrow::io::FileOutputStream::Open(path).Value(&output).ok());
    PARQUET_THROW_NOT_OK(
            parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 1024));
}

IcebergDeleteFileReaderOptions make_delete_file_reader_options(
        RuntimeState* state, RuntimeProfile* profile, const TFileScanRangeParams* scan_params,
        io::IOContext* io_ctx) {
    return {
            .state = state,
            .profile = profile,
            .scan_params = scan_params,
            .io_ctx = io_ctx,
    };
}

IcebergDeleteFileReaderOptions delete_reader_options(RuntimeState* runtime_state,
                                                     RuntimeProfile* profile,
                                                     TFileScanRangeParams* scan_params,
                                                     IcebergDeleteFileIOContext* io_context,
                                                     FileMetaCache* meta_cache) {
    IcebergDeleteFileReaderOptions options;
    options.state = runtime_state;
    options.profile = profile;
    options.scan_params = scan_params;
    options.io_ctx = &io_context->io_ctx;
    options.meta_cache = meta_cache;
    options.batch_size = 1024;
    return options;
}

} // namespace

TEST(IcebergDeleteFileReaderHelperTest, BuildDeleteFileRange) {
    auto range = build_iceberg_delete_file_range("s3://bucket/delete.parquet");
    EXPECT_EQ(range.path, "s3://bucket/delete.parquet");
    EXPECT_EQ(range.start_offset, 0);
    EXPECT_EQ(range.size, -1);
    EXPECT_EQ(range.file_size, -1);
}

TEST(IcebergDeleteFileReaderHelperTest, IsDeletionVector) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__isset.content = true;
    EXPECT_TRUE(is_iceberg_deletion_vector(delete_file));
}

TEST(IcebergDeleteFileReaderHelperTest, IsNotDeletionVectorWhenContentMissing) {
    TIcebergDeleteFileDesc delete_file;
    EXPECT_FALSE(is_iceberg_deletion_vector(delete_file));
}

TEST(IcebergDeleteFileReaderHelperTest, ReadMixedEncodingParquetPositionDeleteFile) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryGlobals()));
    FileMetaCache meta_cache(1024);
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = kMixedPositionDeleteFile;
    delete_file.file_format = TFileFormatType::FORMAT_PARQUET;
    delete_file.__isset.file_format = true;

    IcebergDeleteFileReaderOptions options;
    options.state = &runtime_state;
    options.profile = &profile;
    options.scan_params = &scan_params;
    options.io_ctx = &io_context.io_ctx;
    options.meta_cache = &meta_cache;
    options.batch_size = 1024;

    CollectPositionDeleteVisitor visitor;
    auto st = read_iceberg_position_delete_file(delete_file, options, &visitor);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(visitor.total_rows, 216);

    const auto it = visitor.delete_rows.find(kTargetDataFilePath);
    ASSERT_NE(it, visitor.delete_rows.end());

    const std::vector<int64_t> expected_positions = {0,  2,  4,  6,  8,  10, 12, 14,
                                                     16, 18, 20, 22, 24, 26, 28, 30};
    EXPECT_EQ(it->second, expected_positions);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadOptionalParquetPositionDeleteColumnsWithoutNulls) {
    const auto delete_file_path =
            temp_parquet_path("iceberg_optional_position_delete_without_nulls.parquet");
    write_position_delete_parquet(delete_file_path,
                                  {std::string(kTargetDataFilePath),
                                   std::string(kTargetDataFilePath), "s3://other/file.parquet"},
                                  {3, 9, 11});

    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    FileMetaCache meta_cache(1024);
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = delete_file_path;
    delete_file.file_format = TFileFormatType::FORMAT_PARQUET;
    delete_file.__isset.file_format = true;

    CollectPositionDeleteVisitor visitor;
    auto options =
            delete_reader_options(&runtime_state, &profile, &scan_params, &io_context, &meta_cache);
    auto st = read_iceberg_position_delete_file(delete_file, options, &visitor);

    std::filesystem::remove(delete_file_path);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(visitor.total_rows, 3);
    EXPECT_EQ(visitor.delete_rows[kTargetDataFilePath], std::vector<int64_t>({3, 9}));
}

TEST(IcebergDeleteFileReaderHelperTest, RejectParquetPositionDeleteColumnsWithActualNulls) {
    const auto delete_file_path = temp_parquet_path("iceberg_position_delete_with_nulls.parquet");
    write_position_delete_parquet(delete_file_path,
                                  {std::string(kTargetDataFilePath), std::nullopt}, {3, 9});

    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    FileMetaCache meta_cache(1024);
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;
    scan_params.format_type = TFileFormatType::FORMAT_PARQUET;

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = delete_file_path;
    delete_file.file_format = TFileFormatType::FORMAT_PARQUET;
    delete_file.__isset.file_format = true;

    CollectPositionDeleteVisitor visitor;
    auto options =
            delete_reader_options(&runtime_state, &profile, &scan_params, &io_context, &meta_cache);
    auto st = read_iceberg_position_delete_file(delete_file, options, &visitor);

    std::filesystem::remove(delete_file_path);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("file_path contains null values"), std::string::npos)
            << st.to_string();
}

} // namespace doris
