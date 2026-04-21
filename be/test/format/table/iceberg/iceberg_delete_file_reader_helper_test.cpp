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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <zlib.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/fs/file_meta_cache.h"
#include "roaring/roaring64map.hh"
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

void append_big_endian_u32(std::vector<char>* bytes, uint32_t value) {
    bytes->push_back(static_cast<char>((value >> 24) & 0xFF));
    bytes->push_back(static_cast<char>((value >> 16) & 0xFF));
    bytes->push_back(static_cast<char>((value >> 8) & 0xFF));
    bytes->push_back(static_cast<char>(value & 0xFF));
}

std::string write_temp_deletion_vector_file(const std::vector<uint64_t>& rows,
                                            uint32_t magic_number = 0xD1D33964) {
    roaring::Roaring64Map bitmap;
    for (uint64_t row : rows) {
        bitmap.add(row);
    }

    const size_t bitmap_size = bitmap.getSizeInBytes(true);
    std::vector<char> bitmap_bytes(bitmap_size);
    EXPECT_EQ(bitmap.write(bitmap_bytes.data(), true), bitmap_size);

    std::vector<char> file_bytes;
    append_big_endian_u32(&file_bytes, static_cast<uint32_t>(bitmap_size + 4));
    append_big_endian_u32(&file_bytes, magic_number);
    file_bytes.insert(file_bytes.end(), bitmap_bytes.begin(), bitmap_bytes.end());
    const uint32_t crc =
            static_cast<uint32_t>(::crc32(0, reinterpret_cast<const Bytef*>(file_bytes.data() + 4),
                                          static_cast<uInt>(4 + bitmap_size)));
    append_big_endian_u32(&file_bytes, crc);

    auto temp_dir = std::filesystem::temp_directory_path();
    std::string pattern = (temp_dir / "iceberg_deletion_vector_test_XXXXXX").string();
    std::vector<char> path_buffer(pattern.begin(), pattern.end());
    path_buffer.push_back('\0');
    int fd = mkstemp(path_buffer.data());
    EXPECT_GE(fd, 0);
    if (fd >= 0) {
        close(fd);
    }

    std::string path(path_buffer.data());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(file_bytes.data(), static_cast<std::streamsize>(file_bytes.size()));
    out.close();
    EXPECT_TRUE(out.good());
    return path;
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
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
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

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorFile) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;

    const std::string deletion_vector_path = write_temp_deletion_vector_file({1, 5, 9, 42});

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = deletion_vector_path;
    delete_file.__set_content(3);
    delete_file.__set_content_offset(0);
    delete_file.__set_content_size_in_bytes(
            static_cast<int64_t>(std::filesystem::file_size(deletion_vector_path)));

    IcebergDeleteFileReaderOptions options;
    options.state = &runtime_state;
    options.profile = &profile;
    options.scan_params = &scan_params;
    options.io_ctx = &io_context.io_ctx;

    roaring::Roaring64Map rows_to_delete;
    rows_to_delete.add(static_cast<uint64_t>(100));
    auto st = read_iceberg_deletion_vector(delete_file, options, &rows_to_delete);
    ASSERT_TRUE(st.ok()) << st;

    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(1)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(5)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(9)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(42)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(100)));

    std::error_code ec;
    std::filesystem::remove(deletion_vector_path, ec);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorRejectsInvalidMagic) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;

    const std::string deletion_vector_path = write_temp_deletion_vector_file({3, 7}, 0x12345678);

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = deletion_vector_path;
    delete_file.__set_content(3);
    delete_file.__set_content_offset(0);
    delete_file.__set_content_size_in_bytes(
            static_cast<int64_t>(std::filesystem::file_size(deletion_vector_path)));

    IcebergDeleteFileReaderOptions options;
    options.state = &runtime_state;
    options.profile = &profile;
    options.scan_params = &scan_params;
    options.io_ctx = &io_context.io_ctx;

    roaring::Roaring64Map rows_to_delete;
    auto st = read_iceberg_deletion_vector(delete_file, options, &rows_to_delete);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("magic number mismatch"), std::string::npos);
    EXPECT_TRUE(rows_to_delete.isEmpty());

    std::error_code ec;
    std::filesystem::remove(deletion_vector_path, ec);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorRequiresOffsetAndLength) {
    RuntimeProfile profile("test_profile");
    RuntimeState runtime_state((TQueryOptions()), TQueryGlobals());
    IcebergDeleteFileIOContext io_context(&runtime_state);

    TFileScanRangeParams scan_params;
    scan_params.file_type = TFileType::FILE_LOCAL;

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "unused";
    delete_file.__set_content(3);

    IcebergDeleteFileReaderOptions options;
    options.state = &runtime_state;
    options.profile = &profile;
    options.scan_params = &scan_params;
    options.io_ctx = &io_context.io_ctx;

    roaring::Roaring64Map rows_to_delete;
    auto st = read_iceberg_deletion_vector(delete_file, options, &rows_to_delete);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("missing content offset or length"), std::string::npos);
}

} // namespace doris
