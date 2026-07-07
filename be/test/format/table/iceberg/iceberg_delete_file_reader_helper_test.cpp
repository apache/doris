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

TEST(IcebergDeleteFileReaderHelperTest, DeletionVectorCacheKeyIncludesLocationAndRange) {
    // Scenario: one Puffin file can hold several DV blobs. The cache key must isolate both the
    // data file and the blob range so a scanner never reuses another file's DV.
    TIcebergDeleteFileDesc first_delete_file;
    first_delete_file.__set_path("s3://bucket/shared/delete.puffin");
    first_delete_file.__set_content_offset(128);
    first_delete_file.__set_content_size_in_bytes(64);

    TIcebergDeleteFileDesc different_offset = first_delete_file;
    different_offset.__set_content_offset(256);

    TIcebergDeleteFileDesc different_length = first_delete_file;
    different_length.__set_content_size_in_bytes(96);

    TIcebergDeleteFileDesc different_delete_file = first_delete_file;
    different_delete_file.__set_path("s3://bucket/shared/other-delete.puffin");

    const std::string data_file_path = "s3://bucket/table/data-00001.parquet";
    const auto first_key =
            build_iceberg_deletion_vector_cache_key(data_file_path, first_delete_file);

    EXPECT_NE(first_key, build_iceberg_deletion_vector_cache_key(data_file_path, different_offset));
    EXPECT_NE(first_key, build_iceberg_deletion_vector_cache_key(data_file_path, different_length));
    EXPECT_NE(first_key,
              build_iceberg_deletion_vector_cache_key(data_file_path, different_delete_file));
    EXPECT_NE(first_key,
              build_iceberg_deletion_vector_cache_key(
                      "s3://bucket/table/snapshot-branch/data-00001.parquet", first_delete_file));
}

TEST(IcebergDeleteFileReaderHelperTest, DeletionVectorCacheKeyEscapesPathBoundaries) {
    TIcebergDeleteFileDesc first_delete_file;
    first_delete_file.__set_path("middle#right#tail.puffin");
    first_delete_file.__set_content_offset(1);
    first_delete_file.__set_content_size_in_bytes(2);

    TIcebergDeleteFileDesc second_delete_file = first_delete_file;
    second_delete_file.__set_path("right#tail.puffin");

    const std::string first_data_file_path = "s3://bucket/table/data#left";
    const std::string second_data_file_path = "s3://bucket/table/data#left#middle";
    ASSERT_EQ(first_data_file_path + "#" + first_delete_file.path,
              second_data_file_path + "#" + second_delete_file.path);

    EXPECT_NE(build_iceberg_deletion_vector_cache_key(first_data_file_path, first_delete_file),
              build_iceberg_deletion_vector_cache_key(second_data_file_path, second_delete_file));
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

} // namespace doris
