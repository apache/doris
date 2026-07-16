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
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "io/fs/file_meta_cache.h"
#include "roaring/roaring64map.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/hash_util.hpp"

namespace doris {

namespace {

constexpr const char* kMixedPositionDeleteFile =
        "./be/test/exec/test_data/iceberg_mixed_position_delete_parquet/"
        "mixed_encoding_position_delete.parquet";
constexpr const char* kTargetDataFilePath =
        "s3://warehouse/wh/test_db/000_target_data_file.parquet";

class CloudFileCacheConfigGuard {
public:
    CloudFileCacheConfigGuard()
            : _cloud_unique_id(config::cloud_unique_id),
              _enable_file_cache(config::enable_file_cache) {}

    ~CloudFileCacheConfigGuard() {
        config::cloud_unique_id = _cloud_unique_id;
        config::enable_file_cache = _enable_file_cache;
    }

private:
    std::string _cloud_unique_id;
    bool _enable_file_cache;
};

TUniqueId make_query_id() {
    TUniqueId query_id;
    query_id.hi = 300;
    query_id.lo = 400;
    return query_id;
}

TNetworkAddress make_fe_addr() {
    TNetworkAddress fe_addr;
    fe_addr.hostname = "127.0.0.1";
    fe_addr.port = 9030;
    return fe_addr;
}

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

std::vector<char> build_iceberg_deletion_vector_blob(
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
    const uint32_t crc = HashUtil::zlib_crc_hash(blob.data() + 4, total_length, 0);
    BigEndian::Store32(blob.data() + 8 + bitmap_size, crc);
    return blob;
}

int64_t write_iceberg_deletion_vector_file(const std::string& file_path,
                                           const std::vector<uint64_t>& deleted_positions) {
    const auto blob = build_iceberg_deletion_vector_blob(deleted_positions);

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
            ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 1024));
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

TEST(IcebergDeleteFileReaderHelperTest, ValidateDeletionVectorDescriptor) {
    size_t bytes_read = 0;

    TIcebergDeleteFileDesc missing_path;
    missing_path.__set_content_offset(0);
    missing_path.__set_content_size_in_bytes(12);
    EXPECT_FALSE(validate_iceberg_deletion_vector_descriptor(missing_path, bytes_read).ok());

    EXPECT_FALSE(validate_iceberg_deletion_vector_descriptor(
                         make_iceberg_deletion_vector("dv.puffin", -1, 12), bytes_read)
                         .ok());
    EXPECT_FALSE(validate_iceberg_deletion_vector_descriptor(
                         make_iceberg_deletion_vector("dv.puffin", 0, 11), bytes_read)
                         .ok());
    EXPECT_TRUE(
            validate_iceberg_deletion_vector_descriptor(
                    make_iceberg_deletion_vector("dv.puffin", 0, MAX_ICEBERG_DELETION_VECTOR_BYTES),
                    bytes_read)
                    .ok());
    EXPECT_EQ(static_cast<size_t>(MAX_ICEBERG_DELETION_VECTOR_BYTES), bytes_read);
    const auto unsupported_status = validate_iceberg_deletion_vector_descriptor(
            make_iceberg_deletion_vector("dv.puffin", 0, MAX_ICEBERG_DELETION_VECTOR_BYTES + 1),
            bytes_read);
    EXPECT_TRUE(unsupported_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << unsupported_status;
    EXPECT_FALSE(validate_iceberg_deletion_vector_descriptor(
                         make_iceberg_deletion_vector("dv.puffin",
                                                      std::numeric_limits<int64_t>::max() - 10, 12),
                         bytes_read)
                         .ok());

    EXPECT_TRUE(validate_iceberg_deletion_vector_descriptor(
                        make_iceberg_deletion_vector("dv.puffin", 7, 12), bytes_read)
                        .ok());
    EXPECT_EQ(bytes_read, 12);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorReportsMissingFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_deletion_vector_missing_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto missing_path = (test_dir / "missing-delete-vector.bin").string();

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    IcebergDeleteFileIOContext io_context(&state);
    roaring::Roaring64Map rows_to_delete;
    auto options =
            make_delete_file_reader_options(&state, &profile, &scan_params, &io_context.io_ctx);

    auto status = read_iceberg_deletion_vector(make_iceberg_deletion_vector(missing_path, 0, 16),
                                               options, &rows_to_delete);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(missing_path), std::string::npos);
    EXPECT_EQ(rows_to_delete.cardinality(), 0);
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorReportsShortRead) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_deletion_vector_short_read_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto dv_path = (test_dir / "delete-vector.bin").string();
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {1, 3});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    IcebergDeleteFileIOContext io_context(&state);
    roaring::Roaring64Map rows_to_delete;
    auto options =
            make_delete_file_reader_options(&state, &profile, &scan_params, &io_context.io_ctx);

    auto status = read_iceberg_deletion_vector(
            make_iceberg_deletion_vector(dv_path, 0, dv_size + 1), options, &rows_to_delete);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(rows_to_delete.cardinality(), 0);
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorStopsWhenIoContextStops) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_deletion_vector_stop_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto dv_path = (test_dir / "delete-vector.bin").string();
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    IcebergDeleteFileIOContext io_context(&state);
    io_context.io_ctx.should_stop = true;
    roaring::Roaring64Map rows_to_delete;
    auto options =
            make_delete_file_reader_options(&state, &profile, &scan_params, &io_context.io_ctx);

    auto status = read_iceberg_deletion_vector(make_iceberg_deletion_vector(dv_path, 0, dv_size),
                                               options, &rows_to_delete);

    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>());
    EXPECT_NE(status.to_string().find("stop read"), std::string::npos);
    EXPECT_EQ(rows_to_delete.cardinality(), 0);
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergDeleteFileReaderHelperTest, DecodeDeletionVectorRejectsCorruptPayload) {
    std::vector<char> corrupted(12, 0);
    BigEndian::Store32(corrupted.data(), 4);
    memcpy(corrupted.data() + 4, "BAD!", 4);

    roaring::Roaring64Map rows_to_delete;
    auto status = decode_iceberg_deletion_vector_buffer(corrupted.data(), corrupted.size(),
                                                        &rows_to_delete);

    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("magic number mismatch"), std::string::npos);
    EXPECT_EQ(rows_to_delete.cardinality(), 0);
}

TEST(IcebergDeleteFileReaderHelperTest, DecodeDeletionVectorRejectsCrcMismatch) {
    auto corrupted = build_iceberg_deletion_vector_blob({1, 3});
    corrupted.back() ^= 1;

    roaring::Roaring64Map rows_to_delete;
    auto status = decode_iceberg_deletion_vector_buffer(corrupted.data(), corrupted.size(),
                                                        &rows_to_delete);

    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("CRC32 mismatch"), std::string::npos);
    EXPECT_EQ(rows_to_delete.cardinality(), 0);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorReadsMillionDeletePositions) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_deletion_vector_large_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    constexpr uint64_t delete_position_count = 1'000'000;
    std::vector<uint64_t> deleted_positions;
    deleted_positions.reserve(delete_position_count);
    for (uint64_t position = 0; position < delete_position_count; ++position) {
        deleted_positions.push_back(position * 2);
    }

    const auto dv_path = (test_dir / "delete-vector.bin").string();
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, deleted_positions);

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    IcebergDeleteFileIOContext io_context(&state);
    roaring::Roaring64Map rows_to_delete;
    auto options =
            make_delete_file_reader_options(&state, &profile, &scan_params, &io_context.io_ctx);

    ASSERT_TRUE(read_iceberg_deletion_vector(make_iceberg_deletion_vector(dv_path, 0, dv_size),
                                             options, &rows_to_delete)
                        .ok());

    EXPECT_EQ(rows_to_delete.cardinality(), delete_position_count);
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(0)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(999'998)));
    EXPECT_TRUE(rows_to_delete.contains(static_cast<uint64_t>(1'999'998)));
    EXPECT_FALSE(rows_to_delete.contains(static_cast<uint64_t>(1'999'999)));
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergDeleteFileReaderHelperTest, ReadDeletionVectorConcurrentReadsAreStable) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_deletion_vector_concurrent_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto first_dv_path = (test_dir / "delete-vector-first.bin").string();
    const auto second_dv_path = (test_dir / "delete-vector-second.bin").string();
    const auto first_dv_size = write_iceberg_deletion_vector_file(first_dv_path, {0, 2, 4});
    const auto second_dv_size = write_iceberg_deletion_vector_file(second_dv_path, {1, 3, 5});

    auto read_and_verify = [](const std::string& path, int64_t size,
                              const std::vector<uint64_t>& expected_positions) -> Status {
        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        auto scan_params = make_local_parquet_scan_params();
        IcebergDeleteFileIOContext io_context(&state);
        roaring::Roaring64Map rows_to_delete;
        auto options =
                make_delete_file_reader_options(&state, &profile, &scan_params, &io_context.io_ctx);
        RETURN_IF_ERROR(read_iceberg_deletion_vector(make_iceberg_deletion_vector(path, 0, size),
                                                     options, &rows_to_delete));
        if (rows_to_delete.cardinality() != expected_positions.size()) {
            return Status::InternalError("unexpected deletion vector cardinality");
        }
        for (const auto position : expected_positions) {
            if (!rows_to_delete.contains(position)) {
                return Status::InternalError("missing deletion vector position {}", position);
            }
        }
        return Status::OK();
    };

    std::vector<std::thread> workers;
    std::vector<Status> statuses(16);
    for (size_t idx = 0; idx < statuses.size(); ++idx) {
        workers.emplace_back([&, idx]() {
            SCOPED_INIT_THREAD_CONTEXT();
            if (idx % 2 == 0) {
                statuses[idx] = read_and_verify(first_dv_path, first_dv_size, {0, 2, 4});
            } else {
                statuses[idx] = read_and_verify(second_dv_path, second_dv_size, {1, 3, 5});
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    for (const auto& status : statuses) {
        EXPECT_TRUE(status.ok()) << status;
    }
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergDeleteFileReaderHelperTest, IoContextPropagatesQueryLimiterForSelect) {
    CloudFileCacheConfigGuard config_guard;
    config::cloud_unique_id = "iceberg_delete_file_io_context_ut";
    config::enable_file_cache = true;

    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::SELECT);
    query_options.__set_file_cache_query_limit_bytes(0);

    const auto query_id = make_query_id();
    const auto fe_addr = make_fe_addr();
    auto query_ctx = MockQueryContext::create(query_id, ExecEnv::GetInstance(), query_options,
                                              fe_addr, true, fe_addr);
    ASSERT_NE(query_ctx->remote_scan_cache_write_limiter(), nullptr);

    MockRuntimeState state;
    state._query_id = query_id;
    state.set_query_options(query_options);
    state._query_ctx_uptr = query_ctx;
    state._query_ctx = query_ctx.get();

    IcebergDeleteFileIOContext io_context(&state);

    EXPECT_EQ(io_context.io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_context.io_ctx.reader_type, ReaderType::READER_QUERY);
    EXPECT_EQ(io_context.io_ctx.file_cache_stats, &io_context.file_cache_stats);
    EXPECT_EQ(io_context.io_ctx.file_reader_stats, &io_context.file_reader_stats);
    EXPECT_EQ(io_context.io_ctx.remote_scan_cache_write_limiter,
              query_ctx->remote_scan_cache_write_limiter());
}

TEST(IcebergDeleteFileReaderHelperTest, IoContextDoesNotMarkLoadAsQueryReader) {
    TQueryOptions query_options;
    query_options.__set_query_type(TQueryType::LOAD);
    query_options.__set_file_cache_query_limit_bytes(0);

    MockRuntimeState state;
    state._query_id = make_query_id();
    state.set_query_options(query_options);
    state._query_ctx = nullptr;

    IcebergDeleteFileIOContext io_context(&state);

    EXPECT_EQ(io_context.io_ctx.query_id, &state.query_id());
    EXPECT_EQ(io_context.io_ctx.reader_type, ReaderType::UNKNOWN);
    EXPECT_EQ(io_context.io_ctx.file_cache_stats, &io_context.file_cache_stats);
    EXPECT_EQ(io_context.io_ctx.file_reader_stats, &io_context.file_reader_stats);
    EXPECT_EQ(io_context.io_ctx.remote_scan_cache_write_limiter, nullptr);
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
