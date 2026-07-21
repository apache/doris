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

#include "format/table/paimon_cpp_reader.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "exec/common/endian.h"
#include "format/format_common.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/paimon_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "roaring/roaring.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

namespace {

std::vector<char> build_paimon_deletion_vector_buffer(const std::vector<uint32_t>& positions) {
    roaring::Roaring rows;
    for (const auto position : positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    std::vector<char> buffer(4 + total_length);
    BigEndian::Store32(buffer.data(), total_length);
    constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
    memcpy(buffer.data() + 4, PAIMON_BITMAP_MAGIC, 4);
    rows.write(buffer.data() + 8);
    return buffer;
}

TFileScanRangeParams make_local_paimon_scan_params(TFileFormatType::type format_type) {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(format_type);
    return scan_params;
}

TFileRangeDesc make_paimon_range_with_deletion_file(const std::string& deletion_file_path) {
    TFileRangeDesc range;
    range.__set_path("data.parquet");
    range.__set_start_offset(0);
    range.__set_size(0);
    range.__set_fs_name("");
    range.__isset.table_format_params = true;
    range.table_format_params.__set_table_format_type("paimon");

    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path(deletion_file_path);
    deletion_file.__set_offset(0);
    deletion_file.__set_length(16);
    paimon_params.__set_deletion_file(deletion_file);
    range.table_format_params.__set_paimon_params(paimon_params);
    return range;
}

} // namespace

class PaimonCppReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
    }

    TFileRangeDesc _build_range_with_table_level_row_count(int64_t row_count) {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.table_level_row_count = true;
        range.table_format_params.table_level_row_count = row_count;
        return range;
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_cpp_reader_test"};
    std::vector<SlotDescriptor*> _file_slot_descs;
};

TEST_F(PaimonCppReaderTest, CountPushDownUsesTableLevelRowCount) {
    auto range = _build_range_with_table_level_row_count(5);
    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    auto init_status = reader.init_reader();
    ASSERT_TRUE(init_status.ok()) << init_status;

    Block block;
    size_t read_rows = 0;
    bool eof = false;

    auto first_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(first_status.ok()) << first_status;
    EXPECT_EQ(3, read_rows);
    EXPECT_FALSE(eof);

    auto second_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(second_status.ok()) << second_status;
    EXPECT_EQ(2, read_rows);
    EXPECT_TRUE(eof);

    auto third_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(third_status.ok()) << third_status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST_F(PaimonCppReaderTest, InitReaderFailsWithoutPaimonSplit) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table = "s3://bucket/db.tbl";

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, DecodeValidBuffer) {
    // Scenario: a valid Paimon DV stays compressed after decoding instead of becoming one int64_t
    // per deleted row in the query cache.
    const auto buffer = build_paimon_deletion_vector_buffer({0, 3, 5});
    DeletionVector deletion_vector;
    const auto status =
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(deletion_vector.cardinality(), 3);
    EXPECT_TRUE(deletion_vector.contains(uint64_t {0}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {3}));
    EXPECT_TRUE(deletion_vector.contains(uint64_t {5}));
    EXPECT_FALSE(deletion_vector.contains(uint64_t {4}));
}

TEST(PaimonDeletionVectorTest, RejectShortBuffer) {
    // Scenario: malformed DV content must fail before reading the length and magic fields.
    const std::vector<char> buffer(7, '\0');
    DeletionVector deletion_vector;
    const auto status =
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("file size too small"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, RejectNullBuffer) {
    DeletionVector deletion_vector;
    const auto status = decode_paimon_deletion_vector_buffer(nullptr, 8, &deletion_vector);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("blob is null"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, RejectLengthMismatch) {
    // Scenario: the big-endian length prefix protects against using a truncated or over-read DV
    // slice from a shared deletion-vector file.
    auto buffer = build_paimon_deletion_vector_buffer({1});
    BigEndian::Store32(buffer.data(), static_cast<uint32_t>(buffer.size()));
    DeletionVector deletion_vector;
    const auto status =
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("length mismatch"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, RejectMagicMismatch) {
    // Scenario: Paimon DV buffers have a fixed magic header, so a cache entry or offset pointing
    // to unrelated bytes must be rejected.
    auto buffer = build_paimon_deletion_vector_buffer({1});
    buffer[4] = '\0';
    DeletionVector deletion_vector;
    const auto status =
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("magic number mismatch"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, RejectCorruptRoaringBitmap) {
    // Scenario: a buffer with a valid header but incomplete Roaring payload should surface as a
    // data-quality error instead of silently producing partial delete rows.
    auto buffer = build_paimon_deletion_vector_buffer({1, 2});
    buffer.resize(10);
    BigEndian::Store32(buffer.data(), static_cast<uint32_t>(buffer.size() - 4));
    DeletionVector deletion_vector;
    const auto status =
            decode_paimon_deletion_vector_buffer(buffer.data(), buffer.size(), &deletion_vector);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("failed to deserialize roaring bitmap"), std::string::npos);
}

TEST(PaimonDeletionVectorTest, CacheKeyIncludesOffsetAndLength) {
    // Scenario: different Paimon tables or splits may reference the same DV file with different
    // ranges; cache keys must include both offset and length to avoid sharing the wrong bitmap.
    TPaimonDeletionFileDesc first_deletion_file;
    first_deletion_file.__set_path("s3://bucket/table/deletion.dv");
    first_deletion_file.__set_offset(128);
    first_deletion_file.__set_length(64);

    TPaimonDeletionFileDesc different_offset = first_deletion_file;
    different_offset.__set_offset(256);

    TPaimonDeletionFileDesc different_length = first_deletion_file;
    different_length.__set_length(96);

    const auto first_key = build_paimon_deletion_vector_cache_key(first_deletion_file);
    EXPECT_NE(first_key, build_paimon_deletion_vector_cache_key(different_offset));
    EXPECT_NE(first_key, build_paimon_deletion_vector_cache_key(different_length));
}

TEST(PaimonDeletionVectorTest, ValidateDescriptorRejectsInvalidRange) {
    size_t bytes_read = 0;

    TPaimonDeletionFileDesc missing_path;
    missing_path.__set_offset(0);
    missing_path.__set_length(4);
    EXPECT_FALSE(validate_paimon_deletion_vector_descriptor(missing_path, bytes_read).ok());

    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path("dv.bin");
    deletion_file.__set_offset(-1);
    deletion_file.__set_length(4);
    EXPECT_FALSE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());

    deletion_file.__set_offset(0);
    deletion_file.__set_length(-1);
    EXPECT_FALSE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());

    deletion_file.__set_length(std::numeric_limits<int64_t>::max());
    EXPECT_FALSE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());

    deletion_file.__set_length(MAX_PAIMON_DELETION_VECTOR_BYTES - 4);
    EXPECT_TRUE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());
    EXPECT_EQ(static_cast<size_t>(MAX_PAIMON_DELETION_VECTOR_BYTES), bytes_read);

    deletion_file.__set_length(MAX_PAIMON_DELETION_VECTOR_BYTES - 3);
    EXPECT_FALSE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());

    deletion_file.__set_offset(3);
    deletion_file.__set_length(4);
    EXPECT_TRUE(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read).ok());
    EXPECT_EQ(bytes_read, 8);
}

TEST(PaimonDeletionVectorTest, DecodedCacheReportsHitSeparatelyFromFileCache) {
    // The decoded cache lookup result is reported by ShardedKVCache itself. The creator represents
    // the lower File Cache/read/decode path and must only run for the miss.
    ShardedKVCache cache(1);
    int create_count = 0;
    bool cache_hit = true;
    auto create = [&]() {
        ++create_count;
        auto* deletion_vector = new DeletionVector();
        deletion_vector->add(uint64_t {7});
        return deletion_vector;
    };

    const auto* first = cache.get<DeletionVector>("dv", create, &cache_hit);
    EXPECT_FALSE(cache_hit);
    ASSERT_NE(first, nullptr);
    EXPECT_TRUE(first->contains(uint64_t {7}));

    const auto* second = cache.get<DeletionVector>("dv", create, &cache_hit);
    EXPECT_TRUE(cache_hit);
    EXPECT_EQ(first, second);
    EXPECT_EQ(create_count, 1);
}

TEST(PaimonDeletionVectorTest, V1ParquetReaderReadErrorReleasesCacheEntry) {
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("paimon_v1_parquet_dv_test");
    auto scan_params = make_local_paimon_scan_params(TFileFormatType::FORMAT_PARQUET);
    const auto range = make_paimon_range_with_deletion_file(
            "./be/test/exec/test_data/missing_paimon_v1_delete_vector.bin");
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    io::IOContext io_ctx;
    FileMetaCache meta_cache(1024);
    ShardedKVCache kv_cache(8);

    PaimonParquetReader reader(&profile, scan_params, range, 1024, &ctz, &kv_cache, &io_ctx,
                               &runtime_state, &meta_cache);
    const auto status = reader.TEST_init_deletion_vector();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(range.table_format_params.paimon_params.deletion_file.path),
              std::string::npos);
}

TEST(PaimonDeletionVectorTest, V1OrcReaderReadErrorReleasesCacheEntry) {
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("paimon_v1_orc_dv_test");
    auto scan_params = make_local_paimon_scan_params(TFileFormatType::FORMAT_ORC);
    auto range = make_paimon_range_with_deletion_file(
            "./be/test/exec/test_data/missing_paimon_v1_orc_delete_vector.bin");
    range.table_format_params.paimon_params.__set_file_format("orc");
    io::IOContext io_ctx;
    FileMetaCache meta_cache(1024);
    ShardedKVCache kv_cache(8);

    PaimonOrcReader reader(&profile, &runtime_state, scan_params, range, 1024,
                           TimezoneUtils::default_time_zone, &kv_cache, &io_ctx, &meta_cache);
    const auto status = reader.TEST_init_deletion_vector();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(range.table_format_params.paimon_params.deletion_file.path),
              std::string::npos);
}

} // namespace doris
