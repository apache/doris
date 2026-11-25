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

// Cross-version Group Commit WAL replay for Variant columns.
//
// Master only decodes the Variant V2 block layout.  A WAL written before V2
// existed starts with a subcolumn count, so the V2 reader must reject it before
// reading those bytes as V2 lengths; otherwise the single Block::deserialize
// cursor is desynchronised and the following columns are decoded from garbage
// (which reproduced as a SIGSEGV in production).
//
// The fixtures under test_data/ are real Group Commit WAL files:
//   * wal_variant_v1_31.wal and wal_variant_v1_40.wal were written by 3.1 and
//     4.0 BEs.  Their Variant columns have no doc-value Map and no
//     variant_is_v2 marker.
//   * wal_variant_v1_legacy.wal was written by a 4.1.0-rc01 BE, also without
//     the doc-value Map.
//   * wal_variant_v1_doc_value.wal was written by a later 4.1 BE.  It has the
//     doc-value Map but is still a V1 block without variant_is_v2.
//   * wal_variant_v1_42.wal was written by a 4.2 BE with enable_variant_v2
//     off; it has the doc-value Map and is still V1.
//   * wal_variant_v2_current.wal was written by the current master BE.
//
// A WAL file is a byte stream of length-prefixed PBlocks plus a CRC32C, so it
// can be replayed without a running FE.  These tests replay the fixtures through
// the same WalFileReader + Block::deserialize pair that WalReader::get_next_block
// uses: both V1 files must be rejected with a clear error before any Variant byte
// is decoded, and the V2 file must still round-trip.

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "load/group_commit/wal/wal_file_reader.h"

namespace doris {
namespace {

constexpr const char* kFixtureDir = "./be/test/format/wal/test_data";
constexpr const char* kV1Wal31 = "wal_variant_v1_31.wal";
constexpr const char* kV1Wal40 = "wal_variant_v1_40.wal";
constexpr const char* kV1LegacyWal = "wal_variant_v1_legacy.wal";
constexpr const char* kV1DocValueWal = "wal_variant_v1_doc_value.wal";
constexpr const char* kV1Wal42 = "wal_variant_v1_42.wal";
constexpr const char* kV2CurrentWal = "wal_variant_v2_current.wal";

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.is_open()) << path;
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void write_file(const std::string& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary);
    EXPECT_TRUE(out.is_open()) << path;
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
}

// WalFileReader derives the database and table id from the directory names of
// the file it reads, so a fixture is staged as <root>/<db_id>/<table_id>/<name>.
std::string staged_path(const std::string& name) {
    const auto root = std::filesystem::temp_directory_path() / "wal_variant_wire_compat";
    const auto path = root / "1" / "2" / name;
    std::filesystem::create_directories(path.parent_path());
    return path.string();
}

std::string stage_fixture(const std::string& fixture) {
    const auto path = staged_path(fixture);
    std::filesystem::copy_file(std::string(kFixtureDir) + "/" + fixture, path,
                               std::filesystem::copy_options::overwrite_existing);
    return path;
}

std::string stage_content(const std::string& name, const std::string& content) {
    const auto path = staged_path(name);
    write_file(path, content);
    return path;
}

struct ReplayedBlock {
    PBlock pblock;
    Status deserialize_status;
};

// Replays every record of a WAL file the way WalReader::get_next_block does and
// records the status of Block::deserialize for each record.
std::vector<ReplayedBlock> replay(const std::string& path) {
    std::vector<ReplayedBlock> result;
    WalFileReader reader(path);
    const auto init_status = reader.init();
    EXPECT_TRUE(init_status.ok()) << init_status;
    if (!init_status.ok()) {
        return result;
    }
    uint32_t version = 0;
    std::string column_ids;
    const auto header_status = reader.read_header(version, column_ids);
    EXPECT_TRUE(header_status.ok()) << header_status;
    if (!header_status.ok()) {
        return result;
    }
    while (true) {
        PBlock pblock;
        auto status = reader.read_block(pblock);
        if (status.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        EXPECT_TRUE(status.ok()) << status;
        if (!status.ok()) {
            break;
        }
        Block block;
        size_t uncompressed_bytes = 0;
        int64_t decompress_time = 0;
        auto deserialize_status = block.deserialize(pblock, &uncompressed_bytes, &decompress_time);
        result.push_back({std::move(pblock), deserialize_status});
    }
    return result;
}

const IColumn& strip_nullable(const IColumn& column) {
    const auto* nullable = check_and_get_column<ColumnNullable>(&column);
    return nullable == nullptr ? column : nullable->get_nested_column();
}

// The WAL PBlock stores column metadata without names, so identify the columns
// by value instead: k/tail are BIGINT 71/72 and note is the string
// "current_wal".  If the Variant payload before them were misread, these values
// would come from a shifted cursor.
bool has_expected_scalars(const Block& block) {
    bool found_bigint = false;
    bool found_note = false;
    for (const auto& column : block.get_columns_with_type_and_name()) {
        const auto& stripped = strip_nullable(*column.column);
        if (const auto* ints = check_and_get_column<ColumnInt64>(&stripped); ints != nullptr) {
            for (size_t row = 0; row < stripped.size(); ++row) {
                const auto value = ints->get_element(row);
                if (value == 71 || value == 72) {
                    found_bigint = true;
                }
            }
        } else if (const auto* strings = check_and_get_column<ColumnString>(&stripped);
                   strings != nullptr) {
            for (size_t row = 0; row < stripped.size(); ++row) {
                if (strings->get_data_at(row).to_string() == "current_wal") {
                    found_note = true;
                }
            }
        }
    }
    return found_bigint && found_note;
}

TEST(WalVariantWireCompatTest, V1WalsAreRejectedBeforeVariantDecode) {
    for (const auto* fixture : {kV1Wal31, kV1Wal40, kV1LegacyWal, kV1DocValueWal, kV1Wal42}) {
        const auto blocks = replay(stage_fixture(fixture));
        ASSERT_FALSE(blocks.empty()) << fixture;
        for (const auto& block : blocks) {
            // The V1 layout must never reach the V2 decoder: it starts with a
            // subcolumn count, not the V2 const/row header.
            ASSERT_TRUE(block.deserialize_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>())
                    << fixture << ": " << block.deserialize_status;
            const auto message = block.deserialize_status.to_string();
            EXPECT_NE(message.find("Unsupported VARIANT block format"), std::string::npos)
                    << message;
            EXPECT_NE(message.find("Variant V1 cannot be decoded as Variant V2"), std::string::npos)
                    << message;
        }
    }
}

TEST(WalVariantWireCompatTest, V2WalStillRoundTrips) {
    const auto blocks = replay(stage_fixture(kV2CurrentWal));
    ASSERT_FALSE(blocks.empty());
    size_t rows = 0;
    bool checked_values = false;
    for (const auto& block : blocks) {
        ASSERT_TRUE(block.deserialize_status.ok()) << block.deserialize_status;
        Block decoded;
        size_t uncompressed_bytes = 0;
        int64_t decompress_time = 0;
        ASSERT_TRUE(decoded.deserialize(block.pblock, &uncompressed_bytes, &decompress_time).ok());
        rows += decoded.rows();
        EXPECT_GT(decoded.columns(), 0);
        // If the Variant payload were misread, the columns after it would be
        // decoded from a shifted cursor.  Check the scalar values after it.
        if (has_expected_scalars(decoded)) {
            checked_values = true;
        }
    }
    EXPECT_GT(rows, 0);
    EXPECT_TRUE(checked_values);
}

TEST(WalVariantWireCompatTest, TruncatedWalRecordIsRejectedBeforeDeserialize) {
    const auto content = read_file(std::string(kFixtureDir) + "/" + kV1LegacyWal);
    ASSERT_GT(content.size(), 64);
    // Cut the first record in half: the reader must report the incomplete tail
    // instead of handing a short buffer to the decoder.
    const auto truncated =
            stage_content("wal_variant_truncated.wal", content.substr(0, content.size() / 2));
    WalFileReader reader(truncated);
    ASSERT_TRUE(reader.init().ok());
    uint32_t version = 0;
    std::string column_ids;
    ASSERT_TRUE(reader.read_header(version, column_ids).ok());
    PBlock pblock;
    const auto status = reader.read_block(pblock);
    EXPECT_FALSE(status.ok());
}

TEST(WalVariantWireCompatTest, CorruptedWalChecksumIsRejected) {
    auto content = read_file(std::string(kFixtureDir) + "/" + kV1LegacyWal);
    ASSERT_GT(content.size(), 8);
    // Flipping the last byte corrupts the trailing CRC32C while keeping the
    // record length valid, so the checksum must reject it before decoding.
    content[content.size() - 1] ^= 0x5a;
    WalFileReader reader(stage_content("wal_variant_corrupted.wal", content));
    ASSERT_TRUE(reader.init().ok());
    uint32_t version = 0;
    std::string column_ids;
    ASSERT_TRUE(reader.read_header(version, column_ids).ok());
    PBlock pblock;
    const auto status = reader.read_block(pblock);
    EXPECT_FALSE(status.ok());
}

} // namespace
} // namespace doris
