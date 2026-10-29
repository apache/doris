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

#include "format_v2/table/delta_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <typeinfo>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/endian.h"
#include "format/format_common.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "io/io_common.h"
#include "roaring/roaring.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/scoped_temp_dir.h"
#include "util/hash_util.hpp"

namespace doris::format::delta {
namespace {

std::string encode_z85(const std::vector<char>& input) {
    constexpr char ALPHABET[] =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    auto padded = input;
    padded.resize((padded.size() + 3) / 4 * 4);
    std::string encoded;
    for (size_t position = 0; position < padded.size(); position += 4) {
        uint64_t value = BigEndian::Load32(padded.data() + position);
        char block[5];
        for (int index = 4; index >= 0; --index) {
            block[index] = ALPHABET[value % 85];
            value /= 85;
        }
        encoded.append(block, sizeof(block));
    }
    return encoded;
}

std::vector<char> deletion_vector_payload(const std::vector<uint32_t>& positions) {
    roaring::Roaring bitmap;
    for (const auto position : positions) {
        bitmap.add(position);
    }
    const size_t bitmap_size = bitmap.getSizeInBytes();
    std::vector<char> payload(12 + bitmap_size);
    // Delta's native RoaringBitmapArray: magic, bitmap count, then length-prefixed bitmaps.
    LittleEndian::Store32(payload.data(), 1681511376);
    LittleEndian::Store32(payload.data() + 4, 1);
    LittleEndian::Store32(payload.data() + 8, bitmap_size);
    EXPECT_EQ(bitmap.write(payload.data() + 12), bitmap_size);
    return payload;
}

class DeltaTableReaderTest : public testing::Test {
protected:
    void SetUp() override {
        params.__set_file_type(TFileType::FILE_LOCAL);
        params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        io_ctx->file_reader_stats = &file_reader_stats;
        io_ctx->file_cache_stats = &file_cache_stats;
        ColumnDefinition column;
        column.identifier = Field::create_field<TYPE_STRING>("id");
        column.name = "id";
        column.type = std::make_shared<DataTypeInt32>();
        columns.push_back(column);

        arrow::Int32Builder values;
        ASSERT_TRUE(values.AppendValues(std::vector<int32_t> {0, 1, 2, 3, 4, 5}).ok());
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(values.Finish(&array).ok());
        // Optional Parquet fields are also used by Delta tables with required logical columns.
        auto schema = arrow::schema({arrow::field("id", arrow::int32(), true)});
        auto table = arrow::Table::Make(schema, {array});
        auto result = arrow::io::FileOutputStream::Open(data_path);
        ASSERT_TRUE(result.ok()) << result.status();
        auto output = *result;
        ::parquet::WriterProperties::Builder properties;
        properties.data_page_version(::parquet::ParquetDataPageVersion::V2);
        ASSERT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 2,
                                                 properties.build())
                            .ok());
        ASSERT_TRUE(output->Close().ok());
    }

    Status init_reader(TableReader* reader) {
        return reader->init({.projected_columns = columns,
                             .conjuncts = {},
                             .format = FileFormat::PARQUET,
                             .scan_params = &params,
                             .io_ctx = io_ctx,
                             .runtime_state = &state,
                             .scanner_profile = &profile});
    }

    SplitReadOptions split(const TDeltaFileDesc* delta = nullptr) {
        SplitReadOptions options;
        options.cache = &cache;
        options.current_range.__set_path(data_path);
        options.current_range.__set_file_size(std::filesystem::file_size(data_path));
        options.current_range.table_format_params.__set_table_format_type("delta");
        options.current_range.__isset.table_format_params = true;
        if (delta != nullptr) {
            options.current_range.table_format_params.__set_delta_params(*delta);
        }
        return options;
    }

    TDeltaFileDesc vector_descriptor(const std::string& storage_type,
                                     const std::vector<uint32_t>& positions) {
        const auto payload = deletion_vector_payload(positions);
        TDeltaFileDesc delta;
        delta.__set_storage_type(storage_type);
        delta.__set_size_in_bytes(payload.size());
        delta.__set_cardinality(positions.size());
        if (storage_type == "i") {
            delta.__set_path_or_inline_dv(encode_z85(payload));
            return delta;
        }

        // The UUID suffix is Z85 for an all-zero UUID; the preceding text is the relative prefix.
        delta.__set_path_or_inline_dv(storage_type == "p" ? "file://" + vector_path
                                                          : "prefix00000000000000000000");
        delta.__set_table_path(temp_dir.path().string());
        delta.__set_offset(5);
        const auto path = storage_type == "p"
                                  ? vector_path
                                  : (temp_dir.path() / "prefix" /
                                     "deletion_vector_00000000-0000-0000-0000-000000000000.bin")
                                            .string();
        std::filesystem::create_directories(std::filesystem::path(path).parent_path());
        std::vector<char> sidecar(payload.size() + 8);
        BigEndian::Store32(sidecar.data(), payload.size());
        std::copy(payload.begin(), payload.end(), sidecar.begin() + 4);
        BigEndian::Store32(sidecar.data() + 4 + payload.size(),
                           HashUtil::zlib_crc_hash(payload.data(), payload.size(), 0));
        std::ofstream output(path, std::ios::binary);
        output.write("12345", 5);
        output.write(sidecar.data(), sidecar.size());
        EXPECT_TRUE(output.good());
        return delta;
    }

    void expect_rows(DeltaTableReader* reader, const std::vector<int32_t>& expected) {
        std::vector<int32_t> actual;
        bool eos = false;
        while (!eos) {
            Block block;
            block.insert({columns[0].type->create_column(), columns[0].type, columns[0].name});
            const auto status = reader->get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            if (block.rows() == 0) {
                continue;
            }
            const auto& values = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
            for (size_t row = 0; row < block.rows(); ++row) {
                actual.push_back(values.get_element(row));
            }
            ASSERT_NE(reader->_data_reader.reader, nullptr);
            const FileReader& file_reader = *reader->_data_reader.reader;
            EXPECT_TRUE(typeid(file_reader) == typeid(parquet::ParquetReader));
        }
        EXPECT_EQ(actual, expected);
    }

    test::ScopedTempDirectory temp_dir {"doris_delta_reader"};
    const std::string data_path = (temp_dir.path() / "data.parquet").string();
    const std::string vector_path = (temp_dir.path() / "vector.bin").string();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile {"delta_reader_test"};
    TFileScanRangeParams params;
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    std::shared_ptr<io::IOContext> io_ctx = std::make_shared<io::IOContext>();
    ShardedKVCache cache {1};
    std::vector<ColumnDefinition> columns;
};

TEST_F(DeltaTableReaderTest, ReadsMappedRequiredColumnsWithoutDeletionVector) {
    columns[0].name = "renamed_id";
    columns[0].identifier = Field::create_field<TYPE_STRING>("renamed_id");
    columns[0].has_name_mapping = true;
    columns[0].name_mapping = {"id"};
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    ASSERT_TRUE(reader.prepare_split(split()).ok());
    expect_rows(&reader, {0, 1, 2, 3, 4, 5});
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaTableReaderTest, GenericReaderDoesNotInterpretDeltaDescriptors) {
    TableReader reader;
    const auto delta = vector_descriptor("i", {1});
    const auto options = split(&delta);
    DeleteFileDesc desc;
    bool has_delete_file = true;
    ASSERT_TRUE(reader._parse_deletion_vector_file(options.current_range.table_format_params, &desc,
                                                   &has_delete_file)
                        .ok());
    EXPECT_FALSE(has_delete_file);
}

TEST_F(DeltaTableReaderTest, RejectsMalformedDescriptors) {
    const auto valid = vector_descriptor("p", {1});
    std::vector<TDeltaFileDesc> invalid;
    for (int missing_field = 0; missing_field < 5; ++missing_field) {
        auto delta = valid;
        switch (missing_field) {
        case 0:
            delta.__isset.storage_type = false;
            break;
        case 1:
            delta.__isset.path_or_inline_dv = false;
            break;
        case 2:
            delta.__isset.size_in_bytes = false;
            break;
        case 3:
            delta.__isset.cardinality = false;
            break;
        case 4:
            delta.__isset.offset = false;
            break;
        }
        invalid.push_back(delta);
    }
    auto delta = valid;
    delta.__set_storage_type("unknown");
    invalid.push_back(delta);
    delta = valid;
    delta.__set_size_in_bytes(-1);
    invalid.push_back(delta);
    delta = valid;
    delta.__set_cardinality(-1);
    invalid.push_back(delta);
    delta = valid;
    delta.__set_offset(std::numeric_limits<int64_t>::max());
    invalid.push_back(delta);

    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    for (const auto& descriptor : invalid) {
        const auto status = reader.prepare_split(split(&descriptor));
        EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>()) << status;
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(DeltaTableReaderTest, CacheKeyIncludesRangeAndTableIdentity) {
    DeltaTableReader reader;
    auto delta = vector_descriptor("p", {1});
    auto table_desc = split(&delta).current_range.table_format_params;
    DeleteFileDesc original;
    bool has_delete_file = false;
    ASSERT_TRUE(reader._parse_deletion_vector_file(table_desc, &original, &has_delete_file).ok());
    ASSERT_TRUE(has_delete_file);
    for (int changed_field = 0; changed_field < 4; ++changed_field) {
        table_desc.delta_params = delta;
        switch (changed_field) {
        case 0:
            table_desc.delta_params.offset += 1;
            break;
        case 1:
            table_desc.delta_params.size_in_bytes += 1;
            break;
        case 2:
            table_desc.delta_params.cardinality += 1;
            break;
        case 3:
            table_desc.delta_params.table_path += "/other";
            break;
        }
        DeleteFileDesc changed;
        ASSERT_TRUE(
                reader._parse_deletion_vector_file(table_desc, &changed, &has_delete_file).ok());
        EXPECT_NE(original.key, changed.key);
    }
}

class DeltaTableReaderDVTest : public DeltaTableReaderTest,
                               public testing::WithParamInterface<std::string> {};

TEST_P(DeltaTableReaderDVTest, FiltersParquetRowsAndReusesDecodedCacheAcrossSplits) {
    const auto delta = vector_descriptor(GetParam(), {1, 4});
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    reader.set_batch_size(2);
    ASSERT_TRUE(reader.prepare_split(split(&delta)).ok());
    expect_rows(&reader, {0, 2, 3, 5});
    EXPECT_EQ(reader._profile.decoded_dv_cache_miss_count->value(), 1);
    ASSERT_TRUE(reader.prepare_split(split(&delta)).ok());
    expect_rows(&reader, {0, 2, 3, 5});
    EXPECT_EQ(reader._profile.decoded_dv_cache_hit_count->value(), 1);

    ASSERT_TRUE(reader.prepare_split(split()).ok());
    EXPECT_EQ(reader._deletion_vector, nullptr);
    expect_rows(&reader, {0, 1, 2, 3, 4, 5});
    ASSERT_TRUE(reader.close().ok());
}

TEST_P(DeltaTableReaderDVTest, FiltersAllRowsAcrossRowGroups) {
    const auto delta = vector_descriptor(GetParam(), {0, 1, 2, 3, 4, 5});
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    ASSERT_TRUE(reader.prepare_split(split(&delta)).ok());
    expect_rows(&reader, {});
    ASSERT_TRUE(reader.close().ok());
}

TEST_P(DeltaTableReaderDVTest, EmptyVectorSkipsLoading) {
    auto delta = vector_descriptor(GetParam(), {});
    // A zero-cardinality descriptor needs no bitmap bytes, including for a missing sidecar.
    delta.__set_path_or_inline_dv(GetParam() == "u" ? "missing00000000000000000000" : "missing");
    if (GetParam() == "p") {
        delta.__set_path_or_inline_dv("file://" + (temp_dir.path() / "missing.bin").string());
    }
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    ASSERT_TRUE(reader.prepare_split(split(&delta)).ok());
    expect_rows(&reader, {0, 1, 2, 3, 4, 5});
    EXPECT_EQ(reader._profile.num_delete_rows->value(), 0);
    ASSERT_TRUE(reader.close().ok());
}

TEST_P(DeltaTableReaderDVTest, CardinalityMismatchFailsWithoutCachingPartialVector) {
    auto delta = vector_descriptor(GetParam(), {1, 4});
    delta.__set_cardinality(3);
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    for (int attempt = 0; attempt < 2; ++attempt) {
        const auto status = reader.prepare_split(split(&delta));
        EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>()) << status;
        EXPECT_EQ(reader._deletion_vector, nullptr);
    }
    EXPECT_EQ(reader._profile.decoded_dv_cache_hit_count->value(), 0);
    delta.__set_cardinality(2);
    ASSERT_TRUE(reader.prepare_split(split(&delta)).ok());
    expect_rows(&reader, {0, 2, 3, 5});
    ASSERT_TRUE(reader.close().ok());
}

TEST_P(DeltaTableReaderDVTest, VectorLoadFailureMustNotSkipDataFile) {
    auto delta = vector_descriptor(GetParam(), {1, 4});
    if (GetParam() == "i") {
        delta.__set_path_or_inline_dv("invalid~");
    } else {
        DeleteFileDesc desc;
        bool has_delete_file = false;
        DeltaTableReader reader;
        ASSERT_TRUE(
                reader._parse_deletion_vector_file(split(&delta).current_range.table_format_params,
                                                   &desc, &has_delete_file)
                        .ok());
        ASSERT_TRUE(std::filesystem::remove(GetParam() == "p" ? vector_path : desc.path));
    }
    DeltaTableReader reader;
    ASSERT_TRUE(init_reader(&reader).ok());
    const auto status = reader.prepare_split(split(&delta));
    EXPECT_FALSE(status.ok());
    // A missing DV must not look like a missing data file: the scanner would skip surviving rows.
    EXPECT_FALSE(status.is<ErrorCode::NOT_FOUND>()) << status;
    EXPECT_EQ(reader._deletion_vector, nullptr);
    ASSERT_TRUE(reader.close().ok());
}

INSTANTIATE_TEST_SUITE_P(StorageTypes, DeltaTableReaderDVTest, testing::Values("i", "p", "u"));

} // namespace
} // namespace doris::format::delta
