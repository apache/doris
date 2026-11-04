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

#include "format_v2/blob/paimon_blob_column_reader.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "format_v2/blob/paimon_blob_test_util.h"

namespace doris::format::paimon {
namespace {

class PaimonBlobColumnReaderTest : public blob_test::PaimonBlobTest {};

TEST_F(PaimonBlobColumnReaderTest, AccountsForFooterIndexAndPayloadAcrossBatchesAndFiles) {
    auto first = write_blob("first.blob", 7, 100, {"hello", "", std::nullopt});
    auto second = write_blob("second.blob", 7, 103, {"world"});
    io::FileReaderStats stats;
    _io_context.file_reader_stats = &stats;
    auto context = read_context(); // Local FileFactory reads, with no file cache.
    PaimonBlobColumnReader reader(blob_column("image", 7), {&first, &second}, false);
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({0}), 100, context, &result).ok());
    EXPECT_EQ(stats.read_calls, 3);                    // Footer, index, first complete record.
    EXPECT_EQ(stats.read_bytes, first.file_size - 16); // Empty record not yet read.
    EXPECT_GT(stats.read_time_ns, 0);
    EXPECT_EQ(stats.read_rows, 0); // Anchor rows are already counted by the scanner.

    ASSERT_TRUE(reader.read_blob(*row_positions({1, 2, 3}), 100, context, &result).ok());
    EXPECT_EQ(stats.read_calls, 7); // Empty record, then second file's footer/index/record.
    EXPECT_EQ(stats.read_bytes, first.file_size + second.file_size);
    expect_inline(*result, 0, "");
    expect_inline(*result, 1, std::nullopt);
    expect_inline(*result, 2, "world");
}

TEST_F(PaimonBlobColumnReaderTest, DescriptorAccountsForMetadataWithoutReadingPayload) {
    auto file = write_blob("descriptor.blob", 7, 100, {"hello", "", std::nullopt});
    io::FileReaderStats stats;
    _io_context.file_reader_stats = &stats;
    auto context = read_context();
    PaimonBlobColumnReader reader(blob_column("image", 7), {&file}, true);
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({0, 1, 2}), 100, context, &result).ok());
    EXPECT_EQ(stats.read_calls, 2);
    EXPECT_EQ(stats.read_bytes, file.file_size - (5 + 16) - 16); // Footer and index only.
    EXPECT_GT(stats.read_time_ns, 0);
    ASSERT_TRUE(reader.read_blob(*row_positions({0}), 100, context, &result).ok());
    EXPECT_EQ(stats.read_calls, 2); // Decoded index is reused across batches.
    expect_descriptor(*result, 0, file.path, 4, 5);
}

TEST_F(PaimonBlobColumnReaderTest, SortsFilesAndRequiresContiguousAnchorCoverage) {
    auto first = write_blob("first.blob", 7, 1000, {"a", "b", "c"});
    auto second = write_blob("second.blob", 7, 1003, {"d", "e"});
    std::vector<const TPaimonBlobFileDesc*> files {&second, &first};
    ASSERT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 5).ok());
    EXPECT_EQ(files.front(), &first);
    EXPECT_EQ(files.back(), &second);

    second.__set_first_row_id(1002);
    EXPECT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 5)
                        .is<ErrorCode::CORRUPTION>());
    second.__set_first_row_id(1004);
    second.__set_row_count(1);
    EXPECT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 5)
                        .is<ErrorCode::CORRUPTION>());
    second.__set_first_row_id(1003);
    EXPECT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 5)
                        .is<ErrorCode::CORRUPTION>());
}

TEST_F(PaimonBlobColumnReaderTest, RejectsMissingOrInvalidFileMetadata) {
    const auto valid = write_blob("valid.blob", 7, 1000, {"a"});
    std::vector<TPaimonBlobFileDesc> invalid(9, valid);
    invalid[0].__isset.path = false;
    invalid[1].__set_path("");
    invalid[2].__isset.file_size = false;
    invalid[3].__set_file_size(4);
    invalid[4].__isset.first_row_id = false;
    invalid[5].__set_first_row_id(999);
    invalid[6].__isset.row_count = false;
    invalid[7].__set_row_count(0);
    invalid[8].__set_row_count(2);
    for (const auto& file : invalid) {
        std::vector<const TPaimonBlobFileDesc*> files {&file};
        EXPECT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 1)
                            .is<ErrorCode::INVALID_ARGUMENT>());
    }
    std::vector<const TPaimonBlobFileDesc*> empty;
    EXPECT_TRUE(PaimonBlobColumnReader::validate_blob_files(&empty, 1000, 1)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

TEST_F(PaimonBlobColumnReaderTest, ReadsSparseAndNonMonotonicPositionsAcrossFilesAndBatches) {
    // Anchor starts at 1000. Its position 4 maps to logical row 1004, record 1 of second.blob.
    auto first = write_blob("first.blob", 7, 1000, {"first", std::nullopt, ""});
    auto second = write_blob("second.blob", 7, 1003, {"second", std::string("\0\xff", 2)});
    std::vector<const TPaimonBlobFileDesc*> files {&second, &first};
    ASSERT_TRUE(PaimonBlobColumnReader::validate_blob_files(&files, 1000, 5).ok());
    PaimonBlobColumnReader reader(blob_column("renamed_image", 7), files, false);
    auto context = read_context();
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({4, 0, 2, 1, 3, 4}), 1000, context, &result).ok());
    ASSERT_EQ(result->size(), 6);
    expect_inline(*result, 0, std::string("\0\xff", 2));
    expect_inline(*result, 1, "first");
    expect_inline(*result, 2, "");
    expect_inline(*result, 3, std::nullopt);
    expect_inline(*result, 4, "second");
    expect_inline(*result, 5, std::string("\0\xff", 2));

    MutableColumnPtr next_batch;
    ASSERT_TRUE(reader.read_blob(*row_positions({3, 0}), 1000, context, &next_batch).ok());
    expect_inline(*next_batch, 0, "second");
    expect_inline(*next_batch, 1, "first");
    // Earlier output owns its bytes even after the next batch reuses file readers and scratch.
    expect_inline(*result, 0, std::string("\0\xff", 2));
}

TEST_F(PaimonBlobColumnReaderTest, InlineValuesOwnLargePayloadsAndKeepMetadataNull) {
    const std::string large(8193, 'x');
    auto file = write_blob("inline.blob", 7, 10, {large, "short", "", std::nullopt});
    PaimonBlobColumnReader reader(blob_column("image", 7), {&file}, false);
    auto context = read_context();
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({0, 1, 2, 3}), 10, context, &result).ok());
    ASSERT_EQ(result->size(), 4);
    expect_inline(*result, 0, large);
    expect_inline(*result, 1, "short");
    expect_inline(*result, 2, "");
    expect_inline(*result, 3, std::nullopt);
}

TEST_F(PaimonBlobColumnReaderTest, DescriptorValuesKeepInlineNullAndIgnoreInlineLimit) {
    auto first = write_blob("first.blob", 7, 1000, {"hello", std::nullopt, ""});
    auto second = write_blob("second.blob", 7, 1003, {"world"});
    PaimonBlobColumnReader reader(blob_column("image", 7), {&first, &second}, true);
    auto context = read_context(1);
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({3, 2, 1, 0}), 1000, context, &result).ok());
    ASSERT_EQ(result->size(), 4);
    expect_descriptor(*result, 0, second.path, 4, 5);
    expect_descriptor(*result, 1, first.path, 25, 0);
    expect_inline(*result, 2, std::nullopt);
    expect_descriptor(*result, 3, first.path, 4, 5);
}

TEST_F(PaimonBlobColumnReaderTest, ChecksSizeBeforeReadingPayloadAndAcceptsExactLimit) {
    // The 9-byte row has an invalid CRC. A limit error proves it is rejected before payload I/O.
    auto bytes = blob_test::make_blob_file({"12345678", "123456789", "", std::nullopt});
    bytes[24 + 25 - 1] ^= 1;
    auto file = write_blob_bytes("limit.blob", 7, 0, 4, bytes);
    PaimonBlobColumnReader reader(blob_column("image", 7), {&file}, false);
    auto context = read_context(8);
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({0, 2, 3}), 0, context, &result).ok());
    expect_inline(*result, 0, "12345678");
    expect_inline(*result, 1, "");
    expect_inline(*result, 2, std::nullopt);
    const auto limited = reader.read_blob(*row_positions({1}), 0, context, &result);
    EXPECT_TRUE(limited.is<ErrorCode::INVALID_ARGUMENT>()) << limited;
    EXPECT_NE(limited.to_string().find("paimon_blob_max_size=8"), std::string::npos);
    context.blob_max_size = 9;
    EXPECT_TRUE(
            reader.read_blob(*row_positions({1}), 0, context, &result).is<ErrorCode::CORRUPTION>());
    PaimonBlobColumnReader descriptor_reader(blob_column("image", 7), {&file}, true);
    ASSERT_TRUE(descriptor_reader.read_blob(*row_positions({1}), 0, context, &result).ok());
    expect_descriptor(*result, 0, file.path, 28, 9);
}

TEST_F(PaimonBlobColumnReaderTest, MissingNullableFieldBecomesNullButRequiredFieldFails) {
    auto context = read_context();
    MutableColumnPtr result;
    PaimonBlobColumnReader nullable_reader(blob_column("added_image", 7), {}, false);
    ASSERT_TRUE(nullable_reader.read_blob(*row_positions({2, 0}), 1000, context, &result).ok());
    ASSERT_EQ(result->size(), 2);
    expect_inline(*result, 0, std::nullopt);
    expect_inline(*result, 1, std::nullopt);
    PaimonBlobColumnReader required_reader(blob_column("required_image", 7, false), {}, false);
    EXPECT_TRUE(required_reader.read_blob(*row_positions({0}), 1000, context, &result)
                        .is<ErrorCode::CORRUPTION>());
}

TEST_F(PaimonBlobColumnReaderTest, RejectsNullInRequiredField) {
    auto file = write_blob("null.blob", 7, 0, {std::nullopt});
    PaimonBlobColumnReader reader(blob_column("image", 7, false), {&file}, false);
    auto context = read_context();
    MutableColumnPtr result;
    EXPECT_TRUE(
            reader.read_blob(*row_positions({0}), 0, context, &result).is<ErrorCode::CORRUPTION>());
}

TEST_F(PaimonBlobColumnReaderTest, OpensOnlySelectedFilesAndCopiesDescriptors) {
    auto good = write_blob("good.blob", 7, 10, {"selected"});
    auto absent = good;
    absent.__set_path((_temp_directory.path() / "does_not_exist.blob").string());
    absent.__set_first_row_id(11);
    PaimonBlobColumnReader reader(blob_column("image", 7), {&good, &absent}, false);
    good.__set_path("metadata_was_changed_after_construction");
    auto context = read_context();
    MutableColumnPtr result;
    ASSERT_TRUE(reader.read_blob(*row_positions({}), 10, context, &result).ok());
    EXPECT_EQ(result->size(), 0);
    ASSERT_TRUE(reader.read_blob(*row_positions({0}), 10, context, &result).ok());
    expect_inline(*result, 0, "selected");
    EXPECT_FALSE(reader.read_blob(*row_positions({1}), 10, context, &result).ok());
}

TEST_F(PaimonBlobColumnReaderTest, RejectsOverreportedFileSizeAndCancellation) {
    auto file = write_blob("mismatch.blob", 7, 0, {"hello"});
    file.__set_file_size(file.file_size + 1);
    PaimonBlobColumnReader reader(blob_column("image", 7), {&file}, false);
    auto context = read_context();
    MutableColumnPtr result;
    // Local readers use the manifest size and report EOF when it exceeds the actual file.
    // Preserve that I/O error rather than requiring the BLOB parser's Corruption status.
    const auto status = reader.read_blob(*row_positions({0}), 0, context, &result);
    EXPECT_FALSE(status.ok()) << status;
    EXPECT_NE(status.to_string().find("unexpected EOF"), std::string::npos);
    _io_context.should_stop = true;
    EXPECT_TRUE(
            reader.read_blob(*row_positions({0}), 0, context, &result).is<ErrorCode::CANCELLED>());
}

} // namespace
} // namespace doris::format::paimon
