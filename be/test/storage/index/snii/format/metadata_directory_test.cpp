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

#include "storage/index/snii/format/metadata_directory.h"

#include <gtest/gtest.h>

#include <array>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gen_cpp/snii.pb.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

namespace doris::snii::format {
namespace {

LogicalIndexMetadataRef sample_entry(uint64_t index_id, std::string suffix, uint64_t base) {
    return {.index_id = index_id,
            .index_suffix = std::move(suffix),
            .core_metadata = {.offset = base, .length = 11},
            .sampled_term_index = {.offset = base + 11, .length = 12},
            .dict_block_directory = {.offset = base + 23, .length = 13}};
}

std::vector<LogicalIndexMetadataRef> sample_entries() {
    return {sample_entry(7, "primary", 100), sample_entry(8, "secondary", 200)};
}

std::vector<uint8_t> encode(const std::vector<LogicalIndexMetadataRef>& entries) {
    ByteSink sink;
    EXPECT_TRUE(encode_metadata_directory(entries, &sink).ok());
    return sink.buffer();
}

void expect_entry_eq(const LogicalIndexMetadataRef& expected,
                     const LogicalIndexMetadataRef& actual) {
    EXPECT_EQ(expected.index_id, actual.index_id);
    EXPECT_EQ(expected.index_suffix, actual.index_suffix);
    EXPECT_EQ(expected.core_metadata.offset, actual.core_metadata.offset);
    EXPECT_EQ(expected.core_metadata.length, actual.core_metadata.length);
    EXPECT_EQ(expected.sampled_term_index.offset, actual.sampled_term_index.offset);
    EXPECT_EQ(expected.sampled_term_index.length, actual.sampled_term_index.length);
    EXPECT_EQ(expected.dict_block_directory.offset, actual.dict_block_directory.offset);
    EXPECT_EQ(expected.dict_block_directory.length, actual.dict_block_directory.length);
}

doris::snii::SniiMetadataDirectoryPB valid_pb() {
    doris::snii::SniiMetadataDirectoryPB directory;
    const auto entry = sample_entry(7, "primary", 100);
    auto* index = directory.add_indexes();
    index->set_index_id(entry.index_id);
    index->set_index_suffix(entry.index_suffix);
    auto* core = index->mutable_core_metadata();
    core->set_offset(entry.core_metadata.offset);
    core->set_length(entry.core_metadata.length);
    auto* sti = index->mutable_sampled_term_index();
    sti->set_offset(entry.sampled_term_index.offset);
    sti->set_length(entry.sampled_term_index.length);
    auto* dbd = index->mutable_dict_block_directory();
    dbd->set_offset(entry.dict_block_directory.offset);
    dbd->set_length(entry.dict_block_directory.length);
    return directory;
}

std::vector<uint8_t> serialize(const doris::snii::SniiMetadataDirectoryPB& directory) {
    std::string bytes;
    EXPECT_TRUE(directory.SerializeToString(&bytes));
    return {bytes.begin(), bytes.end()};
}

void expect_corruption(const std::vector<uint8_t>& bytes) {
    MetadataDirectory directory;
    const auto status = MetadataDirectory::decode(Slice(bytes), &directory);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiMetadataDirectory, RoundTripsMultipleIndexesAndFindsDeterministically) {
    const auto expected = sample_entries();
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(encode(expected)), &directory).ok());
    ASSERT_EQ(expected.size(), directory.size());
    ASSERT_EQ(expected.size(), directory.entries().size());
    for (size_t i = 0; i < expected.size(); ++i) {
        expect_entry_eq(expected[i], directory.entries()[i]);
        const auto* found = directory.find(expected[i].index_id, expected[i].index_suffix);
        ASSERT_NE(nullptr, found);
        expect_entry_eq(expected[i], *found);
    }
    EXPECT_EQ(nullptr, directory.find(expected[0].index_id, "missing"));
    EXPECT_EQ(nullptr, directory.find(99, expected[0].index_suffix));
}

TEST(SniiMetadataDirectory, RoundTripsEmptyDirectory) {
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(encode({})), &directory).ok());
    EXPECT_EQ(0U, directory.size());
    EXPECT_TRUE(directory.entries().empty());
    EXPECT_EQ(nullptr, directory.find(7, "primary"));
}

TEST(SniiMetadataDirectory, PreservesBinarySuffix) {
    const auto expected = sample_entry(7, std::string("suffix\0bytes", 12), 100);
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(encode({expected})), &directory).ok());
    const auto* found = directory.find(expected.index_id, expected.index_suffix);
    ASSERT_NE(nullptr, found);
    expect_entry_eq(expected, *found);
}

TEST(SniiMetadataDirectory, RejectsDuplicateKey) {
    auto directory = valid_pb();
    *directory.add_indexes() = directory.indexes(0);
    expect_corruption(serialize(directory));

    const auto duplicate = sample_entry(7, "primary", 200);
    ByteSink sink;
    const auto status =
            encode_metadata_directory({sample_entry(7, "primary", 100), duplicate}, &sink);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_TRUE(sink.buffer().empty());
}

TEST(SniiMetadataDirectory, RejectsMissingLogicalFields) {
    using Mutation = std::function<void(doris::snii::SniiLogicalIndexMetadataPB*)>;
    const std::array<Mutation, 5> mutations {
            [](auto* index) { index->clear_index_id(); },
            [](auto* index) { index->clear_index_suffix(); },
            [](auto* index) { index->clear_core_metadata(); },
            [](auto* index) { index->clear_sampled_term_index(); },
            [](auto* index) { index->clear_dict_block_directory(); },
    };
    for (const auto& mutation : mutations) {
        auto directory = valid_pb();
        mutation(directory.mutable_indexes(0));
        expect_corruption(serialize(directory));
    }
}

TEST(SniiMetadataDirectory, RejectsMissingNestedBlobOffsetOrLength) {
    using BlobGetter = doris::snii::SniiBlobRefPB* (doris::snii::SniiLogicalIndexMetadataPB::*)();
    for (const auto blob : std::array<BlobGetter, 3> {
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_core_metadata,
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_sampled_term_index,
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_dict_block_directory}) {
        for (const auto clear : std::array<void (doris::snii::SniiBlobRefPB::*)(), 2> {
                     &doris::snii::SniiBlobRefPB::clear_offset,
                     &doris::snii::SniiBlobRefPB::clear_length}) {
            auto directory = valid_pb();
            ((directory.mutable_indexes(0)->*blob)()->*clear)();
            expect_corruption(serialize(directory));
        }
    }
}

TEST(SniiMetadataDirectory, RejectsZeroLengthMandatoryBlob) {
    using BlobGetter = doris::snii::SniiBlobRefPB* (doris::snii::SniiLogicalIndexMetadataPB::*)();
    for (const auto blob : std::array<BlobGetter, 3> {
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_core_metadata,
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_sampled_term_index,
                 &doris::snii::SniiLogicalIndexMetadataPB::mutable_dict_block_directory}) {
        auto directory = valid_pb();
        (directory.mutable_indexes(0)->*blob)()->set_length(0);
        expect_corruption(serialize(directory));
    }

    auto invalid = sample_entry(7, "primary", 100);
    invalid.core_metadata.length = 0;
    ByteSink sink;
    const auto status = encode_metadata_directory({invalid}, &sink);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_TRUE(sink.buffer().empty());
}

TEST(SniiMetadataDirectory, RejectsTruncatedProtobuf) {
    expect_corruption({0x12, 0x01});
}

TEST(SniiMetadataDirectory, AcceptsUnknownOptionalField) {
    auto bytes = serialize(valid_pb());
    ByteSink unknown_field;
    unknown_field.put_varint32((100U << 3) | 0U);
    unknown_field.put_varint32(7);
    bytes.insert(bytes.end(), unknown_field.buffer().begin(), unknown_field.buffer().end());

    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(bytes), &directory).ok());
    ASSERT_EQ(1U, directory.size());
    expect_entry_eq(sample_entry(7, "primary", 100), directory.entries()[0]);
}

TEST(SniiMetadataDirectory, RejectsUnknownRequiredFeature) {
    auto directory = valid_pb();
    directory.add_required_features(1);
    MetadataDirectory decoded;
    const auto status = MetadataDirectory::decode(Slice(serialize(directory)), &decoded);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

TEST(SniiMetadataDirectory, RejectsNullOutputPointers) {
    EXPECT_TRUE(MetadataDirectory::decode(Slice(encode(sample_entries())), nullptr)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            encode_metadata_directory(sample_entries(), nullptr).is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::snii::format
