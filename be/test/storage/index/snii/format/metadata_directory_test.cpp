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
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gen_cpp/snii.pb.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {
namespace {

LogicalIndexMetadataRef sample_entry(uint64_t index_id, std::string suffix, uint64_t base) {
    return {.index_id = index_id,
            .index_suffix = std::move(suffix),
            .core_metadata = {.offset = base, .length = 11},
            .sampled_term_index = {.offset = base + 11, .length = 12},
            .dict_block_directory = {.offset = base + 23, .length = 13},
            .kind = LogicalIndexKind::kInverted,
            .files = {}};
}

std::vector<LogicalIndexMetadataRef> sample_entries() {
    return {sample_entry(7, "primary", 100), sample_entry(8, "secondary", 200)};
}

std::vector<uint8_t> encode(const std::vector<LogicalIndexMetadataRef>& entries) {
    ByteSink sink;
    EXPECT_TRUE(encode_metadata_directory(entries, &sink).ok());
    return sink.buffer();
}

// kind/files belong to the identity of an entry too: without comparing them, a
// decoder that mislabelled every entry's kind would pass every round-trip.
void expect_blob_fields_eq(const LogicalIndexMetadataRef& expected,
                           const LogicalIndexMetadataRef& actual) {
    EXPECT_EQ(expected.kind, actual.kind);
    ASSERT_EQ(expected.files.size(), actual.files.size());
    for (size_t i = 0; i < expected.files.size(); ++i) {
        EXPECT_EQ(expected.files[i].name, actual.files[i].name);
        EXPECT_EQ(expected.files[i].offset, actual.files[i].offset);
        EXPECT_EQ(expected.files[i].length, actual.files[i].length);
        EXPECT_EQ(expected.files[i].crc32c, actual.files[i].crc32c);
    }
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
    expect_blob_fields_eq(expected, actual);
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
    // 1 is now the known kFeatureBlobLogicalIndex, so an unknown feature must
    // use a different value.
    auto directory = valid_pb();
    directory.add_required_features(999);
    MetadataDirectory decoded;
    const auto status = MetadataDirectory::decode(Slice(serialize(directory)), &decoded);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

// ---- Blob logical index entries (kind + named-file table) ----

void expect_unsupported(const std::vector<uint8_t>& bytes) {
    MetadataDirectory directory;
    const auto status = MetadataDirectory::decode(Slice(bytes), &directory);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
}

NamedBlobFileRef sample_file(std::string name, uint64_t offset, uint64_t length, uint32_t crc) {
    return {.name = std::move(name), .offset = offset, .length = length, .crc32c = crc};
}

LogicalIndexMetadataRef sample_blob_entry(uint64_t index_id, std::string suffix,
                                          LogicalIndexKind kind,
                                          std::vector<NamedBlobFileRef> files) {
    LogicalIndexMetadataRef entry;
    entry.index_id = index_id;
    entry.index_suffix = std::move(suffix);
    entry.kind = kind;
    entry.files = std::move(files);
    return entry;
}

// Hand-built PB mirror of sample_blob_entry for decode-only scenarios.
doris::snii::SniiLogicalIndexMetadataPB* add_blob_pb(doris::snii::SniiMetadataDirectoryPB* out,
                                                     uint64_t index_id, const std::string& suffix,
                                                     uint32_t kind) {
    auto* index = out->add_indexes();
    index->set_index_id(index_id);
    index->set_index_suffix(suffix);
    index->set_kind(kind);
    auto* meta = index->add_files();
    meta->set_name("bkd_meta");
    meta->set_offset(4096);
    meta->set_length(12);
    meta->set_crc32c(7);
    return index;
}

doris::snii::SniiMetadataDirectoryPB valid_blob_pb() {
    auto directory = valid_pb();
    directory.add_required_features(kFeatureBlobLogicalIndex);
    add_blob_pb(&directory, 9, "bkd", 1);
    return directory;
}

TEST(SniiMetadataDirectory, PureInvertedDirectoryBytesUnchangedByBlobSupport) {
    // The byte gate: a directory without blob entries must serialize exactly as
    // it did before kind/files existed -- no required_features, no kind field.
    EXPECT_EQ(serialize(valid_pb()), encode({sample_entry(7, "primary", 100)}));
}

// A mixed directory: one text entry plus a BKD entry carrying two 0-length
// files (the empty-segment shape) and an ANN entry.
std::vector<LogicalIndexMetadataRef> mixed_entries() {
    return {
            sample_entry(7, "primary", 100),
            sample_blob_entry(
                    9, "bkd", LogicalIndexKind::kBkd,
                    {sample_file("bkd_meta", 4096, 12, 0x11), sample_file("bkd_index", 4108, 0, 0),
                     sample_file("bkd", 300, 0, 0)}),
            sample_blob_entry(10, "", LogicalIndexKind::kAnn,
                              {sample_file("ann.faiss", 500, 1U << 20, 0x22)}),
    };
}

TEST(SniiMetadataDirectory, BlobEntriesSetTheFeatureFlagAndLeaveInvertedEntriesBare) {
    const auto bytes = encode(mixed_entries());
    doris::snii::SniiMetadataDirectoryPB wire;
    ASSERT_TRUE(wire.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    ASSERT_EQ(1, wire.required_features_size());
    EXPECT_EQ(kFeatureBlobLogicalIndex, wire.required_features(0));
    // INVERTED entries must not gain presence bits for kind/files.
    EXPECT_FALSE(wire.indexes(0).has_kind());
    EXPECT_EQ(0, wire.indexes(0).files_size());
}

TEST(SniiMetadataDirectory, BlobEntriesRoundTripWithZeroLengthFile) {
    const auto expected = mixed_entries();
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(encode(expected)), &directory).ok());
    ASSERT_EQ(expected.size(), directory.size());
    EXPECT_EQ(LogicalIndexKind::kInverted, directory.entries()[0].kind);
    for (const auto& want : expected) {
        const auto* got = directory.find(want.index_id, want.index_suffix);
        ASSERT_NE(nullptr, got);
        expect_blob_fields_eq(want, *got);
    }
}

TEST(SniiMetadataDirectory, AcceptsKnownBlobFeatureAndExplicitInvertedKind) {
    MetadataDirectory directory;
    ASSERT_TRUE(MetadataDirectory::decode(Slice(serialize(valid_blob_pb())), &directory).ok());
    EXPECT_EQ(2U, directory.size());

    // kind == 0 spelled out explicitly is still INVERTED (row 1 of the decode
    // matrix covers the default AND the explicit spelling).
    auto explicit_inverted = valid_pb();
    explicit_inverted.mutable_indexes(0)->set_kind(0);
    ASSERT_TRUE(MetadataDirectory::decode(Slice(serialize(explicit_inverted)), &directory).ok());
    EXPECT_EQ(LogicalIndexKind::kInverted, directory.entries()[0].kind);
}

TEST(SniiMetadataDirectory, RejectsUnknownKind) {
    auto directory = valid_blob_pb();
    directory.mutable_indexes(1)->set_kind(3);
    expect_unsupported(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsBlobEntryCarryingInvertedTriplet) {
    auto directory = valid_blob_pb();
    auto* blob = directory.mutable_indexes(1);
    blob->mutable_core_metadata()->set_offset(1);
    blob->mutable_core_metadata()->set_length(1);
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsInvertedEntryCarryingFiles) {
    auto directory = valid_blob_pb();
    auto* file = directory.mutable_indexes(0)->add_files();
    file->set_name("stray");
    file->set_offset(0);
    file->set_length(1);
    file->set_crc32c(0);
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsBlobEntryWithEmptyFileTable) {
    auto directory = valid_blob_pb();
    directory.mutable_indexes(1)->clear_files();
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsBlobFileNameProblems) {
    {
        auto directory = valid_blob_pb();
        *directory.mutable_indexes(1)->add_files() = directory.indexes(1).files(0);
        expect_corruption(serialize(directory));
    }
    {
        auto directory = valid_blob_pb();
        directory.mutable_indexes(1)->mutable_files(0)->set_name("");
        expect_corruption(serialize(directory));
    }
}

TEST(SniiMetadataDirectory, RejectsBlobFileMissingFields) {
    using Mutation = std::function<void(doris::snii::SniiNamedBlobPB*)>;
    const std::array<Mutation, 4> mutations {
            [](auto* file) { file->clear_name(); },
            [](auto* file) { file->clear_offset(); },
            [](auto* file) { file->clear_length(); },
            [](auto* file) { file->clear_crc32c(); },
    };
    for (const auto& mutation : mutations) {
        auto directory = valid_blob_pb();
        mutation(directory.mutable_indexes(1)->mutable_files(0));
        expect_corruption(serialize(directory));
    }
}

TEST(SniiMetadataDirectory, RejectsBlobFileOffsetLengthOverflow) {
    auto directory = valid_blob_pb();
    auto* file = directory.mutable_indexes(1)->mutable_files(0);
    file->set_offset(std::numeric_limits<uint64_t>::max());
    file->set_length(2);
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsBlobEntryWithoutFeatureFlag) {
    auto directory = valid_blob_pb();
    directory.clear_required_features();
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsFeatureFlagWithoutBlobEntry) {
    auto directory = valid_pb();
    directory.add_required_features(kFeatureBlobLogicalIndex);
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, RejectsCrossKindDuplicateKey) {
    auto directory = valid_blob_pb();
    add_blob_pb(&directory, 7, "primary", 1); // collides with the INVERTED entry
    expect_corruption(serialize(directory));
}

TEST(SniiMetadataDirectory, EncodeRejectsInvalidBlobEntries) {
    // The encoder self-checks through the same decode path, so a structurally
    // invalid blob entry must fail the WRITE, not produce a poisoned directory.
    {
        ByteSink sink;
        const auto status = encode_metadata_directory(
                {sample_blob_entry(9, "bkd", LogicalIndexKind::kBkd, {})}, &sink);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
        EXPECT_TRUE(sink.buffer().empty());
    }
    {
        ByteSink sink;
        const auto status = encode_metadata_directory(
                {sample_blob_entry(9, "bkd", LogicalIndexKind::kBkd,
                                   {sample_file("dup", 0, 1, 0), sample_file("dup", 1, 1, 0)})},
                &sink);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
        EXPECT_TRUE(sink.buffer().empty());
    }
}

TEST(SniiMetadataDirectory, RejectsNullOutputPointers) {
    EXPECT_TRUE(MetadataDirectory::decode(Slice(encode(sample_entries())), nullptr)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(
            encode_metadata_directory(sample_entries(), nullptr).is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::snii::format
