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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace {

namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using namespace doris::snii; // NOLINT

class BufferFileReader final : public io::FileReader {
public:
    explicit BufferFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "buffer reader: read past EOF");
        }
        size_t actual = len;
        if (short_read_ && actual != 0) {
            --actual;
        }
        out->resize(actual);
        if (actual != 0) {
            std::memcpy(out->data(), bytes_.data() + offset, actual);
        }
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }
    void set_short_read(bool value) { short_read_ = value; }

private:
    std::vector<uint8_t> bytes_;
    bool short_read_ = false;
};

struct LogicalImage {
    std::vector<uint8_t> file;
    std::vector<uint8_t> core;
    std::vector<uint8_t> sampled_term_index;
    std::vector<uint8_t> dict_block_directory;
};

std::vector<uint8_t> build_null_frame(const std::vector<uint32_t>& null_docids,
                                      uint32_t recorded_doc_count) {
    format::NullBitmapWriter writer;
    for (uint32_t docid : null_docids) {
        writer.add_null(docid);
    }
    ByteSink sink;
    EXPECT_TRUE(writer.finish(recorded_doc_count, &sink).ok());
    return sink.buffer();
}

std::vector<uint8_t> reframe(Slice frame, uint8_t type, bool append_payload_byte) {
    ByteSource source(frame);
    FramedSection section;
    if (!SectionFramer::read(source, &section).ok()) {
        return {};
    }
    std::vector<uint8_t> payload(section.payload.data(),
                                 section.payload.data() + section.payload.size());
    if (append_payload_byte) {
        payload.push_back(0);
    }
    ByteSink sink;
    SectionFramer::write(sink, type, Slice(payload));
    return sink.buffer();
}

LogicalImage build_logical_image(uint64_t stats_doc_count, uint64_t stats_null_count,
                                 std::optional<std::vector<uint8_t>> null_frame,
                                 std::optional<format::RegionRef> null_ref = std::nullopt) {
    LogicalImage image;
    format::SectionRefs refs;
    if (null_frame.has_value()) {
        image.file = std::move(*null_frame);
        refs.null_bitmap = {0, image.file.size()};
    }
    if (null_ref.has_value()) {
        refs.null_bitmap = *null_ref;
    }

    ByteSink sampled_frame;
    format::SampledTermIndexBuilder sampled;
    sampled.finish(&sampled_frame);
    ByteSink directory_frame;
    format::DictBlockDirectoryBuilder directory;
    directory.finish(&directory_frame);

    format::StatsBlock stats;
    stats.doc_count = stats_doc_count;
    stats.indexed_doc_count = stats_doc_count - stats_null_count;
    stats.null_count = stats_null_count;

    format::CoreMetadata core;
    core.index_config = format::IndexConfig::kDocsPositions;
    core.stats = stats;
    core.section_refs = refs;
    ByteSink core_frame;
    EXPECT_TRUE(format::encode_core_metadata(core, &core_frame).ok());
    image.core = core_frame.buffer();
    image.sampled_term_index = sampled_frame.buffer();
    image.dict_block_directory = directory_frame.buffer();
    return image;
}

Status open_logical(BufferFileReader* file, const LogicalImage& image,
                    reader::LogicalIndexReader* out) {
    return reader::LogicalIndexReader::open(file, Slice(image.core),
                                            Slice(image.sampled_term_index),
                                            Slice(image.dict_block_directory), out);
}

TEST(SniiLogicalNullReaderTest, ReturnsSparseNullDocidsInAscendingOrder) {
    constexpr uint32_t kDocCount = 1000000000;
    LogicalImage image = build_logical_image(
            kDocCount, 4, build_null_frame({999999999, 7, 500000, 1}, kDocCount));
    BufferFileReader file(std::move(image.file));
    reader::LogicalIndexReader index;
    ASSERT_TRUE(open_logical(&file, image, &index).ok());

    std::vector<uint32_t> null_docids;
    ASSERT_TRUE(index.read_null_docids(&null_docids).ok());
    EXPECT_EQ(null_docids, (std::vector<uint32_t> {1, 7, 500000, 999999999}));
}

TEST(SniiLogicalNullReaderTest, MissingSectionIsEmptyOnlyWhenStatsAreEmpty) {
    for (uint64_t null_count : {0, 1}) {
        LogicalImage image = build_logical_image(/*stats_doc_count=*/8, null_count, std::nullopt);
        BufferFileReader file(std::move(image.file));
        reader::LogicalIndexReader index;
        ASSERT_TRUE(open_logical(&file, image, &index).ok());

        std::vector<uint32_t> null_docids {99};
        Status status = index.read_null_docids(&null_docids);
        if (null_count == 0) {
            EXPECT_TRUE(status.ok()) << status.to_string();
            EXPECT_TRUE(null_docids.empty());
        } else {
            EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
            EXPECT_TRUE(null_docids.empty());
        }
    }
}

TEST(SniiLogicalNullReaderTest, AccountsEncodedEmptyBitmapFrameWhenStatsAreEmpty) {
    std::vector<uint8_t> frame = build_null_frame({}, /*recorded_doc_count=*/8);
    const uint64_t frame_bytes = frame.size();
    uint64_t expected_decode_bytes = 0;
    ASSERT_TRUE(format::NullBitmapReader::decoded_memory_bytes(Slice(frame), &expected_decode_bytes)
                        .ok());
    LogicalImage image =
            build_logical_image(/*stats_doc_count=*/8, /*stats_null_count=*/0, std::move(frame));
    BufferFileReader file(std::move(image.file));
    reader::LogicalIndexReader index;
    ASSERT_TRUE(open_logical(&file, image, &index).ok());

    reader::NullDocidsScanMemory memory;
    ASSERT_TRUE(index.null_docids_scan_memory(&memory).ok());
    EXPECT_EQ(memory.frame_bytes, frame_bytes);
    EXPECT_EQ(memory.output_bytes, 0);

    uint64_t reserved_decode_bytes = 0;
    std::vector<uint32_t> null_docids;
    ASSERT_TRUE(index.read_null_docids(&null_docids, [&](uint64_t bytes) {
                         reserved_decode_bytes = bytes;
                         return Status::OK();
                     }).ok());
    EXPECT_TRUE(null_docids.empty());
    EXPECT_EQ(reserved_decode_bytes, expected_decode_bytes);
}

TEST(SniiLogicalNullReaderTest, RejectsRegionPastFileAndShortRead) {
    std::vector<uint8_t> frame = build_null_frame({2}, 8);
    LogicalImage past_eof = build_logical_image(
            /*stats_doc_count=*/8, /*stats_null_count=*/1, frame,
            format::RegionRef {.offset = 0, .length = frame.size() + 1});
    BufferFileReader past_eof_file(std::move(past_eof.file));
    reader::LogicalIndexReader past_eof_index;
    ASSERT_TRUE(open_logical(&past_eof_file, past_eof, &past_eof_index).ok());
    reader::NullDocidsScanMemory memory;
    EXPECT_TRUE(past_eof_index.null_docids_scan_memory(&memory)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_EQ(memory.frame_bytes, 0);
    EXPECT_EQ(memory.output_bytes, 0);
    std::vector<uint32_t> null_docids;
    EXPECT_TRUE(past_eof_index.read_null_docids(&null_docids)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    LogicalImage short_read = build_logical_image(
            /*stats_doc_count=*/8, /*stats_null_count=*/1, build_null_frame({2}, 8));
    BufferFileReader short_read_file(std::move(short_read.file));
    reader::LogicalIndexReader short_read_index;
    ASSERT_TRUE(open_logical(&short_read_file, short_read, &short_read_index).ok());
    short_read_file.set_short_read(true);
    EXPECT_TRUE(short_read_index.read_null_docids(&null_docids)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiLogicalNullReaderTest, RejectsInvalidSectionEnvelope) {
    std::vector<uint8_t> corrupt_crc = build_null_frame({2}, 8);
    corrupt_crc.back() ^= 0xff;
    std::vector<uint8_t> wrong_type =
            reframe(Slice(build_null_frame({2}, 8)), format::kNullBitmapSectionType + 1,
                    /*append_payload_byte=*/false);
    std::vector<uint8_t> trailing_payload =
            reframe(Slice(build_null_frame({2}, 8)), format::kNullBitmapSectionType,
                    /*append_payload_byte=*/true);
    std::vector<uint8_t> trailing_frame = build_null_frame({2}, 8);
    trailing_frame.push_back(0);

    std::vector<std::vector<uint8_t>> frames {std::move(corrupt_crc), std::move(wrong_type),
                                              std::move(trailing_payload),
                                              std::move(trailing_frame)};
    for (std::vector<uint8_t>& frame : frames) {
        const uint64_t frame_size = frame.size();
        LogicalImage image = build_logical_image(
                /*stats_doc_count=*/8, /*stats_null_count=*/1, std::move(frame),
                format::RegionRef {.offset = 0, .length = frame_size});
        BufferFileReader file(std::move(image.file));
        reader::LogicalIndexReader index;
        ASSERT_TRUE(open_logical(&file, image, &index).ok());
        std::vector<uint32_t> null_docids;
        EXPECT_TRUE(index.read_null_docids(&null_docids)
                            .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}

TEST(SniiLogicalNullReaderTest, RejectsStatsAndDocumentDomainMismatches) {
    struct Case {
        uint64_t stats_doc_count;
        uint64_t stats_null_count;
        std::vector<uint8_t> frame;
    };
    std::vector<Case> cases;
    cases.push_back({8, 1, build_null_frame({2}, /*recorded_doc_count=*/9)});
    cases.push_back({8, 2, build_null_frame({2}, /*recorded_doc_count=*/8)});
    cases.push_back({8, 1, build_null_frame({8}, /*recorded_doc_count=*/8)});

    for (Case& test_case : cases) {
        LogicalImage image = build_logical_image(
                test_case.stats_doc_count, test_case.stats_null_count, std::move(test_case.frame));
        BufferFileReader file(std::move(image.file));
        reader::LogicalIndexReader index;
        ASSERT_TRUE(open_logical(&file, image, &index).ok());
        std::vector<uint32_t> null_docids;
        EXPECT_TRUE(index.read_null_docids(&null_docids)
                            .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    }
}

} // namespace
