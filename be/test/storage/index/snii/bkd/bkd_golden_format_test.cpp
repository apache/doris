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

// P1-6, design 12.3: the golden byte digests of the on-disk format.
//
// Every other test in this directory asserts BEHAVIOUR, and behaviour survives a
// format change: rename a field, reorder two varints, switch a leaf from kRle to
// kRaw, and every round trip still passes because the same code writes and reads.
// What that cannot catch is FORMAT DRIFT -- a change that silently makes this
// binary unable to read what the previous one wrote.
//
// So a fixed input is pinned to a fixed SHA256 of each sub-file. The digests
// below are DERIVED, not designed: they are whatever the builder emitted for the
// documented inputs. A change to them is not a test failure to paper over -- it
// is the statement "v1 files written before this change no longer decode", and
// the only legitimate ways to make it are (a) the emitted bytes are genuinely
// unchanged and the digest was mis-transcribed, or (b) kFormatVersion is bumped
// and a compatibility policy is written down (design 3).
//
// The digests are stable because the build is: the sort key (value, doc_id) is
// total over the fixed-width record, so the same point multiset always produces
// the same bytes (asserted independently in bkd_property_test.cpp).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/types.h"
#include "util/sha.h"

namespace doris::snii::bkd {
namespace {

using Bytes = std::string;

struct Point {
    uint32_t doc_id = 0;
    int64_t value = 0;
};

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

class MemoryFileWriter final : public io::FileWriter {
public:
    Status append(Slice data) override {
        bytes_.insert(bytes_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }
    Status finalize() override { return Status::OK(); }
    uint64_t bytes_written() const override { return bytes_.size(); }

    const std::vector<uint8_t>& bytes() const { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

class MemoryFileReader final : public io::FileReader {
public:
    explicit MemoryFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        out->resize(len);
        return read_into(offset, out->data(), len);
    }

    Status read_into(uint64_t offset, uint8_t* out, size_t out_len) override {
        if (out_len == 0) {
            return Status::OK();
        }
        if (offset > bytes_.size() || out_len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::CORRUPTION, false>("read past EOF");
        }
        std::memcpy(out, bytes_.data() + offset, out_len);
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

private:
    std::vector<uint8_t> bytes_;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// The one encoder points and bounds both go through (INV-1): unsigned big-endian
// sortable bytes for the index's OWN field type. Pinning a digest means pinning
// these bytes too, which is why the golden cases never hand-assemble a value.
Bytes encode_value(FieldType field_type, int64_t value) {
    Bytes buf;
    if (field_type == FieldType::OLAP_FIELD_TYPE_BIGINT) {
        get_key_coder(field_type)->full_encode_ascending(&value, &buf);
    } else if (field_type == FieldType::OLAP_FIELD_TYPE_INT) {
        const auto narrow = static_cast<int32_t>(value);
        get_key_coder(field_type)->full_encode_ascending(&narrow, &buf);
    } else {
        ADD_FAILURE() << "no encoder wired up for field type " << static_cast<int>(field_type);
    }
    EXPECT_EQ(buf.size(), field_type_size(field_type));
    return buf;
}

Slice to_slice(const Bytes& bytes) {
    return Slice(std::string_view(bytes));
}

std::string sha256_hex(const std::vector<uint8_t>& bytes) {
    // SHA256_Update over a zero-length buffer returns immediately, but a null
    // pointer never reaches it either way: an empty sub-file (the legal empty
    // index, design 5.3) hashes as the empty string, which is a value worth
    // pinning rather than skipping.
    static constexpr uint8_t kNothing = 0;
    SHA256Digest digest;
    digest.reset(bytes.empty() ? &kNothing : bytes.data(), bytes.size());
    return std::string(digest.digest());
}

// The two sub-files of one build, kept as raw bytes so a digest can be taken
// over exactly what would land in the container.
struct Built {
    std::vector<uint8_t> index_bytes;
    std::vector<uint8_t> data_bytes;
    BkdStats stats;
};

Status build(const std::vector<Point>& points, FieldType field_type, uint32_t points_per_leaf,
             Built* out) {
    BkdBuilderOptions options;
    options.bytes_per_dim = static_cast<uint32_t>(field_type_size(field_type));
    options.field_type = field_type;
    options.points_per_leaf = points_per_leaf;

    std::unique_ptr<BkdBuilder> builder;
    RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
    for (const Point& point : points) {
        RETURN_IF_ERROR(
                builder->add(point.doc_id, to_slice(encode_value(field_type, point.value))));
    }
    MemoryFileWriter data;
    ByteSink index;
    RETURN_IF_ERROR(builder->finish(&data, &index, &out->stats));
    out->index_bytes = index.take();
    out->data_bytes = data.bytes();
    return Status::OK();
}

// Lays the two sub-files out inside a container image at non-zero offsets and
// opens a reader over it. The reader borrows the file, so both come back.
struct Opened {
    std::unique_ptr<MemoryFileReader> file;
    std::unique_ptr<BkdReader> reader;
};

Status open_over(const std::vector<uint8_t>& index_bytes, const std::vector<uint8_t>& data_bytes,
                 Opened* out) {
    std::vector<uint8_t> image(11, 0xA5);
    BkdSections sections;
    sections.data_offset = image.size();
    sections.data_length = data_bytes.size();
    image.insert(image.end(), data_bytes.begin(), data_bytes.end());
    image.insert(image.end(), 4, 0x5A);
    sections.index_offset = image.size();
    sections.index_length = index_bytes.size();
    image.insert(image.end(), index_bytes.begin(), index_bytes.end());
    image.insert(image.end(), 2, 0xC3);

    out->file = std::make_unique<MemoryFileReader>(std::move(image));
    return BkdReader::open(out->file.get(), sections, &out->reader);
}

// ---------------------------------------------------------------------------
// The golden cases
// ---------------------------------------------------------------------------
//
// Each point set is generated by ONE documented rule, so the input can be
// reconstructed from this file alone -- a digest whose input is not reproducible
// pins nothing. Between them the cases cover all three leaf value modes, both
// widths in use, the empty index and the array-column shape.

// value i, doc i for i in [0, 200): every value distinct, leaves share a long
// prefix, mode kRaw.
std::vector<Point> ramp_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 200; ++i) {
        points.push_back(Point {i, static_cast<int64_t>(i)});
    }
    return points;
}

// value 7, doc i for i in [0, 64): one value everywhere, mode kAllEqual, doc ids
// ascending across the whole leaf.
std::vector<Point> all_equal_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 64; ++i) {
        points.push_back(Point {i, 7});
    }
    return points;
}

// value i / 8, doc i for i in [0, 128): runs of eight equal values, mode kRle,
// doc id deltas restarting at every run.
std::vector<Point> run_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 128; ++i) {
        points.push_back(Point {i, static_cast<int64_t>(i / 8)});
    }
    return points;
}

// doc i / 3, value (i * 37) % 101 for i in [0, 90): three points per row, i.e.
// the array column -- one doc id repeated, values unordered within the row.
std::vector<Point> array_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 90; ++i) {
        points.push_back(Point {i / 3, static_cast<int64_t>((i * 37) % 101)});
    }
    return points;
}

// value i - 50, doc i for i in [0, 100), as INT: four-byte values spanning the
// sign flip, which is where a KeyCoder change would show up first.
std::vector<Point> signed_int_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 100; ++i) {
        points.push_back(Point {i, static_cast<int64_t>(i) - 50});
    }
    return points;
}

struct GoldenCase {
    const char* name;
    std::vector<Point> (*points)();
    FieldType field_type;
    uint32_t points_per_leaf;
    uint64_t point_count;
    uint32_t doc_count;
    uint32_t leaf_count;
    size_t index_size;
    size_t data_size;
    // SHA256 of the framed bkd_index section and of the bkd_data sub-file,
    // exactly as they would be written into the container.
    const char* index_sha256;
    const char* data_sha256;
};

std::vector<Point> no_points() {
    return {};
}

// The counts are the case's INTENT and are derivable by hand (leaf_count is
// ceil(point_count / points_per_leaf) -- the leaf count is never rounded up to a
// power of two, design 6.4). The sizes and digests are OBSERVATIONS of what the
// builder emits; they are transcribed from a run and must only ever change
// deliberately.
const GoldenCase kGoldenCases[] = {
        // The empty index is header only (design 5.3): 18 framed bytes and a
        // zero-length bkd_data, whose digest is SHA256 of the empty string.
        {"empty", &no_points, FieldType::OLAP_FIELD_TYPE_BIGINT, 16, 0, 0, 0, 18, 0,
         "d114909a532f1ff5da6ac94e98c1897be39a3b7dfa317bb82223cdd5b0fc2690",
         "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {"ramp_bigint", &ramp_points, FieldType::OLAP_FIELD_TYPE_BIGINT, 16, 200, 200, 13, 159, 552,
         "d06c2e8130edf38d9b85787ed55f5a7ecaa392b4bd17b83406fbdc7c8772d3d3",
         "c9bbd4cfcc24458057aabe91b26c2780be7bcf405665f44176a1c55504117fb8"},
        {"all_equal_bigint", &all_equal_points, FieldType::OLAP_FIELD_TYPE_BIGINT, 16, 64, 64, 4,
         66, 74, "77833a0edd9cff206b4f045083ad4bb37e21d5e3502b6f82766ea7a3e253421c",
         "b5ef4be9617892b6113eef5d6a86e8fa558c5f8a9a46a196a8ba8baf9d5a6d32"},
        {"runs_bigint", &run_points, FieldType::OLAP_FIELD_TYPE_BIGINT, 16, 128, 128, 8, 108, 214,
         "609e17d358223817890f2dd34dbdfbbd833976351e98109ef957c6a3980710ea",
         "70d3c346cdee46058f9af529af58d8ee756adffaaa65503488ceda2f3804e4fa"},
        {"array_bigint", &array_points, FieldType::OLAP_FIELD_TYPE_BIGINT, 7, 90, 30, 13, 157, 336,
         "374ccbf7a34a4a3e7a366bb4a3c830c774319c38f726d16dd222c93cad984edf",
         "6c9f5e6ea8dcf8a9edcc292a07594e007814c8883d4087e7b0938fc95ac68bc0"},
        {"signed_int", &signed_int_points, FieldType::OLAP_FIELD_TYPE_INT, 10, 100, 100, 10, 82,
         279, "a948eb25f9860f15975911b6f9f510ab4b18c61a2ba16c18a0978911a989080a",
         "4147cc9860a4e150ace2339510282aeeabec603c44a4b0aff01997455900b6a5"},
};

// ---------------------------------------------------------------------------
// The digests themselves
// ---------------------------------------------------------------------------

TEST(BkdGoldenFormatTest, SubFileBytesMatchTheirPinnedDigests) {
    for (const GoldenCase& golden : kGoldenCases) {
        SCOPED_TRACE(golden.name);
        Built built;
        ASSERT_TRUE(build(golden.points(), golden.field_type, golden.points_per_leaf, &built).ok());

        // Sizes first: they are not the contract, but when a digest moves they
        // say at a glance WHERE the format moved.
        EXPECT_EQ(built.stats.point_count, golden.point_count);
        EXPECT_EQ(built.stats.doc_count, golden.doc_count);
        EXPECT_EQ(built.stats.leaf_count, golden.leaf_count);
        EXPECT_EQ(built.index_bytes.size(), golden.index_size);
        EXPECT_EQ(built.data_bytes.size(), golden.data_size);
        EXPECT_EQ(built.stats.index_bytes, built.index_bytes.size());
        EXPECT_EQ(built.stats.data_bytes, built.data_bytes.size());

        EXPECT_EQ(sha256_hex(built.index_bytes), std::string(golden.index_sha256))
                << "bkd_index bytes changed for case '" << golden.name
                << "'. If the format really changed, bump kFormatVersion (design 3) "
                   "and update this digest deliberately.";
        EXPECT_EQ(sha256_hex(built.data_bytes), std::string(golden.data_sha256))
                << "bkd_data bytes changed for case '" << golden.name
                << "'. If the format really changed, bump kFormatVersion (design 3) "
                   "and update this digest deliberately.";
    }
}

// ---------------------------------------------------------------------------
// Round trip (design 12.3)
// ---------------------------------------------------------------------------

TEST(BkdGoldenFormatTest, EveryGoldenCaseRoundTripsThroughTheReader) {
    for (const GoldenCase& golden : kGoldenCases) {
        SCOPED_TRACE(golden.name);
        const std::vector<Point> points = golden.points();
        Built built;
        ASSERT_TRUE(build(points, golden.field_type, golden.points_per_leaf, &built).ok());

        Opened opened;
        ASSERT_TRUE(open_over(built.index_bytes, built.data_bytes, &opened).ok());
        const BkdReader& reader = *opened.reader;

        EXPECT_EQ(reader.header().format_version, kFormatVersion);
        EXPECT_EQ(reader.header().flags, 0U);
        EXPECT_EQ(reader.header().field_type, golden.field_type);
        EXPECT_EQ(reader.header().bytes_per_dim, field_type_size(golden.field_type));
        EXPECT_EQ(reader.header().points_per_leaf, golden.points_per_leaf);
        EXPECT_EQ(reader.point_count(), golden.point_count);
        EXPECT_EQ(reader.doc_count(), golden.doc_count);
        EXPECT_EQ(reader.leaf_count(), golden.leaf_count);
        EXPECT_EQ(reader.empty(), points.empty());

        // Every value comes back attached to exactly the docs that carried it,
        // which is the part a digest cannot check: the bytes could be stable and
        // still mean the wrong thing.
        std::set<int64_t> values;
        for (const Point& point : points) {
            values.insert(point.value);
        }
        for (const int64_t value : values) {
            const Bytes encoded = encode_value(golden.field_type, value);
            roaring::Roaring hits;
            ASSERT_TRUE(reader.range(to_slice(encoded), true, to_slice(encoded), true, &hits).ok());
            roaring::Roaring expected;
            for (const Point& point : points) {
                if (point.value == value) {
                    expected.add(point.doc_id);
                }
            }
            EXPECT_EQ(hits, expected) << "value " << value;
        }

        roaring::Roaring everything;
        ASSERT_TRUE(reader.range(Slice(), true, Slice(), true, &everything).ok());
        EXPECT_EQ(everything.cardinality(), golden.doc_count);
    }
}

// ---------------------------------------------------------------------------
// Version gate (design 12.3): a file from the future is a CAPABILITY boundary
// ---------------------------------------------------------------------------

// Unwraps the framed bkd_index section into its payload. Decoding goes through
// ByteSource / SectionFramer like every other read in this module -- a test that
// hand-parsed the frame with raw pointers would be asserting against its own
// idea of the format rather than against the format.
std::vector<uint8_t> payload_of(const std::vector<uint8_t>& framed) {
    ByteSource src {Slice(framed)};
    FramedSection section;
    EXPECT_TRUE(SectionFramer::read(src, &section).ok());
    EXPECT_EQ(section.type, kBkdIndexSectionType);
    return std::vector<uint8_t>(section.payload.data(),
                                section.payload.data() + section.payload.size());
}

// Re-wraps a payload, so the checksum matches and the mutation below is judged
// by the HEADER parser rather than rejected by the frame.
std::vector<uint8_t> reframe(const std::vector<uint8_t>& payload) {
    ByteSink sink;
    SectionFramer::write(sink, kBkdIndexSectionType, Slice(payload));
    return sink.take();
}

// The payload is fixed32 magic followed by the format_version varint32
// (design 5.1), so version 1 is the single byte right after the magic. Asserted,
// not assumed: if the layout ever moves, this test must fail loudly instead of
// patching an unrelated field.
constexpr size_t kFormatVersionOffset = 4;

TEST(BkdGoldenFormatTest, AFutureFormatVersionIsNotSupportedRatherThanCorrupted) {
    Built built;
    ASSERT_TRUE(build(ramp_points(), FieldType::OLAP_FIELD_TYPE_BIGINT, 16, &built).ok());
    std::vector<uint8_t> payload = payload_of(built.index_bytes);
    ASSERT_GT(payload.size(), kFormatVersionOffset);
    ASSERT_EQ(payload[kFormatVersionOffset], kFormatVersion);

    // The control: re-framing without touching a byte still opens. Without it a
    // NOT_SUPPORTED below could just mean the harness broke the section.
    {
        Opened opened;
        ASSERT_TRUE(open_over(reframe(payload), built.data_bytes, &opened).ok());
        EXPECT_EQ(opened.reader->header().format_version, kFormatVersion);
        EXPECT_EQ(opened.reader->point_count(), 200U);
    }

    // A version this binary does not know is NOT damage: the caller has to
    // report "index unavailable" and fall back to a scan, which is a different
    // decision from "this segment is corrupt" (design 3 / 8).
    for (const uint8_t version : {uint8_t {2}, uint8_t {3}, uint8_t {127}}) {
        SCOPED_TRACE("format_version=" + std::to_string(version));
        std::vector<uint8_t> future = payload;
        future[kFormatVersionOffset] = version;
        Opened opened;
        const Status status = open_over(reframe(future), built.data_bytes, &opened);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
        EXPECT_FALSE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
        EXPECT_EQ(opened.reader, nullptr);
    }
}

TEST(BkdGoldenFormatTest, VersionZeroIsCorruptionRatherThanACapabilityBoundary) {
    Built built;
    ASSERT_TRUE(build(ramp_points(), FieldType::OLAP_FIELD_TYPE_BIGINT, 16, &built).ok());
    std::vector<uint8_t> payload = payload_of(built.index_bytes);
    ASSERT_GT(payload.size(), kFormatVersionOffset);
    // No binary ever wrote version 0, so unlike a version from the future this is
    // damage -- and the two must not be answered with the same error, or a
    // corrupt segment would be silently downgraded as "unsupported".
    payload[kFormatVersionOffset] = 0;

    Opened opened;
    const Status status = open_over(reframe(payload), built.data_bytes, &opened);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_EQ(opened.reader, nullptr);
}

} // namespace
} // namespace doris::snii::bkd
