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

// P0-3 / P1-6, design 12.4: what damaged bytes do.
//
// THIS IS ONE OF THE TWO DEFECTS THE REWRITE EXISTS TO REMOVE (design 1b / 14
// #1). In the CLucene BKD, `ByteArrayDataInput::readBytes` is an unchecked
// std::copy and `readByte` throws std::out_of_range -- not a CLuceneError -- so
// it escapes every catch in `query` / `try_query` / `BKDIndexSearcherBuilder`.
// A damaged bkd_index there produced a heap overread or an uncaught exception,
// NOT a Status the caller could downgrade on. The specific shape that did it:
// an inflated LENGTH FIELD (an array or inner-node extent read straight off
// disk and then trusted as a size).
//
// The contract asserted here, for every damaged input:
//
//   1. a Status comes back -- never a crash, never a read past the buffer;
//   2. the Status is INVERTED_INDEX_FILE_CORRUPTED (damage) or
//      INVERTED_INDEX_NOT_SUPPORTED (a capability boundary), because those are
//      the two the caller knows how to downgrade on (design 8);
//   3. a failed open leaves the caller's unique_ptr untouched.
//
// Disk bytes are NOT invariants: none of these paths may DORIS_CHECK, or a
// recoverable index downgrade becomes a node crash.
//
// "Never reads past the buffer" is only observable under a sanitizer. run-be-ut.sh
// builds with CMAKE_BUILD_TYPE=ASAN_UT by default (-fsanitize=address), which is
// what makes the sweeps below more than smoke tests; a plain release UT build
// still checks 1-3 but would not notice an overread that happens to stay inside
// the heap.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/bkd/bkd_builder.h"
#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/olap_common.h"

namespace doris::snii::bkd {
namespace {

constexpr uint32_t kBytesPerDim = sizeof(int64_t);
constexpr FieldType kFieldType = FieldType::OLAP_FIELD_TYPE_BIGINT;

// Unsigned big-endian sortable bytes for a BIGINT -- what
// KeyCoder::full_encode_ascending emits (INV-1). Points and query bounds both go
// through it.
std::vector<uint8_t> sortable_bigint(int64_t value) {
    const uint64_t biased = static_cast<uint64_t>(value) ^ (uint64_t {1} << 63);
    std::vector<uint8_t> out(kBytesPerDim);
    for (uint32_t i = 0; i < kBytesPerDim; ++i) {
        out[kBytesPerDim - 1 - i] = static_cast<uint8_t>(biased >> (8 * i));
    }
    return out;
}

struct Point {
    int64_t value = 0;
    uint32_t doc_id = 0;
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
        // A read the reader should never have issued: every extent it uses was
        // bounded at open. If one escapes, the test must see it rather than the
        // memcpy running off the vector.
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
// A container image, and where each sub-file sits inside it
// ---------------------------------------------------------------------------

constexpr size_t kLeadingPad = 13;
constexpr size_t kMiddlePad = 5;
constexpr size_t kTrailingPad = 3;

struct Image {
    std::vector<uint8_t> bytes;
    BkdSections sections;
    size_t data_begin = 0;
    size_t data_size = 0;
    size_t index_begin = 0;
    size_t index_size = 0;

    std::vector<uint8_t> index_bytes() const {
        return std::vector<uint8_t>(bytes.begin() + static_cast<long>(index_begin),
                                    bytes.begin() + static_cast<long>(index_begin + index_size));
    }
};

// Assembles an image around already-built sub-files, so a test can replace
// either one wholesale (a re-framed index, a truncated data file) and keep the
// container shape.
Image assemble(const std::vector<uint8_t>& index_bytes, const std::vector<uint8_t>& data_bytes) {
    Image image;
    image.bytes.assign(kLeadingPad, 0xA5);
    image.data_begin = image.bytes.size();
    image.data_size = data_bytes.size();
    image.bytes.insert(image.bytes.end(), data_bytes.begin(), data_bytes.end());
    image.bytes.insert(image.bytes.end(), kMiddlePad, 0x5A);
    image.index_begin = image.bytes.size();
    image.index_size = index_bytes.size();
    image.bytes.insert(image.bytes.end(), index_bytes.begin(), index_bytes.end());
    image.bytes.insert(image.bytes.end(), kTrailingPad, 0xC3);

    image.sections.data_offset = image.data_begin;
    image.sections.data_length = image.data_size;
    image.sections.index_offset = image.index_begin;
    image.sections.index_length = image.index_size;
    return image;
}

Status build_image(const std::vector<Point>& points, uint32_t points_per_leaf, Image* out) {
    BkdBuilderOptions options;
    options.bytes_per_dim = kBytesPerDim;
    options.field_type = kFieldType;
    options.points_per_leaf = points_per_leaf;

    std::unique_ptr<BkdBuilder> builder;
    RETURN_IF_ERROR(BkdBuilder::create(options, &builder));
    for (const Point& point : points) {
        RETURN_IF_ERROR(builder->add(point.doc_id, Slice(sortable_bigint(point.value))));
    }
    MemoryFileWriter data;
    ByteSink index;
    BkdStats stats;
    RETURN_IF_ERROR(builder->finish(&data, &index, &stats));
    *out = assemble(index.take(), data.bytes());
    return Status::OK();
}

// The point set every sweep runs over: 200 distinct values in 13 leaves, so the
// index has a real split array, real whole-leaf hits and real boundary leaves --
// a single-leaf index would leave most of the decode paths untouched.
std::vector<Point> sweep_points() {
    std::vector<Point> points;
    for (uint32_t i = 0; i < 200; ++i) {
        points.push_back(Point {static_cast<int64_t>(i), i});
    }
    return points;
}

// ---------------------------------------------------------------------------
// Probing a damaged image
// ---------------------------------------------------------------------------

// Queries chosen to exercise every leaf path: the unbounded one walks all 13
// leaves (one boundary leaf, eleven whole-leaf hits, one boundary leaf), the
// narrow ones stay inside a single leaf, and the two single-sided ones test only
// one bound.
const std::vector<std::pair<int64_t, int64_t>>& query_battery() {
    static const std::vector<std::pair<int64_t, int64_t>> battery {{0, 199},   {10, 25},  {33, 33},
                                                                   {150, 400}, {-100, 5}, {96, 97}};
    return battery;
}

struct Outcome {
    Status open_status;
    // The first non-OK query status, or OK if every query in the battery
    // succeeded (or the open failed, in which case no query ran).
    Status query_status;
    bool opened = false;
};

Outcome probe(const Image& image) {
    Outcome outcome;
    MemoryFileReader file(image.bytes);
    std::unique_ptr<BkdReader> reader;
    outcome.open_status = BkdReader::open(&file, image.sections, &reader);
    if (!outcome.open_status.ok()) {
        // Contract 3: nothing is published when open fails.
        EXPECT_EQ(reader, nullptr);
        return outcome;
    }
    EXPECT_NE(reader, nullptr);
    outcome.opened = true;

    BkdQueryScratch scratch;
    roaring::Roaring hits;
    // NOTE: the verdict below is on Status only, and that is a real limit of this
    // suite rather than an oversight. The outcome worth catching is a damaged
    // index returning a plausible bitmap whose doc ids address rows the segment
    // does not have -- the caller indexes a column with them. It cannot be
    // checked here: the header's doc_count is a count of DISTINCT documents, not
    // a bound on doc-id VALUES, and the two differ for any column with gaps (null
    // rows, empty arrays). Asserting `max < doc_count` rejects legitimate sparse
    // indexes -- a single point at doc id 5 has doc_count 1. Closing this needs
    // the format to record a doc-id bound; nothing in bkd_index does today.
    for (const auto& [low, high] : query_battery()) {
        const std::vector<uint8_t> lower = sortable_bigint(low);
        const std::vector<uint8_t> upper = sortable_bigint(high);
        const Status status =
                reader->range(Slice(lower), true, Slice(upper), true, &hits, &scratch);
        if (!status.ok()) {
            outcome.query_status = status;
            break;
        }
    }
    if (outcome.query_status.ok()) {
        // Both sides unbounded: every leaf, including the last one, whose tail
        // carries the docid_block_offset.
        const Status status = reader->range(Slice(), true, Slice(), true, &hits, &scratch);
        if (!status.ok()) {
            outcome.query_status = status;
        }
    }
    return outcome;
}

// The only two verdicts a damaged file may produce. Anything else -- a generic
// CORRUPTION from the file reader, an INTERNAL_ERROR, a MEM_LIMIT_EXCEEDED from
// an allocation sized by a damaged length -- means the caller cannot tell
// "downgrade this index" from "something else went wrong".
::testing::AssertionResult is_downgradeable(const Status& status, std::string_view what) {
    if (status.ok() || status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>() ||
        status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << what << " returned " << status;
}

::testing::AssertionResult survives(const Image& image) {
    const Outcome outcome = probe(image);
    const ::testing::AssertionResult open_ok = is_downgradeable(outcome.open_status, "open()");
    if (!open_ok) {
        return open_ok;
    }
    return is_downgradeable(outcome.query_status, "range()");
}

// Damage that must be REJECTED, not merely survived.
::testing::AssertionResult is_rejected(const Status& status, std::string_view what) {
    if (status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) {
        return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure()
           << what << " should have been reported as corruption, got " << status;
}

// ---------------------------------------------------------------------------
// Section surgery
// ---------------------------------------------------------------------------

// Unwraps the framed bkd_index into its payload. Everything here decodes through
// ByteSource / SectionFramer -- a test that hand-parsed the frame with raw
// pointers would only be asserting against its own idea of the format.
std::vector<uint8_t> payload_of(const std::vector<uint8_t>& framed) {
    ByteSource src {Slice(framed)};
    FramedSection section;
    EXPECT_TRUE(SectionFramer::read(src, &section).ok());
    return std::vector<uint8_t>(section.payload.data(),
                                section.payload.data() + section.payload.size());
}

// Re-wraps a payload with a CORRECT checksum, so a mutation inside it reaches
// the header parser instead of being stopped by the frame. Without this every
// single-byte experiment would only ever be testing crc32c.
std::vector<uint8_t> reframe(const std::vector<uint8_t>& payload) {
    ByteSink sink;
    SectionFramer::write(sink, kBkdIndexSectionType, Slice(payload));
    return sink.take();
}

// Frames a payload while DECLARING a different length -- the inflated length
// field of design 12.4's regression case. The checksum is computed over exactly
// the bytes the framer would checksum, so the declared length is what has to be
// caught, not the crc.
std::vector<uint8_t> frame_with_declared_length(const std::vector<uint8_t>& payload,
                                                uint64_t declared_length) {
    ByteSink sink;
    sink.put_u8(kBkdIndexSectionType);
    sink.put_varint64(declared_length);
    sink.put_bytes(Slice(payload));
    const uint32_t crc = crc32c(sink.view());
    sink.put_fixed32(crc);
    return sink.take();
}

// The decoded bkd_index header plus everything after it. Rebuilding through this
// is how a header field is changed to a value of a DIFFERENT varint length --
// patching bytes in place could only ever produce same-length values.
struct IndexPayload {
    uint32_t format_version = 0;
    uint32_t flags = 0;
    uint32_t bytes_per_dim = 0;
    uint32_t field_type = 0;
    uint64_t point_count = 0;
    uint32_t doc_count = 0;
    uint32_t leaf_count = 0;
    uint32_t points_per_leaf = 0;
    // Bounds, split values and the leaf directory, untouched.
    std::vector<uint8_t> tail;
};

bool parse_payload(const std::vector<uint8_t>& bytes, IndexPayload* out) {
    ByteSource src {Slice(bytes)};
    uint32_t magic = 0;
    if (!src.get_fixed32(&magic).ok() || magic != kBkdIndexMagic) {
        return false;
    }
    const bool decoded =
            src.get_varint32(&out->format_version).ok() && src.get_varint32(&out->flags).ok() &&
            src.get_varint32(&out->bytes_per_dim).ok() && src.get_varint32(&out->field_type).ok() &&
            src.get_varint64(&out->point_count).ok() && src.get_varint32(&out->doc_count).ok() &&
            src.get_varint32(&out->leaf_count).ok() && src.get_varint32(&out->points_per_leaf).ok();
    if (!decoded) {
        return false;
    }
    Slice rest;
    if (!src.get_bytes(src.remaining(), &rest).ok()) {
        return false;
    }
    out->tail.assign(rest.data(), rest.data() + rest.size());
    return true;
}

std::vector<uint8_t> encode_payload(const IndexPayload& payload) {
    ByteSink sink;
    sink.put_fixed32(kBkdIndexMagic);
    sink.put_varint32(payload.format_version);
    sink.put_varint32(payload.flags);
    sink.put_varint32(payload.bytes_per_dim);
    sink.put_varint32(payload.field_type);
    sink.put_varint64(payload.point_count);
    sink.put_varint32(payload.doc_count);
    sink.put_varint32(payload.leaf_count);
    sink.put_varint32(payload.points_per_leaf);
    sink.put_bytes(Slice(payload.tail));
    return sink.take();
}

// A deterministic 64-bit LCG; the seeds are literals so any failure reproduces.
class Rng {
public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ = state_ * 6364136223846793005ULL + 1442695040888963407ULL;
        return state_ >> 11;
    }
    size_t next_below(size_t bound) { return static_cast<size_t>(next() % bound); }

private:
    uint64_t state_;
};

// ===========================================================================
// Single-byte flips
// ===========================================================================

TEST(BkdCorruptionTest, EveryByteFlipInBkdIndexIsCaughtByTheFramerChecksum) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    ASSERT_GT(original.index_size, 0U);

    // bkd_index is one framed section, and the crc covers type + length +
    // payload, so NO single-byte change to it can survive -- not in the header,
    // not in the split array, not in the stored checksum itself. This is what
    // folding the old zero-checked bkd_meta into bkd_index bought (design 14 #4).
    for (size_t offset = 0; offset < original.index_size; ++offset) {
        for (const uint8_t mask : {uint8_t {0x01}, uint8_t {0x40}, uint8_t {0xFF}}) {
            Image damaged = original;
            damaged.bytes[damaged.index_begin + offset] ^= mask;
            const Outcome outcome = probe(damaged);
            EXPECT_TRUE(is_rejected(outcome.open_status,
                                    "flip at index byte " + std::to_string(offset)));
        }
    }
}

TEST(BkdCorruptionTest, EveryByteFlipInBkdDataIsAStatusNeverACrash) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    ASSERT_GT(original.data_size, 0U);

    // Leaf blocks are read lazily and are NOT covered by the open-time
    // validation (design 8.3), so unlike bkd_index there is no checksum standing
    // in front of them: every field a leaf decoder reads is a field it has to
    // range-check itself. A flip may therefore be caught (a length that no
    // longer fits), or absorbed (a value byte still decodes, the answer is just
    // wrong) -- what it may never do is read past the block.
    for (size_t offset = 0; offset < original.data_size; ++offset) {
        for (const uint8_t mask : {uint8_t {0x01}, uint8_t {0x40}, uint8_t {0xFF}}) {
            Image damaged = original;
            damaged.bytes[damaged.data_begin + offset] ^= mask;
            EXPECT_TRUE(survives(damaged)) << "flip at data byte " << offset << " mask 0x"
                                           << std::hex << static_cast<int>(mask);
        }
    }
}

TEST(BkdCorruptionTest, EveryReframedPayloadFlipIsAStatusNeverACrash) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    const std::vector<uint8_t> payload = payload_of(original.index_bytes());
    ASSERT_GT(payload.size(), 0U);

    // With the section re-framed the checksum agrees again, so the mutation is
    // judged by the HEADER and DIRECTORY parsers -- the code path a real
    // half-written or version-skewed file exercises, and the one the old
    // implementation had no bounds checks on at all.
    for (size_t offset = 0; offset < payload.size(); ++offset) {
        for (const uint8_t mask : {uint8_t {0x01}, uint8_t {0x40}, uint8_t {0xFF}}) {
            std::vector<uint8_t> damaged_payload = payload;
            damaged_payload[offset] ^= mask;
            const Image damaged = assemble(reframe(damaged_payload), data_bytes);
            EXPECT_TRUE(survives(damaged)) << "flip at payload byte " << offset << " mask 0x"
                                           << std::hex << static_cast<int>(mask);
        }
    }
}

// ===========================================================================
// Truncation
// ===========================================================================

TEST(BkdCorruptionTest, EveryTruncationOfBkdIndexIsRejected) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());

    // A short hot section can only ever be damage: there is no shape of
    // bkd_index that is a valid prefix of a longer one (the frame declares its
    // own length and the payload is fully consumed, design 5.1).
    for (size_t length = 0; length < original.index_size; ++length) {
        Image damaged = original;
        damaged.sections.index_length = length;
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(
                is_rejected(outcome.open_status, "index truncated to " + std::to_string(length)));
    }
}

TEST(BkdCorruptionTest, EveryTruncationOfBkdDataIsAStatusNeverACrash) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());

    // A short cold sub-file may still open -- open only bounds the LAST leaf
    // offset against the declared length -- so the damage surfaces when the
    // truncated leaf is read. Rejection is not demanded at every length: a leaf
    // decoder that finds a self-consistent block inside the surviving bytes is
    // entitled to return a wrong ANSWER. What it may never do is crash or read
    // past the block, which is the contract survives() states.
    for (size_t length = 0; length < original.data_size; ++length) {
        Image damaged = original;
        damaged.sections.data_length = length;
        EXPECT_TRUE(survives(damaged)) << "data truncated to " << length;
    }

    // The two lengths where the damage is unambiguous: one byte short of the
    // last leaf's trailing offset_length, and half a file.
    for (const size_t length : {original.data_size - 1, original.data_size / 2}) {
        Image damaged = original;
        damaged.sections.data_length = length;
        const Outcome outcome = probe(damaged);
        ASSERT_TRUE(outcome.open_status.ok() ||
                    outcome.open_status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
                << outcome.open_status;
        if (outcome.opened) {
            EXPECT_TRUE(is_rejected(outcome.query_status,
                                    "range() with data truncated to " + std::to_string(length)));
        }
    }
}

// ===========================================================================
// Inflated length fields -- the explicit regression (design 12.4)
// ===========================================================================

TEST(BkdCorruptionTest, InflatedSectionLengthIsCaughtByTheBoundsCheckNotTheChecksum) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    const std::vector<uint8_t> payload = payload_of(original.index_bytes());

    // THE old defect, reproduced: a length field read off disk and then trusted
    // as a size. The checksum here is CORRECT for the bytes present, so nothing
    // but a bounds check inside ByteSource can stop it -- and in the old
    // implementation nothing did: ByteArrayDataInput::readBytes was a plain
    // std::copy of the declared length, i.e. a heap overread.
    for (const uint64_t declared : {payload.size() + 1, payload.size() + 4096, uint64_t {1} << 20,
                                    uint64_t {1} << 40, uint64_t {0xFFFFFFFFFFFFFFFFULL}}) {
        SCOPED_TRACE("declared payload length " + std::to_string(declared));
        const Image damaged = assemble(frame_with_declared_length(payload, declared), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "inflated section length"));
    }

    // A shrunken one is damage too: the payload then has bytes nobody claims.
    for (const uint64_t declared : {uint64_t {0}, uint64_t {1}, payload.size() - 1}) {
        SCOPED_TRACE("declared payload length " + std::to_string(declared));
        const Image damaged = assemble(frame_with_declared_length(payload, declared), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "shrunken section length"));
    }
}

TEST(BkdCorruptionTest, InflatedLeafCountIsRejectedBeforeAnythingIsSizedByIt) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    IndexPayload payload;
    ASSERT_TRUE(parse_payload(payload_of(original.index_bytes()), &payload));
    ASSERT_EQ(payload.leaf_count, 13U);

    // leaf_count drives THREE array lengths (split values, leaf offsets, leaf
    // counts) and one allocation. An inflated one is the same class of bug as
    // the inflated section length, one level in: it must be rejected against the
    // bytes actually present, and rejected BEFORE it can size a reservation --
    // 0x3FFFFFFF leaves would otherwise be a multi-gigabyte allocation on the
    // way to failing.
    for (const uint32_t leaf_count : {14U, 100U, 0x3FFFFFFFU, 0xFFFFFFFFU}) {
        SCOPED_TRACE("leaf_count " + std::to_string(leaf_count));
        IndexPayload damaged_payload = payload;
        damaged_payload.leaf_count = leaf_count;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "inflated leaf_count"));
    }
    // Fewer leaves than the directory describes leaves bytes nobody claims.
    for (const uint32_t leaf_count : {1U, 12U}) {
        SCOPED_TRACE("leaf_count " + std::to_string(leaf_count));
        IndexPayload damaged_payload = payload;
        damaged_payload.leaf_count = leaf_count;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "shrunken leaf_count"));
    }
}

TEST(BkdCorruptionTest, HeaderCountsThatContradictTheDirectoryAreRejected) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    IndexPayload payload;
    ASSERT_TRUE(parse_payload(payload_of(original.index_bytes()), &payload));

    // point_count is what bounds every leaf decode allocation (design 5.2), so a
    // value the leaf counts do not add up to must not survive open.
    for (const uint64_t point_count :
         {uint64_t {0}, uint64_t {199}, uint64_t {201}, uint64_t {1} << 40}) {
        SCOPED_TRACE("point_count " + std::to_string(point_count));
        IndexPayload damaged_payload = payload;
        damaged_payload.point_count = point_count;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "point_count against the directory"));
    }

    // bytes_per_dim is the stride of the split array and of every comparison
    // (INV-2); it may only be what the recorded field type implies.
    for (const uint32_t bytes_per_dim : {0U, 4U, 16U, 0xFFFFFFFFU}) {
        SCOPED_TRACE("bytes_per_dim " + std::to_string(bytes_per_dim));
        IndexPayload damaged_payload = payload;
        damaged_payload.bytes_per_dim = bytes_per_dim;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "bytes_per_dim against field_type"));
    }

    // An unknown field_type must never be cast into the enum and handed to
    // field_type_size(), which LOG(FATAL)s outside its own switch -- that would
    // turn a downgrade into a node crash.
    for (const uint32_t field_type : {0U, 1U, 200U, 0xFFFFFFFFU}) {
        SCOPED_TRACE("field_type " + std::to_string(field_type));
        IndexPayload damaged_payload = payload;
        damaged_payload.field_type = field_type;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "unknown field_type"));
    }
}

TEST(BkdCorruptionTest, InflatedLeafDirectoryCountIsRejected) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    std::vector<uint8_t> payload = payload_of(original.index_bytes());

    // The very last payload byte is the LAST leaf's count varint (design 5.1's
    // directory is offsets then counts). Inflating it breaks the
    // sum(counts) == point_count identity the leaf decode bound rests on.
    ASSERT_EQ(payload.back(), 8U);
    payload.back() = 0x7F;
    const Image damaged = assemble(reframe(payload), data_bytes);
    const Outcome outcome = probe(damaged);
    EXPECT_TRUE(is_rejected(outcome.open_status, "inflated leaf count"));
}

// The directory can be INTERNALLY CONSISTENT and still be a bomb. Inflating the
// leaf's count alone breaks sum(counts) == point_count (the test above), but
// inflating point_count by the same amount restores that identity, so every
// open-time check passes. What then bounds the leaf decode allocation is a
// number that came straight off disk: leaf_codec sizes the doc id vector by it,
// while the leaf block itself can stay ~25 bytes (kAllEqual whose PFOR block is
// zero-width). bkd_types.h documents "count <= points_per_leaf" as an
// invariant -- open has to actually enforce it, or a bad_alloc escapes a module
// that has no catch anywhere, which is precisely the "recoverable degradation
// becomes a node crash" that design 8 exists to prevent.
TEST(BkdCorruptionTest, SelfConsistentButAbsurdLeafCountsAreRejectedAtOpen) {
    // One leaf, so the tail is exactly: min | max | (no splits) | one offset
    // delta varint64 | one count varint32.
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 256, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    IndexPayload payload;
    ASSERT_TRUE(parse_payload(payload_of(original.index_bytes()), &payload));
    ASSERT_EQ(payload.leaf_count, 1U);
    ASSERT_EQ(payload.point_count, 200U);

    // Rebuild the tail keeping the bounds and the offset, replacing only the count.
    ByteSource tail {Slice(payload.tail)};
    Slice bounds;
    ASSERT_TRUE(tail.get_bytes(2 * kBytesPerDim, &bounds).ok());
    uint64_t offset_delta = 0;
    ASSERT_TRUE(tail.get_varint64(&offset_delta).ok());

    for (const uint32_t absurd_count : {1U << 28, 0xFFFFFFFFU}) {
        SCOPED_TRACE("count " + std::to_string(absurd_count));
        IndexPayload damaged_payload = payload;
        damaged_payload.point_count = absurd_count; // keeps sum(counts) == point_count
        ByteSink rebuilt;
        rebuilt.put_bytes(bounds);
        rebuilt.put_varint64(offset_delta);
        rebuilt.put_varint32(absurd_count);
        damaged_payload.tail = rebuilt.take();

        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        // Assert on open only: the point is that the bad number must never reach
        // the sizing site, so no query is run and nothing large is allocated.
        MemoryFileReader file(damaged.bytes);
        std::unique_ptr<BkdReader> reader;
        const Status status = BkdReader::open(&file, damaged.sections, &reader);
        EXPECT_TRUE(is_rejected(status, "leaf count above points_per_leaf"));
        EXPECT_EQ(reader, nullptr);
    }
}

// points_per_leaf is the only thing that can bound a leaf's count, so it must
// itself be bounded -- and it is read off disk with no validation at all.
TEST(BkdCorruptionTest, AbsurdPointsPerLeafIsRejectedAtOpen) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const std::vector<uint8_t> data_bytes(
            original.bytes.begin() + static_cast<long>(original.data_begin),
            original.bytes.begin() + static_cast<long>(original.data_begin + original.data_size));
    IndexPayload payload;
    ASSERT_TRUE(parse_payload(payload_of(original.index_bytes()), &payload));

    for (const uint32_t points_per_leaf : {0U, 1U << 24, 0xFFFFFFFFU}) {
        SCOPED_TRACE("points_per_leaf " + std::to_string(points_per_leaf));
        IndexPayload damaged_payload = payload;
        damaged_payload.points_per_leaf = points_per_leaf;
        const Image damaged = assemble(reframe(encode_payload(damaged_payload)), data_bytes);
        MemoryFileReader file(damaged.bytes);
        std::unique_ptr<BkdReader> reader;
        const Status status = BkdReader::open(&file, damaged.sections, &reader);
        EXPECT_TRUE(is_rejected(status, "points_per_leaf out of range"));
        EXPECT_EQ(reader, nullptr);
    }
}

TEST(BkdCorruptionTest, InflatedLeafBlockLengthFieldsAreRejectedAtQueryTime) {
    // A leaf block starts with { point_count varint32, value_mode u8,
    // common_prefix_len varint32, ... } and ends with { docid_block_offset
    // varint32, offset_length u8 } (design 5.2). Each of those is a length or a
    // tag read straight off disk -- the same class of field that made the old
    // decoder overread -- so each one gets its own inflation here.
    struct Case {
        const char* name;
        // Offset from the START of bkd_data, or from its END when `from_end`.
        size_t offset;
        bool from_end;
        uint8_t value;
    };
    constexpr Case kCases[] = {
            // Leaf 0's point_count, which the leaf directory contradicts.
            {"point_count", 0, false, 0x7F},
            // A value_mode outside the closed three-value enum.
            {"value_mode", 1, false, 0x09},
            // A common prefix longer than the value itself.
            {"common_prefix_len", 2, false, 0x7F},
            // The last leaf's trailing offset_length byte: a varint length that
            // does not fit the block, i.e. the byte that makes the tail
            // reachable from the end at all.
            {"offset_length", 1, true, 0x7F},
            {"offset_length_zero", 1, true, 0x00},
    };

    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    for (const Case& test_case : kCases) {
        SCOPED_TRACE(test_case.name);
        Image damaged = original;
        const size_t position = test_case.from_end
                                        ? damaged.data_begin + damaged.data_size - test_case.offset
                                        : damaged.data_begin + test_case.offset;
        damaged.bytes[position] = test_case.value;
        const Outcome outcome = probe(damaged);
        ASSERT_TRUE(outcome.opened) << "bkd_index was not touched, open must still succeed";
        EXPECT_TRUE(is_rejected(outcome.query_status, test_case.name));
    }
}

// ===========================================================================
// The section table itself is disk data
// ===========================================================================

TEST(BkdCorruptionTest, SectionExtentsAreValidatedNotTrusted) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    const uint64_t file_size = original.bytes.size();

    // An extent that does not fit the file must be rejected BEFORE its length
    // reaches a read -- otherwise a damaged named-file table is a
    // multi-gigabyte allocation, not a downgrade.
    const BkdSections beyond_index {original.sections.index_offset, file_size + 1,
                                    original.sections.data_offset, original.sections.data_length};
    const BkdSections huge_index {original.sections.index_offset, uint64_t {1} << 60,
                                  original.sections.data_offset, original.sections.data_length};
    const BkdSections beyond_data {original.sections.index_offset, original.sections.index_length,
                                   file_size - 2, 16};
    for (const BkdSections& sections : {beyond_index, huge_index, beyond_data}) {
        Image damaged = original;
        damaged.sections = sections;
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "extent past the end of the file"));
    }

    // Extents that fit the file but describe the wrong bytes: a shifted offset,
    // a hot section that swallows the container padding, a cold section that
    // stops short or runs long.
    for (const int64_t shift : {int64_t {-1}, int64_t {1}, int64_t {2}}) {
        Image damaged = original;
        damaged.sections.index_offset =
                static_cast<uint64_t>(static_cast<int64_t>(original.index_begin) + shift);
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "shifted index offset"));
    }
    for (const int64_t delta : {int64_t {-1}, int64_t {1}, int64_t {3}}) {
        Image damaged = original;
        damaged.sections.data_length =
                static_cast<uint64_t>(static_cast<int64_t>(original.data_size) + delta);
        EXPECT_TRUE(survives(damaged)) << "data_length delta " << delta;
    }
    {
        Image damaged = original;
        damaged.sections.data_offset = original.data_begin + 1;
        EXPECT_TRUE(survives(damaged));
    }
    {
        // A zero-length cold sub-file under a non-empty directory: legal shape
        // for the EMPTY index only (design 5.3), damage here.
        Image damaged = original;
        damaged.sections.data_length = 0;
        const Outcome outcome = probe(damaged);
        EXPECT_TRUE(is_rejected(outcome.open_status, "zero-length bkd_data under 13 leaves"));
    }
}

// ===========================================================================
// Random mutation sweep
// ===========================================================================

TEST(BkdCorruptionTest, RandomMutationsNeverEscapeTheStatusContract) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());

    // Shapes a real damaged file takes that a single-byte flip does not:
    // a shredded run, a zeroed region, a file that simply stops, and a section
    // table pointing somewhere plausible but wrong.
    for (const uint64_t seed :
         {0x9E3779B97F4A7C15ULL, 0xC2B2AE3D27D4EB4FULL, 0x165667B19E3779F9ULL}) {
        Rng rng(seed);
        for (int iteration = 0; iteration < 300; ++iteration) {
            SCOPED_TRACE("seed=" + std::to_string(seed) +
                         " iteration=" + std::to_string(iteration));
            Image damaged = original;
            switch (rng.next_below(5)) {
            case 0: { // shred a run of bytes anywhere in the two sub-files
                const size_t begin =
                        damaged.data_begin + rng.next_below(damaged.data_size + damaged.index_size);
                const size_t length = 1 + rng.next_below(24);
                for (size_t i = 0; i < length && begin + i < damaged.bytes.size(); ++i) {
                    damaged.bytes[begin + i] = static_cast<uint8_t>(rng.next_below(256));
                }
                break;
            }
            case 1: { // zero a region
                const size_t begin =
                        damaged.data_begin + rng.next_below(damaged.data_size + damaged.index_size);
                const size_t length = 1 + rng.next_below(40);
                for (size_t i = 0; i < length && begin + i < damaged.bytes.size(); ++i) {
                    damaged.bytes[begin + i] = 0;
                }
                break;
            }
            case 2: { // the file simply stops part way through
                const size_t kept = rng.next_below(damaged.bytes.size());
                damaged.bytes.resize(kept);
                break;
            }
            case 3: { // a plausible but wrong section table
                damaged.sections.index_offset = rng.next_below(damaged.bytes.size());
                damaged.sections.index_length = rng.next_below(damaged.bytes.size());
                break;
            }
            default: { // a plausible but wrong cold extent
                damaged.sections.data_offset = rng.next_below(damaged.bytes.size());
                damaged.sections.data_length = rng.next_below(damaged.bytes.size());
                break;
            }
            }
            EXPECT_TRUE(survives(damaged));
        }
    }
}

// ===========================================================================
// The undamaged control
// ===========================================================================

TEST(BkdCorruptionTest, TheUndamagedImageAnswersEveryQueryInTheBattery) {
    Image original;
    ASSERT_TRUE(build_image(sweep_points(), 16, &original).ok());
    // Without this the sweeps above could all be passing because the harness
    // never produces a readable index in the first place.
    const Outcome outcome = probe(original);
    EXPECT_TRUE(outcome.open_status.ok()) << outcome.open_status;
    EXPECT_TRUE(outcome.query_status.ok()) << outcome.query_status;
    EXPECT_TRUE(outcome.opened);
}

} // namespace
} // namespace doris::snii::bkd
