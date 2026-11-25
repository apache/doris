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

#include "storage/index/inverted/snii/format/norms_pod.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/snii/common/slice.h"
#include "storage/index/inverted/snii/encoding/byte_sink.h"
#include "storage/index/inverted/snii/encoding/section_framer.h"
#include "storage/index/inverted/snii/format/format_constants.h"
#include "storage/index/inverted/snii/format/null_bitmap.h"
#include "storage/index/inverted/snii/query/bm25_scorer.h"

using namespace doris::snii;
using doris::Status; // RETURN_IF_ERROR expands to bare Status
using doris::snii::format::kNullRowNorm;
using doris::snii::format::NormsPodReader;
using doris::snii::format::NormsPodWriter;
using doris::snii::format::NullBitmapReader;
using doris::snii::format::NullBitmapWriter;
using doris::snii::format::SparseNormsMode;

namespace {

// Use writer to encode a sequence of encoded_norms into a framed payload and return the buffer.
std::vector<uint8_t> BuildPod(const std::vector<uint8_t>& norms) {
    NormsPodWriter writer;
    for (uint8_t n : norms) {
        writer.add(n);
    }
    ByteSink sink;
    writer.finish(&sink);
    return sink.buffer();
}

} // namespace

// After writing N norms, read them back per-doc and verify they match.
TEST(SniiNormsPod, RoundTripValues) {
    std::vector<uint8_t> norms = {0, 1, 7, 42, 128, 200, 255};
    NormsPodWriter writer;
    for (uint8_t n : norms) {
        writer.add(n);
    }
    EXPECT_EQ(writer.count(), norms.size());

    ByteSink sink;
    writer.finish(&sink);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(sink.view(), &reader).ok());
    ASSERT_EQ(reader.doc_count(), norms.size());
    for (uint32_t docid = 0; docid < norms.size(); ++docid) {
        EXPECT_EQ(reader.encoded_norm(docid), norms[docid]) << "docid=" << docid;
    }
}

// Large-scale round-trip covering the multi-byte varint doc_count path.
TEST(SniiNormsPod, RoundTripLarge) {
    std::vector<uint8_t> norms;
    norms.reserve(5000);
    for (uint32_t i = 0; i < 5000; ++i) {
        norms.push_back(static_cast<uint8_t>((i * 31 + 7) & 0xFF));
    }
    auto buf = BuildPod(norms);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    ASSERT_EQ(reader.doc_count(), 5000U);
    for (uint32_t docid = 0; docid < 5000; ++docid) {
        EXPECT_EQ(reader.encoded_norm(docid), norms[docid]) << "docid=" << docid;
    }
}

// Empty POD: count = 0 is valid and open should succeed.
TEST(SniiNormsPod, EmptyPod) {
    NormsPodWriter writer;
    EXPECT_EQ(writer.count(), 0U);

    ByteSink sink;
    writer.finish(&sink);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.doc_count(), 0U);
}

// CRC corruption is detectable: flipping a byte in the payload causes open to fail.
// The integrated reader frames via SectionFramer, which reports a CRC mismatch as
// INVERTED_INDEX_FILE_CORRUPTED (the standalone test's generic Corruption code was
// replaced during integration with this inverted-index-specific code).
TEST(SniiNormsPod, DetectsCorruption) {
    std::vector<uint8_t> norms = {10, 20, 30, 40, 50};
    auto buf = BuildPod(norms);
    // Flip a byte near the end so the framer CRC no longer matches the payload.
    buf[buf.size() - 3] ^= 0xFF;

    NormsPodReader reader;
    Status s = NormsPodReader::open(Slice(buf), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// Truncated input should return an error rather than crash.
TEST(SniiNormsPod, DetectsTruncation) {
    std::vector<uint8_t> norms = {1, 2, 3, 4, 5, 6, 7, 8};
    auto buf = BuildPod(norms);
    buf.resize(buf.size() - 4); // Chop off the trailing CRC region.

    NormsPodReader reader;
    Status s = NormsPodReader::open(Slice(buf), &reader);
    EXPECT_FALSE(s.ok());
}

// A mismatch between the declared doc_count and the actual payload byte count should be detected.
// The integrated reader reports this as INVERTED_INDEX_FILE_CORRUPTED.
TEST(SniiNormsPod, DetectsLengthMismatch) {
    // Manually construct: framer payload = [varint doc_count=4][only 2 norm bytes].
    ByteSink payload;
    payload.put_varint64(4);
    payload.put_u8(11);
    payload.put_u8(22);

    ByteSink sink;
    // Reuse the framer to ensure a self-consistent CRC, specifically to trigger the length-mismatch branch.
    doris::snii::SectionFramer::write(
            sink, static_cast<uint8_t>(doris::snii::format::SectionType::kNormsPod),
            payload.view());

    NormsPodReader reader;
    Status s = NormsPodReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

TEST(SniiNormsPod, RejectsWrongSectionType) {
    ByteSink payload;
    payload.put_varint64(1);
    payload.put_u8(7);
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(format::SectionType::kSampledTermIndex),
                         payload.view());

    NormsPodReader reader;
    const Status status = NormsPodReader::open(sink.view(), &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
}

TEST(SniiNormsPod, RejectsStatsBlockFrame) {
    ByteSink payload;
    payload.put_varint64(4);
    ByteSink sink;
    SectionFramer::write(sink, /*obsolete stats frame type=*/1, payload.view());

    NormsPodReader reader;
    const Status status = NormsPodReader::open(sink.view(), &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
}

TEST(SniiNormsPod, RejectsNonCanonicalDocCountVarint) {
    ByteSink payload;
    for (size_t i = 0; i < 9; ++i) {
        payload.put_u8(0x80);
    }
    payload.put_u8(0x02);
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(format::SectionType::kNormsPod),
                         payload.view());

    NormsPodReader reader;
    const Status status = NormsPodReader::open(sink.view(), &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
}

TEST(SniiNormsPod, RejectsTrailingFramedBytes) {
    std::vector<uint8_t> bytes = BuildPod({7});
    bytes.push_back(0);

    NormsPodReader reader;
    const Status status = NormsPodReader::open(Slice(bytes), &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
}

#ifndef NDEBUG
// In a debug build, an out-of-range docid triggers an assertion (death test).
TEST(SniiNormsPodDeathTest, OutOfRangeDocidAsserts) {
    std::vector<uint8_t> norms = {3, 6, 9};
    auto buf = BuildPod(norms);
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ (void)reader.encoded_norm(3); }, "");
}
#endif

// Checked access: a valid docid returns the value, an out-of-range docid returns
// InvalidArgument (also effective in Release builds).
TEST(SniiNormsPod, TryEncodedNormChecksBounds) {
    std::vector<uint8_t> norms = {3, 6, 9};
    auto buf = BuildPod(norms);
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(buf), &reader).ok());
    uint8_t v = 0;
    ASSERT_TRUE(reader.try_encoded_norm(1, &v).ok());
    EXPECT_EQ(v, 6U);
    Status s = reader.try_encoded_norm(3, &v);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVALID_ARGUMENT>()) << s.to_string();
}

namespace {

NullBitmapReader OpenNulls(const std::vector<uint32_t>& nulls, uint32_t doc_count) {
    NullBitmapWriter writer;
    writer.add_many(nulls);
    ByteSink sink;
    EXPECT_TRUE(writer.finish(doc_count, &sink).ok());
    NullBitmapReader reader;
    EXPECT_TRUE(NullBitmapReader::open(sink.view(), &reader).ok());
    return reader;
}

// Encodes the rows of `norms` outside `nulls` and checks the size the writer reserves for it.
std::vector<uint8_t> BuildSparse(const std::vector<uint8_t>& norms,
                                 const std::vector<uint32_t>& nulls) {
    const bool constant = NormsPodWriter::sparse_values_constant(norms, nulls);
    ByteSink sink;
    NormsPodWriter::finish_sparse(norms, nulls, constant, &sink);
    EXPECT_EQ(sink.buffer().size(), NormsPodWriter::sparse_section_size(
                                            norms.size(), norms.size() - nulls.size(), constant));
    return sink.buffer();
}

Status ExpandSparse(const std::vector<uint8_t>& framed, const std::vector<uint32_t>& nulls,
                    uint32_t doc_count, std::vector<uint8_t>* dense, NormsPodReader* reader) {
    const auto bitmap = OpenNulls(nulls, doc_count);
    return NormsPodReader::open_sparse(Slice(framed), doc_count, &bitmap, dense, reader);
}

// What a reader of the dense section sees for the same rows: a NULL row reads kNullRowNorm.
std::vector<uint8_t> DenseView(std::vector<uint8_t> norms, const std::vector<uint32_t>& nulls) {
    for (const uint32_t docid : nulls) {
        norms[docid] = kNullRowNorm;
    }
    return norms;
}

std::vector<uint8_t> FrameSparse(uint8_t mode, uint64_t doc_count, uint64_t value_count,
                                 const std::vector<uint8_t>& bytes) {
    ByteSink payload;
    payload.put_u8(mode);
    payload.put_varint64(doc_count);
    payload.put_varint64(value_count);
    for (const uint8_t byte : bytes) {
        payload.put_u8(byte);
    }
    ByteSink sink;
    SectionFramer::write(sink, static_cast<uint8_t>(format::SectionType::kNormsSparse),
                         payload.view());
    return sink.buffer();
}

} // namespace

// The writer compares this size with the sparse one, so it must be the size finish() writes.
TEST(SniiNormsPod, DenseSectionSizeIsTheWrittenSize) {
    for (const size_t doc_count : {0, 7, 127, 128, 5000}) {
        EXPECT_EQ(NormsPodWriter::dense_section_size(doc_count),
                  BuildPod(std::vector<uint8_t>(doc_count, 3)).size())
                << "doc_count=" << doc_count;
    }
}

// The writer and the reader must agree on the norm of a NULL row, which the reader restores
// without storing it: the writer encodes a NULL row as a document of zero tokens.
TEST(SniiNormsPod, NullRowNormIsTheNormOfAnEmptyDocument) {
    EXPECT_EQ(query::encode_norm(0), kNullRowNorm);
}

// Rows 1, 2 and 5 are NULL: the section keeps the norms of rows 0, 3, 4 and 6 only, and the
// reader rebuilds one byte per row with kNullRowNorm on the NULL rows. The 9s written for the
// NULL rows are dropped, so the reader cannot return them.
TEST(SniiNormsPod, SparseKeepsOnlyNonNullRows) {
    const std::vector<uint8_t> norms = {3, 9, 9, 1, 2, 9, 4};
    const std::vector<uint32_t> nulls = {1, 2, 5};
    const auto framed = BuildSparse(norms, nulls);
    EXPECT_EQ(framed.size(), NormsPodWriter::sparse_section_size(7, 4, /*constant=*/false));

    std::vector<uint8_t> dense;
    NormsPodReader reader;
    ASSERT_TRUE(ExpandSparse(framed, nulls, 7, &dense, &reader).ok());
    EXPECT_EQ(dense, (std::vector<uint8_t> {3, kNullRowNorm, kNullRowNorm, 1, 2, kNullRowNorm, 4}));
    ASSERT_EQ(reader.doc_count(), 7U);
    for (uint32_t docid = 0; docid < 7; ++docid) {
        EXPECT_EQ(reader.encoded_norm(docid), dense[docid]) << "docid=" << docid;
    }
}

// Rows of one token each share one norm, so the section stores that byte once.
TEST(SniiNormsPod, SparseStoresASharedNormOnce) {
    const std::vector<uint8_t> norms = {5, 1, 5, 5, 1, 5};
    const std::vector<uint32_t> nulls = {1, 4};
    ASSERT_TRUE(NormsPodWriter::sparse_values_constant(norms, nulls));
    const auto framed = BuildSparse(norms, nulls);
    EXPECT_EQ(framed.size(), NormsPodWriter::sparse_section_size(6, 4, /*constant=*/true));

    std::vector<uint8_t> dense;
    NormsPodReader reader;
    ASSERT_TRUE(ExpandSparse(framed, nulls, 6, &dense, &reader).ok());
    EXPECT_EQ(dense, DenseView(norms, nulls));
}

TEST(SniiNormsPod, SparseWithEveryRowNullStoresNoValue) {
    const std::vector<uint8_t> norms = {1, 1, 1};
    const std::vector<uint32_t> nulls = {0, 1, 2};
    EXPECT_FALSE(NormsPodWriter::sparse_values_constant(norms, nulls));
    const auto framed = BuildSparse(norms, nulls);

    std::vector<uint8_t> dense;
    NormsPodReader reader;
    ASSERT_TRUE(ExpandSparse(framed, nulls, 3, &dense, &reader).ok());
    EXPECT_EQ(dense, std::vector<uint8_t>(3, kNullRowNorm));
}

// An index without NULL rows has no null bitmap, and its sparse section holds every row.
TEST(SniiNormsPod, SparseWithoutNullRowsHoldsEveryRow) {
    const std::vector<uint8_t> norms = {2, 7, 4};
    const auto framed = BuildSparse(norms, {});

    std::vector<uint8_t> dense;
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open_sparse(Slice(framed), 3, nullptr, &dense, &reader).ok());
    EXPECT_EQ(dense, norms);
}

TEST(SniiNormsPod, SparseRoundTripMatchesDenseOnManyRows) {
    constexpr uint32_t kDocs = 5000;
    std::vector<uint8_t> norms(kDocs);
    std::vector<uint32_t> nulls;
    for (uint32_t docid = 0; docid < kDocs; ++docid) {
        norms[docid] = static_cast<uint8_t>((docid * 31 + 7) & 0xFF);
        // Nine rows in ten are NULL, in runs of different lengths.
        if ((docid * 7919) % 10 != 0) {
            nulls.push_back(docid);
        }
    }
    const auto framed = BuildSparse(norms, nulls);
    EXPECT_LT(framed.size(), BuildPod(norms).size());

    std::vector<uint8_t> dense;
    NormsPodReader reader;
    ASSERT_TRUE(ExpandSparse(framed, nulls, kDocs, &dense, &reader).ok());
    EXPECT_EQ(dense, DenseView(norms, nulls));
}

// The section belongs to one null bitmap: read against another, its values would land on the
// wrong rows, so the counts must agree.
TEST(SniiNormsPod, SparseRejectsAnotherNullBitmap) {
    const auto framed = BuildSparse({3, 1, 1, 4}, {1, 2});
    std::vector<uint8_t> dense;
    NormsPodReader reader;
    const Status status = ExpandSparse(framed, {1}, 4, &dense, &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiNormsPod, SparseRejectsAnotherDocCount) {
    const auto framed = BuildSparse({3, 1, 4}, {1});
    std::vector<uint8_t> dense;
    NormsPodReader reader;
    const auto bitmap = OpenNulls({1}, 3);
    const Status status = NormsPodReader::open_sparse(Slice(framed), 4, &bitmap, &dense, &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiNormsPod, SparseRejectsMalformedSections) {
    const auto values = static_cast<uint8_t>(SparseNormsMode::kValues);
    const auto constant = static_cast<uint8_t>(SparseNormsMode::kConstant);
    const std::vector<std::vector<uint8_t>> malformed = {
            // Unknown mode.
            FrameSparse(2, 3, 2, {3, 4}),
            // Fewer values than non-NULL rows.
            FrameSparse(values, 3, 2, {3}),
            // More bytes than values.
            FrameSparse(values, 3, 2, {3, 4, 5}),
            // A shared norm needs at least one non-NULL row.
            FrameSparse(constant, 3, 0, {3}),
            // A shared norm is one byte.
            FrameSparse(constant, 3, 2, {3, 3}),
            // The dense frame type.
            BuildPod({3, 1, 4}),
    };
    for (const auto& framed : malformed) {
        std::vector<uint8_t> dense;
        NormsPodReader reader;
        const Status status = ExpandSparse(framed, {1}, 3, &dense, &reader);
        EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    }

    auto flipped = BuildSparse({3, 1, 4}, {1});
    ASSERT_GT(flipped.size(), 5U);
    flipped[flipped.size() - 5] ^= 0xFF;
    std::vector<uint8_t> dense;
    NormsPodReader reader;
    const Status status = ExpandSparse(flipped, {1}, 3, &dense, &reader);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}
