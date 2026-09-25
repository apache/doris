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

#include "storage/index/snii/format/norms_pod.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/query/bm25_scorer.h"

using namespace doris::snii;
using doris::Status; // RETURN_IF_ERROR expands to bare Status
using doris::snii::format::NormsLayout;
using doris::snii::format::NormsPodReader;
using doris::snii::format::NormsPodWriter;
using doris::snii::format::NormsSectionInput;
using doris::snii::format::NormsSectionPlan;

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

// ---------------------------------------------------------------------------
// Adaptive norms section: dense (kNormsPod) and sparse (kNormsSparse) layouts.
// ---------------------------------------------------------------------------
namespace {

// One logical index's norms as every writer before the sparse layout stored them: a byte per
// document, kEmptyDocumentNorm for documents without a norm.
struct NormsCase {
    uint32_t doc_count = 0;
    std::vector<uint32_t> nulls;
    std::vector<uint32_t> nulls_with_norms;
    std::vector<uint8_t> dense;
    std::vector<uint8_t> norms; // present-only view handed to the writer

    bool has_norm(uint32_t docid) const {
        return !std::binary_search(nulls.begin(), nulls.end(), docid) ||
               std::binary_search(nulls_with_norms.begin(), nulls_with_norms.end(), docid);
    }

    // Fills dense bytes for the normless documents and derives the present-only norms.
    void finalize() {
        norms.clear();
        for (uint32_t docid = 0; docid < doc_count; ++docid) {
            if (has_norm(docid)) {
                norms.push_back(dense[docid]);
            } else {
                dense[docid] = format::kEmptyDocumentNorm;
            }
        }
    }

    NormsSectionInput input() const {
        return {.doc_count = doc_count,
                .null_docids = nulls,
                .null_docids_with_norms = nulls_with_norms,
                .norms = norms};
    }
};

std::vector<uint8_t> write_section(const NormsCase& c, bool force_dense, NormsSectionPlan* plan) {
    const Status status = format::plan_norms_section(c.input(), force_dense, plan);
    EXPECT_TRUE(status.ok()) << status.to_string();
    ByteSink sink;
    format::write_norms_section(c.input(), *plan, &sink);
    EXPECT_EQ(sink.size(), plan->framed_bytes);
    return sink.buffer();
}

// The dense section exactly as the pre-sparse writers framed it, assembled by hand:
// [u8 14][varint64 payload_len][varint64 doc_count][doc_count bytes][fixed32 crc32c].
std::vector<uint8_t> legacy_dense_section(const std::vector<uint8_t>& dense) {
    std::vector<uint8_t> out;
    out.push_back(14);
    uint8_t varint[10];
    const size_t payload_len = varint_len(dense.size()) + dense.size();
    out.insert(out.end(), varint, varint + encode_varint64(payload_len, varint));
    out.insert(out.end(), varint, varint + encode_varint64(dense.size(), varint));
    out.insert(out.end(), dense.begin(), dense.end());
    const uint32_t crc = doris::snii::crc32c(Slice(out));
    for (int shift = 0; shift < 32; shift += 8) {
        out.push_back(static_cast<uint8_t>(crc >> shift));
    }
    return out;
}

void expect_lookups_match(const NormsCase& c, const NormsPodReader& reader) {
    ASSERT_EQ(reader.doc_count(), c.doc_count);
    // Collect mismatches instead of asserting per docid: this runs over millions of lookups.
    size_t mismatches = 0;
    std::string first_mismatch;
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        uint8_t norm = 0;
        const Status status = reader.try_encoded_norm(docid, &norm);
        bool matches = false;
        if (c.has_norm(docid)) {
            matches = status.ok() && norm == c.dense[docid] &&
                      reader.encoded_norm(docid) == c.dense[docid];
        } else if (reader.is_sparse()) {
            matches = status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>();
        } else {
            matches = status.ok() && norm == format::kEmptyDocumentNorm;
        }
        if (!matches && mismatches++ == 0) {
            first_mismatch = "docid=" + std::to_string(docid) + " norm=" + std::to_string(norm) +
                             " expected=" + std::to_string(c.dense[docid]) + " " +
                             status.to_string();
        }
    }
    ASSERT_EQ(mismatches, 0U) << first_mismatch;
    uint8_t norm = 0;
    EXPECT_TRUE(
            reader.try_encoded_norm(c.doc_count, &norm).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

struct SparseBlockHeader {
    uint16_t key;
    uint8_t kind;
    uint32_t rank_base;
    uint32_t payload_offset;
};

// Parses the documented sparse payload prefix: doc_count, present_count, bytes_per_norm and the
// block headers.
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
std::vector<SparseBlockHeader> sparse_block_headers(const std::vector<uint8_t>& section,
                                                    uint8_t* bytes_per_norm) {
    ByteSource src {Slice(section)};
    FramedSection framed;
    EXPECT_TRUE(SectionFramer::read(src, &framed).ok());
    EXPECT_EQ(framed.type, static_cast<uint8_t>(format::SectionType::kNormsSparse));
    ByteSource payload(framed.payload);
    uint64_t value = 0;
    EXPECT_TRUE(payload.get_varint64(&value).ok());
    EXPECT_TRUE(payload.get_varint64(&value).ok());
    EXPECT_TRUE(payload.get_u8(bytes_per_norm).ok());
    uint64_t block_count = 0;
    EXPECT_TRUE(payload.get_varint64(&block_count).ok());
    std::vector<SparseBlockHeader> headers;
    for (uint64_t i = 0; i < block_count; ++i) {
        SparseBlockHeader header {};
        uint8_t reserved = 0;
        EXPECT_TRUE(payload.get_fixed16(&header.key).ok());
        EXPECT_TRUE(payload.get_u8(&header.kind).ok());
        EXPECT_TRUE(payload.get_u8(&reserved).ok());
        EXPECT_EQ(reserved, 0);
        EXPECT_TRUE(payload.get_fixed32(&header.rank_base).ok());
        EXPECT_TRUE(payload.get_fixed32(&header.payload_offset).ok());
        headers.push_back(header);
    }
    return headers;
}

NormsCase make_random_case(std::mt19937& rng, uint32_t doc_count, double null_ratio,
                           double run_bias, bool constant_norm) {
    NormsCase c;
    c.doc_count = doc_count;
    c.dense.resize(doc_count);
    std::uniform_int_distribution<int> norm_dist(1, 255);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    const auto constant = static_cast<uint8_t>(norm_dist(rng));
    bool in_null = unit(rng) < null_ratio;
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        c.dense[docid] = constant_norm ? constant : static_cast<uint8_t>(norm_dist(rng));
        // With run_bias close to 1 the null state rarely flips, producing long runs.
        if (unit(rng) >= run_bias) {
            in_null = unit(rng) < null_ratio;
        }
        if (in_null) {
            c.nulls.push_back(docid);
            if (unit(rng) < 0.01) {
                c.nulls_with_norms.push_back(docid);
            }
        }
    }
    c.finalize();
    return c;
}

} // namespace

// Segments without NULL rows keep the dense section every earlier writer produced, byte for byte.
TEST(SniiNormsSection, NoNullsWritesLegacyDenseBytes) {
    NormsCase c;
    c.doc_count = 300;
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        c.dense.push_back(query::encode_norm(docid % 7));
    }
    c.finalize();
    NormsSectionPlan plan;
    const std::vector<uint8_t> section = write_section(c, /*force_dense=*/false, &plan);
    EXPECT_EQ(plan.layout, NormsLayout::kDense);
    EXPECT_EQ(section, legacy_dense_section(c.dense));
    ByteSink legacy;
    NormsPodWriter::finish(c.dense, &legacy);
    EXPECT_EQ(section, legacy.buffer());
    EXPECT_EQ(section.size(), format::dense_norms_section_bytes(c.doc_count));

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    EXPECT_FALSE(reader.is_sparse());
    EXPECT_EQ(reader.present_count(), c.doc_count);
    expect_lookups_match(c, reader);
}

// The dense layout of a segment with NULL rows stores kEmptyDocumentNorm for them, which is what
// the earlier writers stored (encode_norm(0)). NULL rows that kept tokens keep their norm.
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(SniiNormsSection, ForcedDenseMatchesLegacyBytesWithNulls) {
    EXPECT_EQ(format::kEmptyDocumentNorm, query::encode_norm(0));
    NormsCase c;
    c.doc_count = 20000;
    c.dense.resize(c.doc_count);
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        c.dense[docid] = query::encode_norm(docid % 13);
        if (docid % 10 != 0) {
            c.nulls.push_back(docid);
        }
    }
    c.nulls_with_norms = {1, 2, 19999};
    c.finalize();
    EXPECT_EQ(c.dense[1], query::encode_norm(1));
    EXPECT_EQ(c.dense[3], format::kEmptyDocumentNorm);

    NormsSectionPlan dense_plan;
    const std::vector<uint8_t> dense = write_section(c, /*force_dense=*/true, &dense_plan);
    EXPECT_EQ(dense_plan.layout, NormsLayout::kDense);
    EXPECT_EQ(dense, legacy_dense_section(c.dense));

    NormsSectionPlan sparse_plan;
    const std::vector<uint8_t> sparse = write_section(c, /*force_dense=*/false, &sparse_plan);
    ASSERT_EQ(sparse_plan.layout, NormsLayout::kSparse);
    EXPECT_LT(sparse.size(), dense.size());

    NormsPodReader dense_reader;
    NormsPodReader sparse_reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(dense), &dense_reader).ok());
    ASSERT_TRUE(NormsPodReader::open(Slice(sparse), &sparse_reader).ok());
    EXPECT_FALSE(dense_reader.is_sparse());
    EXPECT_TRUE(sparse_reader.is_sparse());
    EXPECT_EQ(sparse_reader.present_count(), c.norms.size());
    expect_lookups_match(c, dense_reader);
    expect_lookups_match(c, sparse_reader);
}

TEST(SniiNormsSection, SparseRoundTripWithNormBytes) {
    NormsCase c;
    c.doc_count = 100000;
    c.dense.resize(c.doc_count);
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        c.dense[docid] = query::encode_norm(docid % 200);
        if (docid % 97 != 0) {
            c.nulls.push_back(docid);
        }
    }
    c.finalize();
    NormsSectionPlan plan;
    const std::vector<uint8_t> section = write_section(c, /*force_dense=*/false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    EXPECT_EQ(plan.bytes_per_norm, 1);
    uint8_t bytes_per_norm = 0;
    const auto headers = sparse_block_headers(section, &bytes_per_norm);
    EXPECT_EQ(bytes_per_norm, 1);
    // Blocks 0 and 1; each holds a few hundred present docids -> ARRAY.
    ASSERT_EQ(headers.size(), 2U);
    EXPECT_EQ(headers[0].kind, 1);
    EXPECT_EQ(headers[1].kind, 1);
    EXPECT_EQ(headers[0].rank_base, 0U);
    EXPECT_EQ(headers[1].rank_base, (65535U / 97) + 1);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    ASSERT_TRUE(reader.is_sparse());
    EXPECT_EQ(reader.present_count(), c.norms.size());
    expect_lookups_match(c, reader);
}

TEST(SniiNormsSection, SparseRoundTripWithConstantNorm) {
    NormsCase c;
    c.doc_count = 70000;
    c.dense.assign(c.doc_count, query::encode_norm(3));
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        if (docid < 1000 || docid >= 1100) {
            c.nulls.push_back(docid);
        }
    }
    c.finalize();
    NormsSectionPlan plan;
    const std::vector<uint8_t> section = write_section(c, /*force_dense=*/false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    EXPECT_EQ(plan.bytes_per_norm, 0);
    EXPECT_EQ(plan.constant_norm, query::encode_norm(3));
    uint8_t bytes_per_norm = 1;
    const auto headers = sparse_block_headers(section, &bytes_per_norm);
    EXPECT_EQ(bytes_per_norm, 0);
    ASSERT_EQ(headers.size(), 1U);
    EXPECT_EQ(headers[0].kind, 3); // one run
    // [type][len][doc_count 3B][present 1B][bpn][block_count][12B header][data_len][2+6B][norm]
    EXPECT_LT(section.size(), 40U);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    EXPECT_TRUE(reader.is_sparse());
    EXPECT_EQ(reader.present_count(), 100U);
    expect_lookups_match(c, reader);
}

// Every block kind, a partial tail block and an empty middle block in one section.
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(SniiNormsSection, SparseBlockKinds) {
    constexpr uint32_t kBlock = 1U << 16;
    NormsCase c;
    c.doc_count = 5 * kBlock + 1000;
    c.dense.resize(c.doc_count);
    std::mt19937 rng(7);
    for (uint32_t docid = 0; docid < c.doc_count; ++docid) {
        c.dense[docid] = static_cast<uint8_t>(1 + docid % 251);
        const uint32_t block = docid >> 16;
        const uint32_t low = docid & 0xFFFF;
        bool is_null = false;
        switch (block) {
        case 0: // ALL
            is_null = false;
            break;
        case 1: // ARRAY: 100 present docids
            is_null = low % 600 != 0 || low >= 60000;
            break;
        case 2: // RUNS: three long runs
            is_null = !(low < 5000 || (low >= 20000 && low < 30000) || low >= 65000);
            break;
        case 3: // BITSET: scattered half
            is_null = (rng() & 1U) != 0;
            break;
        case 4: // empty block
            is_null = true;
            break;
        default: // partial tail, ALL except one null -> ARRAY or RUNS
            is_null = low == 500;
            break;
        }
        if (is_null) {
            c.nulls.push_back(docid);
        }
    }
    c.finalize();
    NormsSectionPlan plan;
    const std::vector<uint8_t> section = write_section(c, /*force_dense=*/false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    uint8_t bytes_per_norm = 0;
    const auto headers = sparse_block_headers(section, &bytes_per_norm);
    ASSERT_EQ(headers.size(), 5U);
    EXPECT_EQ(headers[0].key, 0);
    EXPECT_EQ(headers[0].kind, 0);
    EXPECT_EQ(headers[1].key, 1);
    EXPECT_EQ(headers[1].kind, 1);
    EXPECT_EQ(headers[2].key, 2);
    EXPECT_EQ(headers[2].kind, 3);
    EXPECT_EQ(headers[3].key, 3);
    EXPECT_EQ(headers[3].kind, 2);
    EXPECT_EQ(headers[4].key, 5);
    EXPECT_EQ(headers[4].kind, 3); // two runs (14 bytes) beat 999 array entries
    EXPECT_EQ(headers[1].rank_base, kBlock);

    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    expect_lookups_match(c, reader);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(SniiNormsSection, LayoutChoice) {
    // Tiny segments stay dense: the sparse header outweighs the saving.
    NormsCase tiny;
    tiny.doc_count = 4;
    tiny.dense.assign(4, 2);
    tiny.nulls = {1, 3};
    tiny.finalize();
    NormsSectionPlan plan;
    write_section(tiny, false, &plan);
    EXPECT_EQ(plan.layout, NormsLayout::kDense);

    // Every document NULL: a sparse section without blocks.
    NormsCase all_null;
    all_null.doc_count = 1000;
    all_null.dense.assign(1000, 9);
    for (uint32_t docid = 0; docid < 1000; ++docid) {
        all_null.nulls.push_back(docid);
    }
    all_null.finalize();
    std::vector<uint8_t> section = write_section(all_null, false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    uint8_t bytes_per_norm = 1;
    EXPECT_TRUE(sparse_block_headers(section, &bytes_per_norm).empty());
    EXPECT_EQ(bytes_per_norm, 0);
    NormsPodReader reader;
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    EXPECT_EQ(reader.present_count(), 0U);
    expect_lookups_match(all_null, reader);
    write_section(all_null, /*force_dense=*/true, &plan);
    EXPECT_EQ(plan.layout, NormsLayout::kDense);

    // NULL rows that all kept tokens leave no document without a norm: dense.
    NormsCase all_with_norms;
    all_with_norms.doc_count = 1000;
    all_with_norms.dense.assign(1000, 9);
    for (uint32_t docid = 0; docid < 1000; docid += 2) {
        all_with_norms.nulls.push_back(docid);
    }
    all_with_norms.nulls_with_norms = all_with_norms.nulls;
    all_with_norms.finalize();
    write_section(all_with_norms, false, &plan);
    EXPECT_EQ(plan.layout, NormsLayout::kDense);

    // Scattered NULLs: one block of 6554 NULLs saves fewer bytes than its bitset costs: dense.
    NormsCase scattered;
    scattered.doc_count = 65536;
    scattered.dense.resize(65536);
    for (uint32_t docid = 0; docid < 65536; ++docid) {
        scattered.dense[docid] = static_cast<uint8_t>(1 + docid % 200);
        if (docid % 10 == 0) {
            scattered.nulls.push_back(docid);
        }
    }
    scattered.finalize();
    write_section(scattered, false, &plan);
    EXPECT_EQ(plan.layout, NormsLayout::kDense);
    EXPECT_EQ(plan.framed_bytes, format::dense_norms_section_bytes(65536));

    // Every third document NULL across two blocks: a bitset, a 2976-entry array and the norms
    // beat a byte per document.
    for (uint32_t docid = 0; docid < 65536; ++docid) {
        scattered.dense[docid] = static_cast<uint8_t>(1 + docid % 200);
    }
    scattered.doc_count = 70000;
    scattered.dense.resize(70000, 7);
    scattered.nulls.clear();
    for (uint32_t docid = 0; docid < 70000; docid += 3) {
        scattered.nulls.push_back(docid);
    }
    scattered.finalize();
    section = write_section(scattered, false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    const auto headers = sparse_block_headers(section, &bytes_per_norm);
    ASSERT_EQ(headers.size(), 2U);
    EXPECT_EQ(headers[0].kind, 2);
    EXPECT_EQ(headers[1].kind, 1);
    ASSERT_TRUE(NormsPodReader::open(Slice(section), &reader).ok());
    expect_lookups_match(scattered, reader);
}

TEST(SniiNormsSection, PlanRejectsInvalidInput) {
    NormsCase c;
    c.doc_count = 10;
    c.dense.assign(10, 1);
    c.nulls = {2, 5};
    c.finalize();
    NormsSectionPlan plan;

    NormsSectionInput input = c.input();
    std::vector<uint8_t> too_many(c.norms.size() + 1, 1);
    input.norms = too_many;
    EXPECT_TRUE(format::plan_norms_section(input, false, &plan)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());

    std::vector<uint32_t> not_a_subset = {3};
    std::vector<uint8_t> nine(9, 1);
    input = c.input();
    input.null_docids_with_norms = not_a_subset;
    input.norms = nine;
    EXPECT_TRUE(format::plan_norms_section(input, false, &plan)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());

    std::vector<uint32_t> unsorted = {5, 2};
    input = c.input();
    input.null_docids = unsorted;
    EXPECT_TRUE(format::plan_norms_section(input, false, &plan)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());

    std::vector<uint32_t> outside = {2, 10};
    input = c.input();
    input.null_docids = outside;
    EXPECT_TRUE(format::plan_norms_section(input, false, &plan)
                        .is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// Randomized equivalence of sparse lookups against the dense oracle, and identical norms between
// the two layouts, across densities, run lengths and document counts that cross block edges.
TEST(SniiNormsSection, RandomizedLookupsMatchDenseOracle) {
    std::mt19937 rng(20260917);
    const std::vector<uint32_t> doc_counts = {1, 2, 65535, 65536, 65537, 131072, 200003};
    const std::vector<double> null_ratios = {0.0, 0.3, 0.9, 0.99, 0.999, 1.0};
    const std::vector<double> run_biases = {0.0, 0.9, 0.9999};
    size_t sparse_cases = 0;
    for (uint32_t doc_count : doc_counts) {
        for (double null_ratio : null_ratios) {
            for (double run_bias : run_biases) {
                const bool constant = (rng() & 1U) != 0;
                SCOPED_TRACE("doc_count=" + std::to_string(doc_count) + " null_ratio=" +
                             std::to_string(null_ratio) + " run_bias=" + std::to_string(run_bias) +
                             " constant=" + std::to_string(constant));
                const NormsCase c =
                        make_random_case(rng, doc_count, null_ratio, run_bias, constant);
                NormsSectionPlan plan;
                const std::vector<uint8_t> section = write_section(c, false, &plan);
                NormsSectionPlan dense_plan;
                const std::vector<uint8_t> dense = write_section(c, true, &dense_plan);
                EXPECT_EQ(dense, legacy_dense_section(c.dense));
                EXPECT_LE(section.size(), dense.size());
                if (plan.layout == NormsLayout::kSparse) {
                    ++sparse_cases;
                    EXPECT_LT(section.size(), dense.size());
                } else {
                    EXPECT_EQ(section, dense);
                }
                NormsPodReader reader;
                const Status status = NormsPodReader::open(Slice(section), &reader);
                ASSERT_TRUE(status.ok()) << status.to_string();
                EXPECT_EQ(reader.is_sparse(), plan.layout == NormsLayout::kSparse);
                ASSERT_NO_FATAL_FAILURE(expect_lookups_match(c, reader));
            }
        }
    }
    EXPECT_GT(sparse_cases, 20U);
}

// A reader that predates the sparse layout accepts a norms region only when its length equals
// the dense length and its section type is kNormsPod (14). A sparse section fails both checks.
TEST(SniiNormsSection, SparseSectionFailsLegacyReaderChecks) {
    NormsCase c;
    c.doc_count = 50000;
    c.dense.assign(c.doc_count, 4);
    for (uint32_t docid = 0; docid < c.doc_count; docid += 1) {
        if (docid % 1000 != 0) {
            c.nulls.push_back(docid);
        }
    }
    c.finalize();
    NormsSectionPlan plan;
    const std::vector<uint8_t> section = write_section(c, false, &plan);
    ASSERT_EQ(plan.layout, NormsLayout::kSparse);
    EXPECT_NE(section.size(), format::dense_norms_section_bytes(c.doc_count));
    EXPECT_EQ(section[0], static_cast<uint8_t>(format::SectionType::kNormsSparse));
    EXPECT_NE(section[0], static_cast<uint8_t>(format::SectionType::kNormsPod));
}

// Each structural field of a sparse payload is validated; a CRC-valid but inconsistent section
// is corruption, never a misread.
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(SniiNormsSection, RejectsCorruptSparsePayloads) {
    constexpr auto kSparse = static_cast<uint8_t>(format::SectionType::kNormsSparse);
    auto open_payload = [](const ByteSink& payload) {
        ByteSink framed;
        SectionFramer::write(framed, kSparse, payload.view());
        NormsPodReader reader;
        return NormsPodReader::open(framed.view(), &reader);
    };
    auto header = [](ByteSink* out, uint16_t key, uint8_t kind, uint8_t reserved, uint32_t rank,
                     uint32_t offset) {
        out->put_fixed16(key);
        out->put_u8(kind);
        out->put_u8(reserved);
        out->put_fixed32(rank);
        out->put_fixed32(offset);
    };
    // A valid baseline: doc_count 10, present {2, 7}, ARRAY block, constant norm.
    auto build = [&](uint64_t present, uint8_t bpn, uint8_t kind, uint8_t reserved,
                     std::vector<uint16_t> lows, uint64_t data_len_delta, bool trailing) {
        ByteSink payload;
        payload.put_varint64(10);
        payload.put_varint64(present);
        payload.put_u8(bpn);
        payload.put_varint64(1);
        header(&payload, 0, kind, reserved, 0, 0);
        payload.put_varint64(lows.size() * 2 + data_len_delta);
        for (uint16_t low : lows) {
            payload.put_fixed16(low);
        }
        for (uint64_t i = 0; i < data_len_delta; ++i) {
            payload.put_u8(0);
        }
        payload.put_u8(5);
        if (bpn == 1) {
            payload.put_u8(6);
        }
        if (trailing) {
            payload.put_u8(0);
        }
        return payload;
    };
    EXPECT_TRUE(open_payload(build(2, 0, 1, 0, {2, 7}, 0, false)).ok());
    EXPECT_TRUE(open_payload(build(2, 1, 1, 0, {2, 7}, 0, false)).ok());
    auto corrupted = [&](const ByteSink& payload) {
        return open_payload(payload).is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>();
    };
    EXPECT_TRUE(corrupted(build(2, 2, 1, 0, {2, 7}, 0, false)));  // bytes_per_norm
    EXPECT_TRUE(corrupted(build(2, 0, 4, 0, {2, 7}, 0, false)));  // kind
    EXPECT_TRUE(corrupted(build(2, 0, 1, 1, {2, 7}, 0, false)));  // reserved
    EXPECT_TRUE(corrupted(build(2, 0, 1, 0, {7, 2}, 0, false)));  // unsorted lows
    EXPECT_TRUE(corrupted(build(2, 0, 1, 0, {2, 10}, 0, false))); // low past span
    EXPECT_TRUE(corrupted(build(3, 0, 1, 0, {2, 7}, 0, false)));  // cardinality
    EXPECT_TRUE(corrupted(build(2, 0, 1, 0, {2, 7}, 2, false)));  // data length
    EXPECT_TRUE(corrupted(build(2, 0, 1, 0, {2, 7}, 0, true)));   // trailing byte
    EXPECT_TRUE(corrupted(build(11, 0, 1, 0, {2, 7}, 0, false))); // present > doc_count
    EXPECT_TRUE(corrupted(build(2, 0, 0, 0, {}, 0, false)));      // ALL but 2 != span 10

    // Block outside the document domain.
    ByteSink payload;
    payload.put_varint64(10);
    payload.put_varint64(1);
    payload.put_u8(0);
    payload.put_varint64(1);
    header(&payload, 1, 1, 0, 0, 0);
    payload.put_varint64(2);
    payload.put_fixed16(0);
    payload.put_u8(5);
    EXPECT_TRUE(corrupted(payload));

    ByteSink runs;
    runs.put_varint64(10);
    runs.put_varint64(4);
    runs.put_u8(0);
    runs.put_varint64(1);
    header(&runs, 0, 3, 0, 0, 0);
    runs.put_varint64(2 + 12);
    runs.put_fixed16(2);
    runs.put_fixed16(1); // run [1, 2]
    runs.put_fixed16(2);
    runs.put_fixed16(0);
    runs.put_fixed16(3); // adjacent run [3, 4]: runs must be separated
    runs.put_fixed16(4);
    runs.put_fixed16(2);
    runs.put_u8(5);
    EXPECT_TRUE(corrupted(runs));
}
