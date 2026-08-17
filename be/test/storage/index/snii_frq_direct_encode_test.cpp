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

#include <algorithm>
#include <array>
#include <cstdint>
#include <numeric>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii {
namespace {

ByteSink legacy_raw_dd(const std::vector<uint32_t>& docs, uint64_t win_base) {
    ByteSink out;
    out.put_varint32(static_cast<uint32_t>(docs.size()));
    std::vector<uint32_t> deltas(docs.size());
    uint64_t previous = win_base;
    for (size_t i = 0; i < docs.size(); ++i) {
        deltas[i] = static_cast<uint32_t>(docs[i] - previous);
        previous = docs[i];
    }
    for (size_t begin = 0; begin < deltas.size(); begin += format::kFrqBaseUnit) {
        const size_t count =
                std::min(deltas.size() - begin, static_cast<size_t>(format::kFrqBaseUnit));
        pfor_encode(deltas.data() + begin, count, &out);
    }
    return out;
}

ByteSink legacy_raw_freq(const std::vector<uint32_t>& freqs) {
    ByteSink out;
    for (size_t begin = 0; begin < freqs.size(); begin += format::kFrqBaseUnit) {
        const size_t count =
                std::min(freqs.size() - begin, static_cast<size_t>(format::kFrqBaseUnit));
        pfor_encode(freqs.data() + begin, count, &out);
    }
    return out;
}

std::vector<uint32_t> make_docs(size_t count, uint32_t win_base, uint32_t salt) {
    std::vector<uint32_t> docs;
    docs.reserve(count);
    uint32_t doc = win_base;
    for (size_t i = 0; i < count; ++i) {
        doc += 1 + static_cast<uint32_t>((i + salt) % 7);
        docs.push_back(doc);
    }
    return docs;
}

std::vector<uint32_t> make_doc_deltas(const std::vector<uint32_t>& docs, uint64_t win_base) {
    std::vector<uint32_t> deltas;
    deltas.reserve(docs.size());
    uint64_t previous = win_base;
    for (uint32_t doc : docs) {
        deltas.push_back(static_cast<uint32_t>(static_cast<uint64_t>(doc) - previous));
        previous = doc;
    }
    return deltas;
}

std::vector<uint32_t> make_freqs(size_t count, uint32_t salt) {
    std::vector<uint32_t> freqs(count);
    for (size_t i = 0; i < count; ++i) {
        freqs[i] = 1 + static_cast<uint32_t>((i * 5 + salt) % 11);
    }
    return freqs;
}

void expect_raw_meta(const format::FrqRegionMeta& meta, Slice expected) {
    EXPECT_FALSE(meta.zstd);
    EXPECT_EQ(meta.uncomp_len, expected.size());
    EXPECT_EQ(meta.disk_len, expected.size());
    EXPECT_EQ(meta.crc, crc32c(expected));
    EXPECT_TRUE(meta.verify_crc);
}

void expect_meta_equal(const format::FrqRegionMeta& actual, const format::FrqRegionMeta& expected) {
    EXPECT_EQ(actual.zstd, expected.zstd);
    EXPECT_EQ(actual.uncomp_len, expected.uncomp_len);
    EXPECT_EQ(actual.disk_len, expected.disk_len);
    EXPECT_EQ(actual.crc, expected.crc);
    EXPECT_EQ(actual.verify_crc, expected.verify_crc);
}

Slice appended_region(const ByteSink& sink, size_t offset, uint64_t length) {
    return Slice(sink.buffer().data() + offset, static_cast<size_t>(length));
}

constexpr std::array<size_t, 6> kBoundaryCounts {0, 1, 255, 256, 257, 512};

TEST(SniiFrqDirectEncodeTest, RawDdMatchesReferenceAcrossBoundariesAndConsecutiveAppends) {
    for (size_t count : kBoundaryCounts) {
        SCOPED_TRACE(count);
        constexpr uint32_t kFirstBase = 7;
        const std::vector<uint32_t> first_docs = make_docs(count, kFirstBase, 1);
        const uint32_t second_base = first_docs.empty() ? kFirstBase : first_docs.back();
        const std::vector<uint32_t> second_docs = make_docs(count, second_base, 3);
        const ByteSink first_reference = legacy_raw_dd(first_docs, kFirstBase);
        const ByteSink second_reference = legacy_raw_dd(second_docs, second_base);

        ByteSink actual;
        actual.put_fixed32(0x12345678);
        const size_t first_offset = actual.size();
        format::FrqRegionMeta first_meta;
        ASSERT_TRUE(format::build_dd_region(first_docs, kFirstBase, 0, &actual, &first_meta).ok());
        const size_t second_offset = actual.size();
        format::FrqRegionMeta second_meta;
        ASSERT_TRUE(
                format::build_dd_region(second_docs, second_base, 0, &actual, &second_meta).ok());

        ByteSink expected;
        expected.put_fixed32(0x12345678);
        expected.put_bytes(first_reference.view());
        expected.put_bytes(second_reference.view());
        EXPECT_EQ(actual.buffer(), expected.buffer());
        expect_raw_meta(first_meta, first_reference.view());
        expect_raw_meta(second_meta, second_reference.view());

        std::vector<uint32_t> decoded;
        ASSERT_TRUE(
                format::decode_dd_region(appended_region(actual, first_offset, first_meta.disk_len),
                                         first_meta, kFirstBase, &decoded)
                        .ok());
        EXPECT_EQ(decoded, first_docs);
        ASSERT_TRUE(format::decode_dd_region(
                            appended_region(actual, second_offset, second_meta.disk_len),
                            second_meta, second_base, &decoded)
                            .ok());
        EXPECT_EQ(decoded, second_docs);
    }
}

TEST(SniiFrqDirectEncodeTest, RawDdDeltasMatchAbsoluteAcrossBoundariesAndWindows) {
    const std::vector<uint32_t> first_docs {0, 3, 10};
    const std::vector<uint32_t> first_deltas = make_doc_deltas(first_docs, /*win_base=*/0);
    constexpr uint32_t kSecondBase = 10;

    for (size_t count : kBoundaryCounts) {
        SCOPED_TRACE(count);
        const std::vector<uint32_t> second_docs = make_docs(count, kSecondBase, 3);
        const std::vector<uint32_t> second_deltas = make_doc_deltas(second_docs, kSecondBase);

        ByteSink absolute;
        absolute.put_fixed32(0x12345678);
        const size_t first_offset = absolute.size();
        format::FrqRegionMeta absolute_first_meta;
        ASSERT_TRUE(format::build_dd_region(first_docs, /*win_base=*/0, /*level=*/0, &absolute,
                                            &absolute_first_meta)
                            .ok());
        const size_t second_offset = absolute.size();
        format::FrqRegionMeta absolute_second_meta;
        ASSERT_TRUE(format::build_dd_region(second_docs, kSecondBase, /*level=*/0, &absolute,
                                            &absolute_second_meta)
                            .ok());

        ByteSink direct;
        direct.put_fixed32(0x12345678);
        format::FrqRegionMeta direct_first_meta;
        ASSERT_TRUE(format::build_dd_region_from_deltas(std::span<const uint32_t>(first_deltas),
                                                        /*level=*/0, &direct, &direct_first_meta)
                            .ok());
        EXPECT_EQ(direct.size(), second_offset);
        format::FrqRegionMeta direct_second_meta;
        ASSERT_TRUE(format::build_dd_region_from_deltas(std::span<const uint32_t>(second_deltas),
                                                        /*level=*/0, &direct, &direct_second_meta)
                            .ok());

        EXPECT_EQ(direct.buffer(), absolute.buffer());
        expect_meta_equal(direct_first_meta, absolute_first_meta);
        expect_meta_equal(direct_second_meta, absolute_second_meta);

        std::vector<uint32_t> decoded;
        ASSERT_TRUE(format::decode_dd_region(
                            appended_region(direct, first_offset, direct_first_meta.disk_len),
                            direct_first_meta, /*win_base=*/0, &decoded)
                            .ok());
        EXPECT_EQ(decoded, first_docs);
        ASSERT_TRUE(format::decode_dd_region(
                            appended_region(direct, second_offset, direct_second_meta.disk_len),
                            direct_second_meta, kSecondBase, &decoded)
                            .ok());
        EXPECT_EQ(decoded, second_docs);
    }
}

TEST(SniiFrqDirectEncodeTest, RawFreqMatchesReferenceAcrossBoundariesAndConsecutiveAppends) {
    for (size_t count : kBoundaryCounts) {
        SCOPED_TRACE(count);
        const std::vector<uint32_t> first_freqs = make_freqs(count, 2);
        const std::vector<uint32_t> second_freqs = make_freqs(count, 7);
        const ByteSink first_reference = legacy_raw_freq(first_freqs);
        const ByteSink second_reference = legacy_raw_freq(second_freqs);

        ByteSink actual;
        actual.put_fixed32(0x87654321);
        const size_t first_offset = actual.size();
        format::FrqRegionMeta first_meta;
        ASSERT_TRUE(format::build_freq_region(first_freqs, 0, &actual, &first_meta).ok());
        const size_t second_offset = actual.size();
        format::FrqRegionMeta second_meta;
        ASSERT_TRUE(format::build_freq_region(second_freqs, 0, &actual, &second_meta).ok());

        ByteSink expected;
        expected.put_fixed32(0x87654321);
        expected.put_bytes(first_reference.view());
        expected.put_bytes(second_reference.view());
        EXPECT_EQ(actual.buffer(), expected.buffer());
        expect_raw_meta(first_meta, first_reference.view());
        expect_raw_meta(second_meta, second_reference.view());

        std::vector<uint32_t> decoded;
        ASSERT_TRUE(format::decode_freq_region(
                            appended_region(actual, first_offset, first_meta.disk_len), first_meta,
                            first_freqs.size(), &decoded)
                            .ok());
        EXPECT_EQ(decoded, first_freqs);
        ASSERT_TRUE(format::decode_freq_region(
                            appended_region(actual, second_offset, second_meta.disk_len),
                            second_meta, second_freqs.size(), &decoded)
                            .ok());
        EXPECT_EQ(decoded, second_freqs);
    }
}

TEST(SniiFrqDirectEncodeTest, DescendingDocInLaterRunLeavesOutputsUnchanged) {
    std::vector<uint32_t> docs(300);
    std::iota(docs.begin(), docs.end(), 100);
    docs[280] = docs[279] - 1;
    ByteSink out;
    out.put_fixed32(0x12345678);
    const std::vector<uint8_t> original_bytes = out.buffer();
    format::FrqRegionMeta meta;
    meta.zstd = true;
    meta.uncomp_len = 17;
    meta.disk_len = 11;
    meta.crc = 0x87654321;
    meta.verify_crc = false;
    const format::FrqRegionMeta original_meta = meta;

    const Status status = format::build_dd_region(docs, 100, 0, &out, &meta);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("ascending"), std::string::npos);
    EXPECT_EQ(out.buffer(), original_bytes);
    EXPECT_EQ(meta.zstd, original_meta.zstd);
    EXPECT_EQ(meta.uncomp_len, original_meta.uncomp_len);
    EXPECT_EQ(meta.disk_len, original_meta.disk_len);
    EXPECT_EQ(meta.crc, original_meta.crc);
    EXPECT_EQ(meta.verify_crc, original_meta.verify_crc);
}

TEST(SniiFrqDirectEncodeTest, WindowedRegionsRoundTripWithAndWithoutFreq) {
    for (bool write_freq : {false, true}) {
        SCOPED_TRACE(write_freq);
        std::vector<uint32_t> expected_docs;
        std::vector<uint32_t> expected_freqs;
        writer::TermPostings term;
        term.term = "windowed";
        for (uint32_t ordinal = 0; ordinal < format::kSlimDfThreshold; ++ordinal) {
            const uint32_t doc = ordinal * 2 + 1;
            const uint32_t freq = ordinal % 3 + 1;
            expected_docs.push_back(doc);
            expected_freqs.push_back(freq);
            term.docids.push_back(doc);
            term.freqs.push_back(freq);
            for (uint32_t position = 0; position < freq; ++position) {
                term.positions_flat.push_back(position);
            }
        }
        writer::SniiIndexInput input;
        input.index_id = 1;
        input.index_suffix = "body";
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = format::kSlimDfThreshold * 2;
        input.write_freq = write_freq;
        input.terms.push_back(std::move(term));

        testing::reset_frq_raw_encode_work();
        snii_test::MemoryFile file;
        writer::SniiCompoundWriter compound_writer(&file);
        ASSERT_TRUE(compound_writer.add_logical_index(input).ok());
        ASSERT_TRUE(compound_writer.finish().ok());

        reader::SniiSegmentReader segment_reader;
        ASSERT_TRUE(reader::SniiSegmentReader::open(&file, &segment_reader).ok());
        reader::LogicalIndexReader index_reader;
        ASSERT_TRUE(
                segment_reader.open_index(input.index_id, input.index_suffix, &index_reader).ok());
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        ASSERT_TRUE(index_reader.lookup("windowed", &found, &entry, &frq_base, &prx_base).ok());
        ASSERT_TRUE(found);
        ASSERT_EQ(entry.enc, format::DictEntryEnc::kWindowed);

        const uint64_t frq_offset =
                index_reader.section_refs().posting_region.offset + frq_base + entry.frq_off_delta;
        std::vector<uint8_t> frq_bytes;
        ASSERT_TRUE(file.read_at(frq_offset, entry.frq_len, &frq_bytes).ok());
        format::FrqPreludeReader prelude;
        ASSERT_TRUE(
                format::FrqPreludeReader::open(
                        Slice(frq_bytes.data(), static_cast<size_t>(entry.prelude_len)), &prelude)
                        .ok());
        ASSERT_EQ(prelude.n_windows(), 2);
        EXPECT_EQ(prelude.has_freq(), write_freq);
        EXPECT_TRUE(prelude.has_prx());
        EXPECT_EQ(entry.frq_len,
                  entry.prelude_len + prelude.dd_block_len() + prelude.freq_block_len());

        size_t doc_begin = 0;
        for (uint32_t window = 0; window < prelude.n_windows(); ++window) {
            format::WindowMeta window_meta;
            ASSERT_TRUE(prelude.window(window, &window_meta).ok());
            format::FrqRegionMeta dd_meta;
            dd_meta.zstd = window_meta.dd_zstd;
            dd_meta.uncomp_len = window_meta.dd_uncomp_len;
            dd_meta.disk_len = window_meta.dd_disk_len;
            dd_meta.crc = window_meta.crc_dd;
            const size_t dd_offset = static_cast<size_t>(entry.prelude_len + window_meta.dd_off);
            std::vector<uint32_t> decoded_docs;
            ASSERT_TRUE(
                    format::decode_dd_region(Slice(frq_bytes.data() + dd_offset,
                                                   static_cast<size_t>(window_meta.dd_disk_len)),
                                             dd_meta, window_meta.win_base, &decoded_docs)
                            .ok());
            const std::vector<uint32_t> window_docs(
                    expected_docs.begin() + doc_begin,
                    expected_docs.begin() + doc_begin + window_meta.doc_count);
            EXPECT_EQ(decoded_docs, window_docs);

            if (write_freq) {
                format::FrqRegionMeta freq_meta;
                freq_meta.zstd = window_meta.freq_zstd;
                freq_meta.uncomp_len = window_meta.freq_uncomp_len;
                freq_meta.disk_len = window_meta.freq_disk_len;
                freq_meta.crc = window_meta.crc_freq;
                const size_t freq_offset = static_cast<size_t>(
                        entry.prelude_len + prelude.dd_block_len() + window_meta.freq_off);
                std::vector<uint32_t> decoded_freqs;
                ASSERT_TRUE(format::decode_freq_region(
                                    Slice(frq_bytes.data() + freq_offset,
                                          static_cast<size_t>(window_meta.freq_disk_len)),
                                    freq_meta, window_meta.doc_count, &decoded_freqs)
                                    .ok());
                const std::vector<uint32_t> window_freqs(
                        expected_freqs.begin() + doc_begin,
                        expected_freqs.begin() + doc_begin + window_meta.doc_count);
                EXPECT_EQ(decoded_freqs, window_freqs);
                if (window == 1) {
                    std::vector<uint8_t> corrupted(
                            frq_bytes.begin() + freq_offset,
                            frq_bytes.begin() + freq_offset + window_meta.freq_disk_len);
                    corrupted.front() ^= 1;
                    EXPECT_FALSE(format::decode_freq_region(Slice(corrupted), freq_meta,
                                                            window_meta.doc_count, &decoded_freqs)
                                         .ok());
                }
            }
            if (window == 1) {
                std::vector<uint8_t> corrupted(
                        frq_bytes.begin() + dd_offset,
                        frq_bytes.begin() + dd_offset + window_meta.dd_disk_len);
                corrupted.front() ^= 1;
                EXPECT_FALSE(format::decode_dd_region(Slice(corrupted), dd_meta,
                                                      window_meta.win_base, &decoded_docs)
                                     .ok());
            }
            doc_begin += window_meta.doc_count;
        }
        EXPECT_EQ(doc_begin, expected_docs.size());
        EXPECT_EQ(testing::frq_dd_validation_doc_visits(), 0);
        EXPECT_EQ(testing::frq_dd_materialized_values(), 0);
        EXPECT_EQ(testing::frq_raw_region_copy_bytes(), 0);
    }
}

} // namespace
} // namespace doris::snii
