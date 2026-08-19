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

// GOLDEN BYTE pins for the ordinary SNII writer path. Each test
// writes ONE segment from a FIXED corpus through the production writer stack
// and asserts the output file's FNV-1a-64 digest against a recorded constant
// harvested after B2 stopped feeding hidden bigrams. Any change to tokenization
// semantics, position accounting, ignore_above / empty-value handling, or
// ordinary on-disk encoding flips the digest.
//
// The corpus deliberately hits the edge lanes: empty value (analyzed: skipped;
// keyword: a VALID empty token), punctuation-only row (zero analyzed tokens),
// >ignore_above value (keyword: skipped row), long token, repeated terms
// (position increments), unicode/mixed text, multiple add_values
// batches, and interleaved add_nulls runs.
//
// If a digest changes INTENTIONALLY (format or analyzer change), re-harvest by
// running the test and copying the "actual=" value from the failure message --
// and say so loudly in the commit message.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <map>
#include <numeric>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/index/snii/writer/posting_window_emitter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 9;
constexpr const char* kTestDir = "./ut_dir/snii_writer_golden_bytes_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TabletIndex make_meta(const std::map<std::string, std::string>& properties) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("golden_idx");
    pb.add_col_unique_id(0);
    for (const auto& [k, v] : properties) {
        pb.mutable_properties()->insert({k, v});
    }
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

uint64_t fnv1a64(const std::string& bytes) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : bytes) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

uint64_t fnv1a64(std::span<const uint8_t> bytes) {
    uint64_t h = 1469598103934665603ULL;
    for (const uint8_t c : bytes) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

struct ShapeImage {
    std::vector<uint8_t> bytes;
    doris::snii::format::DictEntryKind kind = doris::snii::format::DictEntryKind::kPodRef;
    doris::snii::format::DictEntryEnc encoding = doris::snii::format::DictEntryEnc::kSlim;
    std::vector<uint32_t> window_docs;
};

doris::snii::writer::TermPostings make_shape_term(std::string term, uint32_t doc_count,
                                                  bool irregular_docids = false) {
    doris::snii::writer::TermPostings postings;
    postings.term = std::move(term);
    postings.docids.reserve(doc_count);
    postings.freqs.assign(doc_count, 1);
    postings.positions_flat.assign(doc_count, 0);
    uint32_t docid = 0;
    for (uint32_t i = 0; i < doc_count; ++i) {
        postings.docids.push_back(docid);
        docid += irregular_docids ? 1U + ((i * 7919U) & 0xFFFFU) : 1U;
    }
    return postings;
}

ShapeImage build_shape_image(doris::snii::writer::SniiIndexInput input,
                             std::string_view lookup_term) {
    using doris::snii::Slice;
    using doris::snii::format::DictEntry;
    using doris::snii::format::FrqPreludeReader;
    using doris::snii::reader::LogicalIndexReader;
    using doris::snii::reader::SniiSegmentReader;
    using doris::snii::snii_test::MemoryFile;
    using doris::snii::writer::SniiCompoundWriter;

    const uint64_t index_id = input.index_id;
    const std::string index_suffix = input.index_suffix;
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    assert_ok(compound.add_logical_index(input));
    assert_ok(compound.finish());

    SniiSegmentReader segment;
    LogicalIndexReader index;
    assert_ok(SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(index_id, index_suffix, &index));
    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup(lookup_term, &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found);

    ShapeImage image {
            .bytes = file.data(), .kind = entry.kind, .encoding = entry.enc, .window_docs = {}};
    if (entry.enc == doris::snii::format::DictEntryEnc::kWindowed) {
        const auto& posting = index.section_refs().posting_region;
        const uint64_t prelude_offset = posting.offset + frq_base + entry.frq_off_delta;
        EXPECT_LE(prelude_offset + entry.prelude_len, image.bytes.size());
        if (prelude_offset + entry.prelude_len > image.bytes.size()) {
            return image;
        }
        FrqPreludeReader prelude;
        assert_ok(FrqPreludeReader::open(
                Slice(image.bytes.data() + prelude_offset, entry.prelude_len), &prelude));
        image.window_docs.reserve(prelude.n_windows());
        for (uint32_t i = 0; i < prelude.n_windows(); ++i) {
            doris::snii::format::WindowMeta window;
            assert_ok(prelude.window(i, &window));
            image.window_docs.push_back(window.doc_count);
        }
    }
    return image;
}

doris::snii::writer::SniiIndexInput make_shape_input(
        uint64_t index_id, std::string suffix, doris::snii::writer::TermPostings term,
        doris::snii::format::IndexConfig config =
                doris::snii::format::IndexConfig::kDocsPositions) {
    doris::snii::writer::SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = std::move(suffix);
    input.config = config;
    input.doc_count = term.docids.empty() ? 0 : term.docids.back() + 1;
    input.write_freq = false;
    input.terms.push_back(std::move(term));
    return input;
}

doris::snii::writer::SniiIndexInput make_recut_input(uint64_t index_id, std::string suffix,
                                                     uint32_t doc_count) {
    constexpr uint32_t kPrefixDocs = 8192;
    constexpr uint32_t kFarPosition = 1U << 28;
    auto term = make_shape_term("recut", doc_count);
    term.positions_flat.clear();
    term.positions_flat.reserve(kPrefixDocs + 2 * (doc_count - kPrefixDocs));
    for (uint32_t i = 0; i < doc_count; ++i) {
        if (i < kPrefixDocs) {
            term.positions_flat.push_back(0);
            continue;
        }
        term.freqs[i] = 2;
        term.positions_flat.push_back(0);
        term.positions_flat.push_back(kFarPosition);
    }
    auto input = make_shape_input(index_id, std::move(suffix), std::move(term));
    input.prx_window_limits = {
            .max_docs = 1024,
            .max_positions = 2048,
            .max_uncomp_bytes = 2048,
    };
    return input;
}

std::string read_file_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << path;
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

void add_batch(SniiIndexColumnWriter* writer, const std::vector<std::string>& rows) {
    std::vector<Slice> slices;
    slices.reserve(rows.size());
    for (const std::string& row : rows) {
        slices.emplace_back(row);
    }
    assert_ok(writer->add_values("c1", slices.data(), slices.size()));
}

// The fixed corpus: two add_values batches with add_nulls runs interleaved.
void feed_corpus(SniiIndexColumnWriter* writer) {
    add_batch(writer, {
                              "hello world hello doris",
                              "", // analyzed: no tokens; keyword: valid EMPTY token
                              "The QUICK brown-fox; jumped!! over_the lazy dog 42 times",
                              "重复 重复 重复 词元 Doris 数据库 全文检索 mixed 中英 tokens",
                      });
    assert_ok(writer->add_nulls(3));
    add_batch(writer, {
                              std::string(300, 'x'), // keyword: > ignore_above(256) -> skipped
                              "single",
                              "!!! ??? ,,,", // analyzed: zero tokens survive
                              "hello world again and again and again",
                      });
    assert_ok(writer->add_nulls(1));
}

// Writes the corpus through the production stack and returns the segment
// file's digest.
uint64_t golden_digest(const std::string& name, const TabletIndex& meta) {
    const std::string path = std::string(kTestDir) + "/" + name + ".idx";
    io::FileWriterPtr file_writer;
    EXPECT_TRUE(io::global_local_filesystem()->create_file(path, &file_writer).ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "golden_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/900);
    SniiIndexColumnWriter writer(&index_file_writer, &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    EXPECT_TRUE(writer.init().ok());
    feed_corpus(&writer);
    EXPECT_TRUE(writer.finish().ok());
    EXPECT_TRUE(index_file_writer.begin_close().ok());
    EXPECT_TRUE(index_file_writer.finish_close().ok());
    const std::string bytes = read_file_bytes(path);
    EXPECT_FALSE(bytes.empty());
    return fnv1a64(bytes);
}

class SniiWriterGoldenBytes : public testing::Test {
protected:
    void SetUp() override {
        // Pin every live config the write path reads, so the digests do not move
        // under future default changes.
        _saved_dict_lvl = config::snii_dict_block_zstd_level;
        _saved_prx_lvl = config::snii_prx_zstd_level;
        _saved_prx_load_lvl = config::snii_prx_zstd_level_direct_load;
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        config::snii_dict_block_zstd_level = 3;
        config::snii_prx_zstd_level = 3;
        config::snii_prx_zstd_level_direct_load = 3;
        _saved_write_freq = config::snii_positions_index_write_freq;
        config::snii_positions_index_write_freq = false;
    }

    void TearDown() override {
        config::snii_dict_block_zstd_level = _saved_dict_lvl;
        config::snii_prx_zstd_level = _saved_prx_lvl;
        config::snii_prx_zstd_level_direct_load = _saved_prx_load_lvl;
        config::snii_positions_index_write_freq = _saved_write_freq;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    int32_t _saved_dict_lvl = 3;
    int32_t _saved_prx_lvl = 3;
    int32_t _saved_prx_load_lvl = 3;
    bool _saved_write_freq = false;
};

// Whole-image digests re-harvested for the protobuf v1 metadata layout. They
// still pin posting bytes together with every framing, directory, and metadata
// byte, so future format changes remain explicit.
constexpr uint64_t kGoldenEnglishPhrase = 0x0adb7bf49bed5dc2ULL;
constexpr uint64_t kGoldenUnicodePhrase = 0x2ea85ae3a736665cULL;
constexpr uint64_t kGoldenKeywordDocsOnly = 0xcdfd89278e7a7979ULL;

TEST_F(SniiWriterGoldenBytes, EnglishPhrase) {
    const TabletIndex meta =
            make_meta({{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}});
    const uint64_t digest = golden_digest("english_phrase", meta);
    EXPECT_EQ(digest, kGoldenEnglishPhrase)
            << "actual=0x" << std::hex << digest
            << " -- token-path output changed; see file header before re-harvesting";
}

TEST_F(SniiWriterGoldenBytes, UnicodePhrase) {
    const TabletIndex meta =
            make_meta({{"parser", "unicode"}, {"lower_case", "true"}, {"support_phrase", "true"}});
    const uint64_t digest = golden_digest("unicode_phrase", meta);
    EXPECT_EQ(digest, kGoldenUnicodePhrase)
            << "actual=0x" << std::hex << digest
            << " -- token-path output changed; see file header before re-harvesting";
}

TEST_F(SniiWriterGoldenBytes, KeywordDocsOnly) {
    const TabletIndex meta = make_meta({{"ignore_above", "256"}});
    const uint64_t digest = golden_digest("keyword_docs_only", meta);
    EXPECT_EQ(digest, kGoldenKeywordDocsOnly)
            << "actual=0x" << std::hex << digest
            << " -- token-path output changed; see file header before re-harvesting";
}

TEST_F(SniiWriterGoldenBytes, PostingShapeMatrixCompleteImageDigest) {
    doris::snii::writer::testing::reset_window_emitter_counters();
    auto docs_only_term = make_shape_term("docs-only", 512);
    docs_only_term.positions_flat.clear();
    docs_only_term.retain_positions = false;
    const std::array<ShapeImage, 8> images = {
            build_shape_image(make_shape_input(101, "inline", make_shape_term("inline", 1)),
                              "inline"),
            build_shape_image(make_shape_input(102, "slim", make_shape_term("slim", 511, true)),
                              "slim"),
            build_shape_image(make_shape_input(103, "df-511", make_shape_term("df-511", 511)),
                              "df-511"),
            build_shape_image(make_shape_input(104, "df-512", make_shape_term("df-512", 512)),
                              "df-512"),
            build_shape_image(make_shape_input(105, "df-8191", make_shape_term("df-8191", 8191)),
                              "df-8191"),
            build_shape_image(make_shape_input(106, "df-8192", make_shape_term("df-8192", 8192)),
                              "df-8192"),
            build_shape_image(make_recut_input(107, "recut-full-tail", 8192 + 1024), "recut"),
            build_shape_image(make_recut_input(108, "recut-partial-tail", 8192 + 512), "recut"),
    };
    const ShapeImage docs_only =
            build_shape_image(make_shape_input(109, "docs-only", std::move(docs_only_term),
                                               doris::snii::format::IndexConfig::kDocsOnly),
                              "docs-only");

    EXPECT_EQ(images[0].kind, doris::snii::format::DictEntryKind::kInline);
    EXPECT_EQ(images[0].encoding, doris::snii::format::DictEntryEnc::kSlim);
    EXPECT_EQ(images[1].kind, doris::snii::format::DictEntryKind::kPodRef);
    EXPECT_EQ(images[1].encoding, doris::snii::format::DictEntryEnc::kSlim);
    EXPECT_EQ(images[2].encoding, doris::snii::format::DictEntryEnc::kSlim);
    EXPECT_EQ(images[3].window_docs, (std::vector<uint32_t> {256, 256}));
    EXPECT_EQ(images[4].window_docs.size(), 32U);
    EXPECT_EQ(images[4].window_docs.back(), 255U);
    EXPECT_EQ(images[5].window_docs, (std::vector<uint32_t>(8, 1024)));
    ASSERT_GT(images[6].window_docs.size(), 9U);
    ASSERT_GT(images[7].window_docs.size(), 9U);
    EXPECT_TRUE(std::ranges::all_of(images[6].window_docs.begin(),
                                    images[6].window_docs.begin() + 8,
                                    [](uint32_t docs) { return docs == 1024; }));
    EXPECT_TRUE(std::ranges::all_of(images[7].window_docs.begin(),
                                    images[7].window_docs.begin() + 8,
                                    [](uint32_t docs) { return docs == 1024; }));
    EXPECT_LT(images[6].window_docs[8], 1024U);
    EXPECT_LT(images[7].window_docs[8], 512U);
    EXPECT_EQ(docs_only.encoding, doris::snii::format::DictEntryEnc::kWindowed);

    constexpr std::array<std::string_view, 9> names = {
            "inline",  "slim",       "df-511",     "df-512",    "df-8191",
            "df-8192", "recut-full", "recut-tail", "docs-only",
    };
    constexpr std::array<uint64_t, 9> expected = {
            0x3d00d59799c7d0adULL, 0x1ae78d4f5bcfe8b9ULL, 0x2b6f0cf4ba73bfb0ULL,
            0xc3e82019faf77965ULL, 0x8152796902268ea2ULL, 0xe9219cd6881137a9ULL,
            0xc7da487463843f7aULL, 0xc65096369e752f3eULL, 0x70e35f7c3b9c42a1ULL,
    };
    for (size_t i = 0; i < images.size(); ++i) {
        const uint64_t actual = fnv1a64(images[i].bytes);
        EXPECT_EQ(actual, expected[i])
                << names[i] << " actual=0x" << std::hex << actual
                << "; the complete SNII image changed; never refresh only a local region";
    }
    const uint64_t docs_only_actual = fnv1a64(docs_only.bytes);
    EXPECT_EQ(docs_only_actual, expected.back())
            << names.back() << " actual=0x" << std::hex << docs_only_actual
            << "; the complete SNII image changed; never refresh only a local region";
    EXPECT_EQ(doris::snii::writer::testing::window_emitter_finished_terms(), 6U);
}

} // namespace
} // namespace doris::segment_v2
