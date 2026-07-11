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

// GOLDEN BYTE pins for the SNII writer token path (T1a guard). Each test
// writes ONE segment from a FIXED corpus through the production writer stack
// and asserts the output file's FNV-1a-64 digest against a recorded constant
// (harvested from the pre-T1a materializing token path). Any change to
// tokenization semantics, position accounting, bigram emission, ignore_above /
// empty-value handling, or on-disk encoding flips the digest -- the T1a
// streaming rewrite must keep every digest EXACTLY.
//
// The corpus deliberately hits the edge lanes: empty value (analyzed: skipped;
// keyword: a VALID empty token), punctuation-only row (zero analyzed tokens),
// >ignore_above value (keyword: skipped row), long token, repeated terms
// (position increments + bigram df), unicode/mixed text, multiple add_values
// batches, and interleaved add_nulls runs.
//
// If a digest changes INTENTIONALLY (format or analyzer change), re-harvest by
// running the test and copying the "actual=" value from the failure message --
// and say so loudly in the commit message.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/snii_index_writer.h"
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
    SniiIndexColumnWriter writer(&index_file_writer, &meta, /*single_field=*/true);
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
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        // Pin every config the write path reads, so the digests do not move
        // under future default changes (the golden guards the TOKEN PATH).
        _saved_defer = config::snii_bigram_defer_build_to_compaction;
        _saved_prune_min = config::snii_bigram_prune_min_df;
        _saved_prune_max = config::snii_bigram_prune_max_df_ratio;
        _saved_dict_lvl = config::snii_dict_block_zstd_level;
        _saved_prx_lvl = config::snii_prx_zstd_level;
        _saved_prx_load_lvl = config::snii_prx_zstd_level_direct_load;
        config::snii_bigram_defer_build_to_compaction = false;
        config::snii_bigram_prune_min_df = -1; // auto (default)
        config::snii_bigram_prune_max_df_ratio = 0.25;
        config::snii_dict_block_zstd_level = 3;
        config::snii_prx_zstd_level = 3;
        config::snii_prx_zstd_level_direct_load = 3;
    }

    void TearDown() override {
        config::snii_bigram_defer_build_to_compaction = _saved_defer;
        config::snii_bigram_prune_min_df = _saved_prune_min;
        config::snii_bigram_prune_max_df_ratio = _saved_prune_max;
        config::snii_dict_block_zstd_level = _saved_dict_lvl;
        config::snii_prx_zstd_level = _saved_prx_lvl;
        config::snii_prx_zstd_level_direct_load = _saved_prx_load_lvl;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

private:
    bool _saved_defer = false;
    int32_t _saved_prune_min = -1;
    double _saved_prune_max = 0.25;
    int32_t _saved_dict_lvl = 3;
    int32_t _saved_prx_lvl = 3;
    int32_t _saved_prx_load_lvl = 3;
};

// Harvested from the pre-T1a materializing token path (vector<TermInfo> +
// per-token std::string). The T1a streaming path must reproduce them exactly.
constexpr uint64_t kGoldenEnglishPhrase = 0x09437a1fdef5f893ULL;
constexpr uint64_t kGoldenUnicodePhrase = 0xc096270a7dce3767ULL;
constexpr uint64_t kGoldenKeywordDocsOnly = 0x77f7ac39f81ab366ULL;

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

} // namespace
} // namespace doris::segment_v2
