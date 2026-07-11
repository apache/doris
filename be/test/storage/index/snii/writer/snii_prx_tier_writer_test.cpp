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

// SNII prx-tier writer tests (patch C, see
// config::snii_prx_zstd_level_direct_load): a DIRECT load compresses the prx
// region at the cheaper load-tier zstd level, everything else keeps
// snii_prx_zstd_level. Contract pinned here:
//   1. The tier only changes prx BYTES, never SEMANTICS: a direct segment and
//      its full-level twin answer position-dependent queries identically.
//   2. Non-direct paths (no hint / explicit not-direct) ignore the load-tier
//      config completely -- byte-identical outputs whatever its value, so
//      compaction / schema change / ADD INDEX segments are untouched.
//   3. The level is read at flush (same semantics as snii_prx_zstd_level): a
//      mid-load change lands on the in-flight segment; the direct-load BIT
//      itself stays captured-once (pinned by the defer writer tests).
//   4. The load-tier level is clamped to [3, 19] like the base level.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 7;
constexpr const char* kTestDir = "./ut_dir/snii_prx_tier_writer_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// Positions-capable fulltext index: the only shape with a prx region to tier.
void init_phrase_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("prx_tier_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
}

enum class DirectLoadHint { kNone, kDirect, kNotDirect };

// Same production call order as the defer writer tests: init() before the
// segment writer's set_direct_load, which precedes every row. `flip_after_hint`
// (optional) hot-changes snii_prx_zstd_level_direct_load right after the hint
// -- the earliest instant a live mInt32 change can land -- and keeps it through
// finish(), pinning the level's flush-read semantics.
void write_segment(const std::string& path, const TabletIndex& meta,
                   const std::vector<std::string>& rows, DirectLoadHint hint,
                   const int32_t* flip_after_hint = nullptr) {
    io::FileWriterPtr file_writer;
    assert_ok(io::global_local_filesystem()->create_file(path, &file_writer));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), path, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/300);

    SniiIndexColumnWriter writer(&index_file_writer, &meta, /*single_field=*/true);
    assert_ok(writer.init());
    if (hint == DirectLoadHint::kDirect) {
        writer.set_direct_load(true);
    } else if (hint == DirectLoadHint::kNotDirect) {
        writer.set_direct_load(false);
    }
    if (flip_after_hint != nullptr) {
        config::snii_prx_zstd_level_direct_load = *flip_after_hint;
    }

    std::vector<Slice> slices;
    slices.reserve(rows.size());
    for (const std::string& row : rows) {
        slices.emplace_back(row);
    }
    assert_ok(writer.add_values("c1", slices.data(), slices.size()));
    assert_ok(writer.finish());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
}

// 400 rows x 30 tokens over a 40-word vocabulary with a deterministic pattern:
// enough position payload for the zstd level to visibly change the prx bytes,
// repetitive enough that higher levels find more to squeeze.
std::vector<std::string> positional_rows() {
    static const char* kVocab[] = {
            "alpha", "beta",  "gamma", "delta", "epsil", "zeta",  "eta",   "theta", "iota", "kappa",
            "lam",   "mu",    "nu",    "xi",    "omic",  "pi",    "rho",   "sigma", "tau",  "upsil",
            "phi",   "chi",   "psi",   "omega", "one",   "two",   "three", "four",  "five", "six",
            "seven", "eight", "nine",  "ten",   "red",   "green", "blue",  "cyan",  "lime", "teal"};
    constexpr size_t kVocabSize = sizeof(kVocab) / sizeof(kVocab[0]);
    std::vector<std::string> rows;
    rows.reserve(400);
    for (uint32_t i = 0; i < 400; ++i) {
        std::string row;
        for (uint32_t j = 0; j < 30; ++j) {
            if (j > 0) {
                row += ' ';
            }
            row += kVocab[(i * 31 + j * 7 + (i * j) % 5) % kVocabSize];
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

void open_index(const std::string& path, ::doris::snii::io::LocalFileReader* file,
                ::doris::snii::reader::SniiSegmentReader* segment,
                ::doris::snii::reader::LogicalIndexReader* idx) {
    assert_ok(file->open(path));
    assert_ok(::doris::snii::reader::SniiSegmentReader::open(file, segment));
    assert_ok(segment->open_index(static_cast<uint64_t>(kIndexId), /*index_suffix=*/"", idx));
}

std::vector<uint32_t> run_phrase(const ::doris::snii::reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    EXPECT_TRUE(::doris::snii::query::phrase_query(idx, terms, &docids).ok());
    return docids;
}

std::string read_file_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << path;
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

class SniiPrxTierWriterTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        _saved_prx_level = config::snii_prx_zstd_level;
        _saved_load_level = config::snii_prx_zstd_level_direct_load;
        _saved_defer = config::snii_bigram_defer_build_to_compaction;
        config::snii_bigram_defer_build_to_compaction = false; // isolate the tier
        init_phrase_index_meta(&_meta);
    }

    void TearDown() override {
        config::snii_prx_zstd_level = _saved_prx_level;
        config::snii_prx_zstd_level_direct_load = _saved_load_level;
        config::snii_bigram_defer_build_to_compaction = _saved_defer;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    std::string test_path(const std::string& name) const {
        return std::string(kTestDir) + "/" + name + ".idx";
    }

    TabletIndex _meta;

private:
    int32_t _saved_prx_level = 9;
    int32_t _saved_load_level = 3;
    bool _saved_defer = false;
};

// Contract 1: the tier changes prx bytes, never query semantics. Widest level
// split (base 19 vs load 3) so the size delta cannot vanish into zstd noise.
TEST_F(SniiPrxTierWriterTest, DirectLoadUsesLoadTierPrxLevelWithIdenticalAnswers) {
    const std::vector<std::string> rows = positional_rows();
    config::snii_prx_zstd_level = 19;
    config::snii_prx_zstd_level_direct_load = 3;

    const std::string full_path = test_path("full_level");
    const std::string direct_path = test_path("direct_level");
    write_segment(full_path, _meta, rows, DirectLoadHint::kNone);
    write_segment(direct_path, _meta, rows, DirectLoadHint::kDirect);

    const std::string full_bytes = read_file_bytes(full_path);
    const std::string direct_bytes = read_file_bytes(direct_path);
    // Level 3 compresses the prx region less than level 19: the direct segment
    // must be strictly larger (this is ALSO the observable proving the tier
    // actually took effect -- there is no level field in the format).
    EXPECT_GT(direct_bytes.size(), full_bytes.size());

    ::doris::snii::io::LocalFileReader full_file, direct_file;
    ::doris::snii::reader::SniiSegmentReader full_segment, direct_segment;
    ::doris::snii::reader::LogicalIndexReader full_idx, direct_idx;
    open_index(full_path, &full_file, &full_segment, &full_idx);
    open_index(direct_path, &direct_file, &direct_segment, &direct_idx);

    // Position-dependent answers must be identical and non-trivial across
    // several adjacent pairs of the generated pattern.
    bool any_nonempty = false;
    for (const auto& phrase : std::vector<std::vector<std::string>> {
                 {"alpha", "theta"}, {"kappa", "sigma"}, {"one", "eight"}, {"red", "cyan"}}) {
        const std::vector<uint32_t> expect = run_phrase(full_idx, phrase);
        EXPECT_EQ(run_phrase(direct_idx, phrase), expect) << phrase[0] << ' ' << phrase[1];
        any_nonempty |= !expect.empty();
    }
    EXPECT_TRUE(any_nonempty) << "test corpus produced no phrase matches -- assertions vacuous";
}

// Contract 2: non-direct paths ignore the load-tier config completely.
TEST_F(SniiPrxTierWriterTest, NonDirectPathsIgnoreLoadTierLevel) {
    const std::vector<std::string> rows = positional_rows();
    config::snii_prx_zstd_level = 9;

    config::snii_prx_zstd_level_direct_load = 3;
    const std::string none_lo = test_path("none_lo");
    const std::string notdirect_lo = test_path("notdirect_lo");
    write_segment(none_lo, _meta, rows, DirectLoadHint::kNone);
    write_segment(notdirect_lo, _meta, rows, DirectLoadHint::kNotDirect);

    config::snii_prx_zstd_level_direct_load = 19;
    const std::string none_hi = test_path("none_hi");
    write_segment(none_hi, _meta, rows, DirectLoadHint::kNone);

    const std::string baseline = read_file_bytes(none_lo);
    ASSERT_FALSE(baseline.empty());
    EXPECT_EQ(read_file_bytes(notdirect_lo), baseline); // explicit not-direct == no hint
    EXPECT_EQ(read_file_bytes(none_hi), baseline);      // load-tier value is inert here
}

// Contract 3: the LEVEL is read at flush (same semantics as
// snii_prx_zstd_level): a change landing after the captured hint but before
// finish() takes effect -- equal to the constant-config twin of the NEW value.
TEST_F(SniiPrxTierWriterTest, MidLoadLevelChangeLandsAtFlush) {
    const std::vector<std::string> rows = positional_rows();
    config::snii_prx_zstd_level = 9;

    config::snii_prx_zstd_level_direct_load = 9;
    const std::string ref9 = test_path("flip_ref9");
    write_segment(ref9, _meta, rows, DirectLoadHint::kDirect);

    config::snii_prx_zstd_level_direct_load = 3; // captured hint under level 3 ...
    const int32_t flip_to = 9;                   // ... flipped to 9 before any row
    const std::string flipped = test_path("flip_to9");
    write_segment(flipped, _meta, rows, DirectLoadHint::kDirect, &flip_to);

    EXPECT_EQ(read_file_bytes(flipped), read_file_bytes(ref9));
}

// Contract 4: the load-tier level is clamped to [3, 19].
TEST_F(SniiPrxTierWriterTest, LoadTierLevelClamped) {
    const std::vector<std::string> rows = positional_rows();
    config::snii_prx_zstd_level = 9;

    config::snii_prx_zstd_level_direct_load = 1; // below floor -> 3
    const std::string lvl_under = test_path("clamp_under");
    write_segment(lvl_under, _meta, rows, DirectLoadHint::kDirect);
    config::snii_prx_zstd_level_direct_load = 3;
    const std::string lvl_floor = test_path("clamp_floor");
    write_segment(lvl_floor, _meta, rows, DirectLoadHint::kDirect);
    EXPECT_EQ(read_file_bytes(lvl_under), read_file_bytes(lvl_floor));

    config::snii_prx_zstd_level_direct_load = 25; // above ceiling -> 19
    const std::string lvl_over = test_path("clamp_over");
    write_segment(lvl_over, _meta, rows, DirectLoadHint::kDirect);
    config::snii_prx_zstd_level_direct_load = 19;
    const std::string lvl_ceil = test_path("clamp_ceil");
    write_segment(lvl_ceil, _meta, rows, DirectLoadHint::kDirect);
    EXPECT_EQ(read_file_bytes(lvl_over), read_file_bytes(lvl_ceil));
}

} // namespace
} // namespace doris::segment_v2
