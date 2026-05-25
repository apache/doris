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

#include "storage/index/inverted/spimi/skip_list_writer.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "storage/index/inverted/spimi/lucene_output.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Reuses the simple VInt/VLong decoder pattern from term_dict_writer_test.
class ByteReader {
public:
    explicit ByteReader(const std::vector<uint8_t>& bytes) : _bytes(bytes) {}

    uint8_t Byte() {
        EXPECT_LT(_pos, _bytes.size()) << "Read past end of buffer";
        return _bytes[_pos++];
    }

    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int32_t>(v);
    }

    int64_t ReadVLong() {
        uint64_t v = 0;
        uint64_t shift = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint64_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
        }
        return static_cast<int64_t>(v);
    }

    size_t remaining() const { return _bytes.size() - _pos; }

private:
    const std::vector<uint8_t>& _bytes;
    size_t _pos = 0;
};

} // namespace

TEST(SkipListWriterTest, ZeroDfWritesNothing) {
    SkipListWriter w(16, 10);
    w.Reset(0, /*freq=*/100, /*prox=*/200);
    EXPECT_EQ(w.NumberOfSkipLevels(), 0);

    MemoryLuceneOutput out;
    const int64_t skip_ptr = w.WriteSkip(&out);
    EXPECT_EQ(skip_ptr, 0);
    EXPECT_TRUE(out.bytes().empty());
}

TEST(SkipListWriterTest, SingleLevelEmitsOneEntryPerBoundary) {
    // skip_interval=4, df just enough to need 1 level but not 2.
    // levels = floor(log(df) / log(4)). Need df between 4 (incl.) and 16.
    SkipListWriter w(4, 10);
    w.Reset(8, /*freq=*/100, /*prox=*/200);
    EXPECT_EQ(w.NumberOfSkipLevels(), 1);

    // Simulate writing 8 docs; emit a skip entry at each multiple of
    // skip_interval. CLucene's IndexWriter emits skips at df = 4 and 8.
    w.SetSkipData(3, /*freq=*/110, /*prox=*/220);
    w.BufferSkip(4);
    w.SetSkipData(7, /*freq=*/120, /*prox=*/240);
    w.BufferSkip(8);

    MemoryLuceneOutput out;
    const int64_t skip_ptr = w.WriteSkip(&out);
    EXPECT_EQ(skip_ptr, 0);

    ByteReader r(out.bytes());
    // Level-0 only: 2 entries, each (doc_delta, freq_delta, prox_delta).
    EXPECT_EQ(r.ReadVInt(), 3);  // 3 - 0
    EXPECT_EQ(r.ReadVInt(), 10); // 110 - 100
    EXPECT_EQ(r.ReadVInt(), 20); // 220 - 200
    EXPECT_EQ(r.ReadVInt(), 4);  // 7 - 3
    EXPECT_EQ(r.ReadVInt(), 10); // 120 - 110
    EXPECT_EQ(r.ReadVInt(), 20); // 240 - 220
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SkipListWriterTest, TwoLevelsWritesLengthPrefixAndChildPointer) {
    // skip_interval=2, df=4 → levels = floor(log(4)/log(2)) = 2.
    SkipListWriter w(2, 10);
    w.Reset(4, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 2);

    // Boundaries at df=2 and df=4. df=2 → level-0 only. df=4 → level-0 and
    // level-1 (because 4 % 2 == 0 twice).
    w.SetSkipData(1, /*freq=*/8, /*prox=*/16);
    w.BufferSkip(2);
    w.SetSkipData(3, /*freq=*/24, /*prox=*/48);
    w.BufferSkip(4);

    MemoryLuceneOutput out;
    w.WriteSkip(&out);

    ByteReader r(out.bytes());
    // Output starts with level-N-1 down to level-1, each prefixed by VLong
    // length. Level-1 has one entry (doc_delta=3, freq_delta=24, prox_delta=48)
    // followed by the child pointer (the level-0 file pointer at the boundary
    // when this level-1 entry was written = end of level-0 second entry).
    const int64_t level1_length = r.ReadVLong();
    EXPECT_GT(level1_length, 0);

    EXPECT_EQ(r.ReadVInt(), 3);  // doc_delta
    EXPECT_EQ(r.ReadVInt(), 24); // freq_delta
    EXPECT_EQ(r.ReadVInt(), 48); // prox_delta
    const int64_t child_pointer = r.ReadVLong();
    EXPECT_GT(child_pointer, 0); // points into level-0 buffer

    // Then level-0: two entries.
    EXPECT_EQ(r.ReadVInt(), 1); // 1 - 0
    EXPECT_EQ(r.ReadVInt(), 8);
    EXPECT_EQ(r.ReadVInt(), 16);
    EXPECT_EQ(r.ReadVInt(), 2);  // 3 - 1
    EXPECT_EQ(r.ReadVInt(), 16); // 24 - 8
    EXPECT_EQ(r.ReadVInt(), 32); // 48 - 16
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SkipListWriterTest, ResetReusesLevelBuffers) {
    SkipListWriter w(2, 10);

    // First term: df=2 → 1 level.
    w.Reset(2, 0, 0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 1);
    w.SetSkipData(1, 4, 8);
    w.BufferSkip(2);
    MemoryLuceneOutput out1;
    w.WriteSkip(&out1);
    EXPECT_FALSE(out1.bytes().empty());

    // Second term: df=4 → 2 levels. Reset must scrub stale state from term 1.
    w.Reset(4, 100, 200);
    EXPECT_EQ(w.NumberOfSkipLevels(), 2);
    w.SetSkipData(1, 108, 216);
    w.BufferSkip(2);
    w.SetSkipData(3, 124, 248);
    w.BufferSkip(4);
    MemoryLuceneOutput out2;
    w.WriteSkip(&out2);

    ByteReader r(out2.bytes());
    const int64_t level1_length = r.ReadVLong();
    EXPECT_GT(level1_length, 0);

    // Level-1 entry: doc_delta=3 (3-0), freq_delta=24 (124-100), prox_delta=48
    // (248-200). Pointer deltas reset to the freq_pointer_start passed to
    // Reset().
    EXPECT_EQ(r.ReadVInt(), 3);
    EXPECT_EQ(r.ReadVInt(), 24);
    EXPECT_EQ(r.ReadVInt(), 48);
    (void)r.ReadVLong(); // child pointer

    // Level-0: two entries.
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 8);
    EXPECT_EQ(r.ReadVInt(), 16);
    EXPECT_EQ(r.ReadVInt(), 2);
    EXPECT_EQ(r.ReadVInt(), 16);
    EXPECT_EQ(r.ReadVInt(), 32);
    EXPECT_EQ(r.remaining(), 0U);
}

TEST(SkipListWriterTest, SkipPointerReflectsOutputOffset) {
    SkipListWriter w(4, 10);
    w.Reset(8, 0, 0);

    MemoryLuceneOutput out;
    // Pretend the output already has 17 bytes of "frq" data.
    const std::vector<uint8_t> filler(17, 0xAB);
    out.WriteBytes(filler.data(), filler.size());

    w.SetSkipData(3, 5, 10);
    w.BufferSkip(4);
    w.SetSkipData(7, 11, 22);
    w.BufferSkip(8);

    const int64_t skip_pointer = w.WriteSkip(&out);
    EXPECT_EQ(skip_pointer, 17) << "WriteSkip returns the file pointer at the moment "
                                   "skip data starts, used to compute term skip_offset";
    EXPECT_GT(out.FilePointer(), skip_pointer);
}

TEST(SkipListWriterTest, MaxSkipLevelsCapsTheLevelCount) {
    SkipListWriter w(2, /*max_skip_levels=*/3);
    // df=2^10=1024 would naturally allow 10 levels at skip_interval=2.
    w.Reset(1024, 0, 0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 3);
}

// SPIMI_DESIGN.md § 9.3 — exercise CLucene's production `skipInterval =
// 512` at the critical thresholds that flip the skip-level count.
// `levels = floor(log(df) / log(skipInterval))` per
// `ComputeNumberOfSkipLevels`. At skipInterval=512 the breakpoints are
// df=1 (0 levels, no skip data), df=512 (level 0 starts being useful at
// the first interval boundary), df=512² = 262144 (level 1), and df at
// 512³ would be level 2 — way beyond any realistic term in Doris.

TEST(SkipListWriterTest, DfBelowSkipIntervalProducesZeroLevels) {
    // df = 511 — one short of the first skip-interval boundary. With
    // skipInterval=512 and df=511, no skip block fires, and
    // ComputeNumberOfSkipLevels = floor(log(511) / log(512)) = 0.
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/511, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 0);

    // WriteSkip on an empty multi-level structure returns the current
    // output pointer (which is 0) and writes nothing.
    MemoryLuceneOutput out;
    const int64_t skip_ptr = w.WriteSkip(&out);
    EXPECT_EQ(skip_ptr, 0);
    EXPECT_EQ(out.bytes().size(), 0U) << "no skip data for df below skipInterval";
}

TEST(SkipListWriterTest, DfExactlySkipIntervalProducesOneLevel) {
    // df = 512: exactly at the boundary. CLucene's formula
    // floor(log(512)/log(512)) = 1 — one skip level. The first skip
    // entry would fire at the 512th doc inside `BufferSkip` (caller
    // responsibility); the writer side accepts the level count.
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/512, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 1);
}

TEST(SkipListWriterTest, DfJustAboveSkipIntervalStaysOneLevel) {
    // df = 513: one past the boundary — still 1 level (not 2; needs
    // 512² = 262144 to bump to level 2).
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/513, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 1);
}

TEST(SkipListWriterTest, DfAtDoubleBoundaryStaysOneLevel) {
    // df = 1024 = 2 × 512: still floor(log(1024)/log(512)) = 1.
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/1024, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 1);
}

TEST(SkipListWriterTest, DfAtSkipIntervalSquaredFlipsToTwoLevels) {
    // df = 512 * 512 = 262144 — the threshold where the skip list
    // grows a second level. floor(log(262144)/log(512)) = 2.
    // This is the test that would fail with the OLD float-based log
    // computation. The actual divergence threshold (below) catches
    // that regression directly; this test locks the canonical
    // skip_interval² endpoint.
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/262144, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 2);
}

TEST(SkipListWriterTest, DfOnePastSkipIntervalSquaredStillTwoLevels) {
    // df just past 512² — still 2 levels (not 3; needs 512³).
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/262145, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 2);
}

TEST(SkipListWriterTest, DfAtFloatVsDoubleDivergencePoint) {
    // Phase 25 fix: `ComputeNumberOfSkipLevels` switched
    // `std::log(static_cast<float>(df)) / std::log(static_cast<float>(
    // skip_interval))` → `double`. The named regression: float
    // precision loses bits near df ≈ skip_interval³, producing a
    // level count off by one vs CLucene's double-precision
    // implementation, which makes the SPIMI skip stream unreadable
    // by CLucene's reader.
    //
    // The boundary tests above (511 / 512 / 513 / 1024 / 262144 /
    // 262145) do NOT exercise this — float and double agree on all
    // six. The *empirically-observed* divergence at
    // skip_interval = 512 is at df = 134_217_727 (one below
    // 512³ = 134 217 728): double computes 2, float would compute 3.
    //
    // Lock the correct (double) answer here so a revert to float
    // fails this test directly.
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/134217727, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 2)
            << "Phase 25's float→double fix in ComputeNumberOfSkipLevels"
            << " must produce the same 2 levels at df = skip_interval³ - 1"
            << " that CLucene's double-precision Math.log does. A float"
            << " regression here would yield 3 and break the on-disk"
            << " format.";
}

TEST(SkipListWriterTest, DfAtSkipIntervalCubedFlipsToThreeLevels) {
    // df = 512³ = 134_217_728: the next boundary after which double
    // also says 3. Pinning this proves the formula respects the
    // exact-power boundary (no off-by-one at 512^k).
    SkipListWriter w(/*skip_interval=*/512, /*max_skip_levels=*/10);
    w.Reset(/*df=*/134217728, /*freq=*/0, /*prox=*/0);
    EXPECT_EQ(w.NumberOfSkipLevels(), 3);
}

} // namespace doris::segment_v2::inverted_index::spimi
