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

// Exercises `ValidateClosedSegmentByteCounts` — the post-close on-disk
// length self-check that guards against partial async flushes (the cloud
// P44–P46 bug pattern, where single-node tests passed but the S3 upload
// silently truncated a segment file).
//
// Strategy: drive a real `SpimiFulltextWriter` through a CLucene
// `RAMDirectory` (the same Directory abstraction `DorisFSDirectory` exposes
// in production), capture the per-stream byte counts, then exercise the
// validator under three scenarios:
//   1. Counts match on-disk lengths → validator returns cleanly.
//   2. Counts disagree with on-disk lengths (simulating truncation) → throws
//      INVERTED_INDEX_FILE_CORRUPTED.
//   3. Expected file is missing on the Directory → throws
//      INVERTED_INDEX_FILE_CORRUPTED.

#include <CLucene.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/index_output_byte_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

struct SegmentOutputs {
    lucene::store::IndexOutput* tis = nullptr;
    lucene::store::IndexOutput* tii = nullptr;
    lucene::store::IndexOutput* frq = nullptr;
    lucene::store::IndexOutput* prx = nullptr;
    lucene::store::IndexOutput* fnm = nullptr;
    lucene::store::IndexOutput* nrm = nullptr;
    lucene::store::IndexOutput* segments_n = nullptr;
    lucene::store::IndexOutput* segments_gen = nullptr;
};

constexpr const char* kTisName = "_0.tis";
constexpr const char* kTiiName = "_0.tii";
constexpr const char* kFrqName = "_0.frq";
constexpr const char* kPrxName = "_0.prx";
constexpr const char* kFnmName = "_0.fnm";
constexpr const char* kNrmName = "_0.nrm";
constexpr const char* kSegmentsNName = "segments_1";
constexpr const char* kSegmentsGenName = "segments.gen";

SpimiSegmentFileNames CanonicalNames(bool include_nrm) {
    return SpimiSegmentFileNames {.tis = kTisName,
                                  .tii = kTiiName,
                                  .frq = kFrqName,
                                  .prx = kPrxName,
                                  .fnm = kFnmName,
                                  .nrm = include_nrm ? kNrmName : nullptr,
                                  .segments_n = kSegmentsNName,
                                  .segments_gen = kSegmentsGenName};
}

SegmentOutputs OpenAllOutputs(lucene::store::RAMDirectory* dir) {
    SegmentOutputs out;
    out.tis = dir->createOutput(kTisName);
    out.tii = dir->createOutput(kTiiName);
    out.frq = dir->createOutput(kFrqName);
    out.prx = dir->createOutput(kPrxName);
    out.fnm = dir->createOutput(kFnmName);
    out.nrm = dir->createOutput(kNrmName);
    out.segments_n = dir->createOutput(kSegmentsNName);
    out.segments_gen = dir->createOutput(kSegmentsGenName);
    return out;
}

void CloseAll(SegmentOutputs& outs) {
    for (auto* o : {outs.tis, outs.tii, outs.frq, outs.prx, outs.fnm, outs.nrm, outs.segments_n,
                    outs.segments_gen}) {
        if (o != nullptr) {
            o->close();
            _CLDELETE(o);
        }
    }
}

// Drives a real SpimiFulltextWriter into a fresh RAMDirectory and returns
// the per-stream byte counts the writer recorded at EmitSegment time.
EmittedSegmentByteCounts EmitSampleSegment(lucene::store::RAMDirectory* dir, bool omit_norms) {
    SpimiPostingBuffer buffer;
    buffer.Append("alpha", 0, 0);
    buffer.Append("alpha", 1, 1);
    buffer.Append("beta", 1, 2);
    buffer.Append("gamma", 2, 0);

    SegmentOutputs outs = OpenAllOutputs(dir);
    EmittedSegmentByteCounts counts;
    {
        IndexOutputByteOutput a_tis(outs.tis);
        IndexOutputByteOutput a_tii(outs.tii);
        IndexOutputByteOutput a_frq(outs.frq);
        IndexOutputByteOutput a_prx(outs.prx);
        IndexOutputByteOutput a_fnm(outs.fnm);
        IndexOutputByteOutput a_nrm(outs.nrm);
        IndexOutputByteOutput a_sn(outs.segments_n);
        IndexOutputByteOutput a_sg(outs.segments_gen);
        SpimiSegmentSink sink {.tis = &a_tis,
                               .tii = &a_tii,
                               .frq = &a_frq,
                               .prx = &a_prx,
                               .fnm = &a_fnm,
                               .nrm = &a_nrm,
                               .segments_n = &a_sn,
                               .segments_gen = &a_sg};
        SpimiFulltextWriter::EmitSegment(buffer, sink, "_0", "body",
                                         /*doc_count=*/3, FieldInfosWriter::kIndexVersionV0,
                                         /*omit_term_freq_and_positions=*/false, omit_norms,
                                         &counts);
    }
    CloseAll(outs);
    return counts;
}

} // namespace

// ---- happy path ----------------------------------------------------------

TEST(SpimiSegmentByteCountsValidatorTest, PassesWhenAllFileLengthsMatch) {
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts = EmitSampleSegment(&dir, /*omit_norms=*/false);
    // Sanity: writer recorded non-zero counts for every stream we expect.
    EXPECT_GT(counts.tis, 0);
    EXPECT_GT(counts.frq, 0);
    EXPECT_GT(counts.fnm, 0);
    EXPECT_GT(counts.nrm, 0);
    EXPECT_GT(counts.segments_n, 0);
    EXPECT_GT(counts.segments_gen, 0);
    EXPECT_NO_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/true), counts));
}

TEST(SpimiSegmentByteCountsValidatorTest, OmitNormsSkipsNrmLengthCheck) {
    lucene::store::RAMDirectory dir;
    // omit_norms=true → EmitSegment leaves counts.nrm at 0 and the validator
    // must NOT verify the .nrm file's existence (V4 production path).
    EmittedSegmentByteCounts counts = EmitSampleSegment(&dir, /*omit_norms=*/true);
    EXPECT_EQ(counts.nrm, 0);
    // Even though the .nrm name is wired into seg_names, the validator should
    // short-circuit on the zero expected length.
    EXPECT_NO_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/true), counts));
}

// ---- truncation / mismatch ----------------------------------------------

TEST(SpimiSegmentByteCountsValidatorTest, ThrowsOnTruncatedTis) {
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts = EmitSampleSegment(&dir, /*omit_norms=*/false);
    // Simulate the async-flush partial-write failure: claim the writer
    // recorded one more byte than the directory actually has. The validator
    // must reject — this is the exact pattern of the cloud P44–P46 bugs.
    counts.tis += 1;
    EXPECT_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/true), counts),
            doris::Exception);
}

TEST(SpimiSegmentByteCountsValidatorTest, ThrowsOnTruncatedFrq) {
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts = EmitSampleSegment(&dir, /*omit_norms=*/false);
    counts.frq += 7; // arbitrary non-zero delta
    EXPECT_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/true), counts),
            doris::Exception);
}

TEST(SpimiSegmentByteCountsValidatorTest, ThrowsOnTruncatedSegmentsManifest) {
    // segments_N corruption is the highest-blast-radius case: a half-written
    // manifest causes the reader to lose the whole segment, not just one
    // stream.
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts = EmitSampleSegment(&dir, /*omit_norms=*/false);
    counts.segments_n += 1;
    EXPECT_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/true), counts),
            doris::Exception);
}

// ---- missing file --------------------------------------------------------

TEST(SpimiSegmentByteCountsValidatorTest, ThrowsWhenExpectedFileMissing) {
    // No EmitSegment — Directory is empty. As long as expected counts are
    // non-zero the validator must call fileExists for each and throw on the
    // first missing entry.
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts;
    counts.tis = 100; // pretend we wrote 100 bytes of .tis
    EXPECT_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/false), counts),
            doris::Exception);
}

TEST(SpimiSegmentByteCountsValidatorTest, ZeroExpectedSkipsCheck) {
    // All zero expected counts → validator is a no-op even on an empty
    // directory. (Effectively asserts the contract that a 0-byte expected
    // count means "this stream was not written this run".)
    lucene::store::RAMDirectory dir;
    EmittedSegmentByteCounts counts;
    EXPECT_NO_THROW(
            ValidateClosedSegmentByteCounts(&dir, CanonicalNames(/*include_nrm=*/false), counts));
}

} // namespace doris::segment_v2::inverted_index::spimi
