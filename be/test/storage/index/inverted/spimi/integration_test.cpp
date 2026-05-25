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

// End-to-end integration test for the SPIMI write path. Mimics what the
// integrated InvertedIndexColumnWriter will do (Phase 16 follow-up):
//   1. Open seven outputs in a CLucene `RAMDirectory` (the same abstraction
//      `DorisFSDirectory` exposes in production).
//   2. Wrap each output with `IndexOutputLuceneOutput` so SPIMI's writers
//      treat them as portable `LuceneOutput` sinks.
//   3. Feed synthetic token streams through `SpimiFulltextWriter`.
//   4. Finalize the segment and confirm every output is non-empty and the
//      headers / counts on each stream agree with what was added.
//
// The byte-level guarantees vs CLucene's writer are covered by the
// term-dict differential (clucene_term_dict_differential_test.cpp); this
// test validates the higher-level orchestration is wired correctly through
// the IndexOutput adapter end-to-end.

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/index_output_lucene_output.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

struct SegmentOutputs {
    lucene::store::IndexOutput* tis = nullptr;
    lucene::store::IndexOutput* tii = nullptr;
    lucene::store::IndexOutput* frq = nullptr;
    lucene::store::IndexOutput* prx = nullptr;
    lucene::store::IndexOutput* fnm = nullptr;
    lucene::store::IndexOutput* segments_n = nullptr;
    lucene::store::IndexOutput* segments_gen = nullptr;
};

SegmentOutputs OpenSegmentOutputs(lucene::store::RAMDirectory* dir,
                                  const std::string& segment_name) {
    SegmentOutputs out;
    out.tis = dir->createOutput((segment_name + ".tis").c_str());
    out.tii = dir->createOutput((segment_name + ".tii").c_str());
    out.frq = dir->createOutput((segment_name + ".frq").c_str());
    out.prx = dir->createOutput((segment_name + ".prx").c_str());
    out.fnm = dir->createOutput((segment_name + ".fnm").c_str());
    out.segments_n = dir->createOutput("segments_1");
    out.segments_gen = dir->createOutput("segments.gen");
    return out;
}

void CloseSegmentOutputs(SegmentOutputs& out) {
    for (auto* o :
         {out.tis, out.tii, out.frq, out.prx, out.fnm, out.segments_n, out.segments_gen}) {
        if (o != nullptr) {
            o->close();
            _CLDELETE(o);
        }
    }
}

std::vector<uint8_t> SlurpRamFile(lucene::store::RAMDirectory* dir, const char* name) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError err;
    EXPECT_TRUE(dir->openInput(name, in, err)) << "openInput(" << name << ") failed";
    if (in == nullptr) {
        return {};
    }
    const int64_t size = in->length();
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    if (size > 0) {
        in->readBytes(bytes.data(), size);
    }
    in->close();
    _CLDELETE(in);
    return bytes;
}

class BytesReader {
public:
    explicit BytesReader(const std::vector<uint8_t>& b) : _b(b) {}
    uint8_t Byte() {
        EXPECT_LT(_p, _b.size());
        return _b[_p++];
    }
    int32_t ReadInt() {
        int32_t v = (Byte() << 24);
        v |= (Byte() << 16);
        v |= (Byte() << 8);
        v |= Byte();
        return v;
    }
    int64_t ReadLong() {
        const auto hi = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        const auto lo = static_cast<int64_t>(static_cast<uint32_t>(ReadInt()));
        return (hi << 32) | lo;
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t s = 0;
        while (true) {
            const uint8_t b = Byte();
            v |= static_cast<uint32_t>(b & 0x7FU) << s;
            if ((b & 0x80U) == 0) {
                break;
            }
            s += 7;
        }
        return static_cast<int32_t>(v);
    }
    size_t pos() const { return _p; }
    size_t size() const { return _b.size(); }

private:
    const std::vector<uint8_t>& _b;
    size_t _p = 0;
};

// Runs the full segment-emit and returns the seven on-disk byte streams.
struct EmittedBytes {
    std::vector<uint8_t> tis, tii, frq, prx, fnm, segments_n, segments_gen;
    int64_t term_count = 0;
    int32_t doc_count = 0;
};

EmittedBytes EmitSegmentViaSpimi(
        const std::vector<std::pair<uint32_t, std::vector<std::string>>>& docs_with_tokens,
        const std::string& segment_name = "_0", const std::string& field_name = "body") {
    lucene::store::RAMDirectory ram;
    auto outs = OpenSegmentOutputs(&ram, segment_name);
    {
        IndexOutputLuceneOutput a_tis(outs.tis);
        IndexOutputLuceneOutput a_tii(outs.tii);
        IndexOutputLuceneOutput a_frq(outs.frq);
        IndexOutputLuceneOutput a_prx(outs.prx);
        IndexOutputLuceneOutput a_fnm(outs.fnm);
        IndexOutputLuceneOutput a_sn(outs.segments_n);
        IndexOutputLuceneOutput a_sg(outs.segments_gen);
        SpimiSegmentSink sink {.tis = &a_tis,
                               .tii = &a_tii,
                               .frq = &a_frq,
                               .prx = &a_prx,
                               .fnm = &a_fnm,
                               .segments_n = &a_sn,
                               .segments_gen = &a_sg};
        SpimiFulltextWriter writer(sink, segment_name, field_name);
        for (const auto& [doc_id, tokens] : docs_with_tokens) {
            for (size_t i = 0; i < tokens.size(); ++i) {
                writer.AddOccurrence(doc_id, tokens[i], static_cast<uint32_t>(i));
            }
        }
        writer.Finish();
        EmittedBytes em;
        em.term_count = writer.TermCount();
        em.doc_count = writer.DocCount();
        CloseSegmentOutputs(outs);
        em.tis = SlurpRamFile(&ram, (segment_name + ".tis").c_str());
        em.tii = SlurpRamFile(&ram, (segment_name + ".tii").c_str());
        em.frq = SlurpRamFile(&ram, (segment_name + ".frq").c_str());
        em.prx = SlurpRamFile(&ram, (segment_name + ".prx").c_str());
        em.fnm = SlurpRamFile(&ram, (segment_name + ".fnm").c_str());
        em.segments_n = SlurpRamFile(&ram, "segments_1");
        em.segments_gen = SlurpRamFile(&ram, "segments.gen");
        return em;
    }
}

} // namespace

TEST(SpimiIntegrationTest, ProducesAllSevenStreamsThroughCLuceneIndexOutput) {
    const auto em = EmitSegmentViaSpimi({
            {0, {"hello", "world"}},
            {1, {"hello", "spimi"}},
            {2, {"world", "spimi", "spimi"}},
    });
    EXPECT_EQ(em.term_count, 3); // hello, spimi, world
    EXPECT_EQ(em.doc_count, 3);  // docs 0, 1, 2 ⇒ doc_count 3
    EXPECT_FALSE(em.tis.empty());
    EXPECT_FALSE(em.tii.empty());
    EXPECT_FALSE(em.frq.empty());
    EXPECT_FALSE(em.prx.empty());
    EXPECT_FALSE(em.fnm.empty());
    EXPECT_FALSE(em.segments_n.empty());
    EXPECT_FALSE(em.segments_gen.empty());
}

TEST(SpimiIntegrationTest, FnmRecordsConfiguredFieldName) {
    const auto em = EmitSegmentViaSpimi({{0, {"x"}}}, "_seg", "title");
    BytesReader r(em.fnm);
    EXPECT_EQ(r.ReadVInt(), 1);
    EXPECT_EQ(r.ReadVInt(), 5); // "title" length in wide chars
    EXPECT_EQ(r.Byte(), 't');
    EXPECT_EQ(r.Byte(), 'i');
    EXPECT_EQ(r.Byte(), 't');
    EXPECT_EQ(r.Byte(), 'l');
    EXPECT_EQ(r.Byte(), 'e');
    const uint8_t bits = r.Byte();
    EXPECT_NE(bits & FieldInfosWriter::kIsIndexed, 0U);
    EXPECT_NE(bits & FieldInfosWriter::kTermFreqAndPositions, 0U);
}

TEST(SpimiIntegrationTest, SegmentsNRecordsCorrectDocCount) {
    const auto em = EmitSegmentViaSpimi({
            {0, {"a"}}, {5, {"b"}}, {9, {"a", "b"}}, // max doc id = 9
    });
    EXPECT_EQ(em.doc_count, 10);

    BytesReader r(em.segments_n);
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatSharedDocStore);
    (void)r.ReadLong();         // version
    EXPECT_EQ(r.ReadInt(), 1);  // counter
    EXPECT_EQ(r.ReadInt(), 1);  // segment count
    EXPECT_EQ(r.ReadVInt(), 2); // segment name length "_0"
    (void)r.Byte();
    (void)r.Byte();
    EXPECT_EQ(r.ReadInt(), 10); // doc_count
}

TEST(SpimiIntegrationTest, SegmentsGenIsThreeFixedFields) {
    const auto em = EmitSegmentViaSpimi({{0, {"x"}}});
    BytesReader r(em.segments_gen);
    EXPECT_EQ(r.ReadInt(), SegmentInfosWriter::kFormatLockless);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.ReadLong(), 1);
    EXPECT_EQ(r.pos(), r.size());
}

TEST(SpimiIntegrationTest, RamDirectoryRoundTripsByteIdenticallyToMemoryOutputs) {
    // Same input through both the IndexOutput-backed path and a parallel
    // MemoryLuceneOutput path; the four primary streams must be byte equal.
    const std::vector<std::pair<uint32_t, std::vector<std::string>>> docs = {
            {0, {"apple", "banana"}},
            {1, {"apple", "cherry"}},
            {2, {"banana", "cherry", "fig"}},
    };

    // CLucene-backed path.
    const auto disk = EmitSegmentViaSpimi(docs);

    // Reference: same writer, MemoryLuceneOutput sinks.
    MemoryLuceneOutput m_tis, m_tii, m_frq, m_prx, m_fnm, m_sn, m_sg;
    SpimiSegmentSink ref_sink {.tis = &m_tis,
                               .tii = &m_tii,
                               .frq = &m_frq,
                               .prx = &m_prx,
                               .fnm = &m_fnm,
                               .segments_n = &m_sn,
                               .segments_gen = &m_sg};
    SpimiFulltextWriter ref(ref_sink, "_0", "body");
    for (const auto& [doc_id, tokens] : docs) {
        for (size_t i = 0; i < tokens.size(); ++i) {
            ref.AddOccurrence(doc_id, tokens[i], static_cast<uint32_t>(i));
        }
    }
    ref.Finish();

    EXPECT_EQ(disk.tis, m_tis.bytes());
    EXPECT_EQ(disk.tii, m_tii.bytes());
    EXPECT_EQ(disk.frq, m_frq.bytes());
    EXPECT_EQ(disk.prx, m_prx.bytes());
    EXPECT_EQ(disk.fnm, m_fnm.bytes());
    EXPECT_EQ(disk.segments_n, m_sn.bytes());
    EXPECT_EQ(disk.segments_gen, m_sg.bytes());
}

} // namespace doris::segment_v2::inverted_index::spimi
