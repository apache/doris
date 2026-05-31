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

#include "storage/index/inverted/spimi/query_index_reader.h"

#include <gtest/gtest.h>

// clang-format off
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif
// clang-format on

#include <memory>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// End-to-end fixture: writes a SPIMI segment via the real
// SegmentWriter + FieldInfosWriter, then constructs the
// SpimiQueryIndexReader on the resulting bytes. Tests assert
// CLucene's IndexReader contract holds for SPIMI segments.
struct IndexReaderFixture {
    MemoryByteOutput tis;
    MemoryByteOutput tii;
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    MemoryByteOutput fnm;

    void Build(const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& posts,
               int32_t max_doc, std::string_view field_name = "body") {
        FieldInfoEntry fi {.name = std::string(field_name),
                           .is_indexed = true,
                           .omit_norms = true,
                           .has_prox = true};
        FieldInfosWriter(&fnm).Write({fi});

        SpimiPostingBuffer buffer;
        for (const auto& [term, doc, pos] : posts) {
            buffer.Append(term, doc, pos);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
        _max_doc = max_doc;
    }

    // Stages `bytes` into the fixture's RAMDirectory under `name` and opens it
    // as a template IndexInput. The RAMDirectory is held by the fixture so it
    // outlives the reader. `openInput` rejects a zero-length file.
    lucene::store::IndexInput* StageAndOpen(const char* name, const std::vector<uint8_t>& bytes) {
        auto* out = _ram.createOutput(name);
        if (!bytes.empty()) {
            out->writeBytes(bytes.data(), static_cast<int32_t>(bytes.size()));
        }
        out->close();
        _CLDELETE(out);
        lucene::store::IndexInput* in = nullptr;
        CLuceneError err;
        EXPECT_TRUE(_ram.openInput(name, in, err));
        return in;
    }

    std::unique_ptr<SpimiQueryIndexReader> Make() {
        // The reader now takes `.frq` AND `.prx` as OPEN template IndexInputs
        // (not slurped). Every test term here has postings + positions.
        OwnedFrqInput frq_owned(StageAndOpen("_0.frq", frq.bytes()));
        OwnedPrxInput prx_owned(StageAndOpen("_0.prx", prx.bytes()));
        // Pass nullptr for the directory: the fixture owns `_ram` on the stack
        // and keeps it alive past the reader, so no extra ref is needed.
        return std::make_unique<SpimiQueryIndexReader>(
                tis.bytes(), tii.bytes(), std::move(frq_owned), std::move(prx_owned),
                /*directory=*/nullptr, fnm.bytes(), _max_doc);
    }

    lucene::store::RAMDirectory _ram;
    int32_t _max_doc = 0;
};

} // namespace

TEST(SpimiQueryIndexReaderTest, ReportsBasicMetadata) {
    IndexReaderFixture fx;
    fx.Build({{"alpha", 0, 0}, {"alpha", 1, 0}, {"beta", 2, 0}}, /*max_doc=*/3);
    auto reader = fx.Make();
    EXPECT_EQ(reader->maxDoc(), 3);
    EXPECT_EQ(reader->numDocs(), 3);
    EXPECT_FALSE(reader->hasDeletions());
    EXPECT_FALSE(reader->isDeleted(0));
    EXPECT_FALSE(reader->isDeleted(2));
    EXPECT_NE(reader->getFieldInfos(), nullptr);
}

TEST(SpimiQueryIndexReaderTest, DocFreqLookup) {
    IndexReaderFixture fx;
    fx.Build({{"alpha", 0, 0}, {"alpha", 1, 0}, {"alpha", 2, 0}, {"beta", 3, 0}}, /*max_doc=*/4);
    auto reader = fx.Make();

    auto* alpha = _CLNEW lucene::index::Term(L"body", L"alpha");
    EXPECT_EQ(reader->docFreq(alpha), 3);
    _CLDECDELETE(alpha);

    auto* beta = _CLNEW lucene::index::Term(L"body", L"beta");
    EXPECT_EQ(reader->docFreq(beta), 1);
    _CLDECDELETE(beta);

    auto* missing = _CLNEW lucene::index::Term(L"body", L"missing");
    EXPECT_EQ(reader->docFreq(missing), 0);
    _CLDECDELETE(missing);
}

TEST(SpimiQueryIndexReaderTest, TermsEnumeratesEverything) {
    IndexReaderFixture fx;
    fx.Build({{"alpha", 0, 0}, {"beta", 1, 0}, {"gamma", 2, 0}}, /*max_doc=*/3);
    auto reader = fx.Make();

    auto* en = reader->terms();
    ASSERT_NE(en, nullptr);
    std::vector<std::wstring> got;
    while (en->next()) {
        got.push_back(en->term(/*pointer=*/false)->text());
    }
    en->close();
    delete en;

    EXPECT_EQ(got, (std::vector<std::wstring> {L"alpha", L"beta", L"gamma"}));
}

TEST(SpimiQueryIndexReaderTest, TermsTargetSkipsForward) {
    IndexReaderFixture fx;
    fx.Build({{"alpha", 0, 0}, {"beta", 1, 0}, {"gamma", 2, 0}, {"delta", 3, 0}}, /*max_doc=*/4);
    auto reader = fx.Make();

    auto* target = _CLNEW lucene::index::Term(L"body", L"delta");
    auto* en = reader->terms(target);
    // Order is alpha < beta < delta < gamma. After skipTo("delta"),
    // the enum's current term is "delta".
    ASSERT_NE(en, nullptr);
    EXPECT_EQ(std::wstring(en->term(false)->text()), std::wstring(L"delta"));
    en->close();
    delete en;
    _CLDECDELETE(target);
}

TEST(SpimiQueryIndexReaderTest, TermDocsReturnsPostings) {
    IndexReaderFixture fx;
    fx.Build({{"x", 0, 0}, {"x", 5, 0}, {"x", 5, 3}, {"x", 9, 0}}, /*max_doc=*/10);
    auto reader = fx.Make();

    auto* td = reader->termDocs();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 3);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 0);
    EXPECT_EQ(td->freq(), 1);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 5);
    EXPECT_EQ(td->freq(), 2);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 9);
    EXPECT_FALSE(td->next());
    td->close();
    delete td;
    _CLDECDELETE(term);
}

TEST(SpimiQueryIndexReaderTest, TermPositionsReturnsPositionsAndDocs) {
    IndexReaderFixture fx;
    fx.Build({{"x", 0, 1}, {"x", 0, 4}, {"x", 1, 7}}, /*max_doc=*/2);
    auto reader = fx.Make();

    auto* tp = reader->termPositions();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);
    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 0);
    EXPECT_EQ(tp->freq(), 2);
    EXPECT_EQ(tp->nextPosition(), 1);
    EXPECT_EQ(tp->nextPosition(), 4);
    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 1);
    EXPECT_EQ(tp->nextPosition(), 7);
    tp->close();
    delete tp;
    _CLDECDELETE(term);
}

TEST(SpimiQueryIndexReaderTest, NormsReturnsAll124sBuffer) {
    IndexReaderFixture fx;
    fx.Build({{"x", 0, 0}}, /*max_doc=*/5);
    auto reader = fx.Make();

    uint8_t* n = reader->norms(L"body");
    ASSERT_NE(n, nullptr);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(n[i], 124) << "i=" << i; // Similarity::encodeNorm(1.0f)
    }

    // Same field returns the same cached pointer (callers may hold it).
    EXPECT_EQ(reader->norms(L"body"), n);

    // Two-arg variant copies into a caller-provided buffer.
    std::vector<uint8_t> buf(5, 0);
    reader->norms(L"body", buf.data());
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(buf[i], 124);
    }
}

TEST(SpimiQueryIndexReaderTest, WriteSideMethodsThrow) {
    IndexReaderFixture fx;
    fx.Build({{"x", 0, 0}}, /*max_doc=*/1);
    auto reader = fx.Make();

    EXPECT_THROW(reader->doDelete(0), CLuceneError);
    EXPECT_THROW(reader->doSetNorm(0, L"body", 50), CLuceneError);
    EXPECT_THROW(reader->doUndeleteAll(), CLuceneError);
    // doClose / doCommit are intentionally no-ops (read-only reader
    // can be "closed" without error).
    reader->doClose();
    reader->doCommit();
}

} // namespace doris::segment_v2::inverted_index::spimi
