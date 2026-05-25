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

// Byte-equality differential test: drives the SAME (term, TermInfo)
// sequence through both SPIMI's TermDictWriter and CLucene's existing
// STermInfosWriter<char>, then compares the `.tis` and `.tii` bytes.
// Any drift in the SPIMI encoder against the reader's expectations
// surfaces here as a binary inequality.

#include <CLucene.h>
#include <CLucene/index/IndexVersion.h>
#include <CLucene/index/_FieldInfos.h>
#include <CLucene/index/_TermInfo.h>
#include <CLucene/index/_TermInfosWriter.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/RAMDirectory.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

struct TermFixture {
    std::string term_utf8;
    int32_t doc_freq;
    int64_t freq_pointer;
    int64_t prox_pointer;
    int32_t skip_offset;
};

std::vector<uint8_t> ReadAllFromRamDir(lucene::store::RAMDirectory* dir, const char* file) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError err;
    EXPECT_TRUE(dir->openInput(file, in, err)) << "openInput(" << file << ") failed";
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

void DriveCLuceneWriter(lucene::store::RAMDirectory* dir, const char* segment,
                        lucene::index::FieldInfos* fis, const std::vector<TermFixture>& fixtures) {
    auto* writer =
            _CLNEW lucene::index::STermInfosWriter<char>(dir, segment, fis, /*interval=*/128);
    writer->skipInterval = TermDictWriter::kDefaultSkipInterval;
    writer->maxSkipLevels = TermDictWriter::kMaxSkipLevels;

    for (const auto& f : fixtures) {
        lucene::index::TermInfo ti;
        ti.docFreq = f.doc_freq;
        ti.freqPointer = f.freq_pointer;
        ti.proxPointer = f.prox_pointer;
        ti.skipOffset = f.skip_offset;
        writer->add(static_cast<int32_t>(0), f.term_utf8.c_str(),
                    static_cast<int32_t>(f.term_utf8.size()), &ti);
    }
    writer->close();
    _CLDELETE(writer);
}

void DriveSpimiWriter(MemoryLuceneOutput* tis, MemoryLuceneOutput* tii,
                      const std::vector<TermFixture>& fixtures) {
    TermDictWriter writer(tis, tii);
    for (const auto& f : fixtures) {
        TermInfo info;
        info.doc_freq = f.doc_freq;
        info.freq_pointer = f.freq_pointer;
        info.prox_pointer = f.prox_pointer;
        info.skip_offset = f.skip_offset;
        writer.Add(/*field_number=*/0, f.term_utf8, info);
    }
    writer.Close();
}

class CLuceneSetup : public ::testing::Test {
protected:
    void SetUp() override {
        _field_infos = _CLNEW lucene::index::FieldInfos();
        _field_infos->add(L"body", /*isIndexed=*/true, /*storeTermVector=*/false,
                          /*storePositionWithTermVector=*/false,
                          /*storeOffsetWithTermVector=*/false,
                          /*omitNorms=*/true, /*hasProx=*/true, /*storePayloads=*/false,
                          /*indexVersion=*/::IndexVersion::kV0);
    }
    void TearDown() override { _CLDELETE(_field_infos); }

    lucene::index::FieldInfos* _field_infos = nullptr;
};

} // namespace

TEST_F(CLuceneSetup, TisTiiAreByteEqualForSingleTerm) {
    lucene::store::RAMDirectory ram;
    std::vector<TermFixture> fixtures = {
            {"apple", /*df=*/1, /*fp=*/0, /*pp=*/0, /*skip=*/0},
    };
    DriveCLuceneWriter(&ram, "_diff_a", _field_infos, fixtures);
    const auto clucene_tis = ReadAllFromRamDir(&ram, "_diff_a.tis");
    const auto clucene_tii = ReadAllFromRamDir(&ram, "_diff_a.tii");

    MemoryLuceneOutput tis, tii;
    DriveSpimiWriter(&tis, &tii, fixtures);
    EXPECT_EQ(tis.bytes(), clucene_tis);
    EXPECT_EQ(tii.bytes(), clucene_tii);
}

TEST_F(CLuceneSetup, TisTiiAreByteEqualForAsciiTerms) {
    lucene::store::RAMDirectory ram;
    std::vector<TermFixture> fixtures = {
            {"alpha", 1, 0, 0, 0},    {"apple", 2, 4, 8, 0},    {"apply", 1, 14, 22, 0},
            {"banana", 3, 18, 28, 0}, {"cherry", 1, 30, 50, 0},
    };
    DriveCLuceneWriter(&ram, "_diff_b", _field_infos, fixtures);
    const auto clucene_tis = ReadAllFromRamDir(&ram, "_diff_b.tis");
    const auto clucene_tii = ReadAllFromRamDir(&ram, "_diff_b.tii");

    MemoryLuceneOutput tis, tii;
    DriveSpimiWriter(&tis, &tii, fixtures);
    EXPECT_EQ(tis.bytes(), clucene_tis) << "front-coded prefix + suffix encoding must match";
    EXPECT_EQ(tii.bytes(), clucene_tii);
}

TEST_F(CLuceneSetup, TisTiiAreByteEqualWithSkipOffset) {
    lucene::store::RAMDirectory ram;
    std::vector<TermFixture> fixtures = {
            {"hot", /*df=*/32, /*fp=*/0, /*pp=*/0, /*skip=*/24}, // df >= skipInterval
            {"warm", /*df=*/2, /*fp=*/80, /*pp=*/200, /*skip=*/0},
    };
    DriveCLuceneWriter(&ram, "_diff_c", _field_infos, fixtures);
    const auto clucene_tis = ReadAllFromRamDir(&ram, "_diff_c.tis");
    const auto clucene_tii = ReadAllFromRamDir(&ram, "_diff_c.tii");

    MemoryLuceneOutput tis, tii;
    DriveSpimiWriter(&tis, &tii, fixtures);
    EXPECT_EQ(tis.bytes(), clucene_tis) << "skip_offset path must match";
    EXPECT_EQ(tii.bytes(), clucene_tii);
}

TEST_F(CLuceneSetup, TisTiiAreByteEqualForCjkTerms) {
    lucene::store::RAMDirectory ram;
    std::vector<TermFixture> fixtures = {
            {"\xE4\xB8\xAD\xE5\x8D\x8E", 1, 0, 0, 0},  // 中华 (wide < 中国)
            {"\xE4\xB8\xAD\xE5\x9B\xBD", 1, 5, 10, 0}, // 中国
            {"\xE6\x96\x87", 1, 10, 20, 0},            // 文
    };
    DriveCLuceneWriter(&ram, "_diff_d", _field_infos, fixtures);
    const auto clucene_tis = ReadAllFromRamDir(&ram, "_diff_d.tis");
    const auto clucene_tii = ReadAllFromRamDir(&ram, "_diff_d.tii");

    MemoryLuceneOutput tis, tii;
    DriveSpimiWriter(&tis, &tii, fixtures);
    EXPECT_EQ(tis.bytes(), clucene_tis) << "modified UTF-8 wide-char encoding must match for CJK";
    EXPECT_EQ(tii.bytes(), clucene_tii);
}

TEST_F(CLuceneSetup, TisTiiAreByteEqualWithIndexIntervalBoundaries) {
    lucene::store::RAMDirectory ram;
    // Use 64 fixtures so we cross several index_interval=128 entries on
    // the .tis side. Even with this many we shouldn't fill a tii block
    // beyond a couple of entries; the test mainly exercises monotonic
    // pointer deltas + sentinel handling.
    std::vector<TermFixture> fixtures;
    for (int i = 0; i < 64; ++i) {
        char buf[8];
        std::snprintf(buf, sizeof(buf), "t%04d", i);
        fixtures.push_back({buf, /*df=*/1, /*fp=*/i * 4, /*pp=*/i * 6, /*skip=*/0});
    }
    DriveCLuceneWriter(&ram, "_diff_e", _field_infos, fixtures);
    const auto clucene_tis = ReadAllFromRamDir(&ram, "_diff_e.tis");
    const auto clucene_tii = ReadAllFromRamDir(&ram, "_diff_e.tii");

    MemoryLuceneOutput tis, tii;
    DriveSpimiWriter(&tis, &tii, fixtures);
    EXPECT_EQ(tis.bytes(), clucene_tis);
    EXPECT_EQ(tii.bytes(), clucene_tii);
}

} // namespace doris::segment_v2::inverted_index::spimi
