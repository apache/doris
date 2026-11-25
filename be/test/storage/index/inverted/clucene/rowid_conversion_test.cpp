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
#include "storage/index/inverted/clucene/rowid_conversion.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <iostream>
#include <map>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/cast_set.h"
#include "storage/rowid_conversion.h"

namespace doris::segment_v2 {
namespace {
using namespace lucene::index;
using namespace lucene::store;

// Compaction closes its output directories. Keep RAM files available for checking;
// RAMDirectory's destructor still releases them when these test objects leave scope.
class RetainedRAMDirectory : public RAMDirectory {
public:
    void close() override {}
};

Status prepare_mapping(RowIdConversion& map, uint32_t rows, std::vector<uint32_t>* destinations) {
    RowsetId rowset;
    rowset.init(17);
    RETURN_IF_ERROR(map.init_segment_map(rowset, {7, 42}, {rows, rows}));
    std::vector<RowLocation> live;
    for (uint32_t row = 0; row < rows; ++row) {
        if (row % 13 == 0) continue;
        for (uint32_t segment : {7, 42}) live.emplace_back(rowset, segment, row);
    }
    auto total = cast_set<uint32_t>(live.size());
    *destinations = {total / 2, total - total / 2};
    RETURN_IF_ERROR(map.add(live, *destinations));
    return map.finalize(*destinations);
}

void write_source(RAMDirectory& dir, uint32_t rows) {
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    lucene::util::SStringReader<char> body_reader;
    lucene::util::SStringReader<char> title_reader;
    lucene::document::Document doc;
    const auto flags = static_cast<int32_t>(lucene::document::Field::STORE_NO) |
                       static_cast<int32_t>(lucene::document::Field::INDEX_TOKENIZED);
    auto* body = _CLNEW lucene::document::Field(_T("body"), flags);
    auto* title = _CLNEW lucene::document::Field(_T("title"), flags);
    body->setOmitTermFreqAndPositions(false);
    title->setOmitTermFreqAndPositions(false);
    doc.add(*body);
    doc.add(*title);
    IndexWriter writer(&dir, &analyzer, true);
    writer.setMaxBufferedDocs(-1);
    writer.setRAMBufferSizeMB(512);
    writer.setUseCompoundFile(false);
    for (uint32_t row = 0; row < rows; ++row) {
        const std::string text = row % 2 == 0 ? "alpha alpha omega" : "beta omega";
        const std::string heading = "heading";
        body_reader.init(text.data(), cast_set<int32_t>(text.size()), false);
        title_reader.init(heading.data(), cast_set<int32_t>(heading.size()), false);
        body->setValue(analyzer.tokenStream(body->name(), &body_reader), true);
        title->setValue(analyzer.tokenStream(title->name(), &title_reader), true);
        writer.addDocument(&doc);
    }
    writer.close();
    roaring::Roaring nulls;
    for (uint32_t row = 0; row < rows; row += 17) nulls.add(row);
    std::vector<char> bytes(nulls.getSizeInBytes(false));
    nulls.write(bytes.data(), false);
    auto* output = dir.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME);
    output->writeBytes(reinterpret_cast<const uint8_t*>(bytes.data()),
                       cast_set<int32_t>(bytes.size()));
    output->close();
    _CLDELETE(output);
}

struct IndexSnapshot {
    int32_t documents;
    std::map<std::wstring, std::vector<int32_t>> postings;
    std::array<std::vector<uint8_t>, 2> norms;
    std::array<uint64_t, 2> token_counts;
    std::vector<uint32_t> nulls;
};

IndexSnapshot read_index(RAMDirectory& dir) {
    IndexSnapshot snapshot;
    auto* reader = IndexReader::open(&dir);
    snapshot.documents = reader->maxDoc();
    auto* terms = reader->terms();
    while (terms->next()) {
        auto* term = terms->term();
        auto* positions = reader->termPositions(term);
        auto& values = snapshot.postings[std::wstring(term->field()) + L":" + term->text()];
        while (positions->next()) {
            values.push_back(positions->doc());
            const int32_t frequency = positions->freq();
            values.push_back(frequency);
            for (int32_t i = 0; i < frequency; ++i) values.push_back(positions->nextPosition());
        }
        positions->close();
        _CLDELETE(positions);
        _CLDECDELETE(term);
    }
    terms->close();
    _CLDELETE(terms);
    size_t field = 0;
    for (const auto* name : {_T("body"), _T("title")}) {
        const auto* norms = reader->norms(name);
        snapshot.norms[field].assign(norms, norms + snapshot.documents);
        snapshot.token_counts[field] = reader->sumTotalTermFreq(name).value();
        ++field;
    }
    reader->close();
    _CLDELETE(reader);
    auto* input = static_cast<lucene::store::Directory&>(dir).openInput(
            IndexWriter::NULL_BITMAP_FILE_NAME);
    std::vector<uint8_t> bytes(input->length());
    input->readBytes(bytes.data(), cast_set<int32_t>(bytes.size()));
    auto nulls = roaring::Roaring::read(reinterpret_cast<const char*>(bytes.data()), false);
    for (auto row : nulls) snapshot.nulls.push_back(row);
    input->close();
    _CLDELETE(input);
    return snapshot;
}
} // namespace

TEST(CluceneRowIdMappingTest, DenseAndCompressedSeekAcrossSourcesAndBlocks) {
    constexpr uint32_t rows = 2 * CompressedRowIdMap::BLOCK_ROWS + 7;
    RowIdConversion dense;
    RowIdConversion compressed(RowIdConversion::Mode::COMPRESSED);
    std::vector<uint32_t> destinations;
    ASSERT_TRUE(prepare_mapping(dense, rows, &destinations).ok());
    ASSERT_TRUE(prepare_mapping(compressed, rows, &destinations).ok());
    CluceneRowIdMapping expected(dense);
    CluceneRowIdMapping actual(compressed);
    ASSERT_EQ(actual.sourceCount(), 2);
    const std::array<uint32_t, 9> probes {4095, 4096, rows - 1, 1, 4097, 0, 8192, 13, 2};
    for (auto row : probes) {
        for (uint32_t source : {0, 1}) {
            EXPECT_EQ(actual.get(source, row), expected.get(source, row));
        }
    }
    for (const auto& source : actual._sources) {
        EXPECT_LE(source.cursor.memory_usage(), CompressedRowIdMap::BLOCK_ROWS * sizeof(uint64_t));
    }
    for (auto* mapping : {&expected, &actual}) {
        EXPECT_THROW((void)mapping->get(2, 0), CLuceneError);
        EXPECT_THROW((void)mapping->get(0, rows), CLuceneError);
        EXPECT_EQ(mapping->get(0, 13), (DocIdMapping::Destination {UINT32_MAX, UINT32_MAX}));
        auto start = std::chrono::steady_clock::now();
        for (uint32_t row = 0; row < rows; ++row) {
            for (uint32_t source : {0, 1}) (void)mapping->get(source, row);
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() - start)
                               .count();
        std::cout << "CLucene row-id lookup mode="
                  << (mapping == &expected ? "DENSE" : "COMPRESSED") << " rows=" << 2 * rows
                  << " ns/lookup=" << elapsed / (2 * rows) << " (UT diagnostic)" << std::endl;
    }
}

TEST(CluceneRowIdMappingTest, LookupErrorsAreNotDeletedDocuments) {
    RowIdConversion incomplete(RowIdConversion::Mode::COMPRESSED);
    RowsetId rowset;
    rowset.init(19);
    ASSERT_TRUE(incomplete.init_segment_map(rowset, {42}, {1}).ok());
    CluceneRowIdMapping mapping(incomplete);
    EXPECT_THROW((void)mapping.get(0, 0), CLuceneError);
}

TEST(CluceneRowIdMappingTest, CompactionMatchesDensePostingsNormsAndNulls) {
    constexpr uint32_t rows = CompressedRowIdMap::BLOCK_ROWS + 3;
    RetainedRAMDirectory source0;
    RetainedRAMDirectory source1;
    write_source(source0, rows);
    write_source(source1, rows);
    RowIdConversion dense;
    RowIdConversion compressed(RowIdConversion::Mode::COMPRESSED);
    std::vector<uint32_t> destinations;
    ASSERT_TRUE(prepare_mapping(dense, rows, &destinations).ok());
    ASSERT_TRUE(prepare_mapping(compressed, rows, &destinations).ok());
    RetainedRAMDirectory dense0, dense1, compressed0, compressed1;
    std::vector<Directory*> sources {&source0, &source1};
    auto compact = [&](const RowIdConversion& conversion, std::vector<Directory*> outputs) {
        CluceneRowIdMapping mapping(conversion);
        lucene::analysis::SimpleAnalyzer<char> analyzer;
        RAMDirectory scratch;
        IndexWriter writer(&scratch, &analyzer, true);
        writer.indexCompaction(sources, outputs, mapping, destinations);
        writer.close();
    };
    compact(dense, {&dense0, &dense1});
    compact(compressed, {&compressed0, &compressed1});
    size_t segment = 0;
    for (auto dirs :
         {std::make_pair(&dense0, &compressed0), std::make_pair(&dense1, &compressed1)}) {
        const auto expected = read_index(*dirs.first);
        const auto actual = read_index(*dirs.second);
        EXPECT_EQ(actual.documents, destinations[segment++]);
        EXPECT_EQ(actual.documents, expected.documents);
        EXPECT_EQ(actual.postings, expected.postings);
        EXPECT_EQ(actual.norms, expected.norms);
        EXPECT_EQ(actual.token_counts, expected.token_counts);
        EXPECT_EQ(actual.nulls, expected.nulls);
    }
}

} // namespace doris::segment_v2
