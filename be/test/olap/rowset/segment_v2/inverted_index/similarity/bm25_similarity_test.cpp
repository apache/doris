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

#include "olap/rowset/segment_v2/inverted_index/similarity/bm25_similarity.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>

#include "CLucene.h"
#include "olap/rowset/segment_v2/inverted_index/query/disjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "roaring/roaring.hh"
#include "runtime/exec_env.h"

CL_NS_USE(util)
CL_NS_USE(store)
CL_NS_USE(search)
CL_NS_USE(index)

namespace doris::segment_v2::inverted_index {

class TimeGuard {
public:
    TimeGuard(std::string message) : message_(std::move(message)) {
        begin_ = duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    }

    ~TimeGuard() {
        int64_t end = duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
        std::cout << message_ << ": " << end - begin_ << std::endl;
    }

private:
    std::string message_;
    int64_t begin_ = 0;
};

class BM25SimilarityTest : public ::testing::Test {};

TEST_F(BM25SimilarityTest, Int4EncodingTest) {
    std::vector<int32_t> values = {
            0,         1,         16,        128,        1430,       10000,    65535,
            100000,    1000000,   2000000,   5000000,    10000000,   16777215, 50000000,
            100000000, 268435455, 500000000, 1000000000, 2147483647, 8388608,  16777216};

    std::vector<int32_t> expected_restored = {
            0,        1,         16,        128,       1304,       9240,     61464,
            98328,    983064,    1966104,   4718616,   9437208,    15728664, 46137368,
            92274712, 251658264, 469762072, 939524120, 2013265944, 7864344,  15728664};

    for (size_t i = 0; i < values.size(); ++i) {
        uint8_t c = doris::segment_v2::BM25Similarity::int_to_byte4(values[i]);
        int32_t restored = doris::segment_v2::BM25Similarity::byte4_to_int(c);
        ASSERT_EQ(restored, expected_restored[i]);
    }
}

TEST_F(BM25SimilarityTest, test) {
    std::string name = "name";
    std::string path = "/mnt/disk2/yangsiyu/clucene/index";

    std::vector<std::string> lines;

    std::ifstream ifs("/mnt/disk2/yangsiyu/httplogs/100.json");
    std::string line;
    while (getline(ifs, line)) {
        lines.emplace_back(line);
    }
    ifs.close();

    // lines.emplace_back("A Super_Duper b c d");

    std::cout << "lines size: " << lines.size() << std::endl;

    {
        TimeGuard t("load time");

        auto similarity = std::make_unique<lucene::search::LengthSimilarity>();
        auto analyzer = std::make_shared<lucene::analysis::standard95::StandardAnalyzer>();
        analyzer->set_lowercase(true);
        analyzer->set_stopwords(nullptr);
        lucene::index::IndexWriter indexwriter(path.c_str(), analyzer.get(), true);
        indexwriter.setRAMBufferSizeMB(512);
        indexwriter.setMaxFieldLength(0x7FFFFFFFL);
        indexwriter.setMergeFactor(1000000000);
        indexwriter.setUseCompoundFile(false);
        indexwriter.setSimilarity(similarity.get());

        lucene::util::SStringReader<char> reader;

        lucene::document::Document doc;
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name = std::wstring(name.begin(), name.end());
        auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        field->setOmitNorms(false);
        doc.add(*field);

        for (int32_t j = 0; j < 1; j++) {
            for (size_t k = 0; k < lines.size(); k++) {
                reader.init(lines[k].data(), lines[k].size(), false);
                auto* stream = analyzer->reusableTokenStream(field->name(), &reader);
                field->setValue(stream);

                indexwriter.addDocument(&doc);
            }
        }

        std::cout << "---------------------" << std::endl;

        indexwriter.close();
    }

    std::cout << "-----------" << std::endl;

    try {
        {
            auto* dir = FSDirectory::getDirectory(path.c_str());
            auto* reader = IndexReader::open(dir, 1024 * 1024, true);
            auto searcher = std::make_shared<IndexSearcher>(reader);

            // std::cout << "macDoc: " << reader->maxDoc() << std::endl;

            {
                TimeGuard time("query time");

                {
                    OlapReaderStatistics stats;
                    RuntimeState runtime_state;

                    auto context = std::make_shared<IndexQueryContext>();
                    context->stats = &stats;
                    context->runtime_state = &runtime_state;
                    context->full_segment_id = std::make_shared<FullSegmentId>(RowsetId(), 0);
                    context->collection_statistics = std::make_shared<CollectionStatistics>();
                    context->collection_similarity = std::make_shared<CollectionSimilarity>();

                    SegmentStats segment_stats;
                    segment_stats.row_cnt = reader->maxDoc();
                    context->collection_statistics->collect(segment_stats);

                    InvertedIndexQueryInfo query_info;
                    query_info.field_name = L"name";
                    query_info.terms.emplace_back("images");
                    doris::segment_v2::DisjunctionQuery query(searcher, context);
                    query.pre_search(query_info);

                    roaring::Roaring result;
                    query.add(query_info);
                    query.search(result);

                    std::cout << "phrase_query count: " << result.cardinality() << std::endl;

                    const auto& score_map = context->collection_similarity->get_bm25_scores();
                    std::vector<std::pair<int32_t, float>> sorted_scores(score_map.begin(),
                                                                         score_map.end());
                    std::sort(sorted_scores.begin(), sorted_scores.end(),
                              [](const auto& a, const auto& b) { return a.second > b.second; });
                    for (const auto& score : sorted_scores) {
                        std::cout << std::setprecision(8) << "doc_id: " << score.first + 1
                                  << ", score: " << std::setprecision(8) << score.second
                                  << std::endl;
                    }
                }
            }

            reader->close();
            _CLLDELETE(reader);
            _CLDECDELETE(dir);
        }
    } catch (const CLuceneError& e) {
        std::cout << e.number() << ": " << e.what() << std::endl;
    }
}

} // namespace doris::segment_v2::inverted_index