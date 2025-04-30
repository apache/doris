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

#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "roaring/roaring.hh"

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

class CustomAnalyzerTest : public ::testing::Test {};

TEST_F(CustomAnalyzerTest, all) {
    std::vector<std::string> lines;

    // std::ifstream ifs("/mnt/disk2/yangsiyu/httplogs/wikipedia/wikipedia.json000");
    // std::string line;
    // while (getline(ifs, line)) {
    //     lines.emplace_back(line);
    // }
    // ifs.close();

    std::string line;
    for (int32_t i = 0; i < 16384; i++) {
        line += "\u1ffc";
    }
    std::cout << "line size: " << line.size() << std::endl;
    lines.emplace_back(line);

    std::cout << "lines size: " << lines.size() << std::endl;

    Settings word_delimiter_params;
    word_delimiter_params.set("split_on_numerics", "false");
    word_delimiter_params.set("split_on_case_change", "false");

    Settings edge_ngram_params;
    edge_ngram_params.set("min_gram", "3");
    edge_ngram_params.set("max_gram", "10");
    edge_ngram_params.set("token_chars", "digit");

    CustomAnalyzerConfig::Builder builder;
    builder.add_tokenizer_config("keyword", {});
    builder.add_token_filter_config("asciifolding", {});
    // builder.add_token_filter_config("word_delimiter", word_delimiter_params);
    builder.add_token_filter_config("lowercase", {});
    auto custom_analyzer_config = builder.build();

    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    lucene::util::SStringReader<char> reader;

    size_t total_count = 0;
    Token t;
    for (size_t i = 0; i < 1; ++i) {
        reader.init(lines[i].data(), lines[i].size(), false);
        auto* token_stream = custom_analyzer->reusableTokenStream(L"", &reader);
        token_stream->reset();

        size_t count = 0;
        while (token_stream->next(&t)) {
            std::string_view term(t.termBuffer<char>(), t.termLength<char>());
            // std::cout << "term: " << term << std::endl;
            ++count;
        }
        std::cout << i << ", count: " << count << std::endl;
        total_count += count;
    }
    std::cout << "total count: " << total_count << std::endl;
}

// TEST_F(CustomAnalyzerTest, test) {
//     std::string name = "name";
//     std::string path = "/mnt/disk2/yangsiyu/clucene/index";

//     std::vector<std::string> lines;

//     // std::ifstream ifs("/mnt/disk2/yangsiyu/httplogs/wikipedia/wikipedia.json000");
//     // std::string line;
//     // while (getline(ifs, line)) {
//     //     lines.emplace_back(line);
//     // }
//     // ifs.close();

//     lines.emplace_back("Wi-Fi PowerPoint AT&T H2O 3D");

//     std::cout << "lines size: " << lines.size() << std::endl;

//     Settings word_delimiter_params;
//     word_delimiter_params.set("generate_word_parts", "true");
//     word_delimiter_params.set("generate_number_parts", "true");
//     word_delimiter_params.set("split_on_case_change", "true");
//     word_delimiter_params.set("split_on_numerics", "true");
//     word_delimiter_params.set("stem_english_possessive", "true");
//     word_delimiter_params.set("preserve_original", "true");

//     // Settings edge_ngram_params;
//     // edge_ngram_params.set("min_gram", "3");
//     // edge_ngram_params.set("max_gram", "10");
//     // edge_ngram_params.set("token_chars", "digit");

//     CustomAnalyzerConfig::Builder builder;
//     builder.add_tokenizer_config("standard", {});
//     builder.add_token_filter_config("asciifolding", {});
//     builder.add_token_filter_config("word_delimiter", word_delimiter_params);
//     builder.add_token_filter_config("lowercase", {});
//     auto custom_analyzer_config = builder.build();

//     auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

//     {
//         TimeGuard t("load time");

//         lucene::index::IndexWriter indexwriter(path.c_str(), custom_analyzer.get(), true);
//         indexwriter.setRAMBufferSizeMB(512);
//         indexwriter.setMaxFieldLength(0x7FFFFFFFL);
//         indexwriter.setMergeFactor(1000000000);
//         indexwriter.setUseCompoundFile(false);

//         lucene::util::SStringReader<char> reader;

//         lucene::document::Document doc;
//         int32_t field_config = lucene::document::Field::STORE_NO;
//         field_config |= lucene::document::Field::INDEX_NONORMS;
//         field_config |= lucene::document::Field::INDEX_TOKENIZED;
//         auto field_name = std::wstring(name.begin(), name.end());
//         auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
//         field->setOmitTermFreqAndPositions(false);
//         doc.add(*field);

//         for (int32_t j = 0; j < 1; j++) {
//             for (size_t k = 0; k < lines.size(); k++) {
//                 reader.init(lines[k].data(), lines[k].size(), false);
//                 auto* stream = custom_analyzer->reusableTokenStream(field->name(), &reader);
//                 field->setValue(stream);

//                 indexwriter.addDocument(&doc);
//             }
//         }

//         std::cout << "---------------------" << std::endl;

//         indexwriter.close();
//     }

//     std::cout << "-----------" << std::endl;

//     try {
//         {
//             auto* dir = FSDirectory::getDirectory(path.c_str());
//             auto* reader = IndexReader::open(dir, 1024 * 1024, true);
//             IndexSearcher index_searcher(reader);

//             // std::cout << "macDoc: " << reader->maxDoc() << std::endl;

//             {
//                 std::vector<std::string> terms;
//                 terms.emplace_back("wi");
//                 terms.emplace_back("fi");
//                 terms.emplace_back("powerpoint");

//                 TimeGuard time("query time");

//                 PhraseQuery query;
//                 for (auto& term : terms) {
//                     std::wstring ws_term = StringUtil::string_to_wstring(term);
//                     Term* t = _CLNEW Term(_T("name"), ws_term.c_str());
//                     query.add(t);
//                     _CLLDECDELETE(t);
//                 }
//                 // query.setSlop(10);

//                 roaring::Roaring result;
//                 index_searcher._search(&query,
//                                        [&result](const int32_t docid, const float_t /*score*/) {
//                                            //    std::cout << "doc: " << docid << std::endl;
//                                            result.add(docid);
//                                        });

//                 std::cout << "count: " << result.cardinality() << std::endl;
//             }

//             reader->close();
//             _CLLDELETE(reader);
//             _CLDECDELETE(dir);
//         }
//     } catch (const CLuceneError& e) {
//         std::cout << e.number() << ": " << e.what() << std::endl;
//     }
// }

} // namespace doris::segment_v2::inverted_index