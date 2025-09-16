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
#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"
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

constexpr static uint32_t MAX_PATH_LEN = 1024;

class CustomAnalyzerTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);

        _word_delimiter_params1.set("generate_word_parts", "true");
        _word_delimiter_params1.set("generate_number_parts", "true");
        _word_delimiter_params1.set("catenate_words", "true");
        _word_delimiter_params1.set("catenate_numbers", "true");
        _word_delimiter_params1.set("catenate_all", "true");
        _word_delimiter_params1.set("split_on_case_change", "true");
        _word_delimiter_params1.set("preserve_original", "true");
        _word_delimiter_params1.set("split_on_numerics", "true");
        _word_delimiter_params1.set("stem_english_possessive", "true");

        _word_delimiter_params2.set("generate_word_parts", "false");
        _word_delimiter_params2.set("generate_number_parts", "false");
        _word_delimiter_params2.set("catenate_words", "false");
        _word_delimiter_params2.set("catenate_numbers", "false");
        _word_delimiter_params2.set("catenate_all", "false");
        _word_delimiter_params2.set("split_on_case_change", "false");
        _word_delimiter_params2.set("preserve_original", "false");
        _word_delimiter_params2.set("split_on_numerics", "false");
        _word_delimiter_params2.set("stem_english_possessive", "false");
    }

    std::string _curreent_dir;
    Settings _word_delimiter_params1;
    Settings _word_delimiter_params2;
};

int32_t tokenize(const CustomAnalyzerPtr& custom_analyzer, const std::vector<std::string>& lines) {
    lucene::util::SStringReader<char> reader;
    size_t total_count = 0;
    Token t;
    for (size_t i = 0; i < lines.size(); ++i) {
        reader.init(lines[i].data(), lines[i].size(), false);
        auto* token_stream = custom_analyzer->reusableTokenStream(L"", &reader);
        token_stream->reset();
        while (token_stream->next(&t)) {
            total_count++;
        }
    }
    return total_count;
}

struct ExpectedToken {
    std::string term;
    int32_t pos = 0;

    bool operator==(const ExpectedToken& other) const {
        return term == other.term && pos == other.pos;
    }
};

std::vector<ExpectedToken> tokenize1(const CustomAnalyzerPtr& custom_analyzer,
                                     const std::string line) {
    std::vector<ExpectedToken> results;
    lucene::util::SStringReader<char> reader;
    reader.init(line.data(), line.size(), false);
    auto* token_stream = custom_analyzer->reusableTokenStream(L"", &reader);
    token_stream->reset();
    Token t;
    while (token_stream->next(&t)) {
        results.emplace_back(std::string(t.termBuffer<char>(), t.termLength<char>()),
                             t.getPositionIncrement());
    }
    return results;
}

TEST_F(CustomAnalyzerTest, CustomStandardAnalyzer) {
    std::vector<std::string> lines;
    {
        std::ifstream ifs(
                _curreent_dir +
                "/be/test/olap/rowset/segment_v2/inverted_index/data/sorted_wikipedia-50-1.json");
        std::string line;
        while (getline(ifs, line)) {
            lines.emplace_back(line);
        }
        ifs.close();
    }

    auto custom_tokenize = [&lines](const std::string& tokenizer,
                                    const Settings& word_delimiter_params) {
        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config(tokenizer, {});
        builder.add_token_filter_config("asciifolding", {});
        builder.add_token_filter_config("word_delimiter", word_delimiter_params);
        builder.add_token_filter_config("lowercase", {});
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);
        return tokenize(custom_analyzer, lines);
    };

    {
        auto total1 = custom_tokenize("standard", _word_delimiter_params1);
        auto total2 = custom_tokenize("standard", _word_delimiter_params2);
        EXPECT_EQ(total1, 51658);
        EXPECT_EQ(total2, 42774);
    }
}

TEST_F(CustomAnalyzerTest, CustomNgramAnalyzer) {
    {
        std::string line = "a b";

        Settings ngram_params;
        ngram_params.set("max_gram", "2");
        ngram_params.set("token_chars", "");

        Settings word_delimiter_params;
        word_delimiter_params.set("stem_english_possessive", "true");
        word_delimiter_params.set("catenate_words", "true");
        word_delimiter_params.set("generate_number_parts", "true");
        word_delimiter_params.set("preserve_original", "true");
        word_delimiter_params.set("split_on_case_change", "true");
        word_delimiter_params.set("split_on_numerics", "false");

        Settings ascii_folding_params;
        ascii_folding_params.set("preserve_original", "true");

        CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("ngram", ngram_params);
        builder.add_token_filter_config("word_delimiter", word_delimiter_params);
        auto custom_analyzer_config = builder.build();
        auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

        std::vector<ExpectedToken> expected = {{"a", 1},  {"a ", 1}, {"a", 0}, {" ", 1},
                                               {" b", 1}, {"b", 0},  {"b", 1}};
        EXPECT_EQ(tokenize1(custom_analyzer, line), expected);
    }
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

//     lines.emplace_back("A Super_Duper b c d");

//     std::cout << "lines size: " << lines.size() << std::endl;

//     Settings word_delimiter_params;
//     word_delimiter_params.set("preserve_original", "true");

//     CustomAnalyzerConfig::Builder builder;
//     builder.with_tokenizer_config("standard", {});
//     builder.add_token_filter_config("word_delimiter", word_delimiter_params);
//     // builder.add_token_filter_config("asciifolding", {});
//     // builder.add_token_filter_config("lowercase", {});
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
//             auto searcher = std::make_shared<IndexSearcher>(reader);

//             // std::cout << "macDoc: " << reader->maxDoc() << std::endl;

//             {
//                 TimeGuard time("query time");

//                 {
//                     TQueryOptions query_options;
//                     doris::segment_v2::PhraseQuery query(searcher, query_options, nullptr);

//                     InvertedIndexQueryInfo query_info;
//                     query_info.field_name = L"name";
//                     {
//                         doris::segment_v2::TermInfo t;
//                         t.term = "Super_Duper";
//                         t.position = 1;
//                         query_info.term_infos.emplace_back(std::move(t));
//                     }
//                     {
//                         doris::segment_v2::TermInfo t;
//                         t.term = "Super";
//                         t.position = 1;
//                         query_info.term_infos.emplace_back(std::move(t));
//                     }
//                     {
//                         doris::segment_v2::TermInfo t;
//                         t.term = "Duper";
//                         t.position = 2;
//                         query_info.term_infos.emplace_back(std::move(t));
//                     }
//                     {
//                         doris::segment_v2::TermInfo t;
//                         t.term = "c";
//                         t.position = 3;
//                         query_info.term_infos.emplace_back(std::move(t));
//                     }
//                     query_info.slop = 1;
//                     query_info.ordered = true;
//                     query.add(query_info);

//                     roaring::Roaring result;
//                     query.search(result);

//                     std::cout << "phrase_query count: " << result.cardinality() << std::endl;
//                 }
//                 // {
//                 //     TQueryOptions query_options;
//                 //     doris::segment_v2::PhrasePrefixQuery query(searcher, query_options, nullptr);

//                 //     InvertedIndexQueryInfo query_info;
//                 //     query_info.field_name = L"name";
//                 //     {
//                 //         doris::segment_v2::TermInfo t;
//                 //         t.term = "Super_Duper";
//                 //         t.position = 1;
//                 //         query_info.term_infos.emplace_back(std::move(t));
//                 //     }
//                 //     {
//                 //         doris::segment_v2::TermInfo t;
//                 //         t.term = "Super";
//                 //         t.position = 1;
//                 //         query_info.term_infos.emplace_back(std::move(t));
//                 //     }
//                 //     {
//                 //         doris::segment_v2::TermInfo t;
//                 //         t.term = "Dup";
//                 //         t.position = 2;
//                 //         query_info.term_infos.emplace_back(std::move(t));
//                 //     }
//                 //     query.add(query_info);

//                 //     roaring::Roaring result;
//                 //     query.search(result);

//                 //     std::cout << "phrase_prefix_query count: " << result.cardinality() << std::endl;
//                 // }
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