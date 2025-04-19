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

#include <fstream>

namespace doris::segment_v2::inverted_index {

class CustomAnalyzerTest : public ::testing::Test {};

TEST_F(CustomAnalyzerTest, all) {
    // getchar();

    std::ifstream ifs("/mnt/disk2/yangsiyu/httplogs/wikipedia/1.json");
    std::string line;
    std::vector<std::string> lines;
    while (getline(ifs, line)) {
        lines.emplace_back(line);
    }
    ifs.close();

    std::cout << "lines size: " << lines.size() << std::endl;

    Settings word_delimiter_params;
    word_delimiter_params.set("split_on_numerics", "false");
    word_delimiter_params.set("split_on_case_change", "false");

    CustomAnalyzerConfig::Builder builder;
    builder.add_tokenizer_config("standard", {});
    builder.add_token_filter_config("asciifolding", {});
    builder.add_token_filter_config("word_delimiter", word_delimiter_params);
    builder.add_token_filter_config("lowercase", {});

    // Settings tokenizer_params;
    // tokenizer_params.set("min_gram", "3");
    // tokenizer_params.set("max_gram", "10");
    // tokenizer_params.set("token_chars", "digit");

    // CustomAnalyzerConfig::Builder builder;
    // builder.add_tokenizer_config("edge_ngram", tokenizer_params);

    // CustomAnalyzerConfig::Builder builder;
    // builder.add_tokenizer_config("keyword", {});
    // builder.add_token_filter_config("asciifolding", {});
    // builder.add_token_filter_config("lowercase", {});
    auto custom_analyzer_config = builder.build();

    auto custom_analyzer = CustomAnalyzer::build_custom_analyzer(custom_analyzer_config);

    // std::string line = "Marín";
    // for (int32_t i = 0; i < line.size();) {
    //     UChar32 c = U_UNASSIGNED;
    //     U8_NEXT(line, i, line.size(), c);
    //     std::cout << "c1: " << c << std::endl;
    // }

    lucene::util::SStringReader<char> reader;

    size_t total_count = 0;
    std::ofstream ofs("/mnt/disk2/yangsiyu/httplogs/c.txt");
    for (size_t i = 0; i < lines.size(); ++i) {
        reader.init(lines[i].data(), lines[i].size(), false);
        auto* token_stream = custom_analyzer->reusableTokenStream(L"", &reader);

        Token t;
        size_t count = 0;
        std::string s;
        while (token_stream->next(&t)) {
            std::string_view term(t.termBuffer<char>(), t.termLength<char>());
            std::cout << "term: " << term << std::endl;
            s += std::string(term) + " ";
            ++count;
        }
        s += std::to_string(count);
        ofs << s << std::endl;
        // std::cout << "count: " << count << std::endl;
        total_count += count;
    }
    ofs.close();
    std::cout << "total count: " << total_count << std::endl;
}

} // namespace doris::segment_v2::inverted_index