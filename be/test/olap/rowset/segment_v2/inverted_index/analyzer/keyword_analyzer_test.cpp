
#include <gtest/gtest.h>

#include <unordered_map>

#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class KeywordTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(KeywordTokenizerFactory& factory, const std::string& text) {
    std::vector<std::string> tokens;
    auto tokenizer = factory.create();
    {
        lucene::util::SStringReader<char> reader;
        reader.init(text.data(), text.size(), false);

        tokenizer->reset(&reader);

        Token t;
        while (tokenizer->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            tokens.emplace_back(term);
        }
    }
    return tokens;
}

TEST(KeywordTokenizerTest, BasicTokenization) {
    std::unordered_map<std::string, std::string> args;
    args["max_token_len"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0], "ApacheDoris");
}

TEST(KeywordTokenizerTest, BufferSizeLimit) {
    std::unordered_map<std::string, std::string> args;
    args["max_token_len"] = "5";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0], "Apach");
}

TEST(KeywordTokenizerTest, InvalidBufferSize) {
    bool exception_thrown = false;
    try {
        std::unordered_map<std::string, std::string> args;
        args["max_token_len"] = "-1";
        Settings settings(args);
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
    } catch (...) {
        exception_thrown = true;
    }
    EXPECT_TRUE(exception_thrown);

    exception_thrown = false;
    try {
        std::unordered_map<std::string, std::string> args;
        args["max_token_len"] = "100000";
        Settings settings(args);
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
    } catch (...) {
        exception_thrown = true;
    }
    EXPECT_TRUE(exception_thrown);
}

TEST(KeywordTokenizerTest, FactoryCreatesValidTokenizer) {
    std::unordered_map<std::string, std::string> args;
    args["max_token_len"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0].size(), 11);
}

TEST(KeywordTokenizerTest, EmptyInput) {
    std::unordered_map<std::string, std::string> args;
    args["max_token_len"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, " ");

    EXPECT_EQ(tokens.size(), 1);
}

TEST(KeywordTokenizerTest, LongInput) {
    std::unordered_map<std::string, std::string> args;
    args["max_token_len"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    std::string s;
    for (int32_t i = 0; i < 16383; i++) {
        s += "a";
    }
    auto tokens = tokenize(factory, s);
    EXPECT_EQ(tokens[0].size(), 16383);

    std::string s1;
    for (int32_t i = 0; i < 16384; i++) {
        s1 += "a";
    }
    auto tokens1 = tokenize(factory, s1);
    EXPECT_EQ(tokens1[0].size(), 16383);
}

} // namespace doris::segment_v2