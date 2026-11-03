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

// The two adapters between the CLucene analysis interfaces and the TextAnalyzer interface:
// LuceneBackedTextAnalyzer, a CLucene analyzer for the new consumers, and LuceneAnalyzerBridge, a
// TextAnalyzer for the V1 and V2 writers.

#include <CLucene.h>
#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <climits>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/clucene/inverted_index_writer.h"
#include "storage/index/inverted/clucene/lucene_analyzer_bridge.h"
#include "storage/index/inverted/clucene/lucene_backed_text_analyzer.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index {

namespace {

constexpr const char* kTestDir = "./ut_dir/lucene_adapter_test";

using Tokens = std::vector<CollectedToken>;

std::string words(size_t count) {
    std::string text;
    for (size_t i = 0; i < count; ++i) {
        text += fmt::format("w{} ", i);
    }
    return text;
}

std::string read_all(lucene::util::Reader* reader) {
    const char* start = nullptr;
    const int32_t length = reader->read(reinterpret_cast<const void**>(&start), 0,
                                        static_cast<int32_t>(reader->size()));
    return length > 0 ? std::string(start, length) : std::string();
}

// A word of a script value: words are separated by spaces, "_" is an empty term, a leading '^'
// puts the word at the position of the one before, and "!" is a failure where it stands.
struct ScriptWord {
    std::string_view term;
    int32_t position_increment = 1;
    bool fails = false;
};

// The next word of `text` from `*position`, or false at the end.
bool next_script_word(std::string_view text, size_t* position, ScriptWord* word) {
    while (*position < text.size() && text[*position] == ' ') {
        ++*position;
    }
    if (*position == text.size()) {
        return false;
    }
    const size_t end = std::min(text.find(' ', *position), text.size());
    std::string_view term = text.substr(*position, end - *position);
    *position = end;
    *word = ScriptWord();
    if (term == "!") {
        word->fails = true;
        return true;
    }
    if (term.starts_with('^')) {
        word->position_increment = 0;
        term.remove_prefix(1);
    }
    word->term = term == "_" ? term.substr(0, 0) : term;
    return true;
}

// A CLucene stream over a script value; "!" throws a CLuceneError.
class ScriptedLuceneStream final : public lucene::analysis::TokenStream {
public:
    explicit ScriptedLuceneStream(std::string text) : _text(std::move(text)) {}

    lucene::analysis::Token* next(lucene::analysis::Token* token) override {
        ScriptWord word;
        if (!next_script_word(_text, &_position, &word)) {
            return nullptr;
        }
        if (word.fails) {
            _CLTHROWA(CL_ERR_IO, "scripted CLucene failure");
        }
        token->setTextNoCopy(word.term.data(), static_cast<int32_t>(word.term.size()));
        token->setPositionIncrement(word.position_increment);
        return token;
    }

    void close() override {}

private:
    const std::string _text;
    size_t _position = 0;
};

// A CLucene analyzer of script values; the value "!!" makes tokenStream() itself throw.
class ScriptedLuceneAnalyzer final : public lucene::analysis::Analyzer {
public:
    bool isSDocOpt() override { return true; }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*, const ReaderPtr& reader) override {
        std::string text = read_all(reader.get());
        if (text == "!!") {
            _CLTHROWA(CL_ERR_IO, "scripted CLucene failure in tokenStream");
        }
        return new ScriptedLuceneStream(std::move(text));
    }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*, lucene::util::Reader*) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "not used");
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       lucene::util::Reader*) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "not used");
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*, const ReaderPtr&) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "not used");
    }
};

// A native stream over a script value, handing out views of it; "!" throws a doris::Exception
// from next_batch(), and a value starting with "!reset" throws from reset().
class ScriptedTextStream final : public TextTokenizer {
public:
    void reset(std::string_view text) override {
        if (text.starts_with("!reset")) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, "scripted failure in reset");
        }
        _text = text;
        _position = 0;
    }

    bool next_batch(TokenBatch& batch) override {
        batch.clear();
        ScriptWord word;
        while (batch.size() < kBatchTokens && next_script_word(_text, &_position, &word)) {
            if (word.fails) {
                throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, "scripted failure");
            }
            batch.push(word.term, word.position_increment);
        }
        return batch.size() != 0;
    }

    size_t reserved_bytes() const override { return 0; }

private:
    std::string_view _text;
    size_t _position = 0;
};

class ScriptedTextAnalyzer final : public TextAnalyzer {
public:
    std::unique_ptr<TextTokenStream> create_stream() const override {
        return std::make_unique<ScriptedTextStream>();
    }
};

const Tokens kScriptTokens = {{.term = "a", .position_increment = 1},
                              {.term = "b", .position_increment = 0},
                              {.term = "", .position_increment = 1},
                              {.term = "c", .position_increment = 1}};

ReaderPtr reader_of(const std::string& text) {
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    return reader;
}

// The tokens of a bridged stream, read as SDocumentsWriter::invertField() reads a field: reset(),
// next() until it returns null with one token object, close().
Tokens lucene_tokens(lucene::analysis::TokenStream* stream) {
    Tokens tokens;
    lucene::analysis::Token token;
    stream->reset();
    while (stream->next(&token) != nullptr) {
        tokens.push_back({.term = std::string(token.termBuffer<char>(), token.termLength<char>()),
                          .position_increment = token.getPositionIncrement()});
    }
    stream->close();
    return tokens;
}

// The code of the doris::Exception `body` throws, or OK.
int thrown_code(const std::function<void()>& body) {
    try {
        body();
    } catch (const Exception& e) {
        return e.code();
    }
    return ErrorCode::OK;
}

} // namespace

TEST(LuceneBackedTextAnalyzerTest, HandsOutTheCLuceneTokens) {
    const LuceneBackedTextAnalyzer analyzer(std::make_shared<ScriptedLuceneAnalyzer>(), {});
    const auto stream = analyzer.create_stream();
    EXPECT_EQ(collect_checked(*stream, "a ^b _ c"), kScriptTokens);
    EXPECT_EQ(stream->reserved_bytes(), 0);

    TokenBatch batch;
    std::vector<size_t> sizes;
    const std::string long_text = words(1000);
    stream->reset(long_text);
    while (stream->next_batch(batch)) {
        sizes.push_back(batch.size());
    }
    EXPECT_EQ(sizes, (std::vector<size_t> {256, 256, 256, 232}));
}

TEST(LuceneBackedTextAnalyzerTest, ReadsTheValueThroughTheIndexCharFilter) {
    const LuceneBackedTextAnalyzer analyzer(
            std::make_shared<ScriptedLuceneAnalyzer>(),
            {{INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE},
             {INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, "-"},
             {INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, " "}});
    EXPECT_EQ(collect_checked(*analyzer.create_stream(), "a-b"),
              (Tokens {{.term = "a", .position_increment = 1},
                       {.term = "b", .position_increment = 1}}));
}

TEST(LuceneBackedTextAnalyzerTest, CLuceneErrorsComeOutAsDorisExceptions) {
    const LuceneBackedTextAnalyzer analyzer(std::make_shared<ScriptedLuceneAnalyzer>(), {});
    const auto stream = analyzer.create_stream();
    TokenBatch batch;
    EXPECT_EQ(thrown_code([&] {
                  stream->reset("a b !");
                  while (stream->next_batch(batch)) {
                  }
              }),
              ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_EQ(thrown_code([&] { stream->reset("!!"); }), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    // The next value starts over.
    EXPECT_EQ(collect_checked(*stream, "c"), (Tokens {{.term = "c", .position_increment = 1}}));
}

TEST(LuceneBackedTextAnalyzerTest, KeepsTheTextStreamContract) {
    const LuceneBackedTextAnalyzer scripted(std::make_shared<ScriptedLuceneAnalyzer>(), {});
    expect_text_stream_contract([&] { return scripted.create_stream(); },
                                {"a ^b _ c", words(1000), words(256), "x"});
    const LuceneBackedTextAnalyzer standard(
            InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_STANDARD,
                                                           "", "", ""),
            {});
    expect_text_stream_contract([&] { return standard.create_stream(); },
                                {"The Quick brown fox", words(1000), "中文 mixed 文本", "x"});
}

TEST(LuceneAnalyzerBridgeTest, HandsOutTheTokensOfTheTextAnalyzer) {
    LuceneAnalyzerBridge bridge(std::make_shared<ScriptedTextAnalyzer>());
    const std::string first = "a ^b _ c";
    lucene::analysis::TokenStream* stream = bridge.reusableTokenStream(L"f", reader_of(first));
    EXPECT_EQ(lucene_tokens(stream), kScriptTokens);

    // A scalar column rebinds the same stream for every value.
    const std::string second = words(1000);
    EXPECT_EQ(bridge.reusableTokenStream(L"f", reader_of(second)), stream);
    const Tokens tokens = lucene_tokens(stream);
    ASSERT_EQ(tokens.size(), 1000);
    EXPECT_EQ(tokens[999].term, "w999");

    const std::string empty;
    EXPECT_TRUE(lucene_tokens(bridge.reusableTokenStream(L"f", reader_of(empty))).empty());
}

// invertField() calls reset() before it reads a field. Before that, next() hands out nothing, as
// DorisTokenizer does before reset() promotes its reader; a second reset() starts the value over.
TEST(LuceneAnalyzerBridgeTest, ResetStartsTheValue) {
    LuceneAnalyzerBridge bridge(std::make_shared<ScriptedTextAnalyzer>());
    const std::string value = "a b";
    lucene::analysis::TokenStream* stream = bridge.reusableTokenStream(L"f", reader_of(value));
    lucene::analysis::Token token;
    EXPECT_EQ(stream->next(&token), nullptr);
    EXPECT_EQ(lucene_tokens(stream).size(), 2);
    EXPECT_EQ(lucene_tokens(stream).size(), 2);
}

TEST(LuceneAnalyzerBridgeTest, TokenStreamMakesAStreamTheCallerOwns) {
    LuceneAnalyzerBridge bridge(std::make_shared<ScriptedTextAnalyzer>());
    const std::string first = "a b";
    const std::string second = "c";
    const std::unique_ptr<lucene::analysis::TokenStream> one(
            bridge.tokenStream(L"f", reader_of(first)));
    const std::unique_ptr<lucene::analysis::TokenStream> two(
            bridge.tokenStream(L"f", reader_of(second)));
    EXPECT_NE(one.get(), two.get());
    EXPECT_EQ(lucene_tokens(two.get()), (Tokens {{.term = "c", .position_increment = 1}}));
    EXPECT_EQ(lucene_tokens(one.get()), (Tokens {{.term = "a", .position_increment = 1},
                                                 {.term = "b", .position_increment = 1}}));
}

TEST(LuceneAnalyzerBridgeTest, ReaderPointerOverloadsThrow) {
    LuceneAnalyzerBridge bridge(std::make_shared<ScriptedTextAnalyzer>());
    const std::string value = "a";
    const ReaderPtr reader = reader_of(value);
    EXPECT_EQ(thrown_code([&] { bridge.tokenStream(L"f", reader.get()); }),
              ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_EQ(thrown_code([&] { bridge.reusableTokenStream(L"f", reader.get()); }),
              ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

// A doris::Exception from a bridged stream passes through the CLucene frames of
// IndexWriter::addDocument() to the V2 writer, which reports it, for a string column and for an
// array column, thrown from reset() and from next_batch(). LeakSanitizer checks that the CLucene
// state the exception skipped over is freed.
class LuceneBridgeWriterTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(91);
        pb.set_index_name("bridge_writer_idx");
        pb.add_col_unique_id(0);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"support_phrase", "true"});
        _meta.init_from_pb(pb);
    }

    void TearDown() override {
        _writer.reset();
        _index_file_writer.reset();
        _kept_analyzers.clear();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // A V2 writer for `column` whose analyzer is the bridge over ScriptedTextAnalyzer, in place of
    // the writer before.
    void make_writer(const TabletColumn& column, const std::string& name) {
        _writer.reset();
        _index_file_writer.reset();
        const std::string prefix = std::string(kTestDir) + "/" + name;
        const auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                    &file_writer)
                            .ok());
        _index_file_writer = std::make_unique<IndexFileWriter>(
                fs, prefix, "bridge_rowset", /*seg_id=*/0, InvertedIndexStorageFormatPB::V2,
                std::move(file_writer));
        const Status created =
                IndexColumnWriter::create(&column, &_writer, _index_file_writer.get(), &_meta);
        ASSERT_TRUE(created.ok()) << created.to_string();
        auto* legacy = static_cast<InvertedIndexColumnWriter<FieldType::OLAP_FIELD_TYPE_STRING>*>(
                _writer.get());
        // The CLucene IndexWriter keeps a raw pointer to the analyzer it was made with.
        _kept_analyzers.push_back(legacy->_analyzer);
        legacy->set_analysis_for_test(
                std::make_shared<lucene::util::SStringReader<char>>(),
                std::make_shared<LuceneAnalyzerBridge>(std::make_shared<ScriptedTextAnalyzer>()));
    }

    static TabletColumn text_column() {
        TabletColumn column;
        column.set_unique_id(0);
        column.set_name("text");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        column.set_length(INT_MAX);
        column.set_is_nullable(true);
        return column;
    }

    static TabletColumn array_column() {
        TabletColumn element = text_column();
        TabletColumn column;
        column.set_unique_id(0);
        column.set_name("texts");
        column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        column.set_is_nullable(true);
        column.add_sub_column(element);
        return column;
    }

    TabletIndex _meta;
    std::vector<std::shared_ptr<lucene::analysis::Analyzer>> _kept_analyzers;
    std::unique_ptr<IndexFileWriter> _index_file_writer;
    std::unique_ptr<IndexColumnWriter> _writer;
};

TEST_F(LuceneBridgeWriterTest, AnExceptionFromTheStreamFailsAddValues) {
    for (const std::string bad : {"x ! y", "!reset z"}) {
        make_writer(text_column(), "scalar_" + std::to_string(bad.size()));
        const std::vector<std::string> rows = {"fine words", bad};
        const std::vector<Slice> slices = {Slice(rows[0]), Slice(rows[1])};
        const Status status =
                _writer->add_values(reinterpret_cast<const uint8_t*>(slices.data()), slices.size());
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR)
                << bad << ": " << status.to_string();
        // Caught around IndexWriter::addDocument(), not where the field is made.
        EXPECT_NE(status.to_string().find("while adding document"), std::string::npos)
                << status.to_string();
        EXPECT_NE(status.to_string().find("scripted failure"), std::string::npos)
                << status.to_string();
    }
}

TEST_F(LuceneBridgeWriterTest, AnExceptionFromTheStreamFailsAddArrayValues) {
    for (const std::string bad : {"x ! y", "!reset z"}) {
        make_writer(array_column(), "array_" + std::to_string(bad.size()));
        const std::vector<std::string> elements = {"fine words", bad};
        const std::vector<Slice> slices = {Slice(elements[0]), Slice(elements[1])};
        const std::vector<uint8_t> null_map = {0, 0};
        const std::vector<uint64_t> offsets = {0, 2};
        const Status status = _writer->add_array_values(
                sizeof(Slice), reinterpret_cast<const uint8_t*>(slices.data()), null_map.data(),
                offsets.data(), 1);
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR)
                << bad << ": " << status.to_string();
        // Caught around IndexWriter::addDocument(), not where the field is made.
        EXPECT_NE(status.to_string().find("while adding document"), std::string::npos)
                << status.to_string();
        EXPECT_NE(status.to_string().find("scripted failure"), std::string::npos)
                << status.to_string();
    }
}

} // namespace doris::segment_v2::inverted_index
