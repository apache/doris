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

#include "storage/index/inverted/clucene/lucene_backed_text_analyzer.h"

#include <CLucene.h>

#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/reader.h"

namespace doris::segment_v2::inverted_index {

namespace {

// One value at a time through a CLucene analyzer. The reader, the token and the CLucene stream are
// made by reset() and released once the value is exhausted, so nothing outlives a value.
class LuceneBackedTokenStream final : public TextTokenizer {
public:
    // Both outlive the stream, because a stream does not outlive its analyzer.
    LuceneBackedTokenStream(lucene::analysis::Analyzer* analyzer, const CharFilterMap* char_filters)
            : _analyzer(analyzer), _char_filters(char_filters) {}

    void reset(std::string_view text) override {
        release();
        try {
            _reader = InvertedIndexAnalyzer::create_reader(*_char_filters);
            _reader->init(text.data(), cast_set<int32_t>(text.size()), false);
            _token = std::make_unique<lucene::analysis::Token>();
            _stream.reset(_analyzer->tokenStream(L"", _reader));
        } catch (const CLuceneError& e) {
            release();
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "the CLucene analyzer failed to start a value: {}", e.what());
        }
    }

    bool next_batch(TokenBatch& batch) override {
        batch.clear();
        while (_stream != nullptr && batch.size() < kBatchTokens) {
            lucene::analysis::Token* token = next_lucene_token();
            if (token == nullptr) {
                release();
                break;
            }
            batch.push_copy({token->termBuffer<char>(), token->termLength<char>()},
                            token->getPositionIncrement());
        }
        return batch.size() != 0;
    }

    // Nothing survives a value.
    size_t reserved_bytes() const override { return 0; }

private:
    // The next CLucene token, or nullptr once the value is exhausted.
    lucene::analysis::Token* next_lucene_token() {
        try {
            lucene::analysis::Token* token = _stream->next(_token.get());
            if (token == nullptr) {
                _stream->close();
            }
            return token;
        } catch (const CLuceneError& e) {
            release();
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "the CLucene analyzer failed on a value: {}", e.what());
        }
    }

    void release() {
        _stream.reset();
        _token.reset();
        _reader.reset();
    }

    lucene::analysis::Analyzer* const _analyzer;
    const CharFilterMap* const _char_filters;
    ReaderPtr _reader;
    std::unique_ptr<lucene::analysis::Token> _token;
    std::unique_ptr<lucene::analysis::TokenStream> _stream;
};

} // namespace

LuceneBackedTextAnalyzer::LuceneBackedTextAnalyzer(
        std::shared_ptr<lucene::analysis::Analyzer> analyzer, CharFilterMap char_filters)
        : _analyzer(std::move(analyzer)), _char_filters(std::move(char_filters)) {
    DORIS_CHECK(_analyzer != nullptr);
}

LuceneBackedTextAnalyzer::~LuceneBackedTextAnalyzer() = default;

std::unique_ptr<TextTokenStream> LuceneBackedTextAnalyzer::create_stream() const {
    return std::make_unique<LuceneBackedTokenStream>(_analyzer.get(), &_char_filters);
}

} // namespace doris::segment_v2::inverted_index
