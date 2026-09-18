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

#pragma once

#include <unicode/utext.h>

#include <memory>
#include <span>
#include <string_view>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "common/cast_set.h"
#include "storage/index/inverted/util/reader.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

class DorisTokenizer;
using TokenizerPtr = std::shared_ptr<DorisTokenizer>;

using TokenStreamPtr = std::shared_ptr<TokenStream>;

/**
 * All custom tokenizers and token_filters must use the following functions 
 * to set token information. Using these unified set methods helps avoid 
 * unnecessary data copying.
 * 
 * Note: Must not mix with other set methods
 */
class DorisTokenStream {
public:
    DorisTokenStream() = default;
    virtual ~DorisTokenStream() = default;

    void set(Token* t, const std::string_view& term, int32_t pos = 1) {
        t->setTextNoCopy(term.data(), cast_set<int32_t>(term.size()));
        t->setPositionIncrement(pos);
    }

    void set_text(Token* t, const std::string_view& term) {
        t->setTextNoCopy(term.data(), cast_set<int32_t>(term.size()));
    }

    int32_t get_position_increment(Token* t) { return t->getPositionIncrement(); }
    void set_position_increment(Token* t, int32_t pos) { t->setPositionIncrement(pos); }

    // Return each rune's original relative byte start followed by the token's final byte end.
    virtual std::span<const int32_t> get_source_byte_offsets() const { return {}; }

    // Return separate rune ends when removed delimiters leave gaps between adjacent runes.
    virtual std::span<const int32_t> get_source_byte_end_offsets() const { return {}; }

    // Enable source-boundary tracking only for streams with a downstream consumer.
    virtual void set_source_byte_offsets_enabled(bool enabled) {}
};

class TokenStreamWrapper : public TokenStream {
public:
    explicit TokenStreamWrapper(std::shared_ptr<TokenStream> ts) : _impl(std::move(ts)) {}
    ~TokenStreamWrapper() override = default;

    Token* next(Token* token) override { return _impl->next(token); }
    void close() override { _impl->close(); }
    void reset() override { _impl->reset(); }

private:
    std::shared_ptr<TokenStream> _impl;
};

class TokenStreamComponents {
public:
    TokenStreamComponents(TokenizerPtr tokenizer, TokenStreamPtr result)
            : _source(std::move(tokenizer)), _sink(std::move(result)) {}

    void set_reader(const ReaderPtr& reader);
    TokenStreamPtr get_token_stream();
    TokenizerPtr get_source();

private:
    TokenizerPtr _source;
    TokenStreamPtr _sink;
};
using TokenStreamComponentsPtr = std::shared_ptr<TokenStreamComponents>;

}; // namespace doris::segment_v2::inverted_index
