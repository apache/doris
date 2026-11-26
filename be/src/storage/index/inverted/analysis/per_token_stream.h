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

#include <cstddef>
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"

namespace doris::segment_v2::inverted_index {

// The base of a tokenizer that makes one token at a time. It keeps its state machine and may build
// every term in one reused scratch buffer: the base copies each term into the batch, because views
// of a reused scratch would overwrite each other within one batch.
class PerTokenTokenizer : public TextTokenizer {
public:
    void reset(std::string_view text) final;
    bool next_batch(TokenBatch& batch) final;

protected:
    // Binds `text`, dropping the state of the previous value.
    virtual void on_reset(std::string_view text) = 0;
    // The next token; its term stays valid until the next call. Once it returned false it is not
    // called again before the next reset().
    virtual bool next_token(TokenRef& token) = 0;

private:
    // No value is bound, or next_token() returned false.
    bool _exhausted = true;
};

// The base of a filter that makes one token at a time out of the tokens of its input, which it
// reads with input_token(). The input batch is private and only input_token() refills it, so the
// filter may hold an input token across next_batch() calls, as word_delimiter does while it hands
// out the parts of one token.
class PerTokenFilter : public TextTokenFilter {
public:
    using TextTokenFilter::TextTokenFilter;

    void reset(std::string_view text) final;
    bool next_batch(TokenBatch& batch) final;
    // Adds the input batch. A filter with buffers of its own overrides it and adds them.
    size_t reserved_bytes() const override;

protected:
    // Drops the filter's state of the previous value; the input is reset already.
    virtual void on_reset() {}
    // The next token; its term stays valid until the next call. Once it returned false it is not
    // called again before the next reset().
    virtual bool next_token(TokenRef& token) = 0;
    // The next input token, or false once the input is exhausted. Its term stays valid until the
    // next call.
    bool input_token(TokenRef& token);

private:
    TokenBatch _input;
    size_t _cursor = 0; // the next token of _input
    // No value is bound, or next_token() returned false.
    bool _exhausted = true;
};

} // namespace doris::segment_v2::inverted_index
