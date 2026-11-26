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
#include <memory>
#include <string_view>

#include "storage/index/inverted/analysis/token_batch.h"

namespace doris::segment_v2::inverted_index {

// The tokens of one value at a time, handed over in batches. A consumer keeps one stream and one
// batch for many values:
//
//     stream->reset(value);
//     while (stream->next_batch(batch)) {
//         for (const TokenRef& token : batch.tokens()) { ... }
//     }
//     batch.release_oversized();
//
// A stream belongs to one thread; the analyzer that made it may be shared. It throws only
// doris::Exception, from reset() and from next_batch(), and is not used again after it threw.
class TextTokenStream {
public:
    // A streaming source hands over a batch once it holds this many tokens. A core loop may
    // overshoot by what one step makes, and a filter that makes several tokens out of one may
    // exceed it; a consumer relies only on the return value of next_batch().
    static constexpr size_t kBatchTokens = 256;

    virtual ~TextTokenStream() = default;

    // Binds `text`, in any state, and drops whatever is left of the previous value. `text` must
    // stay valid while the stream iterates over it. An empty `text` makes no token, whatever its
    // data pointer: NFKC_CF can fold a value to an empty view that does not point to null.
    virtual void reset(std::string_view text) = 0;

    // Clears `batch`, then fills it. Returns true with at least one token, or false with an empty
    // batch once the value is exhausted, and only then; it keeps returning false until the next
    // reset(). A filter that drops every token of a batch pulls the next one instead of returning
    // an empty batch. A term stays valid until the next next_batch() or reset() on this stream.
    // An empty term may be in the batch: the consumer decides whether it counts.
    virtual bool next_batch(TokenBatch& batch) = 0;

    // The bytes this stream and the streams it wraps keep from one value to the next. Every buffer
    // that survives a value counts, and reset() releases those above the high water mark
    // (scratch.h), so the number stays bounded however long a value was.
    virtual size_t reserved_bytes() const = 0;

    // Strict handling of invalid UTF-8, which the pinyin filter needs when it reports source
    // offsets: icu, ngram and edge_ngram then throw on it, and ik normalizes strictly. A filter
    // forwards it to its input; a tokenizer without a strict form ignores it.
    virtual void set_strict_utf8(bool /*strict*/) {}
};

// The start of a chain: makes tokens out of the text.
class TextTokenizer : public TextTokenStream {};

// A link of a chain: makes tokens out of the tokens of its input, which it owns.
class TextTokenFilter : public TextTokenStream {
public:
    explicit TextTokenFilter(std::unique_ptr<TextTokenStream> in);

    // Resets the input. A filter with state of its own overrides it and calls this first.
    void reset(std::string_view text) override;
    // The bytes of the input. A filter with buffers of its own overrides it and adds them.
    size_t reserved_bytes() const override;
    void set_strict_utf8(bool strict) override;

protected:
    std::unique_ptr<TextTokenStream> _in;
};

} // namespace doris::segment_v2::inverted_index
