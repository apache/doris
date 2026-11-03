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

#include "storage/index/inverted/clucene/lucene_analyzer_bridge.h"

#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

namespace {

// The whole value of `reader`, without a copy. SStringReader::read() hands out a view of its bytes
// and returns -1, not 0, for an empty value.
std::string_view read_value(const ReaderPtr& reader) {
    const char* start = nullptr;
    const int32_t length = reader->read(reinterpret_cast<const void**>(&start), 0,
                                        cast_set<int32_t>(reader->size()));
    return length > 0 ? std::string_view(start, length) : std::string_view();
}

} // namespace

LuceneTokenStreamBridge::LuceneTokenStreamBridge(std::unique_ptr<TextTokenStream> stream)
        : _stream(std::move(stream)) {
    DORIS_CHECK(_stream != nullptr);
}

LuceneTokenStreamBridge::~LuceneTokenStreamBridge() = default;

void LuceneTokenStreamBridge::bind(std::string_view text) {
    _pending = text;
}

void LuceneTokenStreamBridge::reset() {
    // Exhausted until the stream accepted the value, so a value whose reset() threw makes no token.
    _exhausted = true;
    _batch.clear();
    _cursor = 0;
    _stream->reset(_pending);
    _exhausted = false;
}

lucene::analysis::Token* LuceneTokenStreamBridge::next(lucene::analysis::Token* token) {
    if (_cursor == _batch.size()) {
        _cursor = 0;
        if (_exhausted || !_stream->next_batch(_batch)) {
            _exhausted = true;
            _batch.release_oversized();
            return nullptr;
        }
    }
    const TokenRef& ref = _batch.tokens()[_cursor++];
    token->setTextNoCopy(ref.data, static_cast<int32_t>(ref.size));
    token->setPositionIncrement(ref.position_increment);
    return token;
}

LuceneAnalyzerBridge::LuceneAnalyzerBridge(TextAnalyzerPtr analyzer)
        : _analyzer(std::move(analyzer)) {
    DORIS_CHECK(_analyzer != nullptr);
}

LuceneAnalyzerBridge::~LuceneAnalyzerBridge() = default;

lucene::analysis::TokenStream* LuceneAnalyzerBridge::tokenStream(const TCHAR* /*field_name*/,
                                                                 const ReaderPtr& reader) {
    auto stream = std::make_unique<LuceneTokenStreamBridge>(_analyzer->create_stream());
    stream->bind(read_value(reader));
    return stream.release();
}

lucene::analysis::TokenStream* LuceneAnalyzerBridge::reusableTokenStream(
        const TCHAR* /*field_name*/, const ReaderPtr& reader) {
    if (_reusable == nullptr) {
        _reusable = std::make_unique<LuceneTokenStreamBridge>(_analyzer->create_stream());
    }
    _reusable->bind(read_value(reader));
    return _reusable.get();
}

lucene::analysis::TokenStream* LuceneAnalyzerBridge::tokenStream(const TCHAR* /*field_name*/,
                                                                 lucene::util::Reader* /*reader*/) {
    throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                    "LuceneAnalyzerBridge::tokenStream needs a ReaderPtr");
}

lucene::analysis::TokenStream* LuceneAnalyzerBridge::reusableTokenStream(
        const TCHAR* /*field_name*/, lucene::util::Reader* /*reader*/) {
    throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                    "LuceneAnalyzerBridge::reusableTokenStream needs a ReaderPtr");
}

} // namespace doris::segment_v2::inverted_index
