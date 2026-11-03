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

#include <CLucene.h> // IWYU pragma: keep

#include <cstddef>
#include <memory>
#include <string_view>

#include "CLucene/analysis/AnalysisHeader.h"
#include "storage/index/inverted/analysis/reader.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "storage/index/inverted/analysis/token_batch.h"

namespace doris::segment_v2::inverted_index {

// A TextTokenStream behind the CLucene TokenStream interface, for the V1 and V2 writers. Terms are
// handed over without a copy: a term stays valid until the next batch, and CLucene is done with a
// token before it asks for the next one. An empty term is handed over too, because the V1 and V2
// writers give it a position. A doris::Exception passes through the CLucene frames to the caller
// of IndexWriter::addDocument(), whose _CLFINALLY blocks clean up on the way.
class LuceneTokenStreamBridge final : public lucene::analysis::TokenStream {
public:
    explicit LuceneTokenStreamBridge(std::unique_ptr<TextTokenStream> stream);
    ~LuceneTokenStreamBridge() override;

    // Parks the value; reset() starts it.
    void bind(std::string_view text);
    // SDocumentsWriter::invertField() calls reset() once per field, and that is what starts the
    // parked value, exactly as DorisTokenizer::reset() promotes its pending reader today.
    void reset() override;
    lucene::analysis::Token* next(lucene::analysis::Token* token) override;
    // invertField() closes the stream after every field, and the scalar path reuses it.
    void close() override {}

private:
    std::unique_ptr<TextTokenStream> _stream;
    TokenBatch _batch;
    size_t _cursor = 0; // the next token of _batch
    std::string_view _pending;
    // No value is started, or the stream is done with it.
    bool _exhausted = true;
};

// A TextAnalyzer behind the CLucene Analyzer interface, for the V1 and V2 writers. The writer
// passes each value as an SStringReader on the column's bytes; the char filters are in the
// TextAnalyzer.
class LuceneAnalyzerBridge final : public lucene::analysis::Analyzer {
public:
    explicit LuceneAnalyzerBridge(TextAnalyzerPtr analyzer);
    ~LuceneAnalyzerBridge() override;

    bool isSDocOpt() override { return true; }

    // A new stream the caller owns. Array elements take this path, one stream per element.
    lucene::analysis::TokenStream* tokenStream(const TCHAR* field_name,
                                               const ReaderPtr& reader) override;
    // The bridge's own stream, rebound to the value. A scalar column takes this path.
    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR* field_name,
                                                       const ReaderPtr& reader) override;
    // Pure virtual in the base and never reached, as in CustomAnalyzer: these throw.
    lucene::analysis::TokenStream* tokenStream(const TCHAR* field_name,
                                               lucene::util::Reader* reader) override;
    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR* field_name,
                                                       lucene::util::Reader* reader) override;

private:
    const TextAnalyzerPtr _analyzer;
    std::unique_ptr<LuceneTokenStreamBridge> _reusable;
};

} // namespace doris::segment_v2::inverted_index
