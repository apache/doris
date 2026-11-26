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

#include <memory>

#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/text_analyzer.h"

namespace lucene::analysis {
class Analyzer;
} // namespace lucene::analysis

namespace doris::segment_v2::inverted_index {

// A CLucene analyzer behind the TextAnalyzer interface, for the consumers that move to the
// interface before the analyzers do. It goes once every analyzer is native.
//
// Every value gets a new reader through the index's char filter (InvertedIndexAnalyzer::
// create_reader) and a new CLucene stream from Analyzer::tokenStream(), as a query reads a value
// today; the SNII writer does the same with one reader it re-initializes. Sharing the analyzer
// between threads is as safe as sharing the CLucene analyzer for tokenStream() calls. Terms are
// copied into the batch, because a CLucene token is overwritten by the next one; a CLuceneError
// comes out as a doris::Exception.
class LuceneBackedTextAnalyzer final : public TextAnalyzer {
public:
    LuceneBackedTextAnalyzer(std::shared_ptr<lucene::analysis::Analyzer> analyzer,
                             CharFilterMap char_filters);
    ~LuceneBackedTextAnalyzer() override;

    std::unique_ptr<TextTokenStream> create_stream() const override;

private:
    const std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    const CharFilterMap _char_filters;
};

} // namespace doris::segment_v2::inverted_index
