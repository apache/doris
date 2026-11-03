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

#include "storage/index/inverted/analysis/text_token_stream.h"

namespace doris::segment_v2::inverted_index {

// Makes the token streams of one analysis configuration. An analyzer does not change once it is
// built, so any number of threads may share one, each with streams of its own.
class TextAnalyzer {
public:
    virtual ~TextAnalyzer() = default;

    // A new stream. It must not outlive the analyzer: a stream may refer to what the analyzer
    // holds, as an ICU normalizer refers to the character set its factory keeps.
    virtual std::unique_ptr<TextTokenStream> create_stream() const = 0;
};

using TextAnalyzerPtr = std::shared_ptr<const TextAnalyzer>;

} // namespace doris::segment_v2::inverted_index
