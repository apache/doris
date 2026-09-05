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

#include "storage/index/inverted/analyzer/analyzer_provider.h"

// gram_scheme.h is completely defined only in this .cpp: to keep the forward include closure of
// exec/pipeline/dependency.h under budget, analyzer_provider.h only forward-declares GramScheme
// (see the comment at the top of analyzer_provider.h). The default implementation of
// gram_scheme() has to construct an empty std::optional<GramScheme>, which requires the complete
// type, so it was moved here, where that definition is needed only while this translation unit
// is actually compiled.
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::inverted_index {

std::optional<gram::GramScheme> AnalyzerProvider::gram_scheme() const {
    return std::nullopt;
}

} // namespace doris::segment_v2::inverted_index
