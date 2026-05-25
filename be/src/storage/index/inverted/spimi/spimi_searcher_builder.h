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

#include "common/status.h"
#include "storage/index/inverted/inverted_index_searcher.h"

namespace doris::segment_v2 {

// `IndexSearcherBuilder` that opens a SPIMI segment from a CLucene
// Directory*, builds `SpimiCLuceneIndexReader`, and wraps it in a
// `lucene::search::IndexSearcher`. Returned via the existing
// `IndexSearcherBuilder::create_index_searcher_builder` switch when
// the reader type is `SPIMI_FULLTEXT`.
//
// File names assumed in the Directory: `_0.tis`, `_0.tii`,
// `_0.frq`, `_0.prx`, `_0.fnm`, and `segments_1`. P37c-3 wires the
// V4 writer to produce these exact names.
class SpimiSearcherBuilder : public IndexSearcherBuilder {
public:
    Status build(lucene::store::Directory* directory,
                 OptionalIndexSearcherPtr& output_searcher) override;
};

} // namespace doris::segment_v2
