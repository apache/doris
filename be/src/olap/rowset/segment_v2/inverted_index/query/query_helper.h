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

#include "olap/rowset/segment_v2/inverted_index/query/query.h"

namespace doris::segment_v2 {

class QueryHelper {
public:
    static void collect(const IndexQueryContextPtr& context,
                        const std::vector<SimilarityPtr>& similarities,
                        const std::vector<TermIterPtr>& iterators, int32_t doc);
    static void collect(const IndexQueryContextPtr& context,
                        const std::vector<SimilarityPtr>& similarities,
                        const std::vector<DISI>& iterators, int32_t doc);

    static void collect_many(const IndexQueryContextPtr& context, const SimilarityPtr& similarity,
                             const DocRange& doc_range);
    static void collect_range(const IndexQueryContextPtr& context, const SimilarityPtr& similarity,
                              const DocRange& doc_range);
};

} // namespace doris::segment_v2