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

#include "olap/collection_similarity.h"
#include "olap/collection_statistics.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

struct IndexQueryContext {
    io::IOContext* io_ctx = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;

    CollectionStatisticsPtr collection_statistics;
    CollectionSimilarityPtr collection_similarity;
};
using IndexQueryContextPtr = std::shared_ptr<IndexQueryContext>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2