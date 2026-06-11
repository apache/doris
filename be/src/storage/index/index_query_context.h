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
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "storage/compaction/collection_similarity.h"
#include "storage/compaction/collection_statistics.h"
#include "storage/olap_common.h"

namespace doris::segment_v2 {

struct IndexReadProbe {
    ColumnId column_id = std::numeric_limits<ColumnId>::max();
    int64_t index_id = -1;
    bool is_null_bitmap = false;
};

struct IndexQueryContext {
    io::IOContext* io_ctx = nullptr;
    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;

    CollectionStatisticsPtr collection_statistics;
    CollectionSimilarityPtr collection_similarity;

    size_t query_limit = 0;
    bool is_asc = false;
    std::vector<IndexReadProbe> index_read_probes;
};
using IndexQueryContextPtr = std::shared_ptr<IndexQueryContext>;

} // namespace doris::segment_v2
