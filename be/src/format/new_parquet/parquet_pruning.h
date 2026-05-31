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

#include <cstdint>

namespace doris::parquet {

enum class ParquetRowGroupPruneReason {
    NONE,
    STATISTICS,
    DICTIONARY,
};

struct ParquetPruningStats {
    int64_t total_row_groups = 0;
    int64_t selected_row_groups = 0;
    int64_t filtered_row_groups_by_statistics = 0;
    int64_t filtered_row_groups_by_dictionary = 0;
    int64_t filtered_row_groups_by_page_index = 0;
    int64_t filtered_group_rows = 0;
    int64_t filtered_page_rows = 0;
    int64_t selected_row_ranges = 0;
    int64_t page_index_read_calls = 0;
};

} // namespace doris::parquet
