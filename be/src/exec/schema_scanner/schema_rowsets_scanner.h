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
#include <memory>

#include "common/status.h"
#include "exec/schema_scanner.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
namespace doris {
class SchemaRowsetsScanner : public SchemaScanner {
public:
    SchemaRowsetsScanner();
    ~SchemaRowsetsScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos) override;

private:
    Status get_all_rowsets();
    // Status get_new_segments();
    Status fill_one_row(Tuple* tuple, MemPool* pool);

private:
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
    int64_t backend_id_ = 0;
    std::vector<RowsetSharedPtr> rowsets_;
    // used for traversing rowsets_
    int rowsets_idx_ = 0;
};
} // namespace doris
