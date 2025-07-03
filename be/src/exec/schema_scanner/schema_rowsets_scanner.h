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
#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"
#include "olap/rowset/rowset.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

class SchemaRowsetsScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaRowsetsScanner);

public:
    SchemaRowsetsScanner();
    ~SchemaRowsetsScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

private:
    Status _get_all_rowsets();
    Status _fill_block_impl(vectorized::Block* block);

    static std::vector<SchemaScanner::ColumnDesc> _s_tbls_columns;
    int64_t backend_id_ = 0;
    size_t _rowsets_idx = 0;
    std::vector<RowsetSharedPtr> rowsets_;
};
} // namespace doris
