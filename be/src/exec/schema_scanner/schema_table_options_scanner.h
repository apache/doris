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
#include <gen_cpp/FrontendService_types.h>

#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

class SchemaTableOptionsScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaTableOptionsScanner);

public:
    SchemaTableOptionsScanner();
    ~SchemaTableOptionsScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

    static std::vector<SchemaScanner::ColumnDesc> _s_tbls_columns;

private:
    Status get_onedb_info_from_fe(int64_t dbId);
    bool check_and_mark_eos(bool* eos) const;
    int _block_rows_limit = 4096;
    int _db_index = 0;
    TGetDbsResult _db_result;
    int _row_idx = 0;
    int _total_rows = 0;
    std::unique_ptr<vectorized::Block> _tableoptions_block = nullptr;
    int _rpc_timeout_ms = 3000;
};
}; // namespace doris
