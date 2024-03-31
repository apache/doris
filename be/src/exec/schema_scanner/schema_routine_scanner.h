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

#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

class SchemaRoutinesScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaRoutinesScanner);

public:
    SchemaRoutinesScanner();
    ~SchemaRoutinesScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_block(vectorized::Block* block, bool* eos) override;

    static std::vector<SchemaScanner::ColumnDesc> _s_tbls_columns;

private:
    Status get_block_from_fe();

    int _block_rows_limit = 4096;
    int _row_idx = 0;
    int _total_rows = 0;
    std::unique_ptr<vectorized::Block> _routines_block = nullptr;
    int _rpc_timeout = 3000;
};
}; // namespace doris
