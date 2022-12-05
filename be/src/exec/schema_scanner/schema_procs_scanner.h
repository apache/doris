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

#include "exec/schema_scanner.h"

namespace doris {

class SchemaProcsScanner : public SchemaScanner {
public:
    SchemaProcsScanner();
    ~SchemaProcsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos) override;

private:
    Status fill_one_row(Tuple* tuple, MemPool* pool);
    Status _fetch_procs_info();
    Status fill_one_col(const std::string* src, MemPool* pool, void* slot);

    std::vector<TFunction> _batch_data;
    size_t _row_idx;
    static SchemaScanner::ColumnDesc _s_procs_columns[];
};

} // namespace doris
