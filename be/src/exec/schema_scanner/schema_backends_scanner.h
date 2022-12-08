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

class SchemaBackendsScanner : public SchemaScanner {
public:
    SchemaBackendsScanner();
    ~SchemaBackendsScanner() override = default;

    Status start(RuntimeState* state) override;
    Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos) override;

private:
    Status _fill_one_row(Tuple* tuple, MemPool* pool);
    Status _fetch_backends_info();
    Status _fill_one_col(Tuple* tuple, MemPool* pool, size_t idx);
    Status _set_col_name_to_type();

private:
    // column_name -> type, set by _set_col_name_to_type()
    std::unordered_map<std::string, PrimitiveType> _col_name_to_type;

    std::vector<TRow> _batch_data;
    size_t _row_idx;
};
} // namespace doris
