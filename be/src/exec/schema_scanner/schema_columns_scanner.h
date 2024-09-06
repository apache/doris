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

#include <string>
#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

class SchemaColumnsScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaColumnsScanner);

public:
    SchemaColumnsScanner();
    ~SchemaColumnsScanner() override;
    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

private:
    Status _get_new_table();
    Status _get_new_desc();
    Status _get_create_table(std::string* result);
    Status _fill_block_impl(vectorized::Block* block);
    std::string _to_mysql_data_type_string(TColumnDesc& desc);
    std::string _type_to_string(TColumnDesc& desc);

    int _db_index;
    int _table_index;
    TGetDbsResult _db_result;
    TGetTablesResult _table_result;
    TDescribeTablesResult _desc_result;
    static std::vector<SchemaScanner::ColumnDesc> _s_col_columns;
};

} // namespace doris
