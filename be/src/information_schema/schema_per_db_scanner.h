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
#include "information_schema/schema_scanner.h"

namespace doris {
class RuntimeState;
class Block;

// Scans a schema table that the FE builds one database at a time: list the databases of
// the catalog up front, then ask the master FE for the rows of each in turn. Subclasses
// supply their column list and the two enum values that name the table, and override the
// hooks below if the FE needs more than that to answer.
class SchemaPerDbScanner : public SchemaScanner {
public:
    SchemaPerDbScanner(const std::vector<SchemaScanner::ColumnDesc>& columns,
                       TSchemaTableType::type type, TSchemaTableName::type request_name,
                       std::string display_name);
    // Defined out of line so that the `unique_ptr<Block>` member below does not force every
    // subclass to see the definition of Block.
    ~SchemaPerDbScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(Block* block, bool* eos) override;

protected:
    // Hooks for the tables that need more than the catalog and the current user to say what
    // they want. Most tables need neither, so both default to adding nothing.
    virtual void add_extra_db_params(TGetDbsParams*) {}
    virtual void add_extra_request_params(TSchemaTableRequestParams*) {}

private:
    Status get_onedb_info_from_fe(int64_t db_id);
    Status fill_block_from_result(TFetchSchemaTableDataResult& result);
    bool check_and_mark_eos(bool* eos) const;

    const TSchemaTableName::type _request_name;
    // Used in log messages so a failure names the table it came from.
    const std::string _display_name;
    // Kept so that the loop over databases can give up when the query is cancelled.
    RuntimeState* _state = nullptr;
    int _block_rows_limit = 4096;
    int _db_index = 0;
    TGetDbsResult _db_result;
    int _row_idx = 0;
    int _total_rows = 0;
    std::unique_ptr<Block> _fetched_block = nullptr;
    int _rpc_timeout_ms = 3000;
};

} // namespace doris
