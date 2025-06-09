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

#include <string>

#include "common/status.h"
#include "exec/schema_scanner/materialized_schema_table.h"
#include "exec/schema_scanner/materialized_schema_table_dir.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

class MaterializedSchemaTableMgr {
public:
    static MaterializedSchemaTableMgr* instance() {
        return ExecEnv::GetInstance()->materialized_schema_table_mgr();
    }

    inline static std::unordered_map<TSchemaTableType::type, std::string>
            MaterializedSchemaTablesType = {
                    {TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS_HISTORY,
                     "SCH_BACKEND_ACTIVE_TASKS_HISTORY"},
    };

    MaterializedSchemaTableMgr(
            std::unordered_map<std::string, std::shared_ptr<MaterializedSchemaTableDir>>&&
                    materialized_schema_table_store_map);
    ~MaterializedSchemaTableMgr() = default;

    Status init();
    void stop() {}

    Status put_block(TSchemaTableType::type type, vectorized::Block* block) {
        if (materialized_schema_table_store_map_.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "config::materialized_schema_table_storage_root_path is null");
        }
        RETURN_IF_ERROR(materialized_schema_tables_[type]->put_block(block));
        return Status::OK();
    }

    Status get_reader(TSchemaTableType::type type, const vectorized::VExprContextSPtrs& expr_ctxs,
                      size_t batch_size, cctz::time_zone timezone_obj,
                      MaterializedSchemaTableReaderSPtr* reader) {
        if (materialized_schema_table_store_map_.empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "config::materialized_schema_table_storage_root_path is null");
        }
        return materialized_schema_tables_[type]->get_reader(expr_ctxs, batch_size, timezone_obj,
                                                             reader);
    }

private:
    std::unordered_map<TSchemaTableType::type, std::shared_ptr<MaterializedSchemaTable>>
            materialized_schema_tables_;
    std::unordered_map<std::string, std::shared_ptr<MaterializedSchemaTableDir>>
            materialized_schema_table_store_map_;
};

#include "common/compile_check_end.h"
} // namespace doris
