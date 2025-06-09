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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "exec/schema_scanner/materialized_schema_table_dir.h"
#include "exec/schema_scanner/materialized_schema_table_reader.h"
#include "exec/schema_scanner/materialized_schema_table_writer.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"

class MaterializedSchemaTable {
public:
    MaterializedSchemaTable(
            std::string table_name,
            std::unordered_map<std::string, std::shared_ptr<MaterializedSchemaTableDir>> stores,
            size_t ttl_s);
    ~MaterializedSchemaTable() = default;

    Status init();

    Status put_block(vectorized::Block* block);

    Status get_reader(const vectorized::VExprContextSPtrs& expr_ctxs, size_t batch_size,
                      cctz::time_zone timezone_obj, MaterializedSchemaTableReaderSPtr* reader) {
        if (time_slices_.empty()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("No time slices available for reader");
        }
        *reader = std::make_shared<MaterializedSchemaTableReader>(time_slices_, expr_ctxs,
                                                                  batch_size, timezone_obj);
        RETURN_IF_ERROR((*reader)->prepare(active_block_.get()));
        return Status::OK();
    }

private:
    std::vector<MaterializedSchemaTableWriter*> get_stores_for_flush_(
            TStorageMedium::type storage_medium);

    Status materialized_();

    Status flush_();

    void clear_expired_time_slice_();

    std::string table_name_;
    std::unordered_map<std::string, std::shared_ptr<MaterializedSchemaTableDir>> stores_;
    size_t ttl_s_;

    std::vector<MaterializedSchemaTableTimeSlice> time_slices_;
    std::unique_ptr<vectorized::Block> active_block_ = nullptr;
    std::shared_mutex lock_;
    std::vector<uint64_t> curl_slice_timestamps_;
    std::unordered_map<std::string, MaterializedSchemaTableWriterUPtr> writers_;
};

#include "common/compile_check_end.h"
} // namespace doris
