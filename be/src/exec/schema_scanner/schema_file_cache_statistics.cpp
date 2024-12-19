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

#include "exec/schema_scanner/schema_file_cache_statistics.h"

#include "io/cache/block_file_cache_factory.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaFileCacheStatisticsScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"BE_IP", TYPE_STRING, sizeof(StringRef), false},
        {"CACHE_PATH", TYPE_STRING, sizeof(StringRef), false},
        {"METRIC_NAME", TYPE_STRING, sizeof(StringRef), false},
        {"METRIC_VALUE", TYPE_STRING, sizeof(StringRef), false}};

SchemaFileCacheStatisticsScanner::SchemaFileCacheStatisticsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_FILE_CACHE_STATISTICS) {}

SchemaFileCacheStatisticsScanner::~SchemaFileCacheStatisticsScanner() {}

Status SchemaFileCacheStatisticsScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaFileCacheStatisticsScanner::get_next_block_internal(vectorized::Block* block,
                                                                 bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_stats_block == nullptr) {
        _stats_block = vectorized::Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            TypeDescriptor descriptor(_s_tbls_columns[i].type);
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
            _stats_block->insert(vectorized::ColumnWithTypeAndName(
                    data_type->create_column(), data_type, _s_tbls_columns[i].name));
        }

        _stats_block->reserve(_block_rows_limit);

        ExecEnv::GetInstance()->file_cache_factory()->get_cache_stats_block(_stats_block.get());
        _total_rows = _stats_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_stats_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
