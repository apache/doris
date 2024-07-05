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

#include "exec/schema_scanner/schema_backend_memory_tracker.h"

#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaBackendMemoryTracker::_s_tbls_columns = {
        //   name,       type,          size
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TYPE", TYPE_STRING, sizeof(StringRef), false},
        {"LABEL", TYPE_STRING, sizeof(StringRef), false},
        {"PARENT_LABEL", TYPE_STRING, sizeof(StringRef), false},
        {"MEMORY_LIMIT", TYPE_BIGINT, sizeof(int64_t), false},
        {"CURRENT_MEMORY", TYPE_BIGINT, sizeof(int64_t), false},
        {"PEAK_MEMORY", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBackendMemoryTracker::SchemaBackendMemoryTracker()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_MEMORY_TRACKER) {}

Status SchemaBackendMemoryTracker::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaBackendMemoryTracker::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_memory_tracker_block == nullptr) {
        _memory_tracker_block = vectorized::Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            TypeDescriptor descriptor(_s_tbls_columns[i].type);
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
            _memory_tracker_block->insert(vectorized::ColumnWithTypeAndName(
                    data_type->create_column(), data_type, _s_tbls_columns[i].name));
        }

        std::vector<MemTracker::Snapshot> snapshots;
        MemTrackerLimiter::make_all_memory_state_snapshots(&snapshots);
        _memory_tracker_block->reserve(snapshots.size());

        auto insert_int_value = [&](int col_index, int64_t int_val, vectorized::Block* block) {
            vectorized::MutableColumnPtr mutable_col_ptr;
            mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
            auto* nullable_column =
                    reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
            vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(
                    int_val);
            nullable_column->get_null_map_data().emplace_back(0);
        };

        auto insert_string_value = [&](int col_index, std::string str_val,
                                       vectorized::Block* block) {
            vectorized::MutableColumnPtr mutable_col_ptr;
            mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
            auto* nullable_column =
                    reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
            vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
            reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_val.data(),
                                                                              str_val.size());
            nullable_column->get_null_map_data().emplace_back(0);
        };

        int64_t be_id = ExecEnv::GetInstance()->master_info()->backend_id;
        for (const auto& snapshot : snapshots) {
            insert_int_value(0, be_id, _memory_tracker_block.get());
            insert_string_value(1, snapshot.type, _memory_tracker_block.get());
            insert_string_value(2, snapshot.label, _memory_tracker_block.get());
            insert_string_value(3, snapshot.parent_label, _memory_tracker_block.get());
            insert_int_value(4, snapshot.limit, _memory_tracker_block.get());
            insert_int_value(5, snapshot.cur_consumption, _memory_tracker_block.get());
            insert_int_value(6, snapshot.peak_consumption, _memory_tracker_block.get());
        }

        _total_rows = _memory_tracker_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_memory_tracker_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}
}; // namespace doris