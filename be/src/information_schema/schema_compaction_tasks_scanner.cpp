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

#include "information_schema/schema_compaction_tasks_scanner.h"

#include <gen_cpp/Descriptors_types.h>

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "storage/compaction/compaction_task_tracker.h"
#include "util/time.h"

namespace doris {
class Block;

#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaCompactionTasksScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"BACKEND_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"COMPACTION_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"COMPACTION_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"STATUS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TRIGGER_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COMPACTION_SCORE", TYPE_BIGINT, sizeof(int64_t), true},
        {"SCHEDULED_TIME", TYPE_DATETIME, sizeof(int64_t), true},
        {"START_TIME", TYPE_DATETIME, sizeof(int64_t), true},
        {"END_TIME", TYPE_DATETIME, sizeof(int64_t), true},
        {"ELAPSED_TIME_MS", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_ROWSETS_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_ROW_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_DATA_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_SEGMENTS_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_VERSION_RANGE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"MERGED_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"FILTERED_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_ROW_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_DATA_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_SEGMENTS_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_VERSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"BYTES_READ_FROM_LOCAL", TYPE_BIGINT, sizeof(int64_t), true},
        {"BYTES_READ_FROM_REMOTE", TYPE_BIGINT, sizeof(int64_t), true},
        {"PEAK_MEMORY_BYTES", TYPE_BIGINT, sizeof(int64_t), true},
        {"IS_VERTICAL", TYPE_BOOLEAN, sizeof(bool), true},
        {"PERMITS", TYPE_BIGINT, sizeof(int64_t), true},
        {"STATUS_MSG", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaCompactionTasksScanner::SchemaCompactionTasksScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BE_COMPACTION_TASKS),
          backend_id_(0),
          _tasks_idx(0) {};

Status SchemaCompactionTasksScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    backend_id_ = state->backend_id();
    _tasks = CompactionTaskTracker::instance()->get_all_tasks();
    return Status::OK();
}

Status SchemaCompactionTasksScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_tasks_idx >= _tasks.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaCompactionTasksScanner::_fill_block_impl(Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    size_t fill_tasks_num = std::min(1000UL, _tasks.size() - _tasks_idx);
    size_t fill_idx_begin = _tasks_idx;
    size_t fill_idx_end = _tasks_idx + fill_tasks_num;
    std::vector<void*> datas(fill_tasks_num);

    // col 0: BACKEND_ID
    {
        int64_t src = backend_id_;
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            datas[i - fill_idx_begin] = &src;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // col 1: COMPACTION_ID
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].compaction_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // col 2: TABLE_ID
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].table_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // col 3: PARTITION_ID
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].partition_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // col 4: TABLET_ID
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].tablet_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // col 5: COMPACTION_TYPE
    {
        std::vector<std::string> strs_storage(fill_tasks_num);
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs_storage[i - fill_idx_begin] = to_string(_tasks[i].compaction_type);
            strs[i - fill_idx_begin] = StringRef(strs_storage[i - fill_idx_begin].c_str(),
                                                  strs_storage[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    // col 6: STATUS
    {
        std::vector<std::string> strs_storage(fill_tasks_num);
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs_storage[i - fill_idx_begin] = to_string(_tasks[i].status);
            strs[i - fill_idx_begin] = StringRef(strs_storage[i - fill_idx_begin].c_str(),
                                                  strs_storage[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }
    // col 7: TRIGGER_METHOD
    {
        std::vector<std::string> strs_storage(fill_tasks_num);
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs_storage[i - fill_idx_begin] = to_string(_tasks[i].trigger_method);
            strs[i - fill_idx_begin] = StringRef(strs_storage[i - fill_idx_begin].c_str(),
                                                  strs_storage[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // col 8: COMPACTION_SCORE
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].compaction_score;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // col 9: SCHEDULED_TIME
    {
        std::vector<VecDateTimeValue> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            int64_t ts_ms = _tasks[i].scheduled_time_ms;
            if (ts_ms > 0) {
                srcs[i - fill_idx_begin].from_unixtime(ts_ms / 1000, _timezone_obj);
                datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
            } else {
                datas[i - fill_idx_begin] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));
    }
    // col 10: START_TIME
    {
        std::vector<VecDateTimeValue> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            int64_t ts_ms = _tasks[i].start_time_ms;
            if (ts_ms > 0) {
                srcs[i - fill_idx_begin].from_unixtime(ts_ms / 1000, _timezone_obj);
                datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
            } else {
                datas[i - fill_idx_begin] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));
    }
    // col 11: END_TIME
    {
        std::vector<VecDateTimeValue> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            int64_t ts_ms = _tasks[i].end_time_ms;
            if (ts_ms > 0) {
                srcs[i - fill_idx_begin].from_unixtime(ts_ms / 1000, _timezone_obj);
                datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
            } else {
                datas[i - fill_idx_begin] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));
    }
    // col 12: ELAPSED_TIME_MS
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            const auto& task = _tasks[i];
            if (task.status == CompactionTaskStatus::RUNNING) {
                srcs[i - fill_idx_begin] = UnixMillis() - task.start_time_ms;
            } else if (task.status == CompactionTaskStatus::FINISHED ||
                       task.status == CompactionTaskStatus::FAILED) {
                srcs[i - fill_idx_begin] = task.end_time_ms - task.start_time_ms;
            } else {
                srcs[i - fill_idx_begin] = 0;
            }
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 12, datas));
    }
    // col 13: INPUT_ROWSETS_COUNT
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_rowsets_count;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, datas));
    }
    // col 14: INPUT_ROW_NUM
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_row_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, datas));
    }
    // col 15: INPUT_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_data_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));
    }
    // col 16: INPUT_SEGMENTS_NUM
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_segments_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));
    }
    // col 17: INPUT_VERSION_RANGE
    {
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = StringRef(_tasks[i].input_version_range.c_str(),
                                                  _tasks[i].input_version_range.size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));
    }
    // col 18: MERGED_ROWS
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].merged_rows;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, datas));
    }
    // col 19: FILTERED_ROWS
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].filtered_rows;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 19, datas));
    }
    // col 20: OUTPUT_ROW_NUM
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_row_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 20, datas));
    }
    // col 21: OUTPUT_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_data_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 21, datas));
    }
    // col 22: OUTPUT_SEGMENTS_NUM
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_segments_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 22, datas));
    }
    // col 23: OUTPUT_VERSION
    {
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = StringRef(_tasks[i].output_version.c_str(),
                                                  _tasks[i].output_version.size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 23, datas));
    }
    // col 24: BYTES_READ_FROM_LOCAL
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].bytes_read_from_local;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 24, datas));
    }
    // col 25: BYTES_READ_FROM_REMOTE
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].bytes_read_from_remote;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 25, datas));
    }
    // col 26: PEAK_MEMORY_BYTES
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].peak_memory_bytes;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 26, datas));
    }
    // col 27: IS_VERTICAL (use uint8_t because vector<bool> is bitset, data() is deleted)
    {
        std::vector<uint8_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].is_vertical ? 1 : 0;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 27, datas));
    }
    // col 28: PERMITS
    {
        std::vector<int64_t> srcs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].permits;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 28, datas));
    }
    // col 29: STATUS_MSG
    {
        std::vector<StringRef> strs(fill_tasks_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = StringRef(_tasks[i].status_msg.c_str(),
                                                  _tasks[i].status_msg.size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 29, datas));
    }

    _tasks_idx += fill_tasks_num;
    return Status::OK();
}
} // namespace doris
