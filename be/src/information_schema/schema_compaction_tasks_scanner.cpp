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
#include <memory>
#include <string>

#include "common/status.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "runtime/runtime_state.h"
#include "storage/compaction_task_tracker.h"
#include "util/time.h"

namespace doris {

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
        {"INPUT_INDEX_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_TOTAL_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_SEGMENTS_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"INPUT_VERSION_RANGE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"MERGED_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"FILTERED_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_ROW_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_DATA_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_INDEX_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_TOTAL_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_SEGMENTS_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"OUTPUT_VERSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"MERGE_LATENCY_MS", TYPE_BIGINT, sizeof(int64_t), true},
        {"BYTES_READ_FROM_LOCAL", TYPE_BIGINT, sizeof(int64_t), true},
        {"BYTES_READ_FROM_REMOTE", TYPE_BIGINT, sizeof(int64_t), true},
        {"PEAK_MEMORY_BYTES", TYPE_BIGINT, sizeof(int64_t), true},
        {"IS_VERTICAL", TYPE_BOOLEAN, sizeof(bool), true},
        {"PERMITS", TYPE_BIGINT, sizeof(int64_t), true},
        {"VERTICAL_TOTAL_GROUPS", TYPE_BIGINT, sizeof(int64_t), true},
        {"VERTICAL_COMPLETED_GROUPS", TYPE_BIGINT, sizeof(int64_t), true},
        {"STATUS_MSG", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaCompactionTasksScanner::SchemaCompactionTasksScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BE_COMPACTION_TASKS),
          _backend_id(0),
          _task_idx(0) {};

Status SchemaCompactionTasksScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    _backend_id = state->backend_id();
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

    if (_task_idx >= _tasks.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaCompactionTasksScanner::_fill_block_impl(Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    size_t fill_num = std::min(1000UL, _tasks.size() - _task_idx);
    size_t fill_idx_begin = _task_idx;
    size_t fill_idx_end = _task_idx + fill_num;
    std::vector<void*> datas(fill_num);

    auto now_ms = UnixMillis();

    // col 0: BACKEND_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _backend_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // col 1: COMPACTION_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].compaction_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // col 2: TABLE_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].table_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // col 3: PARTITION_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].partition_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // col 4: TABLET_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].tablet_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // col 5: COMPACTION_TYPE
    {
        std::vector<std::string> strs(fill_num);
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = to_string(_tasks[i].compaction_type);
            refs[i - fill_idx_begin] =
                    StringRef(strs[i - fill_idx_begin].c_str(), strs[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    // col 6: STATUS
    {
        std::vector<std::string> strs(fill_num);
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = to_string(_tasks[i].status);
            refs[i - fill_idx_begin] =
                    StringRef(strs[i - fill_idx_begin].c_str(), strs[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }
    // col 7: TRIGGER_METHOD
    {
        std::vector<std::string> strs(fill_num);
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            strs[i - fill_idx_begin] = to_string(_tasks[i].trigger_method);
            refs[i - fill_idx_begin] =
                    StringRef(strs[i - fill_idx_begin].c_str(), strs[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // col 8: COMPACTION_SCORE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].compaction_score;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // col 9: SCHEDULED_TIME (DATETIME, always set)
    {
        std::vector<VecDateTimeValue> srcs(fill_num);
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
    // col 10: START_TIME (DATETIME, nullable: 0 when PENDING)
    {
        std::vector<VecDateTimeValue> srcs(fill_num);
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
    // col 11: END_TIME (DATETIME, nullable: 0 when not completed)
    {
        std::vector<VecDateTimeValue> srcs(fill_num);
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
    // RUNNING: now - start_time; FINISHED/FAILED: end - start; PENDING: 0
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            const auto& task = _tasks[i];
            if (task.status == CompactionTaskStatus::RUNNING && task.start_time_ms > 0) {
                srcs[i - fill_idx_begin] = now_ms - task.start_time_ms;
            } else if ((task.status == CompactionTaskStatus::FINISHED ||
                        task.status == CompactionTaskStatus::FAILED) &&
                       task.start_time_ms > 0) {
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
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_rowsets_count;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, datas));
    }
    // col 14: INPUT_ROW_NUM
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_row_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, datas));
    }
    // col 15: INPUT_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_data_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));
    }
    // col 16: INPUT_INDEX_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_index_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));
    }
    // col 17: INPUT_TOTAL_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_total_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));
    }
    // col 18: INPUT_SEGMENTS_NUM
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].input_segments_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, datas));
    }
    // col 19: INPUT_VERSION_RANGE
    {
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            const auto& s = _tasks[i].input_version_range;
            refs[i - fill_idx_begin] = StringRef(s.c_str(), s.size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 19, datas));
    }
    // col 20: MERGED_ROWS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].merged_rows;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 20, datas));
    }
    // col 21: FILTERED_ROWS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].filtered_rows;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 21, datas));
    }
    // col 22: OUTPUT_ROWS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_rows;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 22, datas));
    }
    // col 23: OUTPUT_ROW_NUM
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_row_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 23, datas));
    }
    // col 24: OUTPUT_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_data_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 24, datas));
    }
    // col 25: OUTPUT_INDEX_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_index_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 25, datas));
    }
    // col 26: OUTPUT_TOTAL_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_total_size;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 26, datas));
    }
    // col 27: OUTPUT_SEGMENTS_NUM
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].output_segments_num;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 27, datas));
    }
    // col 28: OUTPUT_VERSION
    {
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            const auto& s = _tasks[i].output_version;
            refs[i - fill_idx_begin] = StringRef(s.c_str(), s.size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 28, datas));
    }
    // col 29: MERGE_LATENCY_MS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].merge_latency_ms;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 29, datas));
    }
    // col 30: BYTES_READ_FROM_LOCAL
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].bytes_read_from_local;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 30, datas));
    }
    // col 31: BYTES_READ_FROM_REMOTE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].bytes_read_from_remote;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 31, datas));
    }
    // col 32: PEAK_MEMORY_BYTES
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].peak_memory_bytes;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 32, datas));
    }
    // col 33: IS_VERTICAL (BOOLEAN)
    // Note: std::vector<bool> is bit-packed and does not provide real pointers,
    // so we use a std::unique_ptr<bool[]> instead.
    {
        auto srcs = std::make_unique<bool[]>(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].is_vertical;
            datas[i - fill_idx_begin] = &srcs[i - fill_idx_begin];
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 33, datas));
    }
    // col 34: PERMITS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].permits;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 34, datas));
    }
    // col 35: VERTICAL_TOTAL_GROUPS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].vertical_total_groups;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 35, datas));
    }
    // col 36: VERTICAL_COMPLETED_GROUPS
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _tasks[i].vertical_completed_groups;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 36, datas));
    }
    // col 37: STATUS_MSG
    {
        std::vector<StringRef> refs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            const auto& s = _tasks[i].status_msg;
            refs[i - fill_idx_begin] = StringRef(s.c_str(), s.size());
            datas[i - fill_idx_begin] = refs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 37, datas));
    }

    _task_idx += fill_num;
    return Status::OK();
}
} // namespace doris
