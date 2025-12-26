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

#include "exec/schema_scanner/schema_cluster_snapshots_scanner.h"

#include <cstdint>
#include <string>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "olap/storage_engine.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "vec/common/string_ref.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaClusterSnapshotsScanner::_s_tbls_columns = {
        {"ID", TYPE_STRING, sizeof(StringRef), true},
        {"ANCESTOR", TYPE_STRING, sizeof(StringRef), true},
        {"CREATE_AT", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), true},
        {"FINISH_AT", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), true},
        {"IMAGE_URL", TYPE_STRING, sizeof(StringRef), true},
        {"JOURNAL_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"STATE", TYPE_STRING, sizeof(StringRef), true},
        {"AUTO", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"TTL", TYPE_BIGINT, sizeof(int64_t), true},
        {"LABEL", TYPE_STRING, sizeof(StringRef), true},
        {"MSG", TYPE_STRING, sizeof(StringRef), true},
        {"COUNT", TYPE_INT, sizeof(int32_t), true},
};

SchemaClusterSnapshotsScanner::SchemaClusterSnapshotsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_CLUSTER_SNAPSHOTS) {}

SchemaClusterSnapshotsScanner::~SchemaClusterSnapshotsScanner() {}

Status SchemaClusterSnapshotsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    if (!config::is_cloud_mode()) {
        return Status::InternalError("only support cloud mode");
    }

    return ExecEnv::GetInstance()->storage_engine().to_cloud().meta_mgr().list_snapshot(_snapshots);
}

Status SchemaClusterSnapshotsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_snapshots.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaClusterSnapshotsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    size_t row_num = _snapshots.size();
    if (row_num == 0) {
        return Status::OK();
    }

    std::vector<void*> datas(row_num);
    std::vector<StringRef> strs(row_num);
    // snapshot_id
    {
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_snapshot_id()) {
                strs[i] = StringRef(snapshot.snapshot_id().c_str(), snapshot.snapshot_id().size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // ancestor_id
    {
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_ancestor_id()) {
                strs[i] = StringRef(snapshot.ancestor_id().c_str(), snapshot.ancestor_id().size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // create_at
    {
        std::vector<DateV2Value<DateTimeV2ValueType>> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_create_at()) {
                int64_t value = _snapshots[i].create_at();
                srcs[i].from_unixtime(value, _timezone_obj);
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // finish_at
    {
        std::vector<DateV2Value<DateTimeV2ValueType>> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_finish_at()) {
                int64_t value = _snapshots[i].finish_at();
                srcs[i].from_unixtime(value, _timezone_obj);
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // image_url
    {
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_image_url()) {
                strs[i] = StringRef(snapshot.image_url().c_str(), snapshot.image_url().size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // journal_id
    {
        std::vector<int64_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_journal_id()) {
                srcs[i] = _snapshots[i].journal_id();
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    // status
    {
        std::vector<std::string> status(row_num);
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_status()) {
                status[i] = SnapshotStatus_Name(snapshot.status());
                strs[i] = StringRef(status[i].c_str(), status[i].size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }
    // auto_snapshot
    {
        std::vector<int8_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_auto_snapshot()) {
                srcs[i] = _snapshots[i].auto_snapshot() ? 1 : 0;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // ttl_seconds
    {
        std::vector<int64_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            if (_snapshots[i].has_ttl_seconds()) {
                srcs[i] = _snapshots[i].ttl_seconds();
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // label
    {
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_snapshot_label()) {
                strs[i] = StringRef(snapshot.snapshot_label().c_str(),
                                    snapshot.snapshot_label().size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));
    }
    // reason
    {
        for (int i = 0; i < row_num; ++i) {
            auto& snapshot = _snapshots[i];
            if (snapshot.has_reason()) {
                strs[i] = StringRef(snapshot.reason().c_str(), snapshot.reason().size());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));
    }
    // count
    {
        std::vector<int32_t> srcs(row_num);
        for (int i = 0; i < row_num; ++i) {
            srcs[i] = _snapshots[i].derived_instance_ids_size();
            datas[i] = srcs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
