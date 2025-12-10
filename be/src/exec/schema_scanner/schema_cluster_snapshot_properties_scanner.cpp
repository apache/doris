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

#include "exec/schema_scanner/schema_cluster_snapshot_properties_scanner.h"

#include <cstdint>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "exec/schema_scanner/schema_helper.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaClusterSnapshotPropertiesScanner::_s_tbls_columns = {
        {"SNAPSHOT_ENABLED", TYPE_STRING, sizeof(StringRef), true},
        {"AUTO_SNAPSHOT", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"MAX_RESERVED_SNAPSHOTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"SNAPSHOT_INTERVAL_SECONDS", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaClusterSnapshotPropertiesScanner::SchemaClusterSnapshotPropertiesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_CLUSTER_SNAPSHOT_PROPERTIES) {}

SchemaClusterSnapshotPropertiesScanner::~SchemaClusterSnapshotPropertiesScanner() {}

Status SchemaClusterSnapshotPropertiesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    if (!config::is_cloud_mode()) {
        return Status::InternalError("only support cloud mode");
    }

    return ExecEnv::GetInstance()->storage_engine().to_cloud().meta_mgr().get_snapshot_properties(
            _switch_status, _max_reserved_snapshots, _snapshot_interval_seconds);
}

Status SchemaClusterSnapshotPropertiesScanner::get_next_block_internal(vectorized::Block* block,
                                                                       bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaClusterSnapshotPropertiesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    bool auto_snapshot_enabled =
            _switch_status == cloud::SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON &&
            _max_reserved_snapshots > 0;
    std::string_view snapshot_enable_status;
    switch (_switch_status) {
    case cloud::SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON:
        snapshot_enable_status = "YES";
        break;
    case cloud::SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF:
        snapshot_enable_status = "NO";
        break;
    case cloud::SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED:
        snapshot_enable_status = "DISABLED";
        break;
    default:
        snapshot_enable_status = "UNKNOWN";
        break;
    }
    SchemaScannerHelper::insert_string_value(0, snapshot_enable_status, block);
    SchemaScannerHelper::insert_bool_value(1, auto_snapshot_enabled, block);
    SchemaScannerHelper::insert_int64_value(2, _max_reserved_snapshots, block);
    SchemaScannerHelper::insert_int64_value(3, _snapshot_interval_seconds, block);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
