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

#include "exec/schema_scanner/schema_tablets_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_common.pb.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaTabletsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"REPLICA_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLET_PATH", TYPE_STRING, sizeof(StringRef), true},
        {"TABLET_LOCAL_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLET_REMOTE_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"VERSION_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"SEGMENT_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_COLUMNS", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"COMPACTION_SCORE", TYPE_INT, sizeof(int32_t), true},
        {"COMPRESS_KIND", TYPE_STRING, sizeof(StringRef), true},
        {"IS_USED", TYPE_BOOLEAN, sizeof(bool), true},
        {"IS_ALTER_FAILED", TYPE_BOOLEAN, sizeof(bool), true},
};

SchemaTabletsScanner::SchemaTabletsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_TABLETS),
          _backend_id(0),
          _tablets_idx(0) {};

Status SchemaTabletsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    _backend_id = state->backend_id();
    RETURN_IF_ERROR(_get_all_tablets());
    return Status::OK();
}

Status SchemaTabletsScanner::_get_all_tablets() {
    if (config::is_cloud_mode()) {
        // TODO get tablets on cloud
        return Status::InternalError("Not support get tablets on the cloud");
    }
    auto tablets =
            ExecEnv::GetInstance()->storage_engine().to_local().tablet_manager()->get_all_tablet();
    _tablets = std::move(tablets);
    return Status::OK();
}

Status SchemaTabletsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_tablets_idx >= _tablets.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaTabletsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    size_t row_num = _tablets.size();
    if (row_num == 0) {
        return Status::OK();
    }

    size_t fill_tablets_num = std::min(1000UL, _tablets.size() - _tablets_idx);
    size_t fill_idx_begin = _tablets_idx;
    size_t fill_idx_end = _tablets_idx + fill_tablets_num;
    std::vector<void*> datas(fill_tablets_num);

    auto fill_column = [&](auto&& get_value, size_t column_index) {
        using ValueType = std::decay_t<decltype(get_value(std::declval<TabletSharedPtr>()))>;
        std::vector<ValueType> srcs(fill_tablets_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            TabletSharedPtr tablet = _tablets[i];
            srcs[i - fill_idx_begin] = get_value(tablet);
            datas[i - fill_idx_begin] = &srcs[i - fill_idx_begin];
        }
        return fill_dest_column_for_range(block, column_index, datas);
    };

    auto fill_boolean_column = [&](auto&& get_value, size_t column_index) {
        std::vector<int8_t> srcs(fill_tablets_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            TabletSharedPtr tablet = _tablets[i];
            srcs[i - fill_idx_begin] = get_value(tablet);
            datas[i - fill_idx_begin] = &srcs[i - fill_idx_begin];
        }
        return fill_dest_column_for_range(block, column_index, datas);
    };

    RETURN_IF_ERROR(fill_column([this](auto tablet) { return _backend_id; }, 0));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->table_id(); }, 1));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->replica_id(); }, 2));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->partition_id(); }, 3));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_path(); }, 4));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->tablet_local_size(); }, 5));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->tablet_remote_size(); }, 6));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return static_cast<int64_t>(tablet->tablet_meta()->version_count()); }, 7));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->get_all_segments_size(); }, 8));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->tablet_meta()->tablet_columns_num(); }, 9));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return static_cast<int64_t>(tablet->row_size()); }, 10));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return tablet->get_compaction_score(); }, 11));
    RETURN_IF_ERROR(fill_column([](TabletSharedPtr tablet) { return CompressKind_Name(tablet->compress_kind()); }, 12));
    RETURN_IF_ERROR(fill_boolean_column([](TabletSharedPtr tablet) { return tablet->is_used(); }, 13));
    RETURN_IF_ERROR(fill_boolean_column([](TabletSharedPtr tablet) { return tablet->is_alter_failed(); }, 14));

    _tablets_idx += fill_tablets_num;
    return Status::OK();
}
} // namespace doris