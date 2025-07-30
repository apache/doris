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
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/status.h"
#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "olap/storage_engine.h"
#include "olap/tablet_fwd.h"
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
        {"CREATE_TIME", TYPE_DATETIME, sizeof(int64_t), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(int64_t), true},
        {"IS_OVERLAP", TYPE_BOOLEAN, sizeof(bool), true},
};

SchemaTabletsScanner::SchemaTabletsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_TABLETS) {};

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
        auto tablets =
                ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_mgr().get_all_tablet();
        std::ranges::for_each(tablets, [&](auto& tablet) {
            _tablets.push_back(std::static_pointer_cast<BaseTablet>(tablet));
        });
    } else {
        auto tablets = ExecEnv::GetInstance()
                               ->storage_engine()
                               .to_local()
                               .tablet_manager()
                               ->get_all_tablet();
        std::ranges::for_each(tablets, [&](auto& tablet) {
            _tablets.push_back(std::static_pointer_cast<BaseTablet>(tablet));
        });
    }
    return Status::OK();
}

Status SchemaTabletsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaTabletsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    size_t row_num = _tablets.size();
    if (row_num == 0) {
        return Status::OK();
    }

    size_t fill_tablets_num = _tablets.size();
    std::vector<void*> datas(fill_tablets_num);

    for (int i = 0; i < _tablets.size(); i++) {
        BaseTabletSPtr tablet = _tablets[i];
        // BE_ID
        SchemaScannerHelper::insert_int64_value(0, _backend_id, block);

        // TABLET_ID
        SchemaScannerHelper::insert_int64_value(1, tablet->tablet_meta()->tablet_id(), block);

        // REPLICA_ID
        SchemaScannerHelper::insert_int64_value(2, tablet->tablet_meta()->replica_id(), block);

        // PARTITION_ID
        SchemaScannerHelper::insert_int64_value(3, tablet->tablet_meta()->partition_id(), block);

        // TABLET_PATH
        SchemaScannerHelper::insert_string_value(4, tablet->tablet_path(), block);

        // TABLET_LOCAL_SIZE
        SchemaScannerHelper::insert_int64_value(5, tablet->tablet_meta()->tablet_local_size(),
                                                block);

        // TABLET_REMOTE_SIZE
        SchemaScannerHelper::insert_int64_value(6, tablet->tablet_meta()->tablet_remote_size(),
                                                block);

        // VERSION_COUNT
        SchemaScannerHelper::insert_int64_value(
                7, static_cast<int64_t>(tablet->tablet_meta()->version_count()), block);

        // SEGMENT_COUNT
        SchemaScannerHelper::insert_int64_value(
                8,
                [&tablet]() {
                    auto rs_metas = tablet->tablet_meta()->all_rs_metas();
                    return std::accumulate(rs_metas.begin(), rs_metas.end(), 0,
                                           [](int64_t val, RowsetMetaSharedPtr& rs_meta) {
                                               return val + rs_meta->num_segments();
                                           });
                }(),
                block);

        // NUM_COLUMNS
        SchemaScannerHelper::insert_int64_value(9, tablet->tablet_meta()->tablet_columns_num(),
                                                block);

        // ROW_SIZE
        SchemaScannerHelper::insert_int64_value(10, static_cast<int64_t>(tablet->row_size()),
                                                block);

        // COMPACTION_SCORE
        SchemaScannerHelper::insert_int32_value(11, tablet->get_real_compaction_score(), block);

        // COMPRESS_KIND
        SchemaScannerHelper::insert_string_value(12, CompressKind_Name(tablet->compress_kind()),
                                                 block);

        // IS_USED
        SchemaScannerHelper::insert_bool_value(
                13,
                [&tablet]() {
                    if (config::is_cloud_mode()) {
                        return true;
                    }
                    return std::static_pointer_cast<Tablet>(tablet)->is_used();
                }(),
                block);

        // IS_ALTER_FAILED
        SchemaScannerHelper::insert_bool_value(14, tablet->is_alter_failed(), block);

        // CREATE_TIME
        SchemaScannerHelper::insert_datetime_value(15, tablet->tablet_meta()->creation_time(),
                                                   _timezone_obj, block);

        // UPDATE_TIME
        SchemaScannerHelper::insert_datetime_value(
                16,
                [&tablet]() {
                    auto rowset = tablet->get_rowset_with_max_version();
                    return rowset == nullptr ? 0 : rowset->newest_write_timestamp();
                }(),
                _timezone_obj, block);

        // IS_OVERLAP
        SchemaScannerHelper::insert_bool_value(
                17,
                [&tablet]() {
                    const auto& rs_metas = tablet->tablet_meta()->all_rs_metas();
                    return std::any_of(rs_metas.begin(), rs_metas.end(),
                                       [](const RowsetMetaSharedPtr& rs_meta) {
                                           return rs_meta->is_segments_overlapping();
                                       });
                }(),
                block);
    }

    return Status::OK();
}
} // namespace doris
