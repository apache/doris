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

#include "information_schema/schema_tablets_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_common.pb.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/status.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "information_schema/schema_scanner.h"
#include "information_schema/schema_scanner_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_manager.h"

namespace doris {
class Block;

namespace {
struct TabletInfoSnapshot {
    int64_t tablet_id = 0;
    int64_t replica_id = 0;
    int64_t partition_id = 0;
    std::string tablet_path;
    int64_t tablet_local_size = 0;
    int64_t tablet_remote_size = 0;
    int64_t version_count = 0;
    int64_t segment_count = 0;
    int64_t num_columns = 0;
    int64_t row_size = 0;
    int32_t compaction_score = 0;
    std::string compress_kind;
    bool is_used = false;
    bool is_alter_failed = false;
    int64_t create_time = 0;
    int64_t update_time = 0;
    bool is_overlap = false;
    std::vector<RowsetMetaSharedPtr> rowset_metas;
    std::vector<RowsetMetaSharedPtr> row_binlog_rowset_metas;
    RowsetSharedPtr max_version_rowset;
};
} // namespace

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

Status SchemaTabletsScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaTabletsScanner::_fill_block_impl(Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    size_t row_num = _tablets.size();
    if (row_num == 0) {
        return Status::OK();
    }

    size_t fill_tablets_num = _tablets.size();
    std::vector<void*> datas(fill_tablets_num);

    for (int i = 0; i < _tablets.size(); i++) {
        BaseTabletSPtr tablet = _tablets[i];
        TabletInfoSnapshot snapshot;
        {
            // Snapshot rowset maps under the tablet header lock because compaction rewrites them
            // under the same lock. Heavy aggregation and block filling run after releasing it.
            std::shared_lock rlock(tablet->get_header_lock());
            const auto& tablet_meta = tablet->tablet_meta();
            const auto& rs_metas = tablet_meta->all_rs_metas();
            const auto& row_binlog_rs_metas = tablet_meta->all_row_binlog_rs_metas();

            snapshot.tablet_id = tablet_meta->tablet_id();
            snapshot.replica_id = tablet_meta->replica_id();
            snapshot.partition_id = tablet_meta->partition_id();
            snapshot.tablet_path = tablet->tablet_path();
            snapshot.num_columns = static_cast<int64_t>(tablet_meta->tablet_columns_num());
            snapshot.row_size = static_cast<int64_t>(tablet->row_size());
            snapshot.compress_kind = CompressKind_Name(tablet->compress_kind());
            snapshot.is_used = [&tablet]() {
                if (config::is_cloud_mode()) {
                    return true;
                }
                return std::static_pointer_cast<Tablet>(tablet)->is_used();
            }();
            snapshot.is_alter_failed = tablet->is_alter_failed();
            snapshot.create_time = tablet_meta->creation_time();
            snapshot.rowset_metas.reserve(rs_metas.size());
            for (const auto& [_, rs_meta] : rs_metas) {
                snapshot.rowset_metas.emplace_back(rs_meta);
            }
            snapshot.row_binlog_rowset_metas.reserve(row_binlog_rs_metas.size());
            for (const auto& [_, rs_meta] : row_binlog_rs_metas) {
                snapshot.row_binlog_rowset_metas.emplace_back(rs_meta);
            }
            snapshot.max_version_rowset = tablet->get_rowset_with_max_version();
        }

        snapshot.tablet_local_size =
                std::accumulate(
                        snapshot.rowset_metas.begin(), snapshot.rowset_metas.end(), int64_t {0},
                        [](int64_t total_size, const auto& rs_meta) {
                            if (rs_meta->is_local()) {
                                total_size += static_cast<int64_t>(rs_meta->total_disk_size());
                            }
                            return total_size;
                        }) +
                std::accumulate(snapshot.row_binlog_rowset_metas.begin(),
                                snapshot.row_binlog_rowset_metas.end(), int64_t {0},
                                [](int64_t total_size, const auto& rs_meta) {
                                    if (rs_meta->is_local()) {
                                        total_size +=
                                                static_cast<int64_t>(rs_meta->data_disk_size());
                                    }
                                    return total_size;
                                });
        snapshot.tablet_remote_size = std::accumulate(
                snapshot.rowset_metas.begin(), snapshot.rowset_metas.end(), int64_t {0},
                [](int64_t total_size, const auto& rs_meta) {
                    if (!rs_meta->is_local()) {
                        total_size += static_cast<int64_t>(rs_meta->total_disk_size());
                    }
                    return total_size;
                });
        snapshot.version_count = static_cast<int64_t>(snapshot.rowset_metas.size());
        snapshot.segment_count =
                std::accumulate(snapshot.rowset_metas.begin(), snapshot.rowset_metas.end(),
                                int64_t {0}, [](int64_t val, const auto& rs_meta) {
                                    return val + static_cast<int64_t>(rs_meta->num_segments());
                                });
        snapshot.compaction_score = static_cast<int32_t>(std::accumulate(
                snapshot.rowset_metas.begin(), snapshot.rowset_metas.end(), uint32_t {0},
                [](uint32_t score, const auto& rs_meta) {
                    return score + static_cast<uint32_t>(rs_meta->get_compaction_score());
                }));
        snapshot.update_time = snapshot.max_version_rowset == nullptr
                                       ? 0
                                       : snapshot.max_version_rowset->newest_write_timestamp();
        snapshot.is_overlap =
                std::any_of(snapshot.rowset_metas.begin(), snapshot.rowset_metas.end(),
                            [](const auto& rs_meta) { return rs_meta->is_segments_overlapping(); });

        // BE_ID
        SchemaScannerHelper::insert_int64_value(0, _backend_id, block);

        // TABLET_ID
        SchemaScannerHelper::insert_int64_value(1, snapshot.tablet_id, block);

        // REPLICA_ID
        SchemaScannerHelper::insert_int64_value(2, snapshot.replica_id, block);

        // PARTITION_ID
        SchemaScannerHelper::insert_int64_value(3, snapshot.partition_id, block);

        // TABLET_PATH
        SchemaScannerHelper::insert_string_value(4, snapshot.tablet_path, block);

        // TABLET_LOCAL_SIZE
        SchemaScannerHelper::insert_int64_value(5, snapshot.tablet_local_size, block);

        // TABLET_REMOTE_SIZE
        SchemaScannerHelper::insert_int64_value(6, snapshot.tablet_remote_size, block);

        // VERSION_COUNT
        SchemaScannerHelper::insert_int64_value(7, snapshot.version_count, block);

        // SEGMENT_COUNT
        SchemaScannerHelper::insert_int64_value(8, snapshot.segment_count, block);

        // NUM_COLUMNS
        SchemaScannerHelper::insert_int64_value(9, snapshot.num_columns, block);

        // ROW_SIZE
        SchemaScannerHelper::insert_int64_value(10, snapshot.row_size, block);

        // COMPACTION_SCORE
        SchemaScannerHelper::insert_int32_value(11, snapshot.compaction_score, block);

        // COMPRESS_KIND
        SchemaScannerHelper::insert_string_value(12, snapshot.compress_kind, block);

        // IS_USED
        SchemaScannerHelper::insert_bool_value(13, snapshot.is_used, block);

        // IS_ALTER_FAILED
        SchemaScannerHelper::insert_bool_value(14, snapshot.is_alter_failed, block);

        // CREATE_TIME
        SchemaScannerHelper::insert_datetime_value(15, snapshot.create_time, _timezone_obj, block);

        // UPDATE_TIME
        SchemaScannerHelper::insert_datetime_value(16, snapshot.update_time, _timezone_obj, block);

        // IS_OVERLAP
        SchemaScannerHelper::insert_bool_value(17, snapshot.is_overlap, block);
    }

    return Status::OK();
}
} // namespace doris
