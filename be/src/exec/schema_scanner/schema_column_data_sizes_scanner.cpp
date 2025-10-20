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

#include "exec/schema_scanner/schema_column_data_sizes_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaColumnDataSizesScanner::_s_tbls_columns = {
        //   name,                type,          size,             is_null
        {"BACKEND_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROWSET_ID", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLUMN_UNIQUE_ID", TYPE_INT, sizeof(int32_t), true},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLUMN_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COMPRESSED_DATA_BYTES", TYPE_BIGINT, sizeof(int64_t), true},
        {"UNCOMPRESSED_DATA_BYTES", TYPE_BIGINT, sizeof(int64_t), true},
        {"RAW_DATA_BYTES", TYPE_BIGINT, sizeof(int64_t), true}};

SchemaColumnDataSizesScanner::SchemaColumnDataSizesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_COLUMN_DATA_SIZES),
          backend_id_(0),
          _column_data_sizes_idx(0) {}

SchemaColumnDataSizesScanner::~SchemaColumnDataSizesScanner() = default;

Status SchemaColumnDataSizesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    backend_id_ = state->backend_id();
    RETURN_IF_ERROR(_get_all_column_data_sizes());
    return Status::OK();
}

Status SchemaColumnDataSizesScanner::_get_all_column_data_sizes() {
    auto process_rowsets = [&](const std::vector<RowsetSharedPtr>& rowsets, int64_t table_id,
                               int64_t index_id, int64_t partition_id, int64_t tablet_id) {
        for (const auto& rowset : rowsets) {
            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            if (!beta_rowset) {
                continue;
            }

            if (beta_rowset->num_segments() == 0) {
                continue;
            }

            auto rowset_meta = rowset->rowset_meta();
            const auto& schema = rowset_meta->tablet_schema();
            auto rowset_id = rowset_meta->rowset_id().to_string();

            std::map<int32_t /* column_unique_id */, ColumnDataSizeInfo> aggregated_stats;

            // Load all segments at once
            std::vector<segment_v2::SegmentSharedPtr> segments;
            auto st = beta_rowset->load_segments(&segments);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to load segments for rowset "
                             << beta_rowset->rowset_id().to_string()
                             << ", error: " << st.to_string();
                continue;
            }

            // Get column data page stats from each segment footer and aggregate by column_unique_id
            for (const auto& segment : segments) {
                auto collector = [&](const segment_v2::ColumnMetaPB& column_meta) {
                    if (column_meta.has_compressed_data_bytes() &&
                        column_meta.has_uncompressed_data_bytes()) {
                        auto cid = schema->field_index(column_meta.unique_id());
                        if (cid == -1) {
                            return;
                        }
                        // Aggregate stats by column_unique_id
                        if (aggregated_stats.contains(column_meta.unique_id())) {
                            auto& existing_stats = aggregated_stats[column_meta.unique_id()];
                            existing_stats.compressed_data_bytes +=
                                    column_meta.compressed_data_bytes();
                            existing_stats.uncompressed_data_bytes +=
                                    column_meta.uncompressed_data_bytes();
                            existing_stats.raw_data_bytes += column_meta.raw_data_bytes();
                        } else {
                            aggregated_stats[column_meta.unique_id()] = ColumnDataSizeInfo {
                                    .backend_id = backend_id_,
                                    .table_id = table_id,
                                    .index_id = index_id,
                                    .partition_id = partition_id,
                                    .tablet_id = tablet_id,
                                    .rowset_id = rowset_id,
                                    .column_unique_id = column_meta.unique_id(),
                                    .column_name = schema->column(cid).name(),
                                    .column_type = TabletColumn::get_string_by_field_type(
                                            static_cast<FieldType>(column_meta.type())),
                                    .compressed_data_bytes = column_meta.compressed_data_bytes(),
                                    .uncompressed_data_bytes =
                                            column_meta.uncompressed_data_bytes(),
                                    .raw_data_bytes = column_meta.raw_data_bytes(),
                            };
                        }
                    }
                };
                st = segment->traverse_column_meta_pbs(collector);
                if (!st.ok()) {
                    continue;
                }
            }
            // Append aggregated stats for this rowset to the result vector
            for (const auto& [_, stats] : aggregated_stats) {
                _column_data_sizes.push_back(stats);
            }
        }
    };

    if (config::is_cloud_mode()) {
        // only query cloud tablets in lru cache instead of all tablets
        std::vector<std::weak_ptr<CloudTablet>> tablets =
                ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_mgr().get_weak_tablets();
        for (const std::weak_ptr<CloudTablet>& tablet : tablets) {
            if (!tablet.expired()) {
                auto t = tablet.lock();
                std::vector<RowsetSharedPtr> rowsets;
                {
                    std::shared_lock rowset_ldlock(t->get_header_lock());
                    for (const auto& it : t->rowset_map()) {
                        rowsets.emplace_back(it.second);
                    }
                }
                process_rowsets(rowsets, t->table_id(), t->index_id(), t->partition_id(),
                                t->tablet_id());
            }
        }
    } else {
        std::vector<TabletSharedPtr> tablets = ExecEnv::GetInstance()
                                                       ->storage_engine()
                                                       .to_local()
                                                       .tablet_manager()
                                                       ->get_all_tablet();
        for (const auto& tablet : tablets) {
            std::vector<std::pair<Version, RowsetSharedPtr>> all_rowsets;
            {
                std::shared_lock rowset_rlock(tablet->get_header_lock());
                tablet->acquire_version_and_rowsets(&all_rowsets);
            }
            std::vector<RowsetSharedPtr> rowsets;
            for (const auto& version_and_rowset : all_rowsets) {
                rowsets.emplace_back(version_and_rowset.second);
            }
            process_rowsets(rowsets, tablet->table_id(), tablet->index_id(), tablet->partition_id(),
                            tablet->tablet_id());
        }
    }
    return Status::OK();
}

Status SchemaColumnDataSizesScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_column_data_sizes_idx >= _column_data_sizes.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaColumnDataSizesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    size_t fill_num = std::min(1000UL, _column_data_sizes.size() - _column_data_sizes_idx);
    size_t fill_idx_begin = _column_data_sizes_idx;
    size_t fill_idx_end = _column_data_sizes_idx + fill_num;
    std::vector<void*> datas(fill_num);

    // BACKEND_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].backend_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }

    // TABLE_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].table_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }

    // INDEX_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].index_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }

    // PARTITION_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].partition_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }

    // TABLET_ID
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].tablet_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }

    // ROWSET_ID
    {
        std::vector<std::string> rowset_ids(fill_num);
        std::vector<StringRef> strs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            rowset_ids[i - fill_idx_begin] = _column_data_sizes[i].rowset_id;
            strs[i - fill_idx_begin] = StringRef(rowset_ids[i - fill_idx_begin].c_str(),
                                                 rowset_ids[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }

    // COLUMN_UNIQUE_ID
    {
        std::vector<int32_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].column_unique_id;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }

    // COLUMN_NAME
    {
        std::vector<std::string> column_names(fill_num);
        std::vector<StringRef> strs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            column_names[i - fill_idx_begin] = _column_data_sizes[i].column_name;
            strs[i - fill_idx_begin] = StringRef(column_names[i - fill_idx_begin].c_str(),
                                                 column_names[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }

    // COLUMN_TYPE
    {
        std::vector<std::string> column_types(fill_num);
        std::vector<StringRef> strs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            column_types[i - fill_idx_begin] = _column_data_sizes[i].column_type;
            strs[i - fill_idx_begin] = StringRef(column_types[i - fill_idx_begin].c_str(),
                                                 column_types[i - fill_idx_begin].size());
            datas[i - fill_idx_begin] = strs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }

    // COMPRESSED_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].compressed_data_bytes;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));
    }

    // UNCOMPRESSED_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].uncompressed_data_bytes;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));
    }

    // RAW_DATA_SIZE
    {
        std::vector<int64_t> srcs(fill_num);
        for (size_t i = fill_idx_begin; i < fill_idx_end; ++i) {
            srcs[i - fill_idx_begin] = _column_data_sizes[i].raw_data_bytes;
            datas[i - fill_idx_begin] = srcs.data() + i - fill_idx_begin;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));
    }

    _column_data_sizes_idx += fill_num;
    return Status::OK();
}

} // namespace doris