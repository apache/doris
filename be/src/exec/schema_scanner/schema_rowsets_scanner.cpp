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

#include "exec/schema_scanner/schema_rowsets_scanner.h"

#include <cstddef>

#include "common/status.h"
#include "gutil/integral_types.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"
namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaRowsetsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"BACKEND_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROWSET_ID", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROWSET_NUM_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"TXN_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_SEGMENTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"START_VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"END_VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_DISK_SIZE", TYPE_BIGINT, sizeof(size_t), true},
        {"DATA_DISK_SIZE", TYPE_BIGINT, sizeof(size_t), true},
        {"CREATION_TIME", TYPE_BIGINT, sizeof(int64_t), true},
        {"NEWEST_WRITE_TIMESTAMP", TYPE_BIGINT, sizeof(int64_t), true},

};

SchemaRowsetsScanner::SchemaRowsetsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_ROWSETS),
          backend_id_(0),
          _rowsets_idx(0) {};

Status SchemaRowsetsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    backend_id_ = state->backend_id();
    RETURN_IF_ERROR(_get_all_rowsets());
    return Status::OK();
}

Status SchemaRowsetsScanner::_get_all_rowsets() {
    std::vector<TabletSharedPtr> tablets =
            StorageEngine::instance()->tablet_manager()->get_all_tablet();
    for (const auto& tablet : tablets) {
        // all rowset
        std::vector<std::pair<Version, RowsetSharedPtr>> all_rowsets;
        {
            std::shared_lock rowset_ldlock(tablet->get_header_lock());
            tablet->acquire_version_and_rowsets(&all_rowsets);
        }
        for (const auto& version_and_rowset : all_rowsets) {
            RowsetSharedPtr rowset = version_and_rowset.second;
            rowsets_.emplace_back(rowset);
        }
    }
    return Status::OK();
}

Status SchemaRowsetsScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_rowsets_idx >= rowsets_.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaRowsetsScanner::_fill_block_impl(vectorized::Block* block) {
    size_t fill_rowsets_num = std::min(1000ul, rowsets_.size() - _rowsets_idx);
    auto fill_idx_begin = _rowsets_idx;
    auto fill_idx_end = _rowsets_idx + fill_rowsets_num;
    // BACKEND_ID
    {
        int64_t src = backend_id_;
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            fill_dest_column(block, &src, _s_tbls_columns[0]);
        }
    }
    // ROWSET_ID
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            std::string rowset_id = rowset->rowset_id().to_string();
            StringRef str = StringRef(rowset_id.c_str(), rowset_id.size());
            fill_dest_column(block, &str, _s_tbls_columns[1]);
        }
    }
    // TABLET_ID
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->rowset_meta()->tablet_id();
            fill_dest_column(block, &src, _s_tbls_columns[2]);
        }
    }
    // ROWSET_NUM_ROWS
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->num_rows();
            fill_dest_column(block, &src, _s_tbls_columns[3]);
        }
    }
    // TXN_ID
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->txn_id();
            fill_dest_column(block, &src, _s_tbls_columns[4]);
        }
    }
    // NUM_SEGMENTS
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->num_segments();
            fill_dest_column(block, &src, _s_tbls_columns[5]);
        }
    }
    // START_VERSION
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->start_version();
            fill_dest_column(block, &src, _s_tbls_columns[6]);
        }
    }
    // END_VERSION
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            int64_t src = rowset->end_version();
            fill_dest_column(block, &src, _s_tbls_columns[7]);
        }
    }
    // INDEX_DISK_SIZE
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            size_t src = rowset->index_disk_size();
            fill_dest_column(block, &src, _s_tbls_columns[8]);
        }
    }
    // DATA_DISK_SIZE
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            size_t src = rowset->data_disk_size();
            fill_dest_column(block, &src, _s_tbls_columns[9]);
        }
    }
    // CREATION_TIME
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            size_t src = rowset->creation_time();
            fill_dest_column(block, &src, _s_tbls_columns[10]);
        }
    }
    // NEWEST_WRITE_TIMESTAMP
    {
        for (int i = fill_idx_begin; i < fill_idx_end; ++i) {
            RowsetSharedPtr rowset = rowsets_[i];
            size_t src = rowset->newest_write_timestamp();
            fill_dest_column(block, &src, _s_tbls_columns[12]);
        }
    }
    _rowsets_idx += fill_rowsets_num;
    return Status::OK();
}
} // namespace doris
