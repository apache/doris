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

#include "format/table/index_disk_usage_reader.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>

#include "cloud/cloud_tablet.h"
#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

using segment_v2::IndexDiskUsageCollector;
using segment_v2::IndexDiskUsageLevel;
using segment_v2::IndexDiskUsageRecord;
using segment_v2::IndexDiskUsageRow;
using segment_v2::IndexDiskUsageStructure;

namespace {

const std::vector<std::pair<std::string_view, IndexDiskUsageReader::Column>>& column_names() {
    using C = IndexDiskUsageReader::Column;
    static const std::vector<std::pair<std::string_view, C>> names = {
            {"PARTITION_NAME", C::kPartitionName},
            {"TABLET_ID", C::kTabletId},
            {"BACKEND_ID", C::kBackendId},
            {"ROWSET_ID", C::kRowsetId},
            {"SEGMENT_ID", C::kSegmentId},
            {"INDEX_ID", C::kIndexId},
            {"INDEX_NAME", C::kIndexName},
            {"INDEX_TYPE", C::kIndexType},
            {"COLUMN_NAME", C::kColumnName},
            {"INDEX_SUFFIX", C::kIndexSuffix},
            {"STRUCTURE", C::kStructure},
            {"STORAGE_FORMAT", C::kStorageFormat},
            {"SEGMENT_COUNT", C::kSegmentCount},
            {"ROW_COUNT", C::kRowCount},
            {"TOTAL_BYTES", C::kTotalBytes},
            {"DICT_BYTES", C::kDictBytes},
            {"POSTING_BYTES", C::kPostingBytes},
            {"POSITION_BYTES", C::kPositionBytes},
            {"STATS_BYTES", C::kStatsBytes},
            {"OTHER_BYTES", C::kOtherBytes},
            {"STATS_SOURCE", C::kStatsSource},
    };
    return names;
}

Result<IndexDiskUsageReader::Column> column_of(const std::string& slot_name) {
    const std::string upper = boost::to_upper_copy(slot_name);
    for (const auto& [name, column] : column_names()) {
        if (upper == name) {
            return column;
        }
    }
    return ResultError(Status::InternalError("unknown index_disk_usage column {}", slot_name));
}

Result<IndexDiskUsageLevel> parse_level(const std::string& level) {
    if (level == "tablet") {
        return IndexDiskUsageLevel::kTablet;
    }
    if (level == "rowset") {
        return IndexDiskUsageLevel::kRowset;
    }
    if (level == "segment") {
        return IndexDiskUsageLevel::kSegment;
    }
    return ResultError(Status::InvalidArgument("unsupported index_disk_usage level {}", level));
}

std::string_view structure_name(IndexDiskUsageStructure structure) {
    switch (structure) {
    case IndexDiskUsageStructure::kTerm:
        return "TERM";
    case IndexDiskUsageStructure::kBkd:
        return "BKD";
    case IndexDiskUsageStructure::kAnn:
        return "ANN";
    case IndexDiskUsageStructure::kContainer:
        return "CONTAINER";
    }
    return "UNKNOWN";
}

const TabletIndex* find_index(const TabletSchema& schema, const IndexDiskUsageRecord& record) {
    for (const TabletIndex* index : schema.inverted_and_ann_indexes()) {
        if (index->index_id() == record.index_id &&
            index->get_index_suffix() == record.index_suffix) {
            return index;
        }
    }
    return nullptr;
}

void insert_null(IColumn* column) {
    auto& nullable = reinterpret_cast<ColumnNullable&>(*column);
    nullable.get_nested_column().insert_default();
    nullable.get_null_map_data().push_back(1);
}

IColumn* non_null_nested(IColumn* column) {
    auto& nullable = reinterpret_cast<ColumnNullable&>(*column);
    nullable.get_null_map_data().push_back(0);
    return nullable.get_nested_column_ptr().get();
}

void insert_int64(IColumn* column, int64_t value) {
    assert_cast<ColumnInt64*>(non_null_nested(column))->insert_value(value);
}

void insert_int32(IColumn* column, int32_t value) {
    assert_cast<ColumnInt32*>(non_null_nested(column))->insert_value(value);
}

void insert_string(IColumn* column, std::string_view value) {
    assert_cast<ColumnString*>(non_null_nested(column))->insert_data(value.data(), value.size());
}

// Components are reported only for term indexes, except that a container row carries its
// overhead as other bytes. Unknown components (-1) are NULL.
void insert_component(IColumn* column, const IndexDiskUsageRecord& record, int64_t value,
                      bool is_other) {
    const bool reported = record.structure == IndexDiskUsageStructure::kTerm ||
                          (is_other && record.structure == IndexDiskUsageStructure::kContainer);
    if (!reported || value < 0) {
        insert_null(column);
    } else {
        insert_int64(column, value);
    }
}

} // namespace

IndexDiskUsageReader::IndexDiskUsageReader(std::vector<SlotDescriptor*> slots, RuntimeState* state,
                                           RuntimeProfile* /*profile*/, TMetaScanRange scan_range)
        : _state(state), _slots(std::move(slots)), _scan_range(std::move(scan_range)) {}

Status IndexDiskUsageReader::init_reader() {
    if (!_scan_range.__isset.index_disk_usage_params) {
        return Status::InvalidArgument("index_disk_usage scan range has no parameters");
    }
    const TIndexDiskUsageMetadataParams& params = _scan_range.index_disk_usage_params;
    _level = DORIS_TRY(parse_level(params.level));
    _options.position_detail = params.position_detail;
    _options.index_ids.insert(params.index_ids.begin(), params.index_ids.end());
    _slot_columns.clear();
    for (const SlotDescriptor* slot : _slots) {
        const Column column = DORIS_TRY(column_of(slot->col_name()));
        _slot_columns.push_back(column);
    }
    return Status::OK();
}

Status IndexDiskUsageReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    const auto& tablets = _scan_range.index_disk_usage_params.tablets;
    *read_rows = 0;
    while (_next_tablet < tablets.size()) {
        RETURN_IF_CANCELLED(_state);
        const TIndexDiskUsageTablet& target = tablets[_next_tablet++];
        std::vector<IndexDiskUsageRow> rows;
        TabletSchemaSPtr current_schema;
        RETURN_IF_ERROR(_collect_tablet(target, &rows, &current_schema));
        rows = segment_v2::aggregate_index_disk_usage(std::move(rows), _level);
        if (rows.empty()) {
            continue;
        }
        RETURN_IF_ERROR(_fill_block(block, target, *current_schema, rows));
        *read_rows = rows.size();
        *eof = false;
        return Status::OK();
    }
    *eof = true;
    return Status::OK();
}

Status IndexDiskUsageReader::_collect_tablet(const TIndexDiskUsageTablet& target,
                                             std::vector<IndexDiskUsageRow>* rows,
                                             TabletSchemaSPtr* current_schema) const {
    BaseTabletSPtr tablet = DORIS_TRY(ExecEnv::get_tablet(target.tablet_id));
    if (auto cloud_tablet = std::dynamic_pointer_cast<CloudTablet>(tablet)) {
        SyncOptions options;
        options.query_version = target.version;
        RETURN_IF_ERROR(cloud_tablet->sync_rowsets(options));
    }
    std::vector<RowsetSharedPtr> rowsets;
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        auto captured = DORIS_TRY(tablet->capture_consistent_rowsets_unlocked(
                Version(0, target.version), CaptureRowsetOps {}));
        rowsets = std::move(captured.rowsets);
    }
    *current_schema = tablet->tablet_schema();

    for (const RowsetSharedPtr& rowset : rowsets) {
        const TabletSchemaSPtr schema = rowset->tablet_schema();
        if (!schema->has_inverted_or_ann_index()) {
            continue;
        }
        const InvertedIndexStorageFormatPB format = schema->get_inverted_index_storage_format();
        const std::string rowset_id = rowset->rowset_id().to_string();
        for (auto segment : rowset->segments()) {
            RETURN_IF_CANCELLED(_state);
            const std::string segment_path = DORIS_TRY(segment.path());
            IndexDiskUsageCollector collector(
                    rowset->rowset_meta()->fs(),
                    std::string(InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)),
                    schema, format, target.tablet_id);
            std::vector<IndexDiskUsageRecord> records;
            RETURN_IF_ERROR(collector.collect(_options, &records));
            for (auto& record : records) {
                IndexDiskUsageRow row;
                row.rowset_id = rowset_id;
                row.segment_id = cast_set<int32_t>(segment.id());
                row.segment_count = 1;
                row.row_count = segment.has_num_rows() ? segment.num_rows() : 0;
                row.format = format;
                row.record = std::move(record);
                rows->push_back(std::move(row));
            }
        }
    }
    return Status::OK();
}

Status IndexDiskUsageReader::_fill_block(Block* block, const TIndexDiskUsageTablet& target,
                                         const TabletSchema& schema,
                                         const std::vector<IndexDiskUsageRow>& rows) const {
    if (!block->mem_reuse()) {
        std::vector<MutableColumnPtr> columns(_slots.size());
        for (size_t i = 0; i < _slots.size(); ++i) {
            columns[i] = _slots[i]->get_empty_mutable_column();
        }
        _append_rows(columns, target, schema, rows);
        for (size_t i = 0; i < _slots.size(); ++i) {
            block->insert(ColumnWithTypeAndName(std::move(columns[i]),
                                                _slots[i]->get_data_type_ptr(),
                                                _slots[i]->col_name()));
        }
    } else {
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (size_t i = 0; i < _slots.size(); ++i) {
            columns[i]->clear();
        }
        _append_rows(columns, target, schema, rows);
    }
    return Status::OK();
}

void IndexDiskUsageReader::_append_rows(std::vector<MutableColumnPtr>& columns,
                                        const TIndexDiskUsageTablet& target,
                                        const TabletSchema& schema,
                                        const std::vector<IndexDiskUsageRow>& rows) const {
    const auto& partition_names = _scan_range.index_disk_usage_params.partition_names;
    const auto partition = partition_names.find(target.partition_id);
    for (const IndexDiskUsageRow& row : rows) {
        const IndexDiskUsageRecord& record = row.record;
        const bool is_container = record.structure == IndexDiskUsageStructure::kContainer;
        const TabletIndex* index = is_container ? nullptr : find_index(schema, record);
        std::string column_name;
        if (index != nullptr && !index->col_unique_ids().empty()) {
            const int32_t ordinal = schema.field_index(index->col_unique_ids()[0]);
            if (ordinal >= 0) {
                column_name = schema.column(ordinal).name();
            }
        }
        for (size_t i = 0; i < _slot_columns.size(); ++i) {
            IColumn* column = columns[i].get();
            switch (_slot_columns[i]) {
            case Column::kPartitionName:
                if (partition == partition_names.end()) {
                    insert_null(column);
                } else {
                    insert_string(column, partition->second);
                }
                break;
            case Column::kTabletId:
                insert_int64(column, target.tablet_id);
                break;
            case Column::kBackendId:
                insert_int64(column, _state->backend_id());
                break;
            case Column::kRowsetId:
                if (_level == IndexDiskUsageLevel::kTablet) {
                    insert_null(column);
                } else {
                    insert_string(column, row.rowset_id);
                }
                break;
            case Column::kSegmentId:
                if (_level == IndexDiskUsageLevel::kSegment) {
                    insert_int32(column, row.segment_id);
                } else {
                    insert_null(column);
                }
                break;
            case Column::kIndexId:
                if (is_container) {
                    insert_null(column);
                } else {
                    insert_int64(column, record.index_id);
                }
                break;
            case Column::kIndexName:
                if (index == nullptr) {
                    insert_null(column);
                } else {
                    insert_string(column, index->index_name());
                }
                break;
            case Column::kIndexType:
                if (index == nullptr) {
                    insert_null(column);
                } else {
                    insert_string(column, index->is_ann_index() ? "ANN" : "INVERTED");
                }
                break;
            case Column::kColumnName:
                if (column_name.empty()) {
                    insert_null(column);
                } else {
                    insert_string(column, column_name);
                }
                break;
            case Column::kIndexSuffix:
                if (is_container) {
                    insert_null(column);
                } else {
                    insert_string(column, record.index_suffix);
                }
                break;
            case Column::kStructure:
                insert_string(column, structure_name(record.structure));
                break;
            case Column::kStorageFormat:
                insert_string(column, InvertedIndexStorageFormatPB_Name(row.format));
                break;
            case Column::kSegmentCount:
                insert_int64(column, row.segment_count);
                break;
            case Column::kRowCount:
                insert_int64(column, row.row_count);
                break;
            case Column::kTotalBytes:
                insert_int64(column, record.total_bytes);
                break;
            case Column::kDictBytes:
                insert_component(column, record, record.dict_bytes, false);
                break;
            case Column::kPostingBytes:
                insert_component(column, record, record.posting_bytes, false);
                break;
            case Column::kPositionBytes:
                insert_component(column, record, record.position_bytes, false);
                break;
            case Column::kStatsBytes:
                insert_component(column, record, record.stats_bytes, false);
                break;
            case Column::kOtherBytes:
                insert_component(column, record, record.other_bytes, true);
                break;
            case Column::kStatsSource:
                insert_string(column, "FILE");
                break;
            }
        }
    }
}

} // namespace doris
