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
#include <variant>

#include "cloud/cloud_tablet.h"
#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

using segment_v2::IndexDiskUsageLevel;
using segment_v2::IndexDiskUsageRecord;
using segment_v2::IndexDiskUsageRow;
using segment_v2::IndexDiskUsageStructure;

namespace {

const std::vector<std::pair<std::string_view, IndexDiskUsageReader::Column>>& column_names() {
    using C = IndexDiskUsageReader::Column;
    static const std::vector<std::pair<std::string_view, C>> names = {
            {"PARTITION_NAME", C::kPartitionName}, {"TABLET_ID", C::kTabletId},
            {"BACKEND_ID", C::kBackendId},         {"ROWSET_ID", C::kRowsetId},
            {"SEGMENT_ID", C::kSegmentId},         {"INDEX_ID", C::kIndexId},
            {"INDEX_NAME", C::kIndexName},         {"INDEX_TYPE", C::kIndexType},
            {"COLUMN_NAME", C::kColumnName},       {"INDEX_SUFFIX", C::kIndexSuffix},
            {"STRUCTURE", C::kStructure},          {"STORAGE_FORMAT", C::kStorageFormat},
            {"SEGMENT_COUNT", C::kSegmentCount},   {"ROW_COUNT", C::kRowCount},
            {"TOTAL_BYTES", C::kTotalBytes},       {"DICT_BYTES", C::kDictBytes},
            {"POSTING_BYTES", C::kPostingBytes},   {"POSITION_BYTES", C::kPositionBytes},
            {"STATS_BYTES", C::kStatsBytes},       {"OTHER_BYTES", C::kOtherBytes},
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
    _options.check_cancelled = [state = _state]() {
        RETURN_IF_CANCELLED(state);
        return Status::OK();
    };
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
        RETURN_IF_ERROR(segment_v2::collect_rowset_index_disk_usage(rowset, _options,
                                                                    target.tablet_id, rows));
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
            block->insert(ColumnWithTypeAndName(
                    std::move(columns[i]), _slots[i]->get_data_type_ptr(), _slots[i]->col_name()));
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

namespace {

// One output row with the names resolved from the tablet schema.
struct RowView {
    const TIndexDiskUsageTablet* target = nullptr;
    // nullptr when FE sent no name for the partition.
    const std::string* partition_name = nullptr;
    const IndexDiskUsageRow* row = nullptr;
    // nullptr for container rows and for indexes no longer in the schema.
    const TabletIndex* index = nullptr;
    std::string column_name;
};

// A cell value: NULL, an integer or a string that outlives the insertion.
using Cell = std::variant<std::monostate, int64_t, std::string_view>;

// Components are reported only for term indexes, except that a container row carries its
// overhead as other bytes. Unknown components (-1) are NULL.
Cell component_cell(const IndexDiskUsageRecord& record, int64_t value, bool is_other) {
    const bool reported = record.structure == IndexDiskUsageStructure::kTerm ||
                          (is_other && record.structure == IndexDiskUsageStructure::kContainer);
    return reported && value >= 0 ? Cell {value} : Cell {};
}

Cell cell_of(IndexDiskUsageReader::Column column, const RowView& view, IndexDiskUsageLevel level,
             int64_t backend_id) {
    using C = IndexDiskUsageReader::Column;
    const IndexDiskUsageRow& row = *view.row;
    const IndexDiskUsageRecord& record = row.record;
    const bool is_container = record.structure == IndexDiskUsageStructure::kContainer;
    switch (column) {
    case C::kPartitionName:
        return view.partition_name == nullptr ? Cell {}
                                              : Cell {std::string_view(*view.partition_name)};
    case C::kTabletId:
        return Cell {view.target->tablet_id};
    case C::kBackendId:
        return Cell {backend_id};
    case C::kRowsetId:
        return level == IndexDiskUsageLevel::kTablet ? Cell {}
                                                     : Cell {std::string_view(row.rowset_id)};
    case C::kSegmentId:
        return level == IndexDiskUsageLevel::kSegment ? Cell {int64_t {row.segment_id}} : Cell {};
    case C::kIndexId:
        return is_container ? Cell {} : Cell {record.index_id};
    case C::kIndexName:
        return view.index == nullptr ? Cell {} : Cell {std::string_view(view.index->index_name())};
    case C::kIndexType:
        if (view.index == nullptr) {
            return Cell {};
        }
        return Cell {std::string_view(view.index->is_ann_index() ? "ANN" : "INVERTED")};
    case C::kColumnName:
        return view.column_name.empty() ? Cell {} : Cell {std::string_view(view.column_name)};
    case C::kIndexSuffix:
        return is_container ? Cell {} : Cell {std::string_view(record.index_suffix)};
    case C::kStructure:
        return Cell {structure_name(record.structure)};
    case C::kStorageFormat:
        return Cell {std::string_view(InvertedIndexStorageFormatPB_Name(row.format))};
    case C::kSegmentCount:
        return Cell {row.segment_count};
    case C::kRowCount:
        return Cell {row.row_count};
    case C::kTotalBytes:
        return Cell {record.total_bytes};
    case C::kDictBytes:
        return component_cell(record, record.dict_bytes, false);
    case C::kPostingBytes:
        return component_cell(record, record.posting_bytes, false);
    case C::kPositionBytes:
        return component_cell(record, record.position_bytes, false);
    case C::kStatsBytes:
        return component_cell(record, record.stats_bytes, false);
    case C::kOtherBytes:
        return component_cell(record, record.other_bytes, true);
    case C::kStatsSource:
        return Cell {std::string_view("FILE")};
    }
    return Cell {};
}

void insert_cell(IColumn* column, IndexDiskUsageReader::Column kind, const Cell& cell) {
    if (std::holds_alternative<std::monostate>(cell)) {
        insert_null(column);
    } else if (const auto* value = std::get_if<int64_t>(&cell)) {
        if (kind == IndexDiskUsageReader::Column::kSegmentId) {
            insert_int32(column, cast_set<int32_t>(*value));
        } else {
            insert_int64(column, *value);
        }
    } else {
        insert_string(column, std::get<std::string_view>(cell));
    }
}

} // namespace

void IndexDiskUsageReader::_append_rows(std::vector<MutableColumnPtr>& columns,
                                        const TIndexDiskUsageTablet& target,
                                        const TabletSchema& schema,
                                        const std::vector<IndexDiskUsageRow>& rows) const {
    const auto& partition_names = _scan_range.index_disk_usage_params.partition_names;
    const auto partition = partition_names.find(target.partition_id);
    for (const IndexDiskUsageRow& row : rows) {
        RowView view;
        view.target = &target;
        view.partition_name = partition == partition_names.end() ? nullptr : &partition->second;
        view.row = &row;
        if (row.record.structure != IndexDiskUsageStructure::kContainer) {
            view.index = segment_v2::resolve_disk_usage_index(schema, row.record.index_id,
                                                              row.record.index_suffix);
        }
        if (view.index != nullptr && !view.index->col_unique_ids().empty()) {
            const int32_t ordinal = schema.field_index(view.index->col_unique_ids()[0]);
            if (ordinal >= 0) {
                view.column_name = schema.column(ordinal).name();
            }
        }
        for (size_t i = 0; i < _slot_columns.size(); ++i) {
            insert_cell(columns[i].get(), _slot_columns[i],
                        cell_of(_slot_columns[i], view, _level, _state->backend_id()));
        }
    }
}

} // namespace doris
