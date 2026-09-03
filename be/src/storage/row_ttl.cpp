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

#include "storage/row_ttl.h"

#include <utility>

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {
namespace {

Result<int64_t> checked_multiply(int64_t value, int64_t multiplier) {
    int64_t result = 0;
    if (__builtin_mul_overflow(value, multiplier, &result)) {
        return ResultError(Status::InvalidArgument("row ttl value overflows microseconds"));
    }
    return result;
}

Result<int64_t> checked_add(int64_t left, int64_t right) {
    int64_t result = 0;
    if (__builtin_add_overflow(left, right, &result)) {
        return ResultError(Status::InvalidArgument("row ttl expiration time overflows int64"));
    }
    return result;
}

template <typename ColumnType, typename ValueType>
void extract_epoch_time(const IColumn& source, size_t row, const cctz::time_zone& time_zone,
                        int64_t* epoch_seconds, int64_t* microsecond) {
    const auto& value = assert_cast<const ColumnType&>(source).get_data()[row];
    const auto& date_time = reinterpret_cast<const ValueType&>(value);
    date_time.unix_timestamp(epoch_seconds, time_zone);
    *microsecond = date_time.microsecond();
}

struct RowTtlColumnRef {
    int32_t index = -1;
    int32_t unique_id = -1;
    FieldType type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
};

Status resolve_row_ttl_restore_column(const TabletSchema& schema, RowTtlColumnRef* ttl_column) {
    int32_t hidden_index = -1;
    for (size_t i = 0; i < schema.num_columns(); ++i) {
        if (schema.column(i).name() != TTL_COL) {
            continue;
        }
        if (hidden_index != -1) {
            return Status::InvalidArgument(
                    "tablet schema contains multiple row ttl hidden columns");
        }
        hidden_index = cast_set<int32_t>(i);
    }

    if (!schema.has_ttl_col()) {
        if (hidden_index != -1) {
            return Status::InvalidArgument(
                    "row ttl hidden column exists but the tablet schema does not identify it");
        }
        return Status::OK();
    }

    const int32_t ttl_col_idx = schema.ttl_col_idx();
    if (ttl_col_idx < 0 || static_cast<size_t>(ttl_col_idx) >= schema.num_columns()) {
        return Status::InvalidArgument("row ttl column index {} is outside schema with {} columns",
                                       ttl_col_idx, schema.num_columns());
    }
    if (hidden_index == -1) {
        return Status::InvalidArgument(
                "tablet schema identifies row ttl but the hidden column is missing");
    }
    if (ttl_col_idx != hidden_index) {
        return Status::InvalidArgument(
                "row ttl column index {} does not point to hidden column at {}", ttl_col_idx,
                hidden_index);
    }

    const auto& column = schema.column(ttl_col_idx);
    if (column.unique_id() < 0) {
        return Status::InvalidArgument("row ttl hidden column unique id {} is invalid",
                                       column.unique_id());
    }
    ttl_column->index = ttl_col_idx;
    ttl_column->unique_id = column.unique_id();
    ttl_column->type = column.type();
    switch (ttl_column->type) {
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        if (schema.has_row_ttl_time_zone_offset_seconds()) {
            return Status::InvalidArgument(
                    "direct-expiration row ttl schema must not have a time zone offset");
        }
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_DATE:
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        if (schema.has_row_ttl_time_zone_offset_seconds() &&
            !is_valid_row_ttl_time_zone_offset_seconds(schema.row_ttl_time_zone_offset_seconds())) {
            return Status::InvalidArgument("row ttl time zone offset {} is invalid",
                                           schema.row_ttl_time_zone_offset_seconds());
        }
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        if (schema.has_row_ttl_time_zone_offset_seconds() &&
            schema.row_ttl_time_zone_offset_seconds() != 0) {
            return Status::InvalidArgument("TIMESTAMPTZ row ttl time zone offset must be 0");
        }
        return Status::OK();
    default:
        return Status::InvalidArgument("unsupported row ttl hidden column type {}",
                                       static_cast<int>(ttl_column->type));
    }
}

Status init_row_ttl_restore_schema(const TabletSchemaPB& schema_pb, TabletSchema* schema) {
    const int32_t ttl_col_idx = schema_pb.ttl_col_idx();
    if (ttl_col_idx < -1 || ttl_col_idx >= schema_pb.column_size()) {
        return Status::InvalidArgument("row ttl column index {} is outside schema with {} columns",
                                       ttl_col_idx, schema_pb.column_size());
    }
    schema->init_from_pb(schema_pb);
    return Status::OK();
}

} // namespace

bool row_ttl_uses_source_time(const TabletSchema& tablet_schema) {
    DORIS_CHECK(tablet_schema.has_ttl_col());
    return tablet_schema.column(tablet_schema.ttl_col_idx()).type() !=
           FieldType::OLAP_FIELD_TYPE_BIGINT;
}

bool row_ttl_requires_time_zone(const TabletSchema& tablet_schema) {
    DORIS_CHECK(tablet_schema.has_ttl_col());
    return row_ttl_requires_time_zone(tablet_schema.column(tablet_schema.ttl_col_idx()).type());
}

bool row_ttl_requires_time_zone(FieldType ttl_type) {
    return ttl_type != FieldType::OLAP_FIELD_TYPE_BIGINT &&
           ttl_type != FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ;
}

Status check_row_ttl_restore_schema_compatible(const TabletSchema& source_schema,
                                               const TabletSchema& target_schema) {
    if (source_schema.has_ttl_col() != target_schema.has_ttl_col()) {
        return Status::InvalidArgument(
                "restored row ttl policy does not match target: source has_ttl={}, target "
                "has_ttl={}",
                source_schema.has_ttl_col(), target_schema.has_ttl_col());
    }
    if (!source_schema.has_ttl_col()) {
        return Status::OK();
    }

    RowTtlColumnRef source_ttl;
    RowTtlColumnRef target_ttl;
    RETURN_IF_ERROR(resolve_row_ttl_restore_column(source_schema, &source_ttl));
    RETURN_IF_ERROR(resolve_row_ttl_restore_column(target_schema, &target_ttl));
    if (source_ttl.unique_id != target_ttl.unique_id || source_ttl.type != target_ttl.type) {
        return Status::InvalidArgument(
                "restored row ttl hidden column does not match target: source index={}, uid={}, "
                "type={}; target index={}, uid={}, type={}",
                source_ttl.index, source_ttl.unique_id, static_cast<int>(source_ttl.type),
                target_ttl.index, target_ttl.unique_id, static_cast<int>(target_ttl.type));
    }
    if (source_schema.row_ttl_duration_us() != target_schema.row_ttl_duration_us()) {
        return Status::InvalidArgument(
                "restored row ttl duration does not match target: source={}, target={}",
                source_schema.row_ttl_duration_us(), target_schema.row_ttl_duration_us());
    }

    // A legacy TIMESTAMPTZ schema without the field and a current schema with an explicit zero
    // both describe UTC. Other temporal types need exact presence so an unknown historical time
    // zone is never guessed during restore.
    if (source_ttl.type == FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ) {
        return Status::OK();
    }
    if (source_schema.has_row_ttl_time_zone_offset_seconds() !=
        target_schema.has_row_ttl_time_zone_offset_seconds()) {
        return Status::InvalidArgument(
                "restored row ttl time zone offset presence does not match target");
    }
    if (source_schema.has_row_ttl_time_zone_offset_seconds() &&
        source_schema.row_ttl_time_zone_offset_seconds() !=
                target_schema.row_ttl_time_zone_offset_seconds()) {
        return Status::InvalidArgument(
                "restored row ttl time zone offset does not match target: source={}, target={}",
                source_schema.row_ttl_time_zone_offset_seconds(),
                target_schema.row_ttl_time_zone_offset_seconds());
    }
    return Status::OK();
}

Status check_row_ttl_restore_tablet_meta_compatible(const TabletMetaPB& source_meta,
                                                    const TabletSchema& target_schema) {
    if (!source_meta.has_schema()) {
        return Status::InvalidArgument("restored tablet metadata has no tablet schema");
    }
    TabletSchema source_schema;
    RETURN_IF_ERROR(init_row_ttl_restore_schema(source_meta.schema(), &source_schema));
    RETURN_IF_ERROR(check_row_ttl_restore_schema_compatible(source_schema, target_schema));

    auto check_embedded_schema = [&](const RowsetMetaPB& rowset_meta) -> Status {
        if (!rowset_meta.has_tablet_schema()) {
            return Status::OK();
        }
        TabletSchema embedded_schema;
        RETURN_IF_ERROR(init_row_ttl_restore_schema(rowset_meta.tablet_schema(), &embedded_schema));
        return check_row_ttl_restore_schema_compatible(embedded_schema, target_schema);
    };
    for (const auto& rowset_meta : source_meta.rs_metas()) {
        RETURN_IF_ERROR(check_embedded_schema(rowset_meta));
    }
    for (const auto& rowset_meta : source_meta.inc_rs_metas()) {
        RETURN_IF_ERROR(check_embedded_schema(rowset_meta));
    }
    for (const auto& rowset_meta : source_meta.stale_rs_metas()) {
        RETURN_IF_ERROR(check_embedded_schema(rowset_meta));
    }
    return Status::OK();
}

Status calculate_row_ttl_expiration_us(const IColumn& source, FieldType source_type, size_t row,
                                       const cctz::time_zone& time_zone, int64_t duration_us,
                                       int64_t* expiration_us) {
    int64_t epoch_seconds = 0;
    int64_t microsecond = 0;
    switch (source_type) {
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        const auto& date_time = assert_cast<const ColumnDate&>(source).get_data()[row];
        date_time.unix_timestamp(&epoch_seconds, time_zone);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        const auto& date_time = assert_cast<const ColumnDateTime&>(source).get_data()[row];
        date_time.unix_timestamp(&epoch_seconds, time_zone);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        extract_epoch_time<ColumnDateV2, DateV2Value<DateV2ValueType>>(
                source, row, time_zone, &epoch_seconds, &microsecond);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        extract_epoch_time<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>(
                source, row, time_zone, &epoch_seconds, &microsecond);
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        extract_epoch_time<ColumnTimeStampTz, TimestampTzValue>(source, row, cctz::utc_time_zone(),
                                                                &epoch_seconds, &microsecond);
        break;
    default:
        return Status::InvalidArgument("row ttl source column must be DATE or DATETIME");
    }

    auto epoch_base = checked_multiply(epoch_seconds, 1'000'000);
    if (!epoch_base) {
        return epoch_base.error();
    }
    auto epoch_micros = checked_add(*epoch_base, microsecond);
    if (!epoch_micros) {
        return epoch_micros.error();
    }
    auto expiration = checked_add(*epoch_micros, duration_us);
    if (!expiration) {
        return expiration.error();
    }
    *expiration_us = *expiration;
    return Status::OK();
}

Result<std::optional<int64_t>> convert_row_ttl_time_to_epoch_us(const TabletColumn& source_column,
                                                                const std::string& source_value,
                                                                int32_t time_zone_offset_seconds) {
    if (!is_valid_row_ttl_time_zone_offset_seconds(time_zone_offset_seconds)) {
        return ResultError(Status::InvalidArgument(
                "row ttl time zone offset {} must be a whole minute in [{}, {}]",
                time_zone_offset_seconds, MIN_ROW_TTL_TIME_ZONE_OFFSET_SECONDS,
                MAX_ROW_TTL_TIME_ZONE_OFFSET_SECONDS));
    }
    if (source_column.type() == FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ &&
        time_zone_offset_seconds != 0) {
        return ResultError(
                Status::InvalidArgument("TIMESTAMPTZ row ttl time zone offset must be 0"));
    }
    const cctz::time_zone time_zone =
            cctz::fixed_time_zone(cctz::seconds(time_zone_offset_seconds));

    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(source_column);
    MutableColumnPtr source = data_type->create_column();
    StringRef value(source_value);
    Slice slice = value.to_slice();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    options.timezone = &time_zone;
    Status status = data_type->get_serde()->deserialize_one_cell_from_json(*source, slice, options);
    if (!status.ok()) {
        return ResultError(status);
    }

    const IColumn* source_data = source.get();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(source_data)) {
        if (nullable->is_null_at(0)) {
            return std::optional<int64_t> {};
        }
        source_data = &nullable->get_nested_column();
    }
    int64_t expiration_us = 0;
    status = calculate_row_ttl_expiration_us(*source_data, source_column.type(), 0, time_zone, 0,
                                             &expiration_us);
    if (!status.ok()) {
        return ResultError(status);
    }
    return std::optional<int64_t> {expiration_us};
}

Status build_row_visibility_filter(const Block& block, const TabletSchema& tablet_schema,
                                   bool apply_delete_sign, bool apply_row_ttl, int64_t now_us,
                                   RowVisibilityFilter* filter) {
    filter->selection.resize_fill(block.rows(), 1);
    filter->rows_deleted = 0;

    if (apply_delete_sign) {
        const int delete_sign_position = block.get_position_by_name(DELETE_SIGN);
        DORIS_CHECK_GE(delete_sign_position, 0);
        const auto* delete_sign = check_and_get_column<ColumnInt8>(
                block.get_by_position(delete_sign_position).column.get());
        DORIS_CHECK(delete_sign != nullptr);
        const auto& delete_sign_data = delete_sign->get_data();
        for (size_t row = 0; row < block.rows(); ++row) {
            if (delete_sign_data[row] != 0) {
                filter->selection[row] = 0;
                ++filter->rows_deleted;
            }
        }
    }

    if (!apply_row_ttl) {
        return Status::OK();
    }

    const int ttl_position = block.get_position_by_name(TTL_COL);
    DORIS_CHECK_GE(ttl_position, 0);
    const auto* nullable =
            check_and_get_column<ColumnNullable>(block.get_by_position(ttl_position).column.get());
    DORIS_CHECK(nullable != nullptr);
    const auto& null_map = nullable->get_null_map_data();
    const bool source_time = row_ttl_uses_source_time(tablet_schema);
    const bool requires_time_zone = row_ttl_requires_time_zone(tablet_schema);
    const int64_t duration_us = tablet_schema.row_ttl_duration_us();
    if (source_time && duration_us < 0) {
        return Status::InvalidArgument("row ttl duration is missing from temporal tablet schema");
    }
    cctz::time_zone ttl_time_zone = cctz::utc_time_zone();
    if (requires_time_zone) {
        if (!tablet_schema.has_row_ttl_time_zone_offset_seconds()) {
            return Status::InvalidArgument(
                    "row ttl time zone offset is missing from temporal tablet schema");
        }
        const int32_t offset_seconds = tablet_schema.row_ttl_time_zone_offset_seconds();
        if (!is_valid_row_ttl_time_zone_offset_seconds(offset_seconds)) {
            return Status::InvalidArgument(
                    "row ttl time zone offset {} must be a whole minute in [{}, {}]",
                    offset_seconds, MIN_ROW_TTL_TIME_ZONE_OFFSET_SECONDS,
                    MAX_ROW_TTL_TIME_ZONE_OFFSET_SECONDS);
        }
        ttl_time_zone = cctz::fixed_time_zone(cctz::seconds(offset_seconds));
    } else if (source_time && tablet_schema.has_row_ttl_time_zone_offset_seconds() &&
               tablet_schema.row_ttl_time_zone_offset_seconds() != 0) {
        return Status::InvalidArgument("TIMESTAMPTZ row ttl time zone offset must be 0");
    }
    const auto* direct_expiration =
            source_time ? nullptr
                        : check_and_get_column<ColumnInt64>(&nullable->get_nested_column());
    DORIS_CHECK(source_time || direct_expiration != nullptr);
    for (size_t row = 0; row < block.rows(); ++row) {
        if (!filter->selection[row] || null_map[row]) {
            continue;
        }
        int64_t expiration_us = 0;
        if (source_time) {
            RETURN_IF_ERROR(calculate_row_ttl_expiration_us(
                    nullable->get_nested_column(),
                    tablet_schema.column(tablet_schema.ttl_col_idx()).type(), row, ttl_time_zone,
                    duration_us, &expiration_us));
        } else {
            expiration_us = direct_expiration->get_data()[row];
        }
        if (expiration_us <= now_us) {
            filter->selection[row] = 0;
            ++filter->rows_deleted;
        }
    }
    return Status::OK();
}

Status filter_block_by_row_visibility(Block* block, const IColumn::Filter& filter) {
    if (filter.size() != block->rows()) {
        return Status::InvalidArgument("row visibility filter size {} does not match block rows {}",
                                       filter.size(), block->rows());
    }
    RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, filter));
    return Status::OK();
}

Status copy_row_ttl_source(Block* block, const TabletSchema& tablet_schema, int32_t source_cid,
                           const std::vector<bool>& rows_to_copy, size_t row_pos) {
    DORIS_CHECK(tablet_schema.has_ttl_col());
    DORIS_CHECK(source_cid >= 0);
    DORIS_CHECK(row_pos + rows_to_copy.size() <= block->rows());

    const ColumnWithTypeAndName& source_entry = block->get_by_position(source_cid);
    const auto* nullable_source = check_and_get_column<ColumnNullable>(source_entry.column.get());
    const NullMap* source_null_map =
            nullable_source == nullptr ? nullptr : &nullable_source->get_null_map_data();
    const ColumnPtr source = nullable_source == nullptr ? source_entry.column
                                                        : nullable_source->get_nested_column_ptr();

    const int32_t ttl_cid = tablet_schema.ttl_col_idx();
    MutableColumnPtr mutable_ttl = IColumn::mutate(block->get_by_position(ttl_cid).column);
    auto& ttl = assert_cast<ColumnNullable&>(*mutable_ttl);
    auto& ttl_data = ttl.get_nested_column();
    auto& ttl_null_map = ttl.get_null_map_data();

    for (size_t mask_row = 0; mask_row < rows_to_copy.size(); ++mask_row) {
        if (!rows_to_copy[mask_row]) {
            continue;
        }
        const size_t row = row_pos + mask_row;
        if (source_null_map != nullptr && (*source_null_map)[row]) {
            ttl_null_map[row] = 1;
            ttl_data.replace_column_data(*source, row, row);
            continue;
        }
        ttl_data.replace_column_data(*source, row, row);
        ttl_null_map[row] = 0;
    }
    block->replace_by_position(ttl_cid, std::move(mutable_ttl));
    return Status::OK();
}

bool should_gc_row_ttl(const TabletSchema& tablet_schema, bool enable_unique_key_merge_on_write,
                       bool is_row_binlog_tablet, ReaderType reader_type, const Version& version) {
    if (!tablet_schema.has_ttl_col() || tablet_schema.keys_type() == KeysType::AGG_KEYS) {
        return false;
    }
    if (row_ttl_uses_source_time(tablet_schema) && tablet_schema.row_ttl_duration_us() < 0) {
        return false;
    }
    if (row_ttl_requires_time_zone(tablet_schema) &&
        (!tablet_schema.has_row_ttl_time_zone_offset_seconds() ||
         !is_valid_row_ttl_time_zone_offset_seconds(
                 tablet_schema.row_ttl_time_zone_offset_seconds()))) {
        return false;
    }
    if (row_ttl_uses_source_time(tablet_schema) && !row_ttl_requires_time_zone(tablet_schema) &&
        tablet_schema.has_row_ttl_time_zone_offset_seconds() &&
        tablet_schema.row_ttl_time_zone_offset_seconds() != 0) {
        return false;
    }
    if (is_row_binlog_tablet || reader_type == ReaderType::READER_COLD_DATA_COMPACTION) {
        return false;
    }

    const bool full_coverage =
            reader_type == ReaderType::READER_FULL_COMPACTION ||
            (reader_type == ReaderType::READER_BASE_COMPACTION && version.first == 0);
    if (tablet_schema.keys_type() == KeysType::UNIQUE_KEYS && !enable_unique_key_merge_on_write) {
        return full_coverage;
    }
    return reader_type == ReaderType::READER_CUMULATIVE_COMPACTION ||
           reader_type == ReaderType::READER_BASE_COMPACTION ||
           reader_type == ReaderType::READER_FULL_COMPACTION ||
           reader_type == ReaderType::READER_SEGMENT_COMPACTION;
}

} // namespace doris
