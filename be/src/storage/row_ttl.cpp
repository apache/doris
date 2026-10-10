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

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {
namespace {

template <typename ColumnType, typename ValueType>
void extract_epoch_time(const IColumn& source, size_t row, const cctz::time_zone& time_zone,
                        int64_t* epoch_seconds, int64_t* microsecond) {
    const auto& value = assert_cast<const ColumnType&>(source).get_data()[row];
    const auto& date_time = reinterpret_cast<const ValueType&>(value);
    date_time.unix_timestamp(epoch_seconds, time_zone);
    *microsecond = date_time.microsecond();
}

} // namespace

bool row_ttl_uses_source_time(const TabletSchema& tablet_schema) {
    DORIS_CHECK(tablet_schema.has_ttl_col());
    return tablet_schema.column(tablet_schema.ttl_col_idx()).type() !=
           FieldType::OLAP_FIELD_TYPE_BIGINT;
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

    // Supported calendar values fit in microseconds; adding the configured duration may overflow.
    const int64_t epoch_us = epoch_seconds * 1'000'000 + microsecond;
    if (__builtin_add_overflow(epoch_us, duration_us, expiration_us)) {
        return Status::InvalidArgument("row ttl expiration time overflows int64");
    }
    return Status::OK();
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
    const FieldType ttl_type = tablet_schema.column(tablet_schema.ttl_col_idx()).type();
    const bool source_time = ttl_type != FieldType::OLAP_FIELD_TYPE_BIGINT;
    const int64_t duration_us = tablet_schema.row_ttl_duration_us();
    // DDL validates the immutable policy. TIMESTAMPTZ conversion always uses UTC.
    DORIS_CHECK(!source_time || duration_us >= 0);
    const auto ttl_time_zone =
            cctz::fixed_time_zone(cctz::seconds(tablet_schema.row_ttl_time_zone_offset_seconds()));
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
            RETURN_IF_ERROR(calculate_row_ttl_expiration_us(nullable->get_nested_column(), ttl_type,
                                                            row, ttl_time_zone, duration_us,
                                                            &expiration_us));
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

void copy_row_ttl_source(Block* block, const TabletSchema& tablet_schema, int32_t source_cid,
                         const std::vector<bool>& rows_to_copy, size_t row_pos) {
    DORIS_CHECK(tablet_schema.has_ttl_col());
    DORIS_CHECK(source_cid >= 0);
    DORIS_CHECK(row_pos + rows_to_copy.size() <= block->rows());

    const ColumnWithTypeAndName& source_entry = block->get_by_position(source_cid);
    const auto* nullable_source = check_and_get_column<ColumnNullable>(source_entry.column.get());
    const NullMap* source_null_map =
            nullable_source == nullptr ? nullptr : &nullable_source->get_null_map_data();
    const IColumn& source = nullable_source == nullptr ? *source_entry.column
                                                       : nullable_source->get_nested_column();

    auto ttl_guard = block->mutate_column_scoped(tablet_schema.ttl_col_idx());
    auto& ttl = assert_cast<ColumnNullable&>(*ttl_guard.mutable_column());
    auto& ttl_data = ttl.get_nested_column();
    auto& ttl_null_map = ttl.get_null_map_data();

    for (size_t mask_row = 0; mask_row < rows_to_copy.size(); ++mask_row) {
        if (!rows_to_copy[mask_row]) {
            continue;
        }
        const size_t row = row_pos + mask_row;
        ttl_data.replace_column_data(source, row, row);
        ttl_null_map[row] = source_null_map != nullptr && (*source_null_map)[row];
    }
}

bool should_gc_row_ttl(const TabletSchema& tablet_schema, bool enable_unique_key_merge_on_write,
                       bool is_row_binlog_tablet, ReaderType reader_type, const Version& version) {
    if (!tablet_schema.has_ttl_col() || tablet_schema.keys_type() == KeysType::AGG_KEYS) {
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
