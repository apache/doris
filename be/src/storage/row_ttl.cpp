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

#include <cstring>

#include "common/check.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/rowid_conversion.h"
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

void RowVisibilityFilter::record_filtered_rows(const Block& block,
                                               RowIdConversion* rowid_conversion) const {
    if (rowid_conversion == nullptr || rowid_conversion->visibility_filtered_rows() == nullptr ||
        rows_deleted == 0) {
        return;
    }
    const auto rowid_ordinal = block.get_position_by_name(BeConsts::COMPACTION_ROWID_COL);
    const auto& rowids =
            assert_cast<const ColumnString&>(*block.get_by_position(rowid_ordinal).column);
    DCHECK_EQ(rowids.size(), selection.size());
    auto& filtered_rows = *rowid_conversion->visibility_filtered_rows();
    for (size_t row = 0; row < selection.size(); ++row) {
        if (!selection[row]) {
            const auto encoded = rowids.get_data_at(row);
            DCHECK_EQ(encoded.size, sizeof(RowLocation));
            RowLocation location;
            std::memcpy(&location, encoded.data, sizeof(location));
            filtered_rows.insert(location);
        }
    }
}

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
    // DDL validates the duration and fixed time zone. TIMESTAMPTZ conversion always uses UTC.
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

bool should_gc_row_ttl(const TabletSchema& tablet_schema, bool enable_unique_key_merge_on_write,
                       bool is_changelog_tablet, ReaderType reader_type, const Version& version) {
    if (!tablet_schema.has_ttl_col() || tablet_schema.keys_type() == KeysType::AGG_KEYS) {
        return false;
    }
    if (is_changelog_tablet || reader_type == ReaderType::READER_COLD_DATA_COMPACTION) {
        return false;
    }
    // Segment compaction precedes publish: T1's expired key must survive to delete a live
    // version published by T2 after T1 captured its snapshot. Published MOW rowsets can GC
    // expired rows because partial updates, which would read their missing columns, are rejected.
    if (enable_unique_key_merge_on_write && reader_type == ReaderType::READER_SEGMENT_COMPACTION) {
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
