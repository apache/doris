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
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/timezone_utils.h"

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

Result<std::optional<int64_t>> convert_row_ttl_time_to_epoch_us(
        const TabletColumn& source_column, const std::string& source_value,
        const std::string& timezone) {
    cctz::time_zone time_zone;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, time_zone)) {
        return ResultError(Status::InvalidArgument("invalid time zone for row ttl: {}", timezone));
    }

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

Result<std::optional<int64_t>> calculate_row_ttl_expiration_us(RowTtlOperation operation,
                                                               std::optional<int64_t> value,
                                                               int64_t now_us) {
    if (operation == RowTtlOperation::PERSIST) {
        return std::optional<int64_t> {};
    }
    if (!value.has_value()) {
        return ResultError(Status::InvalidArgument("row ttl operation requires a value"));
    }

    int64_t multiplier = 0;
    bool relative = false;
    switch (operation) {
    case RowTtlOperation::EXPIRE:
        multiplier = 1'000'000;
        relative = true;
        break;
    case RowTtlOperation::PEXPIRE:
        multiplier = 1'000;
        relative = true;
        break;
    case RowTtlOperation::EXPIREAT:
        multiplier = 1'000'000;
        break;
    case RowTtlOperation::PEXPIREAT:
        multiplier = 1'000;
        break;
    case RowTtlOperation::PERSIST:
        __builtin_unreachable();
    }

    auto converted = checked_multiply(*value, multiplier);
    if (!converted) {
        return ResultError(converted.error());
    }
    if (!relative) {
        return std::optional<int64_t> {*converted};
    }
    auto expiration = checked_add(now_us, *converted);
    if (!expiration) {
        return ResultError(expiration.error());
    }
    return std::optional<int64_t> {*expiration};
}

Status apply_row_ttl_update(MutableColumnPtr& ttl_column, size_t row, RowTtlOperation operation,
                            std::optional<int64_t> value, int64_t now_us) {
    if (row >= ttl_column->size()) {
        return Status::InvalidArgument("row ttl target row {} is out of range {}", row,
                                       ttl_column->size());
    }
    auto* nullable = typeid_cast<ColumnNullable*>(ttl_column.get());
    if (nullable == nullptr) {
        return Status::InvalidArgument("row ttl column must be nullable");
    }
    auto* values = typeid_cast<ColumnInt64*>(&nullable->get_nested_column());
    if (values == nullptr) {
        return Status::InvalidArgument("row ttl column must have Int64 nested values");
    }

    auto expiration = calculate_row_ttl_expiration_us(operation, value, now_us);
    if (!expiration) {
        return expiration.error();
    }
    auto& null_map = nullable->get_null_map_data();
    if (!expiration->has_value()) {
        null_map[row] = 1;
        values->get_data()[row] = 0;
        return Status::OK();
    }
    null_map[row] = 0;
    values->get_data()[row] = **expiration;
    return Status::OK();
}

Status apply_row_ttl_update(const RowLocation& location, RowTtlOperation operation,
                            std::optional<int64_t> value, int64_t now_us,
                            const RowTtlLocationWriter& writer) {
    auto expiration = calculate_row_ttl_expiration_us(operation, value, now_us);
    if (!expiration) {
        return expiration.error();
    }
    return writer(location, *expiration);
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
    const int64_t duration_us = tablet_schema.row_ttl_duration_us();
    if (source_time && duration_us < 0) {
        return Status::InvalidArgument("row ttl duration is missing from temporal tablet schema");
    }
    const auto* direct_expiration = source_time
                                            ? nullptr
                                            : check_and_get_column<ColumnInt64>(
                                                      &nullable->get_nested_column());
    DORIS_CHECK(source_time || direct_expiration != nullptr);
    const cctz::time_zone local_time_zone = cctz::local_time_zone();
    for (size_t row = 0; row < block.rows(); ++row) {
        if (!filter->selection[row] || null_map[row]) {
            continue;
        }
        int64_t expiration_us = 0;
        if (source_time) {
            RETURN_IF_ERROR(calculate_row_ttl_expiration_us(
                    nullable->get_nested_column(),
                    tablet_schema.column(tablet_schema.ttl_col_idx()).type(), row,
                    local_time_zone, duration_us, &expiration_us));
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
                       ReaderType reader_type, const Version& version) {
    if (!tablet_schema.has_ttl_col() || tablet_schema.keys_type() == KeysType::AGG_KEYS) {
        return false;
    }
    if (reader_type == ReaderType::READER_BINLOG_COMPACTION ||
        reader_type == ReaderType::READER_COLD_DATA_COMPACTION) {
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
