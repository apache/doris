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

#include "core/data_type_serde/data_type_datetimev2_nano_serde.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include <cctype>
#include <limits>
#include <orc/Vector.hh>
#include <string>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "util/mysql_row_buffer.h"
#include "util/unaligned.h"

namespace doris {
namespace {

constexpr int64_t NANOS_PER_MILLISECOND = 1000000;
constexpr int64_t NANOS_PER_MICROSECOND = 1000;

bool checked_scale_to_nanos(int64_t value, int64_t multiplier, int64_t* result) {
    return !__builtin_mul_overflow(value, multiplier, result);
}

Status utc_epoch_nanos_to_local_epoch_nanos(int64_t source, const cctz::time_zone& timezone,
                                            int64_t* result) {
    const DateTimeV2NanoValue source_value(source);
    DateV2Value<DateTimeV2ValueType> local_value;
    local_value.from_unixtime(source_value.epoch_seconds(), timezone);
    local_value.set_microsecond(source_value.microsecond());
    DateTimeV2NanoValue target_value;
    if (!target_value.from_datetime(local_value, source_value.nanosecond_remainder())) {
        return Status::DataQualityError("Timestamp {} is outside DATETIMEV2 nanosecond range",
                                        source);
    }
    *result = target_value.epoch_nanos();
    return Status::OK();
}

Status local_epoch_nanos_to_utc_epoch_nanos(int64_t source, const cctz::time_zone& timezone,
                                            int64_t* result) {
    const DateTimeV2NanoValue source_value(source);
    auto local_value = source_value.to_datetime();
    int64_t seconds = 0;
    local_value.unix_timestamp(&seconds, timezone);
    const __int128 epoch_nanos =
            static_cast<__int128>(seconds) * DateTimeV2NanoValue::NANOS_PER_SECOND +
            source_value.nanosecond();
    if (epoch_nanos < std::numeric_limits<int64_t>::min() ||
        epoch_nanos > std::numeric_limits<int64_t>::max()) {
        return Status::DataQualityError("DATETIMEV2 value {} is outside epoch nanosecond range",
                                        source_value.to_string(9));
    }
    *result = static_cast<int64_t>(epoch_nanos);
    return Status::OK();
}

} // namespace

Status parse_datetimev2_nano(StringRef str, int scale, int64_t* epoch_nanos,
                             const cctz::time_zone* local_time_zone) {
    DORIS_CHECK_GE(scale, 7);
    DORIS_CHECK_LE(scale, 9);

    std::string input(str.data, str.size);
    size_t fraction_begin = std::string::npos;
    size_t fraction_end = std::string::npos;
    const size_t dot = input.rfind('.');
    if (dot != std::string::npos && dot + 1 < input.size() &&
        std::isdigit(static_cast<unsigned char>(input[dot + 1]))) {
        fraction_begin = dot + 1;
        fraction_end = fraction_begin;
        while (fraction_end < input.size() &&
               std::isdigit(static_cast<unsigned char>(input[fraction_end]))) {
            ++fraction_end;
        }
    }

    uint32_t nanos = 0;
    size_t fraction_length = 0;
    if (fraction_begin != std::string::npos) {
        fraction_length = fraction_end - fraction_begin;
        const size_t copied_digits = std::min<size_t>(fraction_length, 9);
        for (size_t i = 0; i < copied_digits; ++i) {
            nanos = nanos * 10 + static_cast<uint32_t>(input[fraction_begin + i] - '0');
        }
        for (size_t i = copied_digits; i < 9; ++i) {
            nanos *= 10;
        }
    }

    const auto quantum = static_cast<uint32_t>(int_exp10(9 - scale));
    nanos = nanos / quantum * quantum;
    if (fraction_length > static_cast<size_t>(scale) && input[fraction_begin + scale] >= '5') {
        nanos += quantum;
    }

    std::string base = input;
    if (fraction_begin != std::string::npos) {
        base.erase(dot, fraction_end - dot);
    }
    const StringRef base_ref(base.data(), base.size());
    DateV2Value<DateTimeV2ValueType> datetime;
    CastParameters params {.status = Status::OK(), .is_strict = true};
    CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
            base_ref, datetime, local_time_zone, 0, params);
    if (!params.status.ok()) {
        return params.status;
    }

    if (nanos == DateTimeV2NanoValue::NANOS_PER_SECOND) {
        if (!datetime.date_add_interval<TimeUnit::SECOND>(
                    TimeInterval {TimeUnit::SECOND, 1, false})) {
            return Status::InvalidArgument("DATETIMEV2 value overflows while rounding '{}'", input);
        }
        nanos = 0;
    }
    datetime.set_microsecond(nanos / NANOS_PER_MICROSECOND);
    DateTimeV2NanoValue value;
    if (!value.from_datetime(datetime, static_cast<uint16_t>(nanos % NANOS_PER_MICROSECOND))) {
        return Status::InvalidArgument(
                "DATETIMEV2({}) value '{}' is outside [{}, {}]", scale, input,
                DateTimeV2NanoValue(std::numeric_limits<int64_t>::min()).to_string(scale),
                DateTimeV2NanoValue(std::numeric_limits<int64_t>::max()).to_string(scale));
    }
    *epoch_nanos = value.epoch_nanos();
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::from_string_batch(const ColumnString& strings,
                                                      ColumnNullable& result,
                                                      const FormatOptions& options) const {
    auto& data = assert_cast<ColumnDateTimeV2Nano&>(result.get_nested_column()).get_data();
    auto& null_map = result.get_null_map_column().get_data();
    result.resize(strings.size());
    for (size_t i = 0; i < strings.size(); ++i) {
        int64_t value = 0;
        const auto status =
                parse_datetimev2_nano(strings.get_data_at(i), _scale, &value, options.timezone);
        null_map[i] = !status.ok();
        data[i] = DateTimeV2NanoValue(value);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::from_string_strict_mode_batch(
        const ColumnString& strings, IColumn& result, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    auto& data = assert_cast<ColumnDateTimeV2Nano&>(result).get_data();
    result.resize(strings.size());
    for (size_t i = 0; i < strings.size(); ++i) {
        if (null_map != nullptr && null_map[i]) {
            continue;
        }
        int64_t value = 0;
        RETURN_IF_ERROR(
                parse_datetimev2_nano(strings.get_data_at(i), _scale, &value, options.timezone));
        data[i] = DateTimeV2NanoValue(value);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::from_string(StringRef& str, IColumn& column,
                                                const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_datetimev2_nano(str, _scale, &value, options.timezone));
    assert_cast<ColumnDateTimeV2Nano&>(column).insert_value(DateTimeV2NanoValue(value));
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                            const FormatOptions& options) const {
    return from_string(str, column, options);
}

Status DataTypeDateTimeV2NanoSerDe::serialize_column_to_json(const IColumn& column,
                                                             int64_t start_idx, int64_t end_idx,
                                                             BufferWritable& bw,
                                                             FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeV2NanoSerDe::serialize_one_cell_to_json(const IColumn& column,
                                                               int64_t row_num, BufferWritable& bw,
                                                               FormatOptions& options) const {
    auto [column_ptr, index] = check_column_const_set_readability(column, row_num);
    if (_nesting_level > 1) {
        bw.write('"');
    }
    const auto value =
            assert_cast<const ColumnDateTimeV2Nano&, TypeCheckOnRelease::DISABLE>(*column_ptr)
                    .get_element(index);
    const std::string result = value.to_string(_scale);
    bw.write(result.data(), result.size());
    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    StringRef str(slice.data, slice.size);
    return from_string(str, column, options);
}

Status DataTypeDateTimeV2NanoSerDe::write_column_to_arrow(const IColumn& column,
                                                          const NullMap* null_map,
                                                          arrow::ArrayBuilder* array_builder,
                                                          int64_t start, int64_t end,
                                                          const cctz::time_zone& ctz) const {
    const auto& data = assert_cast<const ColumnDateTimeV2Nano&>(column).get_data();
    auto& builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    const auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(builder.type());
    const auto& timezone = timestamp_type->timezone();
    for (int64_t i = start; i < end; ++i) {
        if (null_map != nullptr && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        int64_t value = data[i].epoch_nanos();
        if (!timezone.empty()) {
            RETURN_IF_ERROR(local_epoch_nanos_to_utc_epoch_nanos(value, ctz, &value));
        }
        switch (timestamp_type->unit()) {
        case arrow::TimeUnit::SECOND:
            value /= DateTimeV2NanoValue::NANOS_PER_SECOND;
            break;
        case arrow::TimeUnit::MILLI:
            value /= NANOS_PER_MILLISECOND;
            break;
        case arrow::TimeUnit::MICRO:
            value /= NANOS_PER_MICROSECOND;
            break;
        case arrow::TimeUnit::NANO:
            break;
        }
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(value), column, builder));
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::read_column_from_arrow(IColumn& column,
                                                           const arrow::Array* arrow_array,
                                                           int64_t start, int64_t end,
                                                           const cctz::time_zone& ctz) const {
    if (arrow_array->type_id() != arrow::Type::TIMESTAMP) {
        return Status::InvalidArgument("Expected Arrow timestamp, got {}", arrow_array->type_id());
    }
    if (config::enable_arrow_input_validation) {
        check_arrow_no_offset(*arrow_array);
    }
    const auto& array = assert_cast<const arrow::TimestampArray&>(*arrow_array);
    const auto type = std::static_pointer_cast<arrow::TimestampType>(array.type());
    auto& data = assert_cast<ColumnDateTimeV2Nano&>(column).get_data();
    for (int64_t i = start; i < end; ++i) {
        int64_t value = array.Value(i);
        int64_t nanos = 0;
        switch (type->unit()) {
        case arrow::TimeUnit::SECOND:
            if (!checked_scale_to_nanos(value, DateTimeV2NanoValue::NANOS_PER_SECOND, &nanos)) {
                return Status::DataQualityError("Arrow timestamp {} overflows nanoseconds", value);
            }
            break;
        case arrow::TimeUnit::MILLI:
            if (!checked_scale_to_nanos(value, NANOS_PER_MILLISECOND, &nanos)) {
                return Status::DataQualityError("Arrow timestamp {} overflows nanoseconds", value);
            }
            break;
        case arrow::TimeUnit::MICRO:
            if (!checked_scale_to_nanos(value, NANOS_PER_MICROSECOND, &nanos)) {
                return Status::DataQualityError("Arrow timestamp {} overflows nanoseconds", value);
            }
            break;
        case arrow::TimeUnit::NANO:
            nanos = value;
            break;
        }
        if (!type->timezone().empty()) {
            RETURN_IF_ERROR(utc_epoch_nanos_to_local_epoch_nanos(nanos, ctz, &nanos));
        }
        data.push_back(DateTimeV2NanoValue(nanos));
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::read_column_from_decoded_values(
        IColumn& column, const DecodedColumnView& view) const {
    if (view.value_kind != DecodedValueKind::INT64) {
        return decoded_column_view_handle_conversion_failure(
                column, view,
                Status::NotSupported("DATETIMEV2 nano decoded reader expects INT64 source"));
    }
    auto& data = assert_cast<ColumnDateTimeV2Nano&>(column).get_data();
    const auto* values = reinterpret_cast<const int64_t*>(view.values);
    static const auto utc = cctz::utc_time_zone();
    const auto& timezone = view.timezone == nullptr ? utc : *view.timezone;
    for (int64_t row = 0; row < view.row_count; ++row) {
        if (decoded_column_view_row_is_null(view, row)) {
            data.push_back(DateTimeV2NanoValue(0));
            continue;
        }
        int64_t nanos = 0;
        int64_t multiplier = 1;
        switch (view.time_unit) {
        case DecodedTimeUnit::MILLIS:
            multiplier = NANOS_PER_MILLISECOND;
            break;
        case DecodedTimeUnit::MICROS:
            multiplier = NANOS_PER_MICROSECOND;
            break;
        case DecodedTimeUnit::NANOS:
            break;
        case DecodedTimeUnit::UNKNOWN:
            return decoded_column_view_handle_conversion_failure(
                    column, view,
                    Status::NotSupported("DATETIMEV2 nano decoded reader requires a time unit"));
        }
        if (!checked_scale_to_nanos(values[row], multiplier, &nanos)) {
            return decoded_column_view_handle_conversion_failure(
                    column, view,
                    Status::DataQualityError("Timestamp {} overflows nanoseconds", values[row]));
        }
        if (view.timestamp_is_adjusted_to_utc) {
            RETURN_IF_ERROR(utc_epoch_nanos_to_local_epoch_nanos(nanos, timezone, &nanos));
        }
        data.push_back(DateTimeV2NanoValue(nanos));
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::write_column_to_mysql_binary(
        const IColumn& column, MysqlRowBinaryBuffer& row_buffer, int64_t row_idx, bool col_const,
        const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateTimeV2Nano&>(column).get_data();
    const auto index = index_check_const(row_idx, col_const);
    const auto value = DateTimeV2NanoValue(data[index]).to_string(_scale);
    if (row_buffer.push_string(value.data(), value.size()) != 0) {
        return Status::InternalError("pack MySQL DATETIMEV2 nano buffer failed");
    }
    return Status::OK();
}

Status DataTypeDateTimeV2NanoSerDe::write_column_to_orc(const std::string& timezone,
                                                        const IColumn& column,
                                                        const NullMap* null_map,
                                                        orc::ColumnVectorBatch* orc_col_batch,
                                                        int64_t start, int64_t end, Arena& arena,
                                                        const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnDateTimeV2Nano&>(column).get_data();
    auto& batch = assert_cast<orc::TimestampVectorBatch&>(*orc_col_batch);
    cctz::time_zone parsed_timezone;
    if (!cctz::load_time_zone(timezone, &parsed_timezone)) {
        return Status::InvalidArgument("Invalid timezone '{}'", timezone);
    }
    for (int64_t row = start; row < end; ++row) {
        if (batch.notNull[row] == 0) {
            continue;
        }
        int64_t value = data[row].epoch_nanos();
        RETURN_IF_ERROR(local_epoch_nanos_to_utc_epoch_nanos(value, parsed_timezone, &value));
        const DateTimeV2NanoValue timestamp(value);
        batch.data[row] = timestamp.epoch_seconds();
        batch.nanoseconds[row] = timestamp.nanosecond();
    }
    batch.numElements = end - start;
    return Status::OK();
}

void DataTypeDateTimeV2NanoSerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                           ColumnString::Chars& chars,
                                                           int64_t row_num) const {
    const auto type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2_NANO);
    const auto scale = static_cast<uint8_t>(_scale);
    const auto value = assert_cast<const ColumnDateTimeV2Nano&>(src_column).get_element(row_num);
    const size_t old_size = chars.size();
    chars.resize(old_size + sizeof(type) + sizeof(scale) + sizeof(value));
    memcpy(chars.data() + old_size, &type, sizeof(type));
    memcpy(chars.data() + old_size + sizeof(type), &scale, sizeof(scale));
    memcpy(chars.data() + old_size + sizeof(type) + sizeof(scale), &value, sizeof(value));
}

std::string DataTypeDateTimeV2NanoSerDe::to_olap_string(const Field& field) const {
    return field.get<TYPE_DATETIMEV2_NANO>().to_string(_scale);
}

Status DataTypeDateTimeV2NanoSerDe::from_olap_string(const std::string& str, Field& field,
                                                     const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_datetimev2_nano(StringRef(str.data(), str.size()), _scale, &value));
    field = Field::create_field<TYPE_DATETIMEV2_NANO>(DateTimeV2NanoValue(value));
    return Status::OK();
}

} // namespace doris
