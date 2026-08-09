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

#include "core/data_type_serde/data_type_timestamp_ns_serde.h"

#include <cctz/time_zone.h>

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "util/mysql_row_buffer.h"
#include "util/unaligned.h"

namespace doris {
namespace {

constexpr int64_t NANOS_PER_MILLISECOND = 1000000;
constexpr int64_t NANOS_PER_MICROSECOND = 1000;
constexpr size_t NANOSECOND_SCALE = 9;

bool checked_scale_to_nanos(int64_t value, int64_t multiplier, int64_t* result) {
    return !__builtin_mul_overflow(value, multiplier, result);
}

Status utc_epoch_nanos_to_local_epoch_nanos(int64_t source, const cctz::time_zone& timezone,
                                            int64_t* result) {
    const TimeStampNsValue source_value(source);
    DateV2Value<DateTimeV2ValueType> local_value;
    local_value.from_unixtime(source_value.epoch_seconds(), timezone);
    local_value.set_microsecond(source_value.microsecond());
    TimeStampNsValue target_value;
    if (!target_value.from_datetime(local_value, source_value.nanosecond_remainder())) {
        return Status::DataQualityError("Timestamp {} is outside TIMESTAMP_NS range", source);
    }
    *result = target_value.epoch_nanos();
    return Status::OK();
}

} // namespace

Status parse_timestamp_ns(StringRef str, int64_t* epoch_nanos,
                          const cctz::time_zone* local_time_zone) {
    std::string input(str.data, str.size);
    const size_t dot = input.rfind('.');
    size_t fraction_begin = std::string::npos;
    size_t fraction_end = std::string::npos;
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

    if (fraction_length > NANOSECOND_SCALE && input[fraction_begin + NANOSECOND_SCALE] >= '5') {
        ++nanos;
    }

    // Keep the fractional token in place so that the legacy parser validates its position and all
    // trailing syntax. Zeroing the digits prevents its scale-0 rounding from changing the second;
    // the nanosecond rounding above remains the only source of fractional rounding.
    if (fraction_begin != std::string::npos) {
        std::fill(input.begin() + fraction_begin, input.begin() + fraction_end, '0');
    }
    const StringRef input_ref(input.data(), input.size());
    DateV2Value<DateTimeV2ValueType> datetime;
    CastParameters params {.status = Status::OK(), .is_strict = true};
    CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
            input_ref, datetime, local_time_zone, 0, params);
    if (!params.status.ok()) {
        if (dot != std::string::npos) {
            return Status::InvalidArgument("Invalid TIMESTAMP_NS value '{}'",
                                           std::string(str.data, str.size));
        }
        return params.status;
    }

    if (nanos == TimeStampNsValue::NANOS_PER_SECOND) {
        if (!datetime.date_add_interval<TimeUnit::SECOND>(
                    TimeInterval {TimeUnit::SECOND, 1, false})) {
            return Status::InvalidArgument("TIMESTAMP_NS value overflows while rounding '{}'",
                                           std::string(str.data, str.size));
        }
        nanos = 0;
    }
    datetime.set_microsecond(nanos / NANOS_PER_MICROSECOND);
    TimeStampNsValue value;
    if (!value.from_datetime(datetime, static_cast<uint16_t>(nanos % NANOS_PER_MICROSECOND))) {
        return Status::InvalidArgument(
                "TIMESTAMP_NS value '{}' is outside [{}, {}]", std::string(str.data, str.size),
                TimeStampNsValue(std::numeric_limits<int64_t>::min()).to_string(),
                TimeStampNsValue(std::numeric_limits<int64_t>::max()).to_string());
    }
    *epoch_nanos = value.epoch_nanos();
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::from_string_batch(const ColumnString& strings,
                                                   ColumnNullable& result,
                                                   const FormatOptions& options) const {
    auto& data = assert_cast<ColumnTimeStampNs&>(result.get_nested_column()).get_data();
    auto& null_map = result.get_null_map_column().get_data();
    result.resize(strings.size());
    for (size_t i = 0; i < strings.size(); ++i) {
        int64_t value = 0;
        const auto status = parse_timestamp_ns(strings.get_data_at(i), &value, options.timezone);
        null_map[i] = !status.ok();
        data[i] = TimeStampNsValue(value);
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::from_string_strict_mode_batch(
        const ColumnString& strings, IColumn& result, const FormatOptions& options,
        const NullMap::value_type* null_map) const {
    auto& data = assert_cast<ColumnTimeStampNs&>(result).get_data();
    result.resize(strings.size());
    for (size_t i = 0; i < strings.size(); ++i) {
        if (null_map != nullptr && null_map[i]) {
            continue;
        }
        int64_t value = 0;
        RETURN_IF_ERROR(parse_timestamp_ns(strings.get_data_at(i), &value, options.timezone));
        data[i] = TimeStampNsValue(value);
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::from_string(StringRef& str, IColumn& column,
                                             const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_timestamp_ns(str, &value, options.timezone));
    assert_cast<ColumnTimeStampNs&>(column).insert_value(TimeStampNsValue(value));
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                         const FormatOptions& options) const {
    return from_string(str, column, options);
}

Status DataTypeTimeStampNsSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                          int64_t end_idx, BufferWritable& bw,
                                                          FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeTimeStampNsSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                            BufferWritable& bw,
                                                            FormatOptions& options) const {
    auto [column_ptr, index] = check_column_const_set_readability(column, row_num);
    if (_nesting_level > 1) {
        bw.write('"');
    }
    const auto value =
            assert_cast<const ColumnTimeStampNs&, TypeCheckOnRelease::DISABLE>(*column_ptr)
                    .get_element(index);
    const std::string result = value.to_string(9);
    bw.write(result.data(), result.size());
    if (_nesting_level > 1) {
        bw.write('"');
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::deserialize_one_cell_from_json(
        IColumn& column, Slice& slice, const FormatOptions& options) const {
    if (_nesting_level > 1) {
        slice.trim_quote();
    }
    StringRef str(slice.data, slice.size);
    return from_string(str, column, options);
}

Status DataTypeTimeStampNsSerDe::deserialize_column_from_jsonb(IColumn& column,
                                                               const JsonbValue* jsonb_value,
                                                               CastParameters& cast_params) const {
    DORIS_CHECK(jsonb_value->isString());
    return parse_column_from_jsonb_string(column, jsonb_value, cast_params);
}

Status DataTypeTimeStampNsSerDe::deserialize_column_from_jsonb_vector(
        ColumnNullable& column_to, const ColumnString& column_from,
        CastParameters& cast_params) const {
    return DataTypeSerDe::deserialize_column_from_jsonb_vector(column_to, column_from, cast_params);
}

Status DataTypeTimeStampNsSerDe::write_column_to_arrow(const IColumn& column,
                                                       const NullMap* null_map,
                                                       arrow::ArrayBuilder* array_builder,
                                                       int64_t start, int64_t end,
                                                       const cctz::time_zone& ctz) const {
    return Status::NotSupported("DataTypeTimeStampNsSerDe::write_column_to_arrow");
}

Status DataTypeTimeStampNsSerDe::read_column_from_arrow(IColumn& column,
                                                        const arrow::Array* arrow_array,
                                                        int64_t start, int64_t end,
                                                        const cctz::time_zone& ctz) const {
    return Status::NotSupported("DataTypeTimeStampNsSerDe::read_column_from_arrow");
}

Status DataTypeTimeStampNsSerDe::read_column_from_decoded_values(
        IColumn& column, const DecodedColumnView& view) const {
    if (view.value_kind != DecodedValueKind::INT64) {
        return decoded_column_view_handle_conversion_failure(
                column, view,
                Status::NotSupported("TIMESTAMP_NS decoded reader expects INT64 source"));
    }
    auto& data = assert_cast<ColumnTimeStampNs&>(column).get_data();
    const auto* values = reinterpret_cast<const int64_t*>(view.values);
    static const auto utc = cctz::utc_time_zone();
    const auto& timezone = view.timezone == nullptr ? utc : *view.timezone;
    for (int64_t row = 0; row < view.row_count; ++row) {
        if (decoded_column_view_row_is_null(view, row)) {
            data.push_back(TimeStampNsValue(0));
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
                    Status::NotSupported("TIMESTAMP_NS decoded reader requires a time unit"));
        }
        if (!checked_scale_to_nanos(values[row], multiplier, &nanos)) {
            return decoded_column_view_handle_conversion_failure(
                    column, view,
                    Status::DataQualityError("Timestamp {} overflows nanoseconds", values[row]));
        }
        if (view.timestamp_is_adjusted_to_utc) {
            RETURN_IF_ERROR(utc_epoch_nanos_to_local_epoch_nanos(nanos, timezone, &nanos));
        }
        data.push_back(TimeStampNsValue(nanos));
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                              MysqlRowBinaryBuffer& row_buffer,
                                                              int64_t row_idx, bool col_const,
                                                              const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeStampNs&>(column).get_data();
    const auto index = index_check_const(row_idx, col_const);
    const auto value = TimeStampNsValue(data[index]).to_string(9);
    if (row_buffer.push_string(value.data(), value.size()) != 0) {
        return Status::InternalError("pack MySQL TIMESTAMP_NS buffer failed");
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::write_column_to_orc(const std::string& timezone,
                                                     const IColumn& column, const NullMap* null_map,
                                                     orc::ColumnVectorBatch* orc_col_batch,
                                                     int64_t start, int64_t end, Arena& arena,
                                                     const FormatOptions& options) const {
    return Status::NotSupported("DataTypeTimeStampNsSerDe::write_column_to_orc");
}

void DataTypeTimeStampNsSerDe::write_one_cell_to_binary(const IColumn& src_column,
                                                        ColumnString::Chars& chars,
                                                        int64_t row_num) const {
    const auto type = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS);
    constexpr auto scale = static_cast<uint8_t>(9);
    const auto value = assert_cast<const ColumnTimeStampNs&>(src_column).get_element(row_num);
    const size_t old_size = chars.size();
    chars.resize(old_size + sizeof(type) + sizeof(scale) + sizeof(value));
    memcpy(chars.data() + old_size, &type, sizeof(type));
    memcpy(chars.data() + old_size + sizeof(type), &scale, sizeof(scale));
    memcpy(chars.data() + old_size + sizeof(type) + sizeof(scale), &value, sizeof(value));
}

std::string DataTypeTimeStampNsSerDe::to_olap_string(const Field& field) const {
    return field.get<TYPE_TIMESTAMP_NS>().to_string(9);
}

Status DataTypeTimeStampNsSerDe::from_olap_string(const std::string& str, Field& field,
                                                  const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_timestamp_ns(StringRef(str.data(), str.size()), &value));
    field = Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(value));
    return Status::OK();
}

} // namespace doris
