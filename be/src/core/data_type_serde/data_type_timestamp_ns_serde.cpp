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

#include <arrow/builder.h>
#include <cctz/time_zone.h>

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "util/mysql_row_buffer.h"
#include "util/unaligned.h"

namespace doris {

namespace {

Status get_nanos_per_arrow_timestamp_unit(arrow::TimeUnit::type unit, int64_t* nanos_per_unit) {
    switch (unit) {
    case arrow::TimeUnit::SECOND:
        *nanos_per_unit = TimeStampNsValue::NANOS_PER_SECOND;
        break;
    case arrow::TimeUnit::MILLI:
        *nanos_per_unit = TimeStampNsValue::NANOS_PER_MILLISECOND;
        break;
    case arrow::TimeUnit::MICRO:
        *nanos_per_unit = TimeStampNsValue::NANOS_PER_MICROSECOND;
        break;
    case arrow::TimeUnit::NANO:
        *nanos_per_unit = 1;
        break;
    default:
        return Status::InvalidArgument("Unsupported Arrow timestamp unit: {}",
                                       static_cast<int>(unit));
    }
    return Status::OK();
}

struct FractionRange {
    bool recognized = false;
    size_t begin = std::string::npos;
    size_t end = std::string::npos;
};

bool consume_digits(std::string_view input, size_t& position, size_t min_digits,
                    size_t max_digits) {
    const size_t begin = position;
    while (position < input.size() && position - begin < max_digits &&
           std::isdigit(static_cast<unsigned char>(input[position]))) {
        ++position;
    }
    return position - begin >= min_digits;
}

bool consume_non_alnum_separator(std::string_view input, size_t& position) {
    if (position == input.size() || std::isalnum(static_cast<unsigned char>(input[position]))) {
        return false;
    }
    ++position;
    return true;
}

// Locate the fractional token in the extra string grammar accepted by DATETIMEV2 non-strict
// casts. Unlike strict formats, both date and time fields may use '.', so rfind('.') cannot
// distinguish the second separator from the fractional point.
FractionRange find_non_strict_fraction(std::string_view input) {
    size_t position = 0;
    while (position < input.size() && std::isspace(static_cast<unsigned char>(input[position]))) {
        ++position;
    }

    if (!consume_digits(input, position, 2, 2)) {
        return {};
    }
    if (position < input.size() && std::isdigit(static_cast<unsigned char>(input[position]))) {
        if (!consume_digits(input, position, 2, 2)) {
            return {};
        }
    }
    if (!consume_non_alnum_separator(input, position) || !consume_digits(input, position, 1, 2) ||
        !consume_non_alnum_separator(input, position) || !consume_digits(input, position, 1, 2)) {
        return {};
    }

    size_t trailing = position;
    while (trailing < input.size() && std::isspace(static_cast<unsigned char>(input[trailing]))) {
        ++trailing;
    }
    if (trailing == input.size()) {
        return {.recognized = true};
    }

    if (position == input.size() ||
        (input[position] != ' ' && input[position] != 'T' && input[position] != ':')) {
        return {};
    }
    ++position;
    if (!consume_digits(input, position, 1, 2) || !consume_non_alnum_separator(input, position) ||
        !consume_digits(input, position, 1, 2) || !consume_non_alnum_separator(input, position) ||
        !consume_digits(input, position, 1, 2)) {
        return {};
    }

    FractionRange range {.recognized = true};
    if (position < input.size() && input[position] == '.') {
        range.begin = ++position;
        while (position < input.size() &&
               std::isdigit(static_cast<unsigned char>(input[position]))) {
            ++position;
        }
        range.end = position;
    }
    return range;
}

std::pair<size_t, size_t> find_strict_fraction(std::string_view input) {
    const size_t dot = input.rfind('.');
    if (dot == std::string::npos || dot + 1 == input.size() ||
        !std::isdigit(static_cast<unsigned char>(input[dot + 1]))) {
        return {std::string::npos, std::string::npos};
    }

    size_t fraction_end = dot + 1;
    while (fraction_end < input.size() &&
           std::isdigit(static_cast<unsigned char>(input[fraction_end]))) {
        ++fraction_end;
    }
    return {dot + 1, fraction_end};
}

uint32_t extract_fractional_nanoseconds(std::string& input, size_t fraction_begin,
                                        size_t fraction_end) {
    if (fraction_begin == std::string::npos) {
        return {};
    }

    const size_t fraction_length = fraction_end - fraction_begin;
    const size_t copied_digits =
            std::min<size_t>(fraction_length, TimeStampNsValue::FRACTIONAL_DIGITS);
    uint32_t nanos = 0;
    for (size_t i = 0; i < copied_digits; ++i) {
        nanos = nanos * 10 + static_cast<uint32_t>(input[fraction_begin + i] - '0');
    }
    for (size_t i = copied_digits; i < TimeStampNsValue::FRACTIONAL_DIGITS; ++i) {
        nanos *= 10;
    }
    if (fraction_length > TimeStampNsValue::FRACTIONAL_DIGITS &&
        input[fraction_begin + TimeStampNsValue::FRACTIONAL_DIGITS] >= '5') {
        ++nanos;
    }

    const bool carry_second = nanos == TimeStampNsValue::NANOS_PER_SECOND;
    // Keep the fractional token in place so the DATETIMEV2 parser validates its position and
    // trailing syntax. Its scale-0 rounding performs the nanosecond carry before timezone
    // conversion, so a DST transition is crossed on the source instant timeline.
    std::fill(input.begin() + fraction_begin, input.begin() + fraction_end, '0');
    if (carry_second) {
        input[fraction_begin] = '5';
    }
    return carry_second ? 0 : nanos;
}

template <DatelikeParseMode ParseMode>
Status parse_timestamp_ns_impl(StringRef str, int64_t& epoch_nanos,
                               const cctz::time_zone* local_time_zone) {
    constexpr bool IsStrict = is_datelike_parse_strict(ParseMode);
    std::string input(str.data, str.size);
    size_t fraction_begin = std::string::npos;
    size_t fraction_end = std::string::npos;
    if constexpr (!IsStrict) {
        const auto range = find_non_strict_fraction(input);
        if (range.recognized) {
            fraction_begin = range.begin;
            fraction_end = range.end;
        } else {
            std::tie(fraction_begin, fraction_end) = find_strict_fraction(input);
        }
    } else {
        std::tie(fraction_begin, fraction_end) = find_strict_fraction(input);
    }

    const uint32_t nanos = extract_fractional_nanoseconds(input, fraction_begin, fraction_end);
    const StringRef input_ref(input.data(), input.size());
    DateV2Value<DateTimeV2ValueType> datetime;
    CastParameters params {.status = Status::OK(), .is_strict = IsStrict};
    bool parsed = false;
    if constexpr (IsStrict) {
        parsed = CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                input_ref, datetime, local_time_zone, 0, params);
    } else {
        parsed = CastToDatetimeV2::from_string_non_strict_mode(input_ref, datetime, local_time_zone,
                                                               0, params);
    }
    if (!parsed) {
        if (!params.status.ok() && fraction_begin == std::string::npos) {
            return params.status;
        }
        return Status::InvalidArgument("Invalid TIMESTAMP_NS value '{}'",
                                       std::string(str.data, str.size));
    }

    datetime.set_microsecond(nanos / TimeStampNsValue::NANOS_PER_MICROSECOND);
    TimeStampNsValue value;
    if (!value.from_datetime(
                datetime, static_cast<uint16_t>(nanos % TimeStampNsValue::NANOS_PER_MICROSECOND))) {
        return Status::InvalidArgument(
                "TIMESTAMP_NS value '{}' is outside [{}, {}]", std::string(str.data, str.size),
                TimeStampNsValue(std::numeric_limits<int64_t>::min()).to_string(),
                TimeStampNsValue(std::numeric_limits<int64_t>::max()).to_string());
    }
    epoch_nanos = value.epoch_nanos();
    return Status::OK();
}

Status parse_timestamp_ns_non_strict(StringRef str, int64_t* epoch_nanos,
                                     const cctz::time_zone* local_time_zone) {
    return parse_timestamp_ns_impl<DatelikeParseMode::NON_STRICT>(str, *epoch_nanos,
                                                                  local_time_zone);
}

} // namespace

Status parse_timestamp_ns(StringRef str, int64_t* epoch_nanos,
                          const cctz::time_zone* local_time_zone) {
    return parse_timestamp_ns_impl<DatelikeParseMode::STRICT>(str, *epoch_nanos, local_time_zone);
}

Status DataTypeTimeStampNsSerDe::from_string_batch(const ColumnString& strings,
                                                   ColumnNullable& result,
                                                   const FormatOptions& options) const {
    auto& data = assert_cast<ColumnTimeStampNs&>(result.get_nested_column()).get_data();
    auto& null_map = result.get_null_map_column().get_data();
    result.resize(strings.size());
    for (size_t i = 0; i < strings.size(); ++i) {
        int64_t value = 0;
        const auto status =
                parse_timestamp_ns_non_strict(strings.get_data_at(i), &value, options.timezone);
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
    RETURN_IF_ERROR(parse_timestamp_ns_non_strict(str, &value, options.timezone));
    assert_cast<ColumnTimeStampNs&>(column).insert_value(TimeStampNsValue(value));
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                         const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_timestamp_ns(str, &value, options.timezone));
    assert_cast<ColumnTimeStampNs&>(column).insert_value(TimeStampNsValue(value));
    return Status::OK();
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
    const std::string result = value.to_string();
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
                                                       const cctz::time_zone&) const {
    const auto& data = assert_cast<const ColumnTimeStampNs&>(column).get_data();
    auto& builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    const auto timestamp_type =
            std::static_pointer_cast<arrow::TimestampType>(array_builder->type());
    int64_t nanos_per_unit = 0;
    RETURN_IF_ERROR(get_nanos_per_arrow_timestamp_unit(timestamp_type->unit(), &nanos_per_unit));

    for (int64_t i = start; i < end; ++i) {
        if (null_map != nullptr && (*null_map)[i]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }

        const int64_t epoch_nanos = data[i].epoch_nanos();
        // The default Arrow schema uses NANO. If a caller supplies a coarser timestamp schema,
        // accept it only when conversion is exact instead of silently discarding nanoseconds.
        if (epoch_nanos % nanos_per_unit != 0) {
            return Status::InvalidArgument(
                    "TIMESTAMP_NS value {} cannot be represented exactly as Arrow timestamp unit "
                    "{}",
                    epoch_nanos, static_cast<int>(timestamp_type->unit()));
        }
        RETURN_IF_ERROR(
                checkArrowStatus(builder.Append(epoch_nanos / nanos_per_unit), column, builder));
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::read_column_from_arrow(IColumn& column,
                                                        const arrow::Array* arrow_array,
                                                        int64_t start, int64_t end,
                                                        const cctz::time_zone&) const {
    if (config::enable_arrow_input_validation) {
        check_arrow_array_range(*arrow_array, start, end);
    }
    if (arrow_array->type_id() != arrow::Type::TIMESTAMP) {
        return Status::InvalidArgument("Cannot convert Arrow type {} to TIMESTAMP_NS",
                                       arrow_array->type()->name());
    }

    const auto* timestamp_array = assert_cast<const arrow::TimestampArray*>(arrow_array);
    if (config::enable_arrow_input_validation) {
        check_arrow_fixed_width_buffer(*timestamp_array, sizeof(arrow::TimestampArray::value_type));
    }
    const auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
    int64_t nanos_per_unit = 0;
    RETURN_IF_ERROR(get_nanos_per_arrow_timestamp_unit(timestamp_type->unit(), &nanos_per_unit));

    auto& data = assert_cast<ColumnTimeStampNs&>(column).get_data();
    const auto* raw_values = reinterpret_cast<const uint8_t*>(timestamp_array->raw_values());
    for (int64_t i = start; i < end; ++i) {
        // Nullable SerDe has already copied the validity bitmap. Avoid converting the unspecified
        // payload of a null Arrow slot, which could otherwise produce a spurious overflow error.
        if (timestamp_array->IsNull(i)) {
            data.emplace_back();
            continue;
        }

        const int64_t value = unaligned_load<int64_t>(raw_values + i * sizeof(int64_t));
        int64_t epoch_nanos = 0;
        if (__builtin_mul_overflow(value, nanos_per_unit, &epoch_nanos)) {
            return Status::DataQualityError(
                    "Arrow timestamp {} in unit {} is outside the TIMESTAMP_NS range", value,
                    static_cast<int>(timestamp_type->unit()));
        }
        data.emplace_back(epoch_nanos);
    }
    return Status::OK();
}

Status DataTypeTimeStampNsSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                              MysqlRowBinaryBuffer& row_buffer,
                                                              int64_t row_idx, bool col_const,
                                                              const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeStampNs&>(column).get_data();
    const auto index = index_check_const(row_idx, col_const);
    const auto value = TimeStampNsValue(data[index]).to_string();
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
    return field.get<TYPE_TIMESTAMP_NS>().to_string();
}

Status DataTypeTimeStampNsSerDe::from_olap_string(const std::string& str, Field& field,
                                                  const FormatOptions& options) const {
    int64_t value = 0;
    RETURN_IF_ERROR(parse_timestamp_ns(StringRef(str.data(), str.size()), &value));
    field = Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(value));
    return Status::OK();
}

} // namespace doris
