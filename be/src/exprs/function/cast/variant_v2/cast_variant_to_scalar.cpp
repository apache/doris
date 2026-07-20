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

#include <cctz/time_zone.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function_context.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

constexpr size_t DECIMAL_SCALE_COUNT = 39;

enum GroupIndex : size_t {
    INVALID_GROUP = 0,
    BOOL_GROUP = INVALID_GROUP + 1,
    INT_GROUP = BOOL_GROUP + 1,
    FLOAT_GROUP = INT_GROUP + 1,
    DOUBLE_GROUP = FLOAT_GROUP + 1,
    DECIMAL_GROUP_BEGIN = DOUBLE_GROUP + 1,
    DATE_GROUP = DECIMAL_GROUP_BEGIN + DECIMAL_SCALE_COUNT,
    TIMESTAMP_NTZ_GROUP = DATE_GROUP + 1,
    TIMESTAMP_TZ_GROUP = TIMESTAMP_NTZ_GROUP + 1,
    STRING_GROUP = TIMESTAMP_TZ_GROUP + 1,
    GROUP_COUNT = STRING_GROUP + 1,
};

struct ScalarGroup {
    DataTypePtr type;
    MutableColumnPtr values;
    DorisVector<size_t> source_rows;
};

using ScalarGroups = std::array<ScalarGroup, GROUP_COUNT>;

ScalarGroup& initialize_group(ScalarGroups& groups, size_t index, DataTypePtr type) {
    ScalarGroup& group = groups[index];
    if (!group.values) {
        group.type = std::move(type);
        group.values = group.type->create_column();
    }
    return group;
}

void append_invalid(ScalarGroups& groups, size_t row) {
    groups[INVALID_GROUP].source_rows.push_back(row);
}

std::pair<int64_t, uint32_t> split_epoch_micros(int64_t micros) {
    constexpr int64_t MICROS_PER_SECOND = 1'000'000;
    int64_t seconds = micros / MICROS_PER_SECOND;
    int64_t fraction = micros % MICROS_PER_SECOND;
    if (fraction < 0) {
        --seconds;
        fraction += MICROS_PER_SECOND;
    }
    return {seconds, static_cast<uint32_t>(fraction)};
}

int64_t nanos_to_micros(int64_t nanos) {
    int64_t micros = nanos / 1000;
    if (nanos % 1000 < 0) {
        --micros;
    }
    return micros;
}

bool epoch_micros_to_civil(int64_t micros, cctz::civil_second* civil, uint32_t* fraction) {
    const auto [seconds, micros_fraction] = split_epoch_micros(micros);
    const auto lookup =
            cctz::utc_time_zone().lookup(cctz::time_point<cctz::seconds>(cctz::seconds(seconds)));
    if (lookup.cs.year() < 1 || lookup.cs.year() > 9999) {
        return false;
    }
    *civil = lookup.cs;
    *fraction = micros_fraction;
    return true;
}

void append_bool(ScalarGroups& groups, size_t row, bool value) {
    auto& group = initialize_group(groups, BOOL_GROUP, std::make_shared<DataTypeBool>());
    assert_cast<ColumnUInt8&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_int(ScalarGroups& groups, size_t row, int64_t value) {
    auto& group = initialize_group(groups, INT_GROUP, std::make_shared<DataTypeInt64>());
    assert_cast<ColumnInt64&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_float(ScalarGroups& groups, size_t row, float value) {
    auto& group = initialize_group(groups, FLOAT_GROUP, std::make_shared<DataTypeFloat32>());
    assert_cast<ColumnFloat32&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_double(ScalarGroups& groups, size_t row, double value) {
    auto& group = initialize_group(groups, DOUBLE_GROUP, std::make_shared<DataTypeFloat64>());
    assert_cast<ColumnFloat64&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_decimal(ScalarGroups& groups, size_t row, VariantDecimal value) {
    if (value.scale >= DECIMAL_SCALE_COUNT) {
        append_invalid(groups, row);
        return;
    }
    const size_t index = DECIMAL_GROUP_BEGIN + value.scale;
    auto& group =
            initialize_group(groups, index, std::make_shared<DataTypeDecimal128>(38, value.scale));
    assert_cast<ColumnDecimal128V3&>(*group.values).insert_value(Decimal128V3 {value.unscaled});
    group.source_rows.push_back(row);
}

void append_date(ScalarGroups& groups, size_t row, int32_t days_since_epoch) {
    const cctz::civil_day civil = cctz::civil_day(1970, 1, 1) + days_since_epoch;
    if (civil.year() < 1 || civil.year() > 9999) {
        append_invalid(groups, row);
        return;
    }
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(static_cast<uint16_t>(civil.year()),
                             static_cast<uint8_t>(civil.month()), static_cast<uint8_t>(civil.day()),
                             0, 0, 0);
    auto& group = initialize_group(groups, DATE_GROUP, std::make_shared<DataTypeDateV2>());
    assert_cast<ColumnDateV2&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_timestamp(ScalarGroups& groups, size_t row, int64_t micros, bool utc_adjusted) {
    cctz::civil_second civil;
    uint32_t fraction = 0;
    if (!epoch_micros_to_civil(micros, &civil, &fraction)) {
        append_invalid(groups, row);
        return;
    }
    if (utc_adjusted) {
        TimestampTzValue value;
        value.unchecked_set_time(
                static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
                static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
                static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()),
                fraction);
        auto& group = initialize_group(groups, TIMESTAMP_TZ_GROUP,
                                       std::make_shared<DataTypeTimeStampTz>(6));
        assert_cast<ColumnTimeStampTz&>(*group.values).insert_value(value);
        group.source_rows.push_back(row);
        return;
    }
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(
            static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
            static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
            static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()), fraction);
    auto& group =
            initialize_group(groups, TIMESTAMP_NTZ_GROUP, std::make_shared<DataTypeDateTimeV2>(6));
    assert_cast<ColumnDateTimeV2&>(*group.values).insert_value(value);
    group.source_rows.push_back(row);
}

void append_string(ScalarGroups& groups, size_t row, StringRef value) {
    auto& group = initialize_group(groups, STRING_GROUP, std::make_shared<DataTypeString>());
    assert_cast<ColumnString&>(*group.values).insert_data(value.data, value.size);
    group.source_rows.push_back(row);
}

void classify_value(ScalarGroups& groups, size_t row, VariantRef value, bool forced_null) {
    if (forced_null) {
        append_invalid(groups, row);
        return;
    }
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        append_string(groups, row, value.get_string());
        return;
    case VariantBasicType::OBJECT:
    case VariantBasicType::ARRAY:
        append_invalid(groups, row);
        return;
    case VariantBasicType::PRIMITIVE:
        break;
    }

    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::UUID:
        append_invalid(groups, row);
        return;
    case VariantPrimitiveId::TRUE_VALUE:
        append_bool(groups, row, true);
        return;
    case VariantPrimitiveId::FALSE_VALUE:
        append_bool(groups, row, false);
        return;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        append_int(groups, row, value.get_int());
        return;
    case VariantPrimitiveId::FLOAT:
        append_float(groups, row, value.get_float());
        return;
    case VariantPrimitiveId::DOUBLE:
        append_double(groups, row, value.get_double());
        return;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        append_decimal(groups, row, value.get_decimal());
        return;
    case VariantPrimitiveId::DATE:
        append_date(groups, row, value.get_date());
        return;
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        append_timestamp(groups, row, value.get_timestamp_micros(), true);
        return;
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        append_timestamp(groups, row, value.get_timestamp_ntz_micros(), false);
        return;
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        append_timestamp(groups, row, nanos_to_micros(value.get_timestamp_nanos()), true);
        return;
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        append_timestamp(groups, row, nanos_to_micros(value.get_timestamp_ntz_nanos()), false);
        return;
    case VariantPrimitiveId::STRING:
        append_string(groups, row, value.get_string());
        return;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant primitive id");
}

ColumnPtr as_nullable(ColumnPtr column, size_t rows) {
    if (check_and_get_column<ColumnNullable>(column.get()) != nullptr) {
        return column;
    }
    return ColumnNullable::create(column, ColumnUInt8::create(rows, 0));
}

Status execute_concrete_cast(FunctionContext* context, const ScalarGroup& group,
                             const DataTypePtr& target_type, ColumnPtr* output) {
    if (context == nullptr) {
        return Status::InvalidArgument("Variant V2 scalar CAST requires a FunctionContext");
    }
    auto cast_context = context->clone();
    cast_context->set_enable_strict_mode(false);
    const DataTypePtr nullable_target = make_nullable(target_type);
    Block temporary {{group.values->get_ptr(), group.type, "variant-v2-group"},
                     {nullable_target->create_column(), nullable_target, "variant-v2-result"}};
    WrapperType wrapper =
            prepare_unpack_dictionaries(cast_context.get(), group.type, nullable_target);
    Status status =
            wrapper(cast_context.get(), temporary, {0}, 1, group.source_rows.size(), nullptr);
    if (!status.ok()) {
        if (status.is<ErrorCode::INVALID_ARGUMENT>()) {
            *output = make_all_null_column(target_type, group.source_rows.size());
            return Status::OK();
        }
        return status;
    }
    *output = as_nullable(temporary.get_by_position(1).column, group.source_rows.size());
    return Status::OK();
}

Status assemble_groups(FunctionContext* context, const ScalarGroups& groups,
                       const DataTypePtr& target_type, size_t rows, ColumnPtr* output) {
    MutableColumnPtr nested = target_type->create_column();
    auto nulls = ColumnUInt8::create();
    nested->reserve(rows);
    nulls->reserve(rows);
    IColumn::Permutation permutation(rows);
    size_t concatenated_row = 0;

    for (size_t index = 0; index < groups.size(); ++index) {
        const ScalarGroup& group = groups[index];
        if (group.source_rows.empty()) {
            continue;
        }
        ColumnPtr cast_result;
        if (index == INVALID_GROUP) {
            cast_result = make_all_null_column(target_type, group.source_rows.size());
        } else {
            RETURN_IF_ERROR(execute_concrete_cast(context, group, target_type, &cast_result));
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*cast_result);
        nested->insert_range_from(nullable.get_nested_column(), 0, group.source_rows.size());
        nulls->insert_range_from(nullable.get_null_map_column(), 0, group.source_rows.size());
        for (size_t source_row : group.source_rows) {
            permutation[source_row] = concatenated_row++;
        }
    }
    if (concatenated_row != rows) {
        return Status::InternalError("Variant V2 scalar grouping produced {} rows, expected {}",
                                     concatenated_row, rows);
    }
    ColumnPtr concatenated = ColumnNullable::create(std::move(nested), std::move(nulls));
    *output = concatenated->permute(permutation, rows);
    return Status::OK();
}

Status execute_typed_cast(FunctionContext* context, const ColumnPtr& source,
                          const DataTypePtr& source_type, const DataTypePtr& target_type,
                          size_t rows, ColumnPtr* output) {
    if (context == nullptr) {
        return Status::InvalidArgument("Variant V2 scalar CAST requires a FunctionContext");
    }
    auto cast_context = context->clone();
    cast_context->set_enable_strict_mode(false);
    const DataTypePtr nullable_source = make_nullable(source_type);
    const DataTypePtr nullable_target = make_nullable(target_type);
    Block temporary {{source, nullable_source, "variant-v2-typed"},
                     {nullable_target->create_column(), nullable_target, "variant-v2-result"}};
    WrapperType wrapper =
            prepare_unpack_dictionaries(cast_context.get(), nullable_source, nullable_target);
    Status status = wrapper(cast_context.get(), temporary, {0}, 1, rows, nullptr);
    if (!status.ok()) {
        if (status.is<ErrorCode::INVALID_ARGUMENT>()) {
            *output = make_all_null_column(target_type, rows);
            return Status::OK();
        }
        return status;
    }
    *output = as_nullable(temporary.get_by_position(1).column, rows);
    return Status::OK();
}

} // namespace

bool is_supported_scalar_source(const DataTypePtr& type) {
    switch (remove_nullable(type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return true;
    default:
        return false;
    }
}

bool is_supported_scalar_target(const DataTypePtr& type) {
    switch (remove_nullable(type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
        return true;
    default:
        return false;
    }
}

Status cast_scalar_to_variant(const ColumnPtr& source, const DataTypePtr& source_type, size_t rows,
                              ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!source || source->size() != rows ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid scalar input shape for Variant V2 CAST");
    }
    MutableColumnPtr nested = IColumn::mutate(source);
    auto nulls = ColumnUInt8::create(rows, 0);
    if (!forced_nulls.empty()) {
        std::ranges::copy(forced_nulls, nulls->get_data().begin());
    }
    ColumnPtr nullable = ColumnNullable::create(std::move(nested), std::move(nulls));
    *output = ColumnVariantV2::create_typed(std::move(nullable), source_type);
    return Status::OK();
}

Status cast_typed_variant_to_scalar(FunctionContext* context, const ColumnVariantV2& source,
                                    const DataTypePtr& target_type, size_t rows,
                                    ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!source.is_typed() || source.size() != rows) {
        return Status::InvalidArgument("Expected a typed Variant V2 source with {} rows", rows);
    }
    ColumnPtr converted;
    RETURN_IF_ERROR(execute_typed_cast(context, source.typed_column().get_ptr(),
                                       source.typed_type(), target_type, rows, &converted));
    return apply_forced_nulls(std::move(converted), forced_nulls, output);
}

Status cast_variant_refs_to_scalar(FunctionContext* context, std::span<const VariantRef> values,
                                   const DataTypePtr& target_type, ForcedNulls forced_nulls,
                                   ColumnPtr* output) {
    if (!forced_nulls.empty() && forced_nulls.size() != values.size()) {
        return Status::InvalidArgument("Variant V2 CAST null map has {} rows, expected {}",
                                       forced_nulls.size(), values.size());
    }
    ScalarGroups groups;
    for (size_t row = 0; row < values.size(); ++row) {
        classify_value(groups, row, values[row], !forced_nulls.empty() && forced_nulls[row] != 0);
    }
    return assemble_groups(context, groups, target_type, values.size(), output);
}

ColumnPtr make_all_null_column(const DataTypePtr& nested_type, size_t rows) {
    const DataTypePtr concrete_type = remove_nullable(nested_type);
    MutableColumnPtr nested = concrete_type->create_column();
    nested->insert_many_defaults(rows);
    return ColumnNullable::create(std::move(nested), ColumnUInt8::create(rows, 1));
}

Status apply_forced_nulls(ColumnPtr column, ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!column) {
        return Status::InvalidArgument("Cannot apply a null map to an empty Variant V2 result");
    }
    if (forced_nulls.empty()) {
        *output = std::move(column);
        return Status::OK();
    }
    if (forced_nulls.size() != column->size()) {
        return Status::InvalidArgument("Variant V2 CAST null map has {} rows, expected {}",
                                       forced_nulls.size(), column->size());
    }
    auto nulls = ColumnUInt8::create(column->size(), 0);
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        const NullMap& existing = nullable->get_null_map_data();
        for (size_t row = 0; row < column->size(); ++row) {
            nulls->get_data()[row] = existing[row] | forced_nulls[row];
        }
        *output = ColumnNullable::create(nullable->get_nested_column_ptr(), std::move(nulls));
        return Status::OK();
    }
    std::ranges::copy(forced_nulls, nulls->get_data().begin());
    *output = ColumnNullable::create(column, std::move(nulls));
    return Status::OK();
}

Status clone_as_encoded(const ColumnVariantV2& source, ColumnPtr* output) {
    MutableColumnPtr detached = source.clone_resized(source.size());
    auto& variant = assert_cast<ColumnVariantV2&>(*detached);
    variant.ensure_encoded();
    *output = std::move(detached);
    return Status::OK();
}

} // namespace doris::CastWrapper::variant_v2_internal
