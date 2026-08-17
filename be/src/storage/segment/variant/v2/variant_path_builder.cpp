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

#include "storage/segment/variant/v2/variant_path_builder.h"

#include <cctz/time_zone.h>

#include <algorithm>
#include <array>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/get_least_supertype.h"
#include "core/data_type/primitive_type.h"
#include "core/typeid_cast.h"
#include "core/value/timestamp_ns_value.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris::segment_v2 {
namespace {

enum class ValueKind : uint8_t {
    NULL_VALUE,
    BOOL,
    INT64,
    LARGEINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIMESTAMP_NTZ,
    TIMESTAMP_NTZ_NANOS,
    TIMESTAMP_TZ,
    STRING,
    JSONB_REF,
    ARRAY,
};

const DataTypePtr& jsonb_type() {
    static const DataTypePtr type = std::make_shared<DataTypeJsonb>();
    return type;
}

const DataTypePtr& nothing_type() {
    static const DataTypePtr type = std::make_shared<DataTypeNothing>();
    return type;
}

DataTypePtr path_least_common_type(const DataTypePtr& left, const DataTypePtr& right);

bool date_fits_doris_range(int32_t days) {
    const cctz::civil_day civil = cctz::civil_day(1970, 1, 1) + days;
    return civil.year() >= 1 && civil.year() <= 9999;
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

bool timestamp_fits_doris_range(int64_t micros) {
    cctz::civil_second civil;
    uint32_t fraction = 0;
    return epoch_micros_to_civil(micros, &civil, &fraction);
}

// Keep the exhaustive primitive-id mapping in one switch so newly added ids cannot bypass the
// explicit storage fallback. This descriptor is stack-only; it never owns decoded values.
ValueKind value_kind(VariantRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return ValueKind::STRING;
    case VariantBasicType::OBJECT:
        return ValueKind::JSONB_REF;
    case VariantBasicType::ARRAY:
        return ValueKind::ARRAY;
    case VariantBasicType::PRIMITIVE:
        break;
    }
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return ValueKind::NULL_VALUE;
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return ValueKind::BOOL;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return ValueKind::INT64;
    case VariantPrimitiveId::FLOAT:
        return ValueKind::FLOAT;
    case VariantPrimitiveId::DOUBLE:
        return ValueKind::DOUBLE;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        const VariantDecimal decimal = value.get_decimal();
        if (decimal.width == 16 && decimal.scale == 0) {
            return ValueKind::LARGEINT;
        }
        return ValueKind::DECIMAL;
    }
    case VariantPrimitiveId::STRING:
        return ValueKind::STRING;
    case VariantPrimitiveId::DATE:
        return ValueKind::DATE;
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        return ValueKind::TIMESTAMP_TZ;
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        return ValueKind::TIMESTAMP_NTZ;
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return ValueKind::TIMESTAMP_NTZ_NANOS;
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::UUID:
        return ValueKind::JSONB_REF;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant primitive id");
}

const DataTypePtr& cached_decimal_type(uint32_t scale) {
    static const std::array<DataTypePtr, 39> types = [] {
        std::array<DataTypePtr, 39> result;
        for (uint32_t current_scale = 0; current_scale < result.size(); ++current_scale) {
            result[current_scale] = std::make_shared<DataTypeDecimal128>(38, current_scale);
        }
        return result;
    }();
    DORIS_CHECK_LT(scale, types.size());
    return types[scale];
}

DataTypePtr infer_type(VariantRef value, const DataTypePtr& reusable_type = nullptr) {
    const ValueKind kind = value_kind(value);
    switch (kind) {
    case ValueKind::NULL_VALUE:
        return nothing_type();
    case ValueKind::BOOL: {
        static const DataTypePtr type = std::make_shared<DataTypeBool>();
        return type;
    }
    case ValueKind::INT64: {
        PrimitiveType primitive = TYPE_BIGINT;
        switch (value.primitive_id()) {
        case VariantPrimitiveId::INT8:
            primitive = TYPE_TINYINT;
            break;
        case VariantPrimitiveId::INT16:
            primitive = TYPE_SMALLINT;
            break;
        case VariantPrimitiveId::INT32:
            primitive = TYPE_INT;
            break;
        case VariantPrimitiveId::INT64:
            break;
        default:
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant integer primitive id");
        }
        static const std::array<DataTypePtr, 4> types {
                std::make_shared<DataTypeInt8>(), std::make_shared<DataTypeInt16>(),
                std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()};
        switch (primitive) {
        case TYPE_TINYINT:
            return types[0];
        case TYPE_SMALLINT:
            return types[1];
        case TYPE_INT:
            return types[2];
        case TYPE_BIGINT:
            return types[3];
        default:
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant integer type {}", primitive);
        }
    }
    case ValueKind::LARGEINT: {
        static const DataTypePtr type = std::make_shared<DataTypeInt128>();
        return type;
    }
    case ValueKind::FLOAT: {
        static const DataTypePtr type = std::make_shared<DataTypeFloat32>();
        return type;
    }
    case ValueKind::DOUBLE: {
        static const DataTypePtr type = std::make_shared<DataTypeFloat64>();
        return type;
    }
    case ValueKind::DECIMAL:
        return cached_decimal_type(value.get_decimal().scale);
    case ValueKind::DATE: {
        if (!date_fits_doris_range(value.get_date())) {
            return jsonb_type();
        }
        static const DataTypePtr type = std::make_shared<DataTypeDateV2>();
        return type;
    }
    case ValueKind::TIMESTAMP_NTZ: {
        if (!timestamp_fits_doris_range(value.get_timestamp_ntz_micros())) {
            return jsonb_type();
        }
        static const DataTypePtr type = std::make_shared<DataTypeDateTimeV2>(6);
        return type;
    }
    case ValueKind::TIMESTAMP_NTZ_NANOS: {
        static const DataTypePtr type = std::make_shared<DataTypeTimeStampNs>();
        return type;
    }
    case ValueKind::TIMESTAMP_TZ: {
        if (!timestamp_fits_doris_range(value.get_timestamp_micros())) {
            return jsonb_type();
        }
        static const DataTypePtr type = std::make_shared<DataTypeTimeStampTz>(6);
        return type;
    }
    case ValueKind::STRING: {
        static const DataTypePtr type = std::make_shared<DataTypeString>();
        return type;
    }
    case ValueKind::JSONB_REF:
        return jsonb_type();
    case ValueKind::ARRAY:
        break;
    }

    DataTypePtr element_type;
    const uint32_t element_count = value.num_elements();
    for (uint32_t index = 0; index < element_count; ++index) {
        const VariantRef element = value.array_at(index);
        const ValueKind element_kind = value_kind(element);
        if (element_kind == ValueKind::ARRAY ||
            (element_kind == ValueKind::JSONB_REF &&
             element.basic_type() == VariantBasicType::OBJECT)) {
            return jsonb_type();
        }
        DataTypePtr inferred = infer_type(element);
        if (inferred->get_primitive_type() == INVALID_TYPE) {
            continue;
        }
        element_type = element_type == nullptr ? std::move(inferred)
                                               : path_least_common_type(element_type, inferred);
    }

    if (element_type == nullptr) {
        element_type = nothing_type();
    }

    // A path commonly sees the same ARRAY element type on every row. Reuse the builder's
    // DataTypeArray in that case instead of allocating a temporary shared_ptr per value. The
    // comparison against the common type also covers rows containing only narrower values or
    // JSON nulls after the path has already promoted.
    if (const auto* reusable_array =
                reusable_type == nullptr ? nullptr
                                         : typeid_cast<const DataTypeArray*>(reusable_type.get())) {
        const DataTypePtr& reusable_element = reusable_array->get_nested_type();
        if (reusable_element.get() == element_type.get() ||
            reusable_element->equals(*element_type)) {
            return reusable_type;
        }
        DataTypePtr common_element = path_least_common_type(reusable_element, element_type);
        if (reusable_element->equals(*common_element)) {
            return reusable_type;
        }
    }
    return std::make_shared<DataTypeArray>(element_type);
}

bool is_small_or_regular_integer(PrimitiveType type) {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT || type == TYPE_BIGINT;
}

size_t array_dimensions(const DataTypePtr& type) {
    size_t dimensions = 0;
    DataTypePtr current = remove_nullable(type);
    while (const auto* array = typeid_cast<const DataTypeArray*>(current.get())) {
        ++dimensions;
        current = remove_nullable(array->get_nested_type());
    }
    return dimensions;
}

DataTypePtr path_least_common_type(const DataTypePtr& left, const DataTypePtr& right) {
    if (left.get() == right.get() || left->equals(*right)) {
        return left;
    }
    const auto* left_array = typeid_cast<const DataTypeArray*>(left.get());
    const auto* right_array = typeid_cast<const DataTypeArray*>(right.get());
    if (left_array != nullptr || right_array != nullptr) {
        if (left_array == nullptr || right_array == nullptr) {
            return jsonb_type();
        }
        if (array_dimensions(left) != array_dimensions(right)) {
            return jsonb_type();
        }
        return std::make_shared<DataTypeArray>(path_least_common_type(
                left_array->get_nested_type(), right_array->get_nested_type()));
    }
    const PrimitiveType left_primitive = left->get_primitive_type();
    const PrimitiveType right_primitive = right->get_primitive_type();
    if ((left_primitive == TYPE_TIMESTAMP_NS && right_primitive == TYPE_DATETIMEV2) ||
        (left_primitive == TYPE_DATETIMEV2 && right_primitive == TYPE_TIMESTAMP_NS)) {
        return left_primitive == TYPE_TIMESTAMP_NS ? left : right;
    }
    const bool left_decimal = left_primitive == TYPE_DECIMAL128I;
    const bool right_decimal = right_primitive == TYPE_DECIMAL128I;
    // get_least_supertype_jsonb() treats equal primitive ids as the same type. Decimal128(38, 2)
    // and Decimal128(38, 4) therefore make its result depend on input order, even though the scale
    // 4 representation is the lossless common type. Resolve Decimal and ARRAY metadata here, then
    // delegate ordinary scalar promotion to the shared helper below.
    if (left_decimal && right_decimal) {
        return left->get_scale() >= right->get_scale() ? left : right;
    }
    if ((left_decimal && is_small_or_regular_integer(right_primitive)) ||
        (right_decimal && is_small_or_regular_integer(left_primitive))) {
        const DataTypePtr& decimal = left_decimal ? left : right;
        if (decimal->get_scale() <= 19) {
            return decimal;
        }
        return jsonb_type();
    }
    DataTypePtr result;
    get_least_supertype_jsonb(DataTypes {left, right}, &result);
    return result ? result : jsonb_type();
}

bool rescale_decimal(__int128 source, uint32_t source_scale, uint32_t target_scale,
                     __int128* result) {
    if (source_scale == target_scale) {
        *result = source;
        return true;
    }
    if (source_scale < target_scale) {
        __int128 value = source;
        for (uint32_t scale = source_scale; scale < target_scale; ++scale) {
            if (__builtin_mul_overflow(value, static_cast<__int128>(10), &value)) {
                return false;
            }
        }
        *result = value;
        return true;
    }
    __int128 divisor = 1;
    for (uint32_t scale = target_scale; scale < source_scale; ++scale) {
        if (__builtin_mul_overflow(divisor, static_cast<__int128>(10), &divisor)) {
            return false;
        }
    }
    if (source % divisor != 0) {
        return false;
    }
    *result = source / divisor;
    return true;
}

bool try_rescale_decimal_value(VariantRef value, const DataTypePtr& target_type, __int128* result) {
    const ValueKind kind = value_kind(value);
    if (kind != ValueKind::DECIMAL && kind != ValueKind::INT64) {
        return false;
    }
    const VariantDecimal decimal =
            kind == ValueKind::DECIMAL ? value.get_decimal() : VariantDecimal {};
    const uint32_t source_scale = kind == ValueKind::DECIMAL ? decimal.scale : 0;
    const __int128 source_value =
            kind == ValueKind::DECIMAL ? decimal.unscaled : static_cast<__int128>(value.get_int());
    if (!rescale_decimal(source_value, source_scale, target_type->get_scale(), result)) {
        return false;
    }
    const __int128 max_value =
            DataTypeDecimal128::get_max_digits_number(target_type->get_precision());
    return *result >= -max_value && *result <= max_value;
}

bool value_is_representable(VariantRef value, const DataTypePtr& target_type) {
    const ValueKind kind = value_kind(value);
    switch (target_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return kind == ValueKind::BOOL;
    case TYPE_TINYINT:
        return kind == ValueKind::INT64 && value.get_int() >= std::numeric_limits<int8_t>::min() &&
               value.get_int() <= std::numeric_limits<int8_t>::max();
    case TYPE_SMALLINT:
        return kind == ValueKind::INT64 && value.get_int() >= std::numeric_limits<int16_t>::min() &&
               value.get_int() <= std::numeric_limits<int16_t>::max();
    case TYPE_INT:
        return kind == ValueKind::INT64 && value.get_int() >= std::numeric_limits<int32_t>::min() &&
               value.get_int() <= std::numeric_limits<int32_t>::max();
    case TYPE_BIGINT:
        return kind == ValueKind::INT64;
    case TYPE_LARGEINT:
        return kind == ValueKind::LARGEINT || kind == ValueKind::INT64;
    case TYPE_FLOAT:
        return kind == ValueKind::FLOAT;
    case TYPE_DOUBLE:
        return kind == ValueKind::DOUBLE || kind == ValueKind::FLOAT || kind == ValueKind::INT64;
    case TYPE_DECIMAL128I: {
        __int128 converted = 0;
        return try_rescale_decimal_value(value, target_type, &converted);
    }
    case TYPE_DATEV2:
        return kind == ValueKind::DATE && date_fits_doris_range(value.get_date());
    case TYPE_DATETIMEV2:
        return kind == ValueKind::TIMESTAMP_NTZ &&
               timestamp_fits_doris_range(value.get_timestamp_ntz_micros());
    case TYPE_TIMESTAMP_NS: {
        if (kind == ValueKind::TIMESTAMP_NTZ_NANOS) {
            return true;
        }
        int64_t nanos = 0;
        return kind == ValueKind::TIMESTAMP_NTZ &&
               !__builtin_mul_overflow(value.get_timestamp_ntz_micros(),
                                       TimeStampNsValue::NANOS_PER_MICROSECOND, &nanos);
    }
    case TYPE_TIMESTAMPTZ:
        return kind == ValueKind::TIMESTAMP_TZ &&
               timestamp_fits_doris_range(value.get_timestamp_micros());
    case TYPE_STRING:
        return kind == ValueKind::STRING;
    case TYPE_JSONB:
        return true;
    case TYPE_ARRAY: {
        if (kind != ValueKind::ARRAY) {
            return false;
        }
        const DataTypePtr element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*target_type).get_nested_type());
        const uint32_t count = value.num_elements();
        for (uint32_t index = 0; index < count; ++index) {
            const VariantRef element = value.array_at(index);
            if (!element.is_null() && !value_is_representable(element, element_type)) {
                return false;
            }
        }
        return true;
    }
    case INVALID_TYPE:
        return kind == ValueKind::NULL_VALUE;
    default:
        return false;
    }
}

void require_jsonb_write(bool ok, std::string_view description) {
    if (!ok) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Failed to write {} to JSONB", description);
    }
}

void write_path_jsonb(VariantRef value, JsonbWriter* writer) {
    switch (value_kind(value)) {
    case ValueKind::INT64:
        // Preserve the previous path-builder fallback format: integer widths are canonicalized to
        // Int64 even though the shared Variant converter preserves the source physical tag.
        require_jsonb_write(writer->writeInt64(value.get_int()), "integer");
        return;
    case ValueKind::LARGEINT:
        require_jsonb_write(writer->writeInt128(value.get_decimal().unscaled), "large integer");
        return;
    case ValueKind::FLOAT:
        require_jsonb_write(writer->writeFloat(value.get_float()), "float");
        return;
    case ValueKind::DOUBLE:
        require_jsonb_write(writer->writeDouble(value.get_double()), "double");
        return;
    case ValueKind::DECIMAL: {
        const VariantDecimal decimal = value.get_decimal();
        require_jsonb_write(
                writer->writeDecimal(Decimal128V3 {decimal.unscaled}, 38, decimal.scale),
                "decimal");
        return;
    }
    case ValueKind::ARRAY: {
        require_jsonb_write(writer->writeStartArray(), "array start");
        const uint32_t count = value.num_elements();
        for (uint32_t index = 0; index < count; ++index) {
            write_path_jsonb(value.array_at(index), writer);
        }
        require_jsonb_write(writer->writeEndArray(), "array end");
        return;
    }
    case ValueKind::NULL_VALUE:
    case ValueKind::BOOL:
    case ValueKind::DATE:
    case ValueKind::TIMESTAMP_NTZ:
    case ValueKind::TIMESTAMP_NTZ_NANOS:
    case ValueKind::TIMESTAMP_TZ:
    case ValueKind::STRING:
    case ValueKind::JSONB_REF: {
        JsonbWriter nested;
        variant_to_jsonb(value, nested);
        const JsonbValue* nested_value = JsonbDocument::createValue(nested.getOutput()->getBuffer(),
                                                                    nested.getOutput()->getSize());
        require_jsonb_write(writer->writeValue(nested_value), "Variant subtree");
        return;
    }
    }
}

void append_jsonb(VariantRef value, ColumnString* column) {
    JsonbWriter writer;
    switch (value_kind(value)) {
    case ValueKind::NULL_VALUE:
    case ValueKind::BOOL:
    case ValueKind::DATE:
    case ValueKind::TIMESTAMP_NTZ:
    case ValueKind::TIMESTAMP_NTZ_NANOS:
    case ValueKind::TIMESTAMP_TZ:
    case ValueKind::STRING:
    case ValueKind::JSONB_REF:
        // For a complete subtree the canonical converter can write directly into the output
        // document. ARRAY uses write_path_jsonb() because the legacy path-builder fallback
        // intentionally normalizes integer/decimal children before writing them.
        variant_to_jsonb(value, writer);
        break;
    case ValueKind::INT64:
    case ValueKind::LARGEINT:
    case ValueKind::FLOAT:
    case ValueKind::DOUBLE:
    case ValueKind::DECIMAL:
    case ValueKind::ARRAY:
        write_path_jsonb(value, &writer);
        break;
    }
    column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
}

void append_integer(VariantRef value, PrimitiveType target_type, IColumn* target) {
    if (value_kind(value) != ValueKind::INT64) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to integer path builder");
    }
    const int64_t integer = value.get_int();
    switch (target_type) {
    case TYPE_TINYINT:
        assert_cast<ColumnInt8&>(*target).insert_value(static_cast<int8_t>(integer));
        return;
    case TYPE_SMALLINT:
        assert_cast<ColumnInt16&>(*target).insert_value(static_cast<int16_t>(integer));
        return;
    case TYPE_INT:
        assert_cast<ColumnInt32&>(*target).insert_value(static_cast<int32_t>(integer));
        return;
    case TYPE_BIGINT:
        assert_cast<ColumnInt64&>(*target).insert_value(integer);
        return;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid integer target type {}", target_type);
    }
}

void append_largeint(VariantRef value, IColumn* target) {
    __int128 converted = 0;
    const ValueKind kind = value_kind(value);
    if (kind == ValueKind::LARGEINT) {
        converted = value.get_decimal().unscaled;
    } else if (kind == ValueKind::INT64) {
        converted = value.get_int();
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to LARGEINT path builder");
    }
    assert_cast<ColumnInt128&>(*target).insert_value(converted);
}

void append_floating(VariantRef value, PrimitiveType target_type, IColumn* target) {
    const ValueKind kind = value_kind(value);
    if (target_type == TYPE_FLOAT) {
        if (kind != ValueKind::FLOAT) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to FLOAT path builder");
        }
        assert_cast<ColumnFloat32&>(*target).insert_value(value.get_float());
        return;
    }
    double converted = 0;
    if (kind == ValueKind::DOUBLE) {
        converted = value.get_double();
    } else if (kind == ValueKind::FLOAT) {
        converted = value.get_float();
    } else if (kind == ValueKind::INT64) {
        converted = value.get_int();
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DOUBLE path builder");
    }
    assert_cast<ColumnFloat64&>(*target).insert_value(converted);
}

void append_decimal(VariantRef value, const DataTypePtr& target_type, IColumn* target) {
    const ValueKind kind = value_kind(value);
    if (kind != ValueKind::DECIMAL && kind != ValueKind::INT64) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DECIMAL path builder");
    }
    __int128 converted = 0;
    if (!try_rescale_decimal_value(value, target_type, &converted)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal value cannot be represented at scale {}",
                        target_type->get_scale());
    }
    assert_cast<ColumnDecimal128V3&>(*target).insert_value(Decimal128V3 {converted});
}

void append_date(VariantRef value, IColumn* target) {
    if (value_kind(value) != ValueKind::DATE) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DATEV2 path builder");
    }
    const cctz::civil_day civil = cctz::civil_day(1970, 1, 1) + value.get_date();
    if (civil.year() < 1 || civil.year() > 9999) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant date is outside the Doris DATEV2 range");
    }
    DateV2Value<DateV2ValueType> converted;
    converted.unchecked_set_time(static_cast<uint16_t>(civil.year()),
                                 static_cast<uint8_t>(civil.month()),
                                 static_cast<uint8_t>(civil.day()), 0, 0, 0);
    assert_cast<ColumnDateV2&>(*target).insert_value(converted);
}

void append_timestamp(VariantRef value, PrimitiveType target_type, IColumn* target) {
    const ValueKind kind = value_kind(value);
    if (kind != ValueKind::TIMESTAMP_NTZ && kind != ValueKind::TIMESTAMP_NTZ_NANOS &&
        kind != ValueKind::TIMESTAMP_TZ) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to timestamp path builder");
    }
    if (target_type == TYPE_TIMESTAMP_NS) {
        int64_t nanos = 0;
        if (kind == ValueKind::TIMESTAMP_NTZ_NANOS) {
            nanos = value.get_timestamp_ntz_nanos();
        } else if (kind != ValueKind::TIMESTAMP_NTZ ||
                   __builtin_mul_overflow(value.get_timestamp_ntz_micros(),
                                          TimeStampNsValue::NANOS_PER_MICROSECOND, &nanos)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant timestamp cannot be represented as TIMESTAMP_NS");
        }
        assert_cast<ColumnTimeStampNs&>(*target).insert_value(TimeStampNsValue(nanos));
        return;
    }
    DORIS_CHECK(kind != ValueKind::TIMESTAMP_NTZ_NANOS);
    const int64_t micros = kind == ValueKind::TIMESTAMP_NTZ ? value.get_timestamp_ntz_micros()
                                                            : value.get_timestamp_micros();
    cctz::civil_second civil;
    uint32_t fraction = 0;
    if (!epoch_micros_to_civil(micros, &civil, &fraction)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant timestamp is outside the Doris datetime range");
    }
    if (target_type == TYPE_DATETIMEV2) {
        if (kind != ValueKind::TIMESTAMP_NTZ) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append timezone-adjusted Variant timestamp to DATETIMEV2 "
                            "path builder");
        }
        DateV2Value<DateTimeV2ValueType> converted;
        converted.unchecked_set_time(
                static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
                static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
                static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()),
                fraction);
        assert_cast<ColumnDateTimeV2&>(*target).insert_value(converted);
        return;
    }
    if (target_type != TYPE_TIMESTAMPTZ || kind != ValueKind::TIMESTAMP_TZ) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant timestamp to TIMESTAMPTZ path builder");
    }
    TimestampTzValue converted;
    converted.unchecked_set_time(
            static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
            static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
            static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()), fraction);
    assert_cast<ColumnTimeStampTz&>(*target).insert_value(converted);
}

void append_value(VariantRef value, const DataTypePtr& target_type, IColumn* target);

void append_array(VariantRef value, const DataTypePtr& target_type, IColumn* target) {
    if (value_kind(value) != ValueKind::ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append scalar Variant value to ARRAY path builder");
    }
    const auto& array_type = assert_cast<const DataTypeArray&>(*target_type);
    auto& array = assert_cast<ColumnArray&>(*target);
    auto& elements = assert_cast<ColumnNullable&>(array.get_data());
    const DataTypePtr element_type = remove_nullable(array_type.get_nested_type());
    // infer_type() made the first borrowed pass. Revisit the encoded children only after path type
    // promotion is complete, appending directly without an owning recursive scratch tree.
    const uint32_t count = value.num_elements();
    for (uint32_t index = 0; index < count; ++index) {
        const VariantRef element = value.array_at(index);
        if (element.is_null()) {
            elements.get_nested_column().insert_default();
            elements.get_null_map_data().push_back(1);
        } else {
            append_value(element, element_type, &elements.get_nested_column());
            elements.get_null_map_data().push_back(0);
        }
    }
    array.get_offsets().push_back(elements.size());
}

void append_value(VariantRef value, const DataTypePtr& target_type, IColumn* target) {
    const ValueKind kind = value_kind(value);
    switch (target_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        if (kind != ValueKind::BOOL) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to BOOLEAN path builder");
        }
        assert_cast<ColumnUInt8&>(*target).insert_value(value.get_bool());
        return;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        append_integer(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_LARGEINT:
        append_largeint(value, target);
        return;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        append_floating(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_DECIMAL128I:
        append_decimal(value, target_type, target);
        return;
    case TYPE_DATEV2:
        append_date(value, target);
        return;
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMP_NS:
    case TYPE_TIMESTAMPTZ:
        append_timestamp(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_STRING: {
        if (kind != ValueKind::STRING) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to STRING path builder");
        }
        const StringRef string = value.get_string();
        assert_cast<ColumnString&>(*target).insert_data(string.data, string.size);
        return;
    }
    case TYPE_JSONB:
        append_jsonb(value, &assert_cast<ColumnString&>(*target));
        return;
    case TYPE_ARRAY:
        append_array(value, target_type, target);
        return;
    case INVALID_TYPE:
        if (kind != ValueKind::NULL_VALUE) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append non-null Variant value to Nothing path builder");
        }
        assert_cast<ColumnNothing&>(*target).insert_default();
        return;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant path builder does not support target type {}",
                        target_type->get_name());
    }
}

Status stringify_complex_column(const DataTypePtr& source_type, const ColumnNullable& source,
                                ColumnPtr* result) {
    auto jsonb = ColumnString::create();
    RETURN_IF_ERROR(source_type->get_serde()->serialize_column_to_jsonb_vector(
            source.get_nested_column(), *jsonb));

    auto strings = ColumnString::create();
    DataTypeSerDe::FormatOptions options;
    DataTypeJsonb().get_serde()->to_string_batch(*jsonb, *strings, options);
    *result = ColumnNullable::create(std::move(strings),
                                     source.get_null_map_column().clone_resized(source.size()));
    return Status::OK();
}

Status replace_array_nothing(const DataTypePtr& source_type, const IColumn& source,
                             const DataTypePtr& target_type, MutableColumnPtr* result) {
    if (source_type->is_nullable()) {
        const auto* target_nullable = typeid_cast<const DataTypeNullable*>(target_type.get());
        if (target_nullable == nullptr) {
            return Status::InvalidArgument("Cannot convert nullable Variant array path to {}",
                                           target_type->get_name());
        }
        const auto& source_nullable = assert_cast<const ColumnNullable&>(source);
        MutableColumnPtr nested;
        RETURN_IF_ERROR(replace_array_nothing(
                assert_cast<const DataTypeNullable&>(*source_type).get_nested_type(),
                source_nullable.get_nested_column(), target_nullable->get_nested_type(), &nested));
        *result = ColumnNullable::create(
                std::move(nested),
                source_nullable.get_null_map_column().clone_resized(source.size()));
        return Status::OK();
    }

    if (source_type->get_primitive_type() == INVALID_TYPE) {
        if (check_and_get_column<ColumnNothing>(&source) == nullptr) {
            return Status::InternalError("Variant Nothing type does not use ColumnNothing");
        }
        MutableColumnPtr materialized = target_type->create_column();
        materialized->insert_many_defaults(source.size());
        *result = std::move(materialized);
        return Status::OK();
    }

    if (source_type->get_primitive_type() != TYPE_ARRAY ||
        target_type->get_primitive_type() != TYPE_ARRAY) {
        return Status::InvalidArgument(
                "Cannot preserve Variant array shape while converting {} to {}",
                source_type->get_name(), target_type->get_name());
    }
    const auto& source_array_type = assert_cast<const DataTypeArray&>(*source_type);
    const auto& target_array_type = assert_cast<const DataTypeArray&>(*target_type);
    const auto& source_array = assert_cast<const ColumnArray&>(source);
    MutableColumnPtr nested;
    RETURN_IF_ERROR(replace_array_nothing(source_array_type.get_nested_type(),
                                          source_array.get_data(),
                                          target_array_type.get_nested_type(), &nested));
    *result = ColumnArray::create(
            std::move(nested),
            source_array.get_offsets_column().clone_resized(source_array.size()));
    return Status::OK();
}

size_t dotted_path_depth(const PathInData& path) {
    return path.get_parts().size();
}

size_t recursive_null_count(const IColumn& column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        size_t count = 0;
        for (UInt8 is_null : nullable->get_null_map_data()) {
            count += is_null != 0;
        }
        return count + recursive_null_count(nullable->get_nested_column());
    }
    if (const auto* array = check_and_get_column<ColumnArray>(column)) {
        return recursive_null_count(array->get_data());
    }
    return 0;
}

bool cast_introduced_null(const IColumn& source, const IColumn& target) {
    const auto* source_nullable = check_and_get_column<ColumnNullable>(source);
    const auto* target_nullable = check_and_get_column<ColumnNullable>(target);
    if (source_nullable != nullptr || target_nullable != nullptr) {
        if (source_nullable != nullptr && target_nullable != nullptr &&
            source_nullable->size() == target_nullable->size()) {
            for (size_t index = 0; index < source_nullable->size(); ++index) {
                if (!source_nullable->is_null_at(index) && target_nullable->is_null_at(index)) {
                    return true;
                }
            }
            return cast_introduced_null(source_nullable->get_nested_column(),
                                        target_nullable->get_nested_column());
        }
        return recursive_null_count(target) > recursive_null_count(source);
    }

    const auto* source_array = check_and_get_column<ColumnArray>(source);
    const auto* target_array = check_and_get_column<ColumnArray>(target);
    if (source_array != nullptr || target_array != nullptr) {
        if (source_array != nullptr && target_array != nullptr &&
            source_array->size() == target_array->size()) {
            return cast_introduced_null(source_array->get_data(), target_array->get_data());
        }
        return recursive_null_count(target) > recursive_null_count(source);
    }
    return false;
}

} // namespace

DataTypePtr normalize_variant_path_integer_widths(const DataTypePtr& type) {
    const DataTypePtr base = remove_nullable(type);
    if (const auto* array = typeid_cast<const DataTypeArray*>(base.get())) {
        return std::make_shared<DataTypeArray>(
                normalize_variant_path_integer_widths(array->get_nested_type()));
    }
    switch (base->get_primitive_type()) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return std::make_shared<DataTypeInt64>();
    default:
        return base;
    }
}

bool variant_path_type_contains_nothing(const DataTypePtr& type) {
    const DataTypePtr base = remove_nullable(type);
    if (base->get_primitive_type() == INVALID_TYPE) {
        return true;
    }
    if (const auto* array = typeid_cast<const DataTypeArray*>(base.get())) {
        return variant_path_type_contains_nothing(array->get_nested_type());
    }
    return false;
}

struct VariantPathBuilder::Impl {
    explicit Impl(PathInData path_, size_t prefix_rows_)
            : path(path_), logical_rows(prefix_rows_) {}

    Status initialize(const DataTypePtr& initial_type) {
        type = remove_nullable(initial_type);
        nullable_type = make_nullable(type);
        column = nullable_type->create_column();
        return Status::OK();
    }

    Status promote(const DataTypePtr& target_type, bool filter_cast_nulls) {
        DataTypePtr target = remove_nullable(target_type);
        if (target->equals(*type)) {
            return Status::OK();
        }
        ColumnPtr promoted;
        if (target->get_primitive_type() == TYPE_STRING &&
            type->get_primitive_type() == TYPE_ARRAY) {
            RETURN_IF_ERROR(stringify_complex_column(
                    type, assert_cast<const ColumnNullable&>(*column), &promoted));
        } else if (type->get_primitive_type() == TYPE_ARRAY &&
                   target->get_primitive_type() == TYPE_ARRAY &&
                   variant_path_type_contains_nothing(type)) {
            MutableColumnPtr materialized;
            RETURN_IF_ERROR(replace_array_nothing(nullable_type, *column, make_nullable(target),
                                                  &materialized));
            promoted = std::move(materialized);
        } else {
            RETURN_IF_ERROR(
                    variant_util::cast_column({column->get_ptr(), nullable_type, path.get_path()},
                                              make_nullable(target), &promoted));
        }
        // Inferred widening is only a storage representation choice. If CAST loses a valid value
        // at any array depth, preserve the whole path as JSONB instead. Forced typed-path
        // conversion intentionally retains its existing cast-null filtering below.
        if (!filter_cast_nulls && target->get_primitive_type() != TYPE_JSONB &&
            cast_introduced_null(*column, *promoted)) {
            target = jsonb_type();
            RETURN_IF_ERROR(
                    variant_util::cast_column({column->get_ptr(), nullable_type, path.get_path()},
                                              make_nullable(target), &promoted));
            DORIS_CHECK(!assert_cast<const ColumnNullable&>(*promoted).has_null());
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*promoted);
        if (nullable.has_null()) {
            DORIS_CHECK(filter_cast_nulls);
            DORIS_CHECK_EQ(promoted->size(), rowids.size());
            IColumn::Filter non_null_filter(promoted->size(), 1);
            DorisVector<uint32_t> non_null_rowids;
            non_null_rowids.reserve(rowids.size());
            for (size_t index = 0; index < rowids.size(); ++index) {
                if (nullable.is_null_at(index)) {
                    non_null_filter[index] = 0;
                } else {
                    non_null_rowids.push_back(rowids[index]);
                }
            }
            promoted =
                    promoted->filter(non_null_filter, static_cast<ssize_t>(non_null_rowids.size()));
            rowids = std::move(non_null_rowids);
        }
        column = IColumn::mutate(std::move(promoted));
        type = std::move(target);
        nullable_type = make_nullable(type);
#ifdef BE_TEST
        ++promotions;
#endif
        return Status::OK();
    }

    PathInData path;
    DataTypePtr type;
    DataTypePtr nullable_type;
    MutableColumnPtr column;
    DorisVector<uint32_t> rowids;
    size_t logical_rows = 0;
#ifdef BE_TEST
    size_t promotions = 0;
#endif
};

VariantPathBuilder::VariantPathBuilder(PathInData path, size_t prefix_rows)
        : _impl(std::make_unique<Impl>(std::move(path), prefix_rows)) {}
VariantPathBuilder::~VariantPathBuilder() = default;
VariantPathBuilder::VariantPathBuilder(VariantPathBuilder&&) noexcept = default;
VariantPathBuilder& VariantPathBuilder::operator=(VariantPathBuilder&&) noexcept = default;

Status VariantPathBuilder::append(VariantRef value, size_t row) {
    try {
        if (value.is_null()) {
            return Status::InvalidArgument("Variant path builder {} must not append JSON null",
                                           _impl->path.get_path());
        }
        if (row < _impl->logical_rows) {
            return Status::InvalidArgument("Variant path builder {} already has row {}",
                                           _impl->path.get_path(), row);
        }
        if (row > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("Variant path builder {} row {} exceeds uint32 limit",
                                           _impl->path.get_path(), row);
        }
        RETURN_IF_ERROR(complete_rows(row));

        if (!_impl->column) {
            RETURN_IF_ERROR(_impl->initialize(infer_type(value)));
        } else if (_impl->type->get_primitive_type() != TYPE_JSONB) {
            DataTypePtr inferred_type = infer_type(value, _impl->type);
            DataTypePtr common_type = path_least_common_type(_impl->type, inferred_type);
            RETURN_IF_ERROR(_impl->promote(common_type, false));
        }
        const bool is_array = value.basic_type() == VariantBasicType::ARRAY;
        if (is_array && !value_is_representable(value, _impl->type)) {
            RETURN_IF_ERROR(_impl->promote(jsonb_type(), false));
        }

        try {
            auto& nullable = assert_cast<ColumnNullable&>(*_impl->column);
            append_value(value, _impl->type, &nullable.get_nested_column());
            nullable.get_null_map_data().push_back(0);
        } catch (const Exception&) {
            if (is_array || _impl->type->get_primitive_type() == TYPE_JSONB) {
                throw;
            }
            RETURN_IF_ERROR(_impl->promote(jsonb_type(), false));
            auto& nullable = assert_cast<ColumnNullable&>(*_impl->column);
            append_value(value, _impl->type, &nullable.get_nested_column());
            nullable.get_null_map_data().push_back(0);
        }
        _impl->rowids.push_back(static_cast<uint32_t>(row));
        _impl->logical_rows = row + 1;
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status VariantPathBuilder::complete_rows(size_t rows) {
    if (rows < _impl->logical_rows) {
        return Status::InvalidArgument("Variant path builder {} cannot shrink from {} to {} rows",
                                       _impl->path.get_path(), _impl->logical_rows, rows);
    }
    _impl->logical_rows = rows;
    return Status::OK();
}

Status VariantPathBuilder::convert_to(const DataTypePtr& storage_type) {
    if (!_impl->column) {
        return _impl->initialize(storage_type);
    }
    return _impl->promote(storage_type, true);
}

const PathInData& VariantPathBuilder::path() const {
    return _impl->path;
}

const DataTypePtr& VariantPathBuilder::type() const {
    return _impl->nullable_type;
}

ColumnPtr VariantPathBuilder::column() const {
    return _impl->column ? _impl->column->get_ptr() : nullptr;
}

std::span<const uint32_t> VariantPathBuilder::rowids() const {
    return _impl->rowids;
}

uint32_t VariantPathBuilder::non_null_rows() const {
    DORIS_CHECK_LE(_impl->rowids.size(), std::numeric_limits<uint32_t>::max());
    return static_cast<uint32_t>(_impl->rowids.size());
}

#ifdef BE_TEST
size_t VariantPathBuilder::rows() const {
    return _impl->logical_rows;
}

size_t VariantPathBuilder::promotion_count() const {
    return _impl->promotions;
}
#endif

size_t VariantPathBuilder::byte_size() const {
    return sizeof(Impl) + path_allocated_bytes(_impl->path) +
           _impl->rowids.capacity() * sizeof(uint32_t) +
           (_impl->column ? _impl->column->allocated_bytes() : 0);
}

#ifdef BE_TEST
bool VariantPathBuilder::is_null_at(size_t row) const {
    if (row >= _impl->logical_rows) {
        throw Exception(ErrorCode::OUT_OF_BOUND, "Variant path row {} exceeds {} rows for path {}",
                        row, _impl->logical_rows, _impl->path.get_path());
    }
    if (row > std::numeric_limits<uint32_t>::max()) {
        return true;
    }
    return !std::binary_search(_impl->rowids.begin(), _impl->rowids.end(),
                               static_cast<uint32_t>(row));
}
#endif

#ifdef BE_TEST
Status VariantPathBuilder::materialize(ColumnPtr* result) const {
    if (result == nullptr) {
        return Status::InvalidArgument("Variant materialized output must not be null");
    }
    if (!_impl->column || _impl->column->size() != _impl->rowids.size()) {
        return Status::InternalError(
                "Variant path {} has {} compact values for {} row ids", _impl->path.get_path(),
                _impl->column ? _impl->column->size() : 0, _impl->rowids.size());
    }

    MutableColumnPtr materialized = _impl->column->clone_empty();
    materialized->reserve(_impl->logical_rows);
    size_t next_row = 0;
    size_t value_index = 0;
    while (value_index < _impl->rowids.size()) {
        const size_t row = _impl->rowids[value_index];
        DORIS_CHECK_GE(row, next_row);
        materialized->insert_many_defaults(row - next_row);

        size_t run_length = 1;
        while (value_index + run_length < _impl->rowids.size() &&
               _impl->rowids[value_index + run_length] == row + run_length) {
            ++run_length;
        }
        materialized->insert_range_from(*_impl->column, value_index, run_length);
        value_index += run_length;
        next_row = row + run_length;
    }
    DORIS_CHECK_LE(next_row, _impl->logical_rows);
    materialized->insert_many_defaults(_impl->logical_rows - next_row);
    *result = std::move(materialized);
    return Status::OK();
}
#endif

// NOLINTNEXTLINE(readability-non-const-parameter): the serde appends bytes through this pointer.
Status VariantPathBuilder::write_sparse_cell(size_t value_index, ColumnString::Chars* chars) const {
    if (chars == nullptr) {
        return Status::InvalidArgument("Sparse output chars must not be null");
    }
    if (!_impl->column || value_index >= _impl->column->size()) {
        return Status::InvalidArgument("Sparse value {} is out of range for path {}", value_index,
                                       _impl->path.get_path());
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*_impl->column);
    if (nullable.is_null_at(value_index)) {
        return Status::InternalError("Compact sparse value {} is null for path {}", value_index,
                                     _impl->path.get_path());
    }
    try {
        _impl->type->get_serde(2)->write_one_cell_to_binary(nullable.get_nested_column(), *chars,
                                                            value_index);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

VariantPathSelection select_variant_paths(std::span<const VariantPathSelectionCandidate> candidates,
                                          size_t max_dynamic_materialized_paths,
                                          bool typed_paths_to_sparse) {
    struct DynamicCandidate {
        size_t index = 0;
        uint32_t non_null_rows = 0;
    };

    VariantPathSelection result;
    DorisVector<DynamicCandidate> dynamic;
    dynamic.reserve(candidates.size());
    for (size_t index = 0; index < candidates.size(); ++index) {
        const VariantPathSelectionCandidate& candidate = candidates[index];
        DORIS_CHECK(candidate.builder != nullptr);
        if (candidate.is_typed_path && !typed_paths_to_sparse) {
            result.materialized.push_back(index);
        } else if (candidate.builder->non_null_rows() == 0) {
            continue;
        } else {
            dynamic.push_back(
                    {.index = index, .non_null_rows = candidate.builder->non_null_rows()});
        }
    }

    std::ranges::sort(dynamic, [&](const auto& left, const auto& right) {
        if (left.non_null_rows != right.non_null_rows) {
            return left.non_null_rows > right.non_null_rows;
        }
        const PathInData& left_path = candidates[left.index].builder->path();
        const PathInData& right_path = candidates[right.index].builder->path();
        if (dotted_path_depth(left_path) != dotted_path_depth(right_path)) {
            return dotted_path_depth(left_path) > dotted_path_depth(right_path);
        }
        return left_path.get_path() > right_path.get_path();
    });

    const size_t selected_dynamic =
            max_dynamic_materialized_paths == 0
                    ? dynamic.size()
                    : std::min(max_dynamic_materialized_paths, dynamic.size());
    for (size_t index = 0; index < dynamic.size(); ++index) {
        (index < selected_dynamic ? result.materialized : result.sparse)
                .push_back(dynamic[index].index);
    }
    const auto by_path = [&](size_t left, size_t right) {
        return candidates[left].builder->path() < candidates[right].builder->path();
    };
    std::ranges::sort(result.materialized, by_path);
    std::ranges::sort(result.sparse, by_path);
    return result;
}

} // namespace doris::segment_v2
