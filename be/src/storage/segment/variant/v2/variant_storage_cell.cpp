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

#include "storage/segment/variant/v2/variant_storage_cell.h"

#include <array>
#include <cstring>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_column_utils.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/storage_field_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/timestamp_ns_value.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/common/format_ip.h"
#include "exprs/function/parse/variant_jsonb_parse.h"

namespace doris::segment_v2::variant_v2 {
namespace {

// DataTypeSerDe's legacy decoder receives only a pointer. Keep bounds validation local to the V1
// storage-cell adapter so the decoder is called only after the complete scalar cell is known to fit
// inside its enclosing StringRef.
class BinaryCellCursor {
public:
    explicit BinaryCellCursor(StringRef cell)
            : _current(reinterpret_cast<const uint8_t*>(cell.data)),
              _remaining(cell.size),
              _valid(cell.data != nullptr || cell.size == 0) {}

    template <typename T>
    Status read(T* value, std::string_view description) {
        DORIS_CHECK(value != nullptr);
        StringRef bytes;
        RETURN_IF_ERROR(read_bytes(sizeof(T), &bytes, description));
        std::memcpy(value, bytes.data, sizeof(T));
        return Status::OK();
    }

    Status read_bytes(size_t size, StringRef* value, std::string_view description) {
        DORIS_CHECK(value != nullptr);
        if (!_valid) {
            return Status::Corruption("Binary storage cell has a null pointer for {} bytes",
                                      _remaining);
        }
        if (size > _remaining) {
            return Status::Corruption(
                    "Truncated binary storage cell while reading {}: need {} bytes, have {}",
                    description, size, _remaining);
        }
        *value = {reinterpret_cast<const char*>(_current), size};
        if (size != 0) {
            _current += size;
        }
        _remaining -= size;
        return Status::OK();
    }

    Status skip(size_t size, std::string_view description) {
        StringRef ignored;
        return read_bytes(size, &ignored, description);
    }

    [[nodiscard]] size_t remaining() const noexcept { return _remaining; }

private:
    const uint8_t* _current = nullptr;
    size_t _remaining = 0;
    bool _valid = true;
};

Status validate_scalar_cell(BinaryCellCursor& cursor, FieldType field_type, uint8_t* precision,
                            uint8_t* scale) {
    DORIS_CHECK(precision != nullptr);
    DORIS_CHECK(scale != nullptr);
    *precision = 0;
    *scale = 0;

    switch (field_type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return cursor.skip(sizeof(uint8_t), "boolean");
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return cursor.skip(sizeof(int8_t), "tinyint");
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return cursor.skip(sizeof(int16_t), "smallint");
    case FieldType::OLAP_FIELD_TYPE_INT:
        return cursor.skip(sizeof(int32_t), "int");
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return cursor.skip(sizeof(int64_t), "bigint");
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return cursor.skip(sizeof(__int128), "largeint");
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        return cursor.skip(sizeof(float), "float");
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return cursor.skip(sizeof(double), "double");
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        size_t size = 0;
        RETURN_IF_ERROR(cursor.read(&size, "string size"));
        return cursor.skip(size, "string payload");
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return cursor.skip(sizeof(IPv4), "IPv4");
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return cursor.skip(sizeof(IPv6), "IPv6");
    case FieldType::OLAP_FIELD_TYPE_DATE:
        return cursor.skip(sizeof(VecDateTimeValue), "legacy DATE");
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        return cursor.skip(sizeof(VecDateTimeValue), "legacy DATETIME");
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        return cursor.skip(sizeof(UInt32), "DateV2");
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        RETURN_IF_ERROR(cursor.read(scale, "timestamp scale"));
        return cursor.skip(sizeof(UInt64), "timestamp value");
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS:
        RETURN_IF_ERROR(cursor.read(scale, "timestamp_ns scale"));
        return cursor.skip(sizeof(Int64), "timestamp_ns value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        RETURN_IF_ERROR(cursor.read(precision, "legacy DecimalV2 precision"));
        RETURN_IF_ERROR(cursor.read(scale, "legacy DecimalV2 scale"));
        return cursor.skip(sizeof(__int128), "legacy DecimalV2 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        RETURN_IF_ERROR(cursor.read(precision, "Decimal32 precision"));
        RETURN_IF_ERROR(cursor.read(scale, "Decimal32 scale"));
        return cursor.skip(sizeof(int32_t), "Decimal32 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        RETURN_IF_ERROR(cursor.read(precision, "Decimal64 precision"));
        RETURN_IF_ERROR(cursor.read(scale, "Decimal64 scale"));
        return cursor.skip(sizeof(int64_t), "Decimal64 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        RETURN_IF_ERROR(cursor.read(precision, "Decimal128 precision"));
        RETURN_IF_ERROR(cursor.read(scale, "Decimal128 scale"));
        return cursor.skip(sizeof(__int128), "Decimal128 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        RETURN_IF_ERROR(cursor.read(precision, "Decimal256 precision"));
        RETURN_IF_ERROR(cursor.read(scale, "Decimal256 scale"));
        return cursor.skip(sizeof(wide::Int256), "Decimal256 value");
    default:
        return Status::Corruption("Unsupported binary storage scalar FieldType {}",
                                  static_cast<uint8_t>(field_type));
    }
}

template <typename DateValue>
int32_t storage_date_days(DateValue value, std::string_view description) {
    return variant_days_since_epoch(value, 0, description);
}

template <typename DateTimeValue>
int64_t storage_timestamp_micros(DateTimeValue value, std::string_view description) {
    return variant_timestamp_micros(value, 0, description);
}

// Decode the persisted V1 value and convert it to Variant semantics. The typed fast path below uses
// validate_scalar_cell() instead, then delegates materialization to the legacy DataTypeSerDe.
// NOLINTNEXTLINE(readability-function-size)
Status append_binary_value(BinaryCellCursor& cursor, VariantBatchBuilder::Row& output,
                           uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        return Status::Corruption("Binary storage cell exceeds maximum nesting depth {}",
                                  VARIANT_MAX_NESTING_DEPTH);
    }

    uint8_t raw_type = 0;
    RETURN_IF_ERROR(cursor.read(&raw_type, "field type"));
    const auto type = static_cast<FieldType>(raw_type);
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_NONE:
        output.add_null();
        return Status::OK();
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        size_t size = 0;
        RETURN_IF_ERROR(cursor.read(&size, "JSONB size"));
        StringRef payload;
        RETURN_IF_ERROR(cursor.read_bytes(size, &payload, "JSONB payload"));
        jsonb_to_variant(payload, output, depth, nullptr);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        if (depth >= VARIANT_MAX_NESTING_DEPTH) {
            return Status::Corruption("Binary storage container exceeds maximum nesting depth {}",
                                      VARIANT_MAX_NESTING_DEPTH);
        }
        size_t count = 0;
        RETURN_IF_ERROR(cursor.read(&count, "array element count"));
        if (count > cursor.remaining()) {
            return Status::Corruption("Binary storage array count {} exceeds remaining {} bytes",
                                      count, cursor.remaining());
        }
        auto array = output.start_array();
        for (size_t index = 0; index < count; ++index) {
            RETURN_IF_ERROR(append_binary_value(cursor, output, depth + 1));
        }
        array.finish();
        return Status::OK();
    }
    default:
        break;
    }

    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        uint8_t value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "boolean"));
        output.add_bool(value != 0);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        int8_t value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "tinyint"));
        output.add_int(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        int16_t value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "smallint"));
        output.add_int(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        int32_t value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "int"));
        output.add_int(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        int64_t value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "bigint"));
        output.add_int(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        __int128 value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "largeint"));
        output.add_largeint(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        float value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "float"));
        output.add_float(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        double value = 0;
        RETURN_IF_ERROR(cursor.read(&value, "double"));
        output.add_double(value);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        size_t size = 0;
        RETURN_IF_ERROR(cursor.read(&size, "string size"));
        StringRef payload;
        RETURN_IF_ERROR(cursor.read_bytes(size, &payload, "string payload"));
        output.add_string(payload);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        IPv4 value {};
        RETURN_IF_ERROR(cursor.read(&value, "IPv4"));
        std::array<char, IPV4_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        format_ipv4(reinterpret_cast<const unsigned char*>(&value), end);
        output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        IPv6 value {};
        RETURN_IF_ERROR(cursor.read(&value, "IPv6"));
        std::array<char, IPV6_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        format_ipv6(reinterpret_cast<unsigned char*>(&value), end);
        output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        VecDateTimeValue value;
        RETURN_IF_ERROR(cursor.read(&value, "legacy DATE"));
        output.add_date(storage_date_days(value, "legacy DATE"));
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        VecDateTimeValue value;
        RETURN_IF_ERROR(cursor.read(&value, "legacy DATETIME"));
        output.add_timestamp_micros(storage_timestamp_micros(value, "legacy DATETIME"), false);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        UInt32 raw = 0;
        RETURN_IF_ERROR(cursor.read(&raw, "DateV2"));
        const auto value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(raw);
        output.add_date(storage_date_days(value, "DATEV2"));
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ: {
        uint8_t scale = 0;
        UInt64 raw = 0;
        RETURN_IF_ERROR(cursor.read(&scale, "timestamp scale"));
        RETURN_IF_ERROR(cursor.read(&raw, "timestamp value"));
        if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
            const auto value = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "DATETIMEV2"), false);
        } else {
            const auto value = binary_cast<UInt64, TimestampTzValue>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "TIMESTAMPTZ"), true);
        }
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS: {
        uint8_t scale = 0;
        Int64 raw = 0;
        RETURN_IF_ERROR(cursor.read(&scale, "timestamp_ns scale"));
        RETURN_IF_ERROR(cursor.read(&raw, "timestamp_ns value"));
        DORIS_CHECK_EQ(scale, TimeStampNsValue::FRACTIONAL_DIGITS);
        output.add_timestamp_nanos(raw, false);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        uint8_t precision = 0;
        uint8_t scale = 0;
        __int128 value = 0;
        RETURN_IF_ERROR(cursor.read(&precision, "legacy DecimalV2 precision"));
        RETURN_IF_ERROR(cursor.read(&scale, "legacy DecimalV2 scale"));
        RETURN_IF_ERROR(cursor.read(&value, "legacy DecimalV2 value"));
        output.add_decimal(value, DecimalV2Value::SCALE, 16);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        uint8_t precision = 0;
        uint8_t scale = 0;
        int32_t value = 0;
        RETURN_IF_ERROR(cursor.read(&precision, "Decimal32 precision"));
        RETURN_IF_ERROR(cursor.read(&scale, "Decimal32 scale"));
        RETURN_IF_ERROR(cursor.read(&value, "Decimal32 value"));
        output.add_decimal(value, scale, 4);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        uint8_t precision = 0;
        uint8_t scale = 0;
        int64_t value = 0;
        RETURN_IF_ERROR(cursor.read(&precision, "Decimal64 precision"));
        RETURN_IF_ERROR(cursor.read(&scale, "Decimal64 scale"));
        RETURN_IF_ERROR(cursor.read(&value, "Decimal64 value"));
        output.add_decimal(value, scale, 8);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        uint8_t precision = 0;
        uint8_t scale = 0;
        __int128 value = 0;
        RETURN_IF_ERROR(cursor.read(&precision, "Decimal128 precision"));
        RETURN_IF_ERROR(cursor.read(&scale, "Decimal128 scale"));
        RETURN_IF_ERROR(cursor.read(&value, "Decimal128 value"));
        output.add_decimal(value, scale, 16);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        uint8_t precision = 0;
        uint8_t scale = 0;
        wide::Int256 value {};
        RETURN_IF_ERROR(cursor.read(&precision, "Decimal256 precision"));
        RETURN_IF_ERROR(cursor.read(&scale, "Decimal256 scale"));
        RETURN_IF_ERROR(cursor.read(&value, "Decimal256 value"));
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Conversion from Decimal256 storage cell to Variant V2 is not supported");
    }
    default:
        return Status::Corruption("Unknown binary storage FieldType {}",
                                  static_cast<uint8_t>(type));
    }
}

} // namespace

Status append_v1_storage_cell(StringRef cell, VariantBatchBuilder::Row& output, uint32_t depth) {
    BinaryCellCursor cursor(cell);
    RETURN_IF_ERROR(append_binary_value(cursor, output, depth));
    if (cursor.remaining() != 0) {
        return Status::Corruption("Binary storage cell has {} trailing bytes", cursor.remaining());
    }
    return Status::OK();
}

namespace {

// Same shape as variant_assembler.cpp's helper, kept separate on purpose: both
// files sit in one anonymous namespace once the unity build merges them, so the
// two names have to differ.
void publish_encoded_storage_cells(VariantBatchBuilder* builder,
                                   ColumnUInt8::MutablePtr outer_nulls,
                                   ColumnNullable::MutablePtr* output) {
    VariantBatchBuilder block = builder->finish_batch();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(block);
    *output = ColumnNullable::create(std::move(values), std::move(outer_nulls));
}

// Typed output is possible only when every visible cell has the same scalar signature. Each scalar
// cell is bounded before the legacy pointer decoder is called. The candidate stays local until the
// full batch succeeds; a late signature mismatch discards it and lets the caller replay generically.
Status try_build_typed_storage_cells(std::span<const StringRef> cells,
                                     std::span<const uint8_t> outer_nulls,
                                     std::span<const uint8_t> missing,
                                     ColumnNullable::MutablePtr* output, bool* built) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(built != nullptr);
    *built = false;
    const size_t rows = cells.size();
    FieldType candidate_field_type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    uint8_t candidate_precision = 0;
    uint8_t candidate_scale = 0;
    DataTypePtr scalar_type;
    ColumnNullable::MutablePtr nullable;
    auto result_outer = ColumnUInt8::create();
    result_outer->reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const bool is_missing = (!outer_nulls.empty() && outer_nulls[row] != 0) ||
                                (!missing.empty() && missing[row] != 0);
        result_outer->insert_value(is_missing ? 1 : 0);
        if (is_missing) {
            if (nullable.get() != nullptr) {
                nullable->insert_default();
            }
            continue;
        }

        BinaryCellCursor cursor(cells[row]);
        uint8_t raw_field_type = 0;
        RETURN_IF_ERROR(cursor.read(&raw_field_type, "field type"));
        const auto field_type = static_cast<FieldType>(raw_field_type);
        if (field_type == FieldType::OLAP_FIELD_TYPE_NONE ||
            field_type == FieldType::OLAP_FIELD_TYPE_JSONB ||
            field_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            return Status::OK();
        }
        if (nullable.get() != nullptr && field_type != candidate_field_type) {
            return Status::OK();
        }

        uint8_t precision = 0;
        uint8_t scale = 0;
        RETURN_IF_ERROR(validate_scalar_cell(cursor, field_type, &precision, &scale));
        if (cursor.remaining() != 0) {
            return Status::Corruption("Binary storage cell has {} trailing bytes",
                                      cursor.remaining());
        }

        if (nullable.get() == nullptr) {
            candidate_field_type = field_type;
            candidate_precision = precision;
            candidate_scale = scale;
            scalar_type = DataTypeFactory::instance().create_data_type(
                    candidate_field_type, candidate_precision, candidate_scale);
            if (scalar_type == nullptr) {
                return Status::Corruption("Cannot create typed Variant output for FieldType {}",
                                          static_cast<uint8_t>(candidate_field_type));
            }
            // Decimal factories choose the physical column width from precision, while the cell
            // tag persists that width independently. Preserve that invariant before materializing.
            if (scalar_type->get_primitive_type() !=
                storage_field_type_to_primitive_type(candidate_field_type)) {
                return Status::Corruption(
                        "Binary storage FieldType {} is incompatible with precision {} and scale "
                        "{}",
                        static_cast<uint8_t>(candidate_field_type), candidate_precision,
                        candidate_scale);
            }
            if (!is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
                return Status::OK();
            }
            nullable = ColumnNullable::create(scalar_type->create_column(), ColumnUInt8::create());
            nullable->reserve(rows);
            nullable->insert_many_defaults(row);
        } else if (precision != candidate_precision || scale != candidate_scale) {
            return Status::OK();
        }

        const auto* cell_begin = reinterpret_cast<const uint8_t*>(cells[row].data);
        const uint8_t* decoded_end =
                DataTypeSerDe::deserialize_binary_to_column(cell_begin, *nullable);
        const auto* cell_end = cell_begin + cells[row].size;
        if (decoded_end != cell_end) {
            return Status::Corruption("Binary storage cell decoder consumed {} of {} bytes",
                                      decoded_end - cell_begin, cells[row].size);
        }
    }
    if (nullable.get() == nullptr) {
        return Status::OK();
    }
    *output =
            ColumnNullable::create(ColumnVariantV2::create_typed(std::move(nullable), scalar_type),
                                   std::move(result_outer));
    *built = true;
    return Status::OK();
}

} // namespace

Status decode_v1_storage_cells(std::span<const StringRef> cells,
                               std::span<const uint8_t> outer_nulls,
                               std::span<const uint8_t> missing,
                               ColumnNullable::MutablePtr* output) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(outer_nulls.empty() || outer_nulls.size() == cells.size());
    DORIS_CHECK(missing.empty() || missing.size() == cells.size());

    try {
        ColumnNullable::MutablePtr result;
        bool built_typed = false;
        RETURN_IF_ERROR(
                try_build_typed_storage_cells(cells, outer_nulls, missing, &result, &built_typed));
        if (!built_typed) {
            VariantBatchBuilder builder({.rows = cells.size()});
            auto result_outer = ColumnUInt8::create();
            result_outer->reserve(cells.size());
            // Missing/SQL NULL is carried only by the outer map. A present NONE or JSONB null cell
            // remains an encoded Variant null payload.
            for (size_t row_index = 0; row_index < cells.size(); ++row_index) {
                auto row = builder.begin_row();
                const bool is_missing = (!outer_nulls.empty() && outer_nulls[row_index] != 0) ||
                                        (!missing.empty() && missing[row_index] != 0);
                if (is_missing) {
                    result_outer->insert_value(1);
                    row.add_null();
                } else {
                    RETURN_IF_ERROR(append_v1_storage_cell(cells[row_index], row, 0));
                    result_outer->insert_value(0);
                }
                row.finish();
            }
            publish_encoded_storage_cells(&builder, std::move(result_outer), &result);
        }
        *output = std::move(result);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

} // namespace doris::segment_v2::variant_v2
