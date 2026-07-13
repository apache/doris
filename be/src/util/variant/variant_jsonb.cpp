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

#include "util/variant/variant_jsonb.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "core/types.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_builder.h"
#include "util/variant/variant_encoding.h"

namespace doris {
namespace {

static_assert(MaxNestingLevel <= VARIANT_MAX_NESTING_DEPTH);

class BoundedJsonbCursor {
public:
    BoundedJsonbCursor(const char* data, size_t size) : _current(data), _remaining(size) {}

    template <typename T>
    T read(const char* description) {
        require(sizeof(T), description);
        T value;
        std::memcpy(&value, _current, sizeof(T));
        _current += sizeof(T);
        _remaining -= sizeof(T);
        return value;
    }

    StringRef read_bytes(size_t size, const char* description) {
        require(size, description);
        const StringRef result {_current, size};
        _current += size;
        _remaining -= size;
        return result;
    }

    BoundedJsonbCursor read_region(size_t size, const char* description) {
        const StringRef region = read_bytes(size, description);
        return {region.data, region.size};
    }

    bool empty() const noexcept { return _remaining == 0; }
    size_t remaining() const noexcept { return _remaining; }

private:
    void require(size_t size, const char* description) const {
        if (_remaining < size) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Truncated JSONB while reading {}: need {} bytes, have {}", description,
                            size, _remaining);
        }
    }

    const char* _current;
    size_t _remaining;
};

template <typename Integer>
uint32_t decimal_digits(Integer value) {
    uint32_t digits = 0;
    do {
        value /= 10;
        ++digits;
    } while (value != 0);
    return digits;
}

template <typename Integer>
void require_decimal(Integer value, uint32_t precision, uint32_t scale, uint32_t maximum_precision,
                     uint32_t maximum_scale, const char* description) {
    if (precision == 0 || precision > maximum_precision) {
        throw Exception(ErrorCode::CORRUPTION, "JSONB {} precision {} is outside [1, {}]",
                        description, precision, maximum_precision);
    }
    if (scale > maximum_scale) {
        throw Exception(ErrorCode::CORRUPTION, "JSONB {} scale {} exceeds {}", description, scale,
                        maximum_scale);
    }
    const uint32_t digits = decimal_digits(value);
    if (digits > precision) {
        throw Exception(ErrorCode::CORRUPTION,
                        "JSONB {} value has {} digits but declared precision is {}", description,
                        digits, precision);
    }
}

variant_json_detail::FormattedScalar format_decimal256(wide::Int256 value, uint32_t scale) {
    variant_json_detail::FormattedScalar result;
    std::array<char, 76> reversed {};
    size_t digits = 0;
    wide::Int256 remaining = value;
    do {
        int digit = static_cast<int>(remaining % 10);
        if (digit < 0) {
            digit = -digit;
        }
        reversed[digits++] = static_cast<char>('0' + digit);
        remaining /= 10;
    } while (remaining != 0);

    const auto append = [&](char byte) { result.bytes[result.size++] = byte; };
    if (value < 0) {
        append('-');
    }
    if (scale == 0) {
        while (digits != 0) {
            append(reversed[--digits]);
        }
        return result;
    }
    if (digits <= scale) {
        append('0');
        append('.');
        for (size_t zero = digits; zero < scale; ++zero) {
            append('0');
        }
    } else {
        while (digits > scale) {
            append(reversed[--digits]);
        }
        append('.');
    }
    while (digits != 0) {
        append(reversed[--digits]);
    }
    return result;
}

void require_jsonb_depth(uint32_t depth, bool is_container) {
    if (depth > MaxNestingLevel || (is_container && depth == MaxNestingLevel)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "JSONB traversal exceeds maximum container nesting {}", MaxNestingLevel);
    }
}

void require_variant_depth(uint32_t base_depth, uint32_t relative_depth, bool is_container) {
    if (base_depth > VARIANT_MAX_NESTING_DEPTH ||
        relative_depth > VARIANT_MAX_NESTING_DEPTH - base_depth ||
        (is_container && base_depth + relative_depth == VARIANT_MAX_NESTING_DEPTH)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "JSONB traversal exceeds maximum Variant container nesting {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

template <typename Builder>
void collect_jsonb_value(BoundedJsonbCursor& cursor, Builder& builder, uint32_t relative_depth,
                         uint32_t base_depth);

template <typename Builder>
void collect_jsonb_object(BoundedJsonbCursor& cursor, Builder& builder, uint32_t relative_depth,
                          uint32_t base_depth) {
    require_jsonb_depth(relative_depth, true);
    require_variant_depth(base_depth, relative_depth, true);
    const auto payload_size = cursor.read<uint32_t>("object payload size");
    BoundedJsonbCursor payload = cursor.read_region(payload_size, "object payload");
    auto scope = builder.start_object();
    while (!payload.empty()) {
        const auto key_size = payload.read<uint8_t>("object key size");
        StringRef key;
        if (key_size == 0) {
            const auto key_id = payload.read<JsonbKeyValue::keyid_type>("object key id");
            if (key_id != JsonbKeyValue::sMaxKeyId) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Cannot resolve external JSONB object key id {} without a "
                                "dictionary",
                                key_id);
            }
        } else {
            key = payload.read_bytes(key_size, "object key");
        }
        scope.add_key(key);
        collect_jsonb_value(payload, builder, relative_depth + 1, base_depth);
    }
    scope.finish();
}

template <typename Builder>
void collect_jsonb_array(BoundedJsonbCursor& cursor, Builder& builder, uint32_t relative_depth,
                         uint32_t base_depth) {
    require_jsonb_depth(relative_depth, true);
    require_variant_depth(base_depth, relative_depth, true);
    const auto payload_size = cursor.read<uint32_t>("array payload size");
    BoundedJsonbCursor payload = cursor.read_region(payload_size, "array payload");
    auto scope = builder.start_array();
    while (!payload.empty()) {
        collect_jsonb_value(payload, builder, relative_depth + 1, base_depth);
    }
    scope.finish();
}

template <typename Native, typename Builder>
void collect_jsonb_decimal(BoundedJsonbCursor& cursor, Builder& builder, uint8_t width,
                           uint32_t maximum_precision, const char* description) {
    const auto precision = cursor.read<uint32_t>("decimal precision");
    const auto scale = cursor.read<uint32_t>("decimal scale");
    const auto value = cursor.read<Native>("decimal value");
    require_decimal(value, precision, scale, maximum_precision, 38, description);
    builder.add_decimal(static_cast<__int128>(value), static_cast<uint8_t>(scale), width);
}

template <typename Builder>
void collect_jsonb_value(BoundedJsonbCursor& cursor, Builder& builder, uint32_t relative_depth,
                         uint32_t base_depth) {
    require_jsonb_depth(relative_depth, false);
    require_variant_depth(base_depth, relative_depth, false);
    const auto type = static_cast<JsonbType>(cursor.read<uint8_t>("value type"));
    switch (type) {
    case JsonbType::T_Null:
        builder.add_null();
        return;
    case JsonbType::T_True:
        builder.add_bool(true);
        return;
    case JsonbType::T_False:
        builder.add_bool(false);
        return;
    case JsonbType::T_Int8:
        builder.add_int(cursor.read<int8_t>("int8 value"));
        return;
    case JsonbType::T_Int16:
        builder.add_int(cursor.read<int16_t>("int16 value"));
        return;
    case JsonbType::T_Int32:
        builder.add_int(cursor.read<int32_t>("int32 value"));
        return;
    case JsonbType::T_Int64:
        builder.add_int(cursor.read<int64_t>("int64 value"));
        return;
    case JsonbType::T_Double:
        builder.add_double(cursor.read<double>("double value"));
        return;
    case JsonbType::T_String: {
        const auto size = cursor.read<uint32_t>("string size");
        builder.add_string(cursor.read_bytes(size, "string payload"));
        return;
    }
    case JsonbType::T_Binary: {
        const auto size = cursor.read<uint32_t>("binary size");
        builder.add_binary(cursor.read_bytes(size, "binary payload"));
        return;
    }
    case JsonbType::T_Object:
        collect_jsonb_object(cursor, builder, relative_depth, base_depth);
        return;
    case JsonbType::T_Array:
        collect_jsonb_array(cursor, builder, relative_depth, base_depth);
        return;
    case JsonbType::T_Int128:
        builder.add_largeint(cursor.read<__int128>("int128 value"));
        return;
    case JsonbType::T_Float:
        builder.add_float(cursor.read<float>("float value"));
        return;
    case JsonbType::T_Decimal32:
        collect_jsonb_decimal<int32_t>(cursor, builder, 4, 9, "Decimal32");
        return;
    case JsonbType::T_Decimal64:
        collect_jsonb_decimal<int64_t>(cursor, builder, 8, 18, "Decimal64");
        return;
    case JsonbType::T_Decimal128:
        collect_jsonb_decimal<__int128>(cursor, builder, 16, 38, "Decimal128");
        return;
    case JsonbType::T_Decimal256: {
        const auto precision = cursor.read<uint32_t>("Decimal256 precision");
        const auto scale = cursor.read<uint32_t>("Decimal256 scale");
        const auto value = cursor.read<wide::Int256>("Decimal256 value");
        require_decimal(value, precision, scale, 76, 76, "Decimal256");
        const auto formatted = format_decimal256(value, scale);
        builder.add_string({formatted.bytes.data(), formatted.size});
        return;
    }
    case JsonbType::NUM_TYPES:
        break;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown JSONB value type {}",
                    static_cast<uint8_t>(type));
}

template <typename Builder>
void collect_jsonb_document(StringRef document, Builder& builder, uint32_t initial_depth = 0) {
    if (document.data == nullptr && document.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "JSONB input has a null data pointer for {} bytes", document.size);
    }
    if (document.size == 0) {
        builder.add_null();
        return;
    }

    BoundedJsonbCursor cursor(document.data, document.size);
    const auto version = cursor.read<uint8_t>("document version");
    if (version != JSONB_VER) {
        throw Exception(ErrorCode::CORRUPTION, "Unsupported JSONB version {}, expected {}", version,
                        JSONB_VER);
    }
    collect_jsonb_value(cursor, builder, 0, initial_depth);
    if (!cursor.empty()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "JSONB document has {} trailing bytes after its root value",
                        cursor.remaining());
    }
}

void require_jsonb_write(bool written, const char* operation) {
    if (!written) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "JSONB writer rejected {}", operation);
    }
}

void write_jsonb_string(JsonbWriter& writer, StringRef value, const char* description) {
    variant_json_detail::require_valid_json_utf8(value, description);
    require_jsonb_write(writer.writeStartString(), "string start");
    require_jsonb_write(writer.writeString(value.data, value.size), "string payload");
    require_jsonb_write(writer.writeEndString(), "string end");
}

void write_jsonb_string(JsonbWriter& writer, const variant_json_detail::FormattedScalar& value) {
    write_jsonb_string(writer, {value.bytes.data(), value.size}, "formatted string");
}

void write_jsonb_binary(JsonbWriter& writer, StringRef value) {
    require_jsonb_write(writer.writeStartBinary(), "binary start");
    require_jsonb_write(writer.writeBinary(value.data, value.size), "binary payload");
    require_jsonb_write(writer.writeEndBinary(), "binary end");
}

class VariantToJsonbConverter {
public:
    VariantToJsonbConverter(JsonbWriter& writer, const VariantJsonFormatOptions& options)
            : _writer(writer), _options(options) {}

    void write(VariantValueRef value, uint32_t depth) {
        const VariantBasicType type = value.basic_type();
        const bool is_container =
                type == VariantBasicType::OBJECT || type == VariantBasicType::ARRAY;
        require_jsonb_depth(depth, is_container);
        switch (type) {
        case VariantBasicType::SHORT_STRING:
            write_jsonb_string(_writer, value.get_string(), "Variant string");
            return;
        case VariantBasicType::PRIMITIVE:
            write_primitive(value);
            return;
        case VariantBasicType::OBJECT:
            write_object(value, depth);
            return;
        case VariantBasicType::ARRAY:
            write_array(value, depth);
            return;
        }
    }

private:
    void write_decimal(VariantDecimal decimal) {
        uint32_t precision = 38;
        if (decimal.width == 4) {
            precision = 9;
        } else if (decimal.width == 8) {
            precision = 18;
        }
        require_decimal(decimal.unscaled, precision, decimal.scale, precision, 38,
                        "Variant decimal");
        if (decimal.width == 4) {
            require_jsonb_write(
                    _writer.writeDecimal(Decimal32 {static_cast<int32_t>(decimal.unscaled)},
                                         precision, decimal.scale),
                    "Decimal32 value");
        } else if (decimal.width == 8) {
            require_jsonb_write(
                    _writer.writeDecimal(Decimal64 {static_cast<int64_t>(decimal.unscaled)},
                                         precision, decimal.scale),
                    "Decimal64 value");
        } else {
            require_jsonb_write(
                    _writer.writeDecimal(Decimal128V3 {decimal.unscaled}, precision, decimal.scale),
                    "Decimal128 value");
        }
    }

    void write_primitive(VariantValueRef value) {
        switch (value.primitive_id()) {
        case VariantPrimitiveId::NULL_VALUE:
            require_jsonb_write(_writer.writeNull(), "null value");
            return;
        case VariantPrimitiveId::TRUE_VALUE:
            require_jsonb_write(_writer.writeBool(true), "true value");
            return;
        case VariantPrimitiveId::FALSE_VALUE:
            require_jsonb_write(_writer.writeBool(false), "false value");
            return;
        case VariantPrimitiveId::INT8:
            require_jsonb_write(_writer.writeInt8(static_cast<int8_t>(value.get_int())),
                                "int8 value");
            return;
        case VariantPrimitiveId::INT16:
            require_jsonb_write(_writer.writeInt16(static_cast<int16_t>(value.get_int())),
                                "int16 value");
            return;
        case VariantPrimitiveId::INT32:
            require_jsonb_write(_writer.writeInt32(static_cast<int32_t>(value.get_int())),
                                "int32 value");
            return;
        case VariantPrimitiveId::INT64:
            require_jsonb_write(_writer.writeInt64(value.get_int()), "int64 value");
            return;
        case VariantPrimitiveId::DOUBLE:
            require_jsonb_write(_writer.writeDouble(value.get_double()), "double value");
            return;
        case VariantPrimitiveId::DECIMAL4:
        case VariantPrimitiveId::DECIMAL8:
        case VariantPrimitiveId::DECIMAL16:
            write_decimal(value.get_decimal());
            return;
        case VariantPrimitiveId::DATE:
            write_jsonb_string(_writer, variant_json_detail::format_json_date(value.get_date()));
            return;
        case VariantPrimitiveId::TIMESTAMP_MICROS:
            write_jsonb_string(_writer,
                               variant_json_detail::format_json_timestamp(
                                       value.get_timestamp_micros(), 6, true, _options.timezone));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
            write_jsonb_string(_writer,
                               variant_json_detail::format_json_timestamp(
                                       value.get_timestamp_ntz_micros(), 6, false, nullptr));
            return;
        case VariantPrimitiveId::FLOAT:
            require_jsonb_write(_writer.writeFloat(value.get_float()), "float value");
            return;
        case VariantPrimitiveId::BINARY:
            write_jsonb_binary(_writer, value.get_binary());
            return;
        case VariantPrimitiveId::STRING:
            write_jsonb_string(_writer, value.get_string(), "Variant string");
            return;
        case VariantPrimitiveId::TIME_NTZ_MICROS:
            write_jsonb_string(_writer, variant_json_detail::format_json_time_micros(
                                                value.get_time_ntz_micros()));
            return;
        case VariantPrimitiveId::TIMESTAMP_NANOS:
            write_jsonb_string(_writer,
                               variant_json_detail::format_json_timestamp(
                                       value.get_timestamp_nanos(), 9, true, _options.timezone));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
            write_jsonb_string(_writer,
                               variant_json_detail::format_json_timestamp(
                                       value.get_timestamp_ntz_nanos(), 9, false, nullptr));
            return;
        case VariantPrimitiveId::UUID:
            write_jsonb_string(_writer, variant_json_detail::format_json_uuid(value.get_uuid()));
            return;
        }
        throw Exception(ErrorCode::CORRUPTION, "Unknown Variant primitive id");
    }

    void write_object(VariantValueRef value, uint32_t depth) {
        require_jsonb_write(_writer.writeStartObject(), "object start");
        const uint32_t count = value.num_elements();
        StringRef previous_key;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantValueRef child = value.object_value_at(index, &field_id);
            const StringRef key = value.metadata.key_at(field_id);
            variant_json_detail::require_valid_json_utf8(key, "Variant object key");
            if (key.size > std::numeric_limits<uint8_t>::max()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant object key length {} exceeds JSONB maximum {}", key.size,
                                std::numeric_limits<uint8_t>::max());
            }
            if (index != 0 && previous_key.compare(key) >= 0) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant object keys are not strictly byte-sorted at field {}",
                                index);
            }
            require_jsonb_write(_writer.writeKey(key.data, static_cast<uint8_t>(key.size)),
                                "object key");
            write(child, depth + 1);
            previous_key = key;
        }
        require_jsonb_write(_writer.writeEndObject(), "object end");
    }

    void write_array(VariantValueRef value, uint32_t depth) {
        require_jsonb_write(_writer.writeStartArray(), "array start");
        const uint32_t count = value.num_elements();
        for (uint32_t index = 0; index < count; ++index) {
            write(value.array_at(index), depth + 1);
        }
        require_jsonb_write(_writer.writeEndArray(), "array end");
    }

    JsonbWriter& _writer;
    const VariantJsonFormatOptions& _options;
};

} // namespace

struct JsonbToVariantEncoder::Impl {
    enum class State : uint8_t { COLLECTING, FINISHED, FAILED };

    explicit Impl(VariantBlockBuilder::ReserveHint hint) : builder(hint) {}

    void require_collecting() const {
        if (state == State::FINISHED) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSONB encoder is already finished");
        }
        if (state == State::FAILED) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant JSONB encoder is in a terminal failed state");
        }
    }

    VariantBlockBuilder builder;
    State state = State::COLLECTING;
};

JsonbToVariantEncoder::JsonbToVariantEncoder()
        : JsonbToVariantEncoder(VariantBlockBuilder::ReserveHint {}) {}
JsonbToVariantEncoder::JsonbToVariantEncoder(VariantBlockBuilder::ReserveHint hint)
        : _impl(std::make_unique<Impl>(hint)) {}
JsonbToVariantEncoder::~JsonbToVariantEncoder() = default;
JsonbToVariantEncoder::JsonbToVariantEncoder(JsonbToVariantEncoder&&) noexcept = default;
JsonbToVariantEncoder& JsonbToVariantEncoder::operator=(JsonbToVariantEncoder&&) noexcept = default;

void JsonbToVariantEncoder::add_null() {
    _impl->require_collecting();
    try {
        auto row = _impl->builder.begin_row();
        row.add_null();
        row.finish();
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

void JsonbToVariantEncoder::add_jsonb(StringRef document) {
    _impl->require_collecting();
    try {
        auto row = _impl->builder.begin_row();
        collect_jsonb_document(document, row);
        row.finish();
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

VariantEncodedBlock JsonbToVariantEncoder::finish_block() {
    _impl->require_collecting();
    try {
        VariantEncodedBlock block = _impl->builder.finish_block();
        _impl->state = Impl::State::FINISHED;
        return block;
    } catch (...) {
        _impl->state = Impl::State::FAILED;
        throw;
    }
}

void jsonb_to_variant(StringRef document, VariantBuilder& builder) {
    try {
        collect_jsonb_document(document, builder);
    } catch (...) {
        builder.abort();
        throw;
    }
}

void jsonb_to_variant(StringRef document, VariantBlockBuilder::Row& row, uint32_t initial_depth) {
    try {
        collect_jsonb_document(document, row, initial_depth);
    } catch (...) {
        row.abort();
        throw;
    }
}

void variant_to_jsonb(VariantValueRef value, JsonbWriter& writer,
                      const VariantJsonFormatOptions& options) {
    writer.reset();
    try {
        if (value.metadata.version() != VARIANT_ENCODING_VERSION) {
            throw Exception(ErrorCode::CORRUPTION, "Unsupported Variant metadata version {}",
                            value.metadata.version());
        }
        static_cast<void>(value.metadata.dict_size());
        variant_json_detail::require_exact_json_value(value);
        VariantToJsonbConverter(writer, options).write(value, 0);
    } catch (...) {
        writer.reset();
        throw;
    }
}

} // namespace doris
