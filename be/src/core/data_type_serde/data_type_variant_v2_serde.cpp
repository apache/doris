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

#include "core/data_type_serde/data_type_variant_v2_serde.h"

#include <arrow/array/builder_binary.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <orc/Vector.hh>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/column/variant_v2/variant_shredded_path.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVector<TYPE_UINT32>;

std::span<const NullMap::value_type> forced_nulls(const NullMap* null_map) {
    return null_map == nullptr
                   ? std::span<const NullMap::value_type> {}
                   : std::span<const NullMap::value_type> {null_map->data(), null_map->size()};
}

size_t checked_row(int64_t row) {
    if (row < 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant SerDe row {} is negative", row);
    }
    return static_cast<size_t>(row);
}

struct CountingWriter {
    void write(const char*, size_t size) {
        if (size > std::numeric_limits<size_t>::max() - count) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant JSON output is too large");
        }
        count += size;
    }
    size_t count = 0;
};

struct FixedWriter {
    void write(const char* data, size_t size) {
        DCHECK_LE(written, capacity);
        DCHECK_LE(size, capacity - written);
        std::memcpy(destination + written, data, size);
        written += size;
    }
    char* destination;
    size_t capacity;
    size_t written = 0;
};

template <typename Writer>
void write_json_value(VariantRef value, Writer& writer,
                      const DataTypeSerDe::FormatOptions& options) {
    VariantJsonFormatOptions json_options {.timezone = options.timezone};
    to_json(value, writer, json_options);
}

template <typename Writer>
void write_sql_value(VariantRef value, Writer& writer,
                     const DataTypeSerDe::FormatOptions& options) {
    VariantJsonFormatOptions json_options {.timezone = options.timezone};
    to_sql_string(value, writer, json_options);
}

constexpr size_t VARIANT_V2_TYPE_META_BYTES = sizeof(int32_t) * 4;
constexpr size_t VARIANT_V2_COLUMN_HEADER_BYTES = sizeof(bool) + sizeof(size_t) * 2;
static_assert(sizeof(bool) == sizeof(uint8_t));

enum class VariantV2WireRepresentation : uint8_t {
    ENCODED = 0,
    TYPED_SCALAR = 1,
    SHREDDED = 2,
};

constexpr uint8_t SHREDDED_WIRE_VERSION = 1;
constexpr size_t SHREDDED_WIRE_HEADER_BYTES = sizeof(uint8_t) * 2 + sizeof(uint64_t);

class ShreddedWireCursor {
public:
    ShreddedWireCursor(const char* data, size_t size) : _position(data), _remaining(size) {}

    template <typename T>
    T read(const char* description) {
        _require(sizeof(T), description);
        const T value = unaligned_load<T>(_position);
        _position += sizeof(T);
        _remaining -= sizeof(T);
        return value;
    }

    std::span<const char> read_bytes(uint64_t size, const char* description) {
        if (size > std::numeric_limits<size_t>::max()) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 {} length {} exceeds size_t", description, size));
        }
        const auto bytes = static_cast<size_t>(size);
        _require(bytes, description);
        const std::span<const char> result {_position, bytes};
        _position += bytes;
        _remaining -= bytes;
        return result;
    }

    const char* position() const { return _position; }
    size_t remaining() const { return _remaining; }

    void require_empty(const char* description) const {
        if (_remaining != 0) {
            throw Exception(Status::Corruption("Shredded ColumnVariantV2 {} has {} trailing bytes",
                                               description, _remaining));
        }
    }

private:
    void _require(size_t size, const char* description) const {
        if (size > _remaining) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 {} needs {} bytes but only {} remain", description,
                    size, _remaining));
        }
    }

    const char* _position;
    size_t _remaining;
};

struct SerializedColumnHeader {
    bool is_const;
    size_t logical_rows;
    size_t saved_rows;
};

SerializedColumnHeader read_serialized_column_header(ShreddedWireCursor& cursor,
                                                     const char* description) {
    const uint8_t const_flag = cursor.read<uint8_t>(description);
    if (const_flag > 1) {
        throw Exception(Status::Corruption("Shredded ColumnVariantV2 {} has invalid const flag {}",
                                           description, const_flag));
    }
    const auto logical_rows = cursor.read<size_t>(description);
    const auto saved_rows = cursor.read<size_t>(description);
    if ((const_flag != 0 && saved_rows != 1) || (const_flag == 0 && saved_rows != logical_rows)) {
        throw Exception(Status::Corruption(
                "Shredded ColumnVariantV2 {} has invalid row header: const={}, logical rows={}, "
                "saved rows={}",
                description, const_flag, logical_rows, saved_rows));
    }
    return {.is_const = const_flag != 0, .logical_rows = logical_rows, .saved_rows = saved_rows};
}

void add_serialized_size(int64_t& total, int64_t addition) {
    if (addition < 0 || total > std::numeric_limits<int64_t>::max() - addition) {
        throw Exception(ErrorCode::BUFFER_OVERFLOW,
                        "ColumnVariantV2 serialized size exceeds int64 limit");
    }
    total += addition;
}

void add_serialized_size(int64_t& total, size_t addition) {
    if (addition > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw Exception(ErrorCode::BUFFER_OVERFLOW,
                        "ColumnVariantV2 serialized size exceeds int64 limit");
    }
    add_serialized_size(total, static_cast<int64_t>(addition));
}

void add_serialized_array_size(int64_t& total, size_t count, size_t element_size) {
    if (element_size != 0 && count > std::numeric_limits<size_t>::max() / element_size) {
        throw Exception(ErrorCode::BUFFER_OVERFLOW,
                        "ColumnVariantV2 serialized array size overflows size_t");
    }
    add_serialized_size(total, count * element_size);
}

uint32_t checked_wire_count(size_t value, const char* name) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::BUFFER_OVERFLOW,
                        "ColumnVariantV2 {} count {} exceeds uint32 limit", name, value);
    }
    return static_cast<uint32_t>(value);
}

uint64_t checked_wire_length(int64_t value, const char* name) {
    if (value < 0) {
        throw Exception(ErrorCode::BUFFER_OVERFLOW, "ColumnVariantV2 {} length {} is negative",
                        name, value);
    }
    return static_cast<uint64_t>(value);
}

VariantV2WireRepresentation wire_representation(const ColumnVariantV2& column) {
    switch (column.representation()) {
    case ColumnVariantV2::Representation::ENCODED:
        return VariantV2WireRepresentation::ENCODED;
    case ColumnVariantV2::Representation::TYPED_SCALAR:
        return VariantV2WireRepresentation::TYPED_SCALAR;
    case ColumnVariantV2::Representation::SHREDDED:
        return VariantV2WireRepresentation::SHREDDED;
    }
    __builtin_unreachable();
}

void validate_shredded_wire_fields(const ColumnVariantV2& residual,
                                   const ColumnVariantV2::ShreddedFields& fields) {
    const auto residual_view = residual.read_view();
    for (const auto& field : fields) {
        const auto& values = assert_cast<const ColumnVariantV2&>(*field.values);
        const auto values_view = values.read_view();
        const auto& presence = static_cast<const ColumnUInt8::Ptr&>(field.presence)->get_data();
        const auto& parts = field.path.get_parts();
        for (size_t row = 0; row < presence.size(); ++row) {
            if (presence[row] == 0) {
                continue;
            }
            if (values.is_encoded()) {
                const VariantBasicType basic_type = values_view.value_at(row).basic_type();
                if (basic_type == VariantBasicType::OBJECT ||
                    basic_type == VariantBasicType::ARRAY) {
                    throw Exception(Status::Corruption(
                            "ColumnVariantV2 active shredded field must be scalar at path {} row "
                            "{}",
                            field.path.get_path(), row));
                }
            }
            VariantRef current = residual_view.value_at(row);
            for (size_t depth = 0; depth < parts.size(); ++depth) {
                if (current.basic_type() != VariantBasicType::OBJECT) {
                    throw Exception(Status::Corruption(
                            "ColumnVariantV2 residual has a scalar or array ancestor of shredded "
                            "path {} at row {}",
                            field.path.get_path(), row));
                }
                VariantRef child;
                const auto& key = parts[depth].key;
                if (!current.object_find({key.data(), key.size()}, &child)) {
                    break;
                }
                if (depth + 1 == parts.size()) {
                    throw Exception(Status::Corruption(
                            "ColumnVariantV2 residual overlaps a present shredded field at path "
                            "{} at row {}",
                            field.path.get_path(), row));
                }
                current = child;
            }
        }
    }
}

PathInData deserialize_shredded_path(ShreddedWireCursor& cursor, uint32_t field_index,
                                     const PathInData* previous) {
    const auto part_count = cursor.read<uint32_t>("path part count");
    if (part_count == 0) {
        throw Exception(Status::Corruption("Shredded ColumnVariantV2 field {} has an empty path",
                                           field_index));
    }
    if (part_count > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(Status::Corruption(
                "Shredded ColumnVariantV2 field {} path depth {} exceeds maximum {}", field_index,
                part_count, VARIANT_MAX_NESTING_DEPTH));
    }

    std::vector<std::string> keys;
    PathInData::Parts parts;
    keys.reserve(part_count);
    parts.reserve(part_count);
    for (uint32_t part_index = 0; part_index < part_count; ++part_index) {
        const auto key_size = cursor.read<uint32_t>("path key length");
        const auto key = cursor.read_bytes(key_size, "path key");
        keys.emplace_back(key.data(), key.size());
        if (!keys.back().empty() && !validate_utf8(keys.back().data(), keys.back().size())) {
            throw Exception(
                    Status::Corruption("Shredded ColumnVariantV2 path part is not valid UTF-8"));
        }
        const auto is_nested = cursor.read<uint8_t>("path nested flag");
        const auto anonymous_array_level = cursor.read<uint8_t>("path array level");
        if (is_nested > 1) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 path part has invalid nested flag {}", is_nested));
        }
        if (is_nested != 0 || anonymous_array_level != 0) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 does not support array path parts"));
        }
        parts.emplace_back(keys.back(), is_nested != 0, anonymous_array_level);
    }

    PathInData path(parts);
    if (previous != nullptr) {
        if (!variant_shredded_path_less(*previous, path)) {
            throw Exception(
                    Status::Corruption("Shredded ColumnVariantV2 paths are not strictly ordered"));
        }
        if (variant_shredded_path_is_prefix(*previous, path)) {
            throw Exception(
                    Status::Corruption("Shredded ColumnVariantV2 paths are not prefix-free"));
        }
    }
    return path;
}

ColumnUInt8::MutablePtr deserialize_shredded_presence(ShreddedWireCursor& cursor,
                                                      size_t saved_rows) {
    const auto presence_count = cursor.read<uint64_t>("presence row count");
    if (presence_count != static_cast<uint64_t>(saved_rows)) {
        throw Exception(Status::Corruption(
                "Shredded ColumnVariantV2 presence row count {} does not match saved row count {}",
                presence_count, saved_rows));
    }
    const auto presence_bytes = cursor.read_bytes(presence_count, "presence values");
    auto presence = ColumnUInt8::create();
    auto& data = presence->get_data();
    data.resize(saved_rows);
    if (!presence_bytes.empty()) {
        std::memcpy(data.data(), presence_bytes.data(), presence_bytes.size());
    }
    if (!std::ranges::all_of(data, [](uint8_t value) { return value <= 1; })) {
        throw Exception(Status::Corruption(
                "Shredded ColumnVariantV2 presence contains a value other than zero or one"));
    }
    return presence;
}

const ColumnVariantV2& get_variant_v2_column(const IColumn& column) {
    const IColumn* physical = &column;
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        physical = &constant->get_data_column();
    }
    return assert_cast<const ColumnVariantV2&>(*physical);
}

void write_variant_v2_type(const DataTypePtr& type, char*& buf) {
    uint32_t precision = type->get_precision();
    uint32_t scale = type->get_scale();
    if (type->get_primitive_type() == TYPE_DECIMALV2) {
        const auto& decimal = assert_cast<const DataTypeDecimalV2&>(*type);
        precision = decimal.get_original_precision();
        scale = decimal.get_original_scale();
    }
    const int32_t length = is_string_type(type->get_primitive_type())
                                   ? assert_cast<const DataTypeString&>(*type).len()
                                   : -1;
    unaligned_store<int32_t>(buf, static_cast<int32_t>(type->get_primitive_type()));
    buf += sizeof(int32_t);
    unaligned_store<uint32_t>(buf, precision);
    buf += sizeof(uint32_t);
    unaligned_store<uint32_t>(buf, scale);
    buf += sizeof(uint32_t);
    unaligned_store<int32_t>(buf, length);
    buf += sizeof(int32_t);
}

DataTypePtr read_variant_v2_type(const char*& buf) {
    const auto primitive = static_cast<PrimitiveType>(unaligned_load<int32_t>(buf));
    buf += sizeof(int32_t);
    const auto precision = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);
    const auto scale = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);
    const auto length = unaligned_load<int32_t>(buf);
    buf += sizeof(int32_t);
    if (!is_supported_variant_typed_identity(primitive)) {
        throw Exception(Status::Corruption("Unsupported Variant V2 typed identity {}",
                                           static_cast<int32_t>(primitive)));
    }
    return DataTypeFactory::instance().create_data_type(primitive, false, precision, scale, length);
}

char* serialize_meta_ids(const IColumn& column, char* buf) {
    const auto& ids = assert_cast<const MetaIdsColumn&>(column).get_data();
    unaligned_store<size_t>(buf, ids.size());
    buf += sizeof(size_t);
    const size_t bytes = ids.size() * sizeof(uint32_t);
    if (bytes != 0) {
        std::memcpy(buf, ids.data(), bytes);
    }
    return buf + bytes;
}

const char* deserialize_meta_ids(const char* buf, MutableColumnPtr* column) {
    const auto size = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);
    auto& ids = assert_cast<MetaIdsColumn&>(**column).get_data();
    ids.resize(size);
    const size_t bytes = size * sizeof(uint32_t);
    if (bytes != 0) {
        std::memcpy(ids.data(), buf, bytes);
    }
    return buf + bytes;
}

void preflight_json(const IColumn& column, size_t start, size_t end,
                    const DataTypeSerDe::FormatOptions& options) {
    visit_variant_v2_values(
            column, start, end, {}, [](size_t) {},
            [&](size_t, VariantRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
            });
}

} // namespace

DataTypeVariantV2SerDe::DataTypeVariantV2SerDe(int nesting_level) : DataTypeSerDe(nesting_level) {}

int64_t DataTypeVariantV2SerDe::get_encoded_payload_size(const ColumnVariantV2& column,
                                                         int be_exec_version) {
    const DataTypeString string_type;
    int64_t size = 0;
    add_serialized_size(size, string_type.get_uncompressed_serialized_bytes(*column._metadatas,
                                                                            be_exec_version));
    add_serialized_size(size, sizeof(size_t));
    add_serialized_array_size(size, column._meta_ids->size(), sizeof(uint32_t));
    add_serialized_size(
            size, string_type.get_uncompressed_serialized_bytes(*column._values, be_exec_version));
    return size;
}

char* DataTypeVariantV2SerDe::serialize_encoded_payload(const ColumnVariantV2& column, char* buf,
                                                        int be_exec_version) {
    const DataTypeString string_type;
    buf = string_type.serialize(*column._metadatas, buf, be_exec_version);
    buf = serialize_meta_ids(*column._meta_ids, buf);
    return string_type.serialize(*column._values, buf, be_exec_version);
}

const char* DataTypeVariantV2SerDe::deserialize_encoded_payload(const char* buf,
                                                                ColumnVariantV2* column,
                                                                int be_exec_version) {
    const DataTypeString string_type;
    MutableColumnPtr metadatas = string_type.create_column();
    MutableColumnPtr meta_ids = MetaIdsColumn::create();
    MutableColumnPtr values = string_type.create_column();
    buf = string_type.deserialize(buf, &metadatas, be_exec_version);
    buf = deserialize_meta_ids(buf, &meta_ids);
    buf = string_type.deserialize(buf, &values, be_exec_version);

    const auto& ids = assert_cast<const MetaIdsColumn&>(*meta_ids).get_data();
    if (ids.size() != values->size()) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 metadata id count {} does not match value count {}", ids.size(),
                values->size()));
    }
    for (uint32_t id : ids) {
        if (id >= metadatas->size()) {
            throw Exception(
                    Status::Corruption("ColumnVariantV2 metadata id {} exceeds metadata count {}",
                                       id, metadatas->size()));
        }
    }

    auto decoded = ColumnVariantV2::create();
    static_cast<ColumnString::Ptr&>(decoded->_metadatas) =
            ColumnString::cast_to_column_ptr(assert_cast<const ColumnString*>(metadatas.get()));
    static_cast<MetaIdsColumn::Ptr&>(decoded->_meta_ids) =
            MetaIdsColumn::cast_to_column_ptr(assert_cast<const MetaIdsColumn*>(meta_ids.get()));
    static_cast<ColumnString::Ptr&>(decoded->_values) =
            ColumnString::cast_to_column_ptr(assert_cast<const ColumnString*>(values.get()));
    decoded->sanity_check();
    column->_adopt_state_from(*decoded);
    return buf;
}

int64_t DataTypeVariantV2SerDe::get_non_shredded_payload_size(const ColumnVariantV2& column,
                                                              int be_exec_version) {
    if (column.is_shredded()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Nested shredded ColumnVariantV2 payload is not supported");
    }
    int64_t size = sizeof(uint8_t);
    if (column.is_encoded()) {
        add_serialized_size(size, get_encoded_payload_size(column, be_exec_version));
        return size;
    }
    add_serialized_size(size, VARIANT_V2_TYPE_META_BYTES);
    const DataTypePtr nullable_type = make_nullable(column._typed_type);
    add_serialized_size(size, nullable_type->get_uncompressed_serialized_bytes(*column._typed,
                                                                               be_exec_version));
    return size;
}

char* DataTypeVariantV2SerDe::serialize_non_shredded_payload(const ColumnVariantV2& column,
                                                             char* buf, int be_exec_version) {
    if (column.is_shredded()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Nested shredded ColumnVariantV2 payload is not supported");
    }
    const auto representation = wire_representation(column);
    unaligned_store<uint8_t>(buf, static_cast<uint8_t>(representation));
    buf += sizeof(uint8_t);
    if (column.is_encoded()) {
        return serialize_encoded_payload(column, buf, be_exec_version);
    }
    write_variant_v2_type(column._typed_type, buf);
    return make_nullable(column._typed_type)->serialize(*column._typed, buf, be_exec_version);
}

const char* DataTypeVariantV2SerDe::deserialize_non_shredded_payload(const char* buf,
                                                                     MutableColumnPtr* column,
                                                                     int be_exec_version) {
    const auto representation =
            static_cast<VariantV2WireRepresentation>(unaligned_load<uint8_t>(buf));
    buf += sizeof(uint8_t);
    if (representation == VariantV2WireRepresentation::ENCODED) {
        auto decoded = ColumnVariantV2::create();
        buf = deserialize_encoded_payload(buf, decoded.get(), be_exec_version);
        *column = std::move(decoded);
        return buf;
    }
    if (representation == VariantV2WireRepresentation::TYPED_SCALAR) {
        DataTypePtr type = read_variant_v2_type(buf);
        const DataTypePtr nullable_type = make_nullable(type);
        MutableColumnPtr typed_column = nullable_type->create_column();
        buf = nullable_type->deserialize(buf, &typed_column, be_exec_version);
        *column = ColumnVariantV2::create_typed(std::move(typed_column), std::move(type));
        return buf;
    }
    if (representation == VariantV2WireRepresentation::SHREDDED) {
        throw Exception(Status::Corruption(
                "Nested shredded ColumnVariantV2 field payload is not supported"));
    }
    throw Exception(Status::Corruption("Unknown ColumnVariantV2 representation tag {}",
                                       static_cast<uint8_t>(representation)));
}

int64_t DataTypeVariantV2SerDe::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                  int be_exec_version) {
    const auto& variant = get_variant_v2_column(column);
    int64_t size = VARIANT_V2_COLUMN_HEADER_BYTES;
    if (!variant.is_shredded()) {
        add_serialized_size(size, get_non_shredded_payload_size(variant, be_exec_version));
        return size;
    }

    add_serialized_size(size, SHREDDED_WIRE_HEADER_BYTES);
    add_serialized_size(size, sizeof(uint64_t));
    add_serialized_size(size, get_encoded_payload_size(variant, be_exec_version));
    checked_wire_count(variant._shredded_fields.size(), "shredded field");
    add_serialized_size(size, sizeof(uint32_t));
    for (const auto& field : variant._shredded_fields) {
        const auto& parts = field.path.get_parts();
        checked_wire_count(parts.size(), "path part");
        add_serialized_size(size, sizeof(uint32_t));
        for (const auto& part : parts) {
            checked_wire_count(part.key.size(), "path key byte");
            add_serialized_size(size, sizeof(uint32_t));
            add_serialized_size(size, part.key.size());
            add_serialized_size(size, sizeof(uint8_t) * 2);
        }
        const auto& presence = static_cast<const ColumnUInt8::Ptr&>(field.presence)->get_data();
        add_serialized_size(size, sizeof(uint64_t));
        add_serialized_size(size, presence.size());
        add_serialized_size(size, sizeof(uint64_t));
        add_serialized_size(
                size, get_non_shredded_payload_size(
                              assert_cast<const ColumnVariantV2&>(*field.values), be_exec_version));
    }
    return size;
}

char* DataTypeVariantV2SerDe::serialize(const IColumn& column, char* buf, int be_exec_version) {
    const IColumn* physical = &column;
    size_t saved_rows = 0;
    buf = serialize_const_flag_and_row_num(&physical, buf, &saved_rows);
    const auto& variant = assert_cast<const ColumnVariantV2&>(*physical);
    DCHECK_EQ(variant.size(), saved_rows);

    if (!variant.is_shredded()) {
        return serialize_non_shredded_payload(variant, buf, be_exec_version);
    }

    unaligned_store<uint8_t>(buf, static_cast<uint8_t>(VariantV2WireRepresentation::SHREDDED));
    buf += sizeof(uint8_t);
    unaligned_store<uint8_t>(buf, SHREDDED_WIRE_VERSION);
    buf += sizeof(uint8_t);
    char* payload_size_position = buf;
    buf += sizeof(uint64_t);
    char* const payload_begin = buf;

    char* residual_size_position = buf;
    buf += sizeof(uint64_t);
    char* const residual_begin = buf;
    buf = serialize_encoded_payload(variant, buf, be_exec_version);
    unaligned_store<uint64_t>(
            residual_size_position,
            checked_wire_length(buf - residual_begin, "shredded residual payload"));
    unaligned_store<uint32_t>(
            buf, checked_wire_count(variant._shredded_fields.size(), "shredded field"));
    buf += sizeof(uint32_t);
    for (const auto& field : variant._shredded_fields) {
        const auto& parts = field.path.get_parts();
        unaligned_store<uint32_t>(buf, checked_wire_count(parts.size(), "path part"));
        buf += sizeof(uint32_t);
        for (const auto& part : parts) {
            unaligned_store<uint32_t>(buf, checked_wire_count(part.key.size(), "path key byte"));
            buf += sizeof(uint32_t);
            if (!part.key.empty()) {
                std::memcpy(buf, part.key.data(), part.key.size());
                buf += part.key.size();
            }
            unaligned_store<uint8_t>(buf, part.is_nested ? 1 : 0);
            buf += sizeof(uint8_t);
            unaligned_store<uint8_t>(buf, part.anonymous_array_level);
            buf += sizeof(uint8_t);
        }
        const auto& presence = static_cast<const ColumnUInt8::Ptr&>(field.presence)->get_data();
        unaligned_store<uint64_t>(buf, presence.size());
        buf += sizeof(uint64_t);
        if (!presence.empty()) {
            std::memcpy(buf, presence.data(), presence.size());
            buf += presence.size();
        }
        char* child_size_position = buf;
        buf += sizeof(uint64_t);
        char* const child_begin = buf;
        buf = serialize_non_shredded_payload(assert_cast<const ColumnVariantV2&>(*field.values),
                                             buf, be_exec_version);
        unaligned_store<uint64_t>(child_size_position,
                                  checked_wire_length(buf - child_begin, "shredded child payload"));
    }
    unaligned_store<uint64_t>(payload_size_position,
                              checked_wire_length(buf - payload_begin, "shredded payload"));
    return buf;
}

const char* DataTypeVariantV2SerDe::deserialize(const char* buf, MutableColumnPtr* column,
                                                int be_exec_version) {
    constexpr size_t REPRESENTATION_OFFSET = VARIANT_V2_COLUMN_HEADER_BYTES;
    const char* const representation_position = buf + REPRESENTATION_OFFSET;
    const auto representation = static_cast<VariantV2WireRepresentation>(
            unaligned_load<uint8_t>(representation_position));
    if (representation == VariantV2WireRepresentation::SHREDDED) {
        const char* const payload_size_position = representation_position + sizeof(uint8_t) * 2;
        const uint64_t payload_size = unaligned_load<uint64_t>(payload_size_position);
        const uintptr_t payload_address =
                reinterpret_cast<uintptr_t>(payload_size_position) + sizeof(uint64_t);
        if (payload_size > std::numeric_limits<uintptr_t>::max() - payload_address) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 payload length {} overflows address space",
                    payload_size));
        }
        return deserialize(buf, reinterpret_cast<const char*>(payload_address + payload_size),
                           column, be_exec_version);
    }

    auto* destination = assert_cast<ColumnVariantV2*>(column->get());
    const auto is_const_flag = unaligned_load<uint8_t>(buf);
    if (is_const_flag > 1) {
        throw Exception(
                Status::Corruption("ColumnVariantV2 has invalid const flag {}", is_const_flag));
    }
    const bool is_const = is_const_flag != 0;
    buf += sizeof(bool);
    const auto logical_rows = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);
    const auto saved_rows = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);
    if ((is_const && saved_rows != 1) || (!is_const && saved_rows != logical_rows)) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 invalid row header: const={}, logical rows={}, saved rows={}",
                is_const, logical_rows, saved_rows));
    }

    MutableColumnPtr decoded;
    if (representation != VariantV2WireRepresentation::ENCODED &&
        representation != VariantV2WireRepresentation::TYPED_SCALAR) {
        throw Exception(Status::Corruption("Unknown ColumnVariantV2 representation tag {}",
                                           static_cast<uint8_t>(representation)));
    }
    buf = deserialize_non_shredded_payload(buf, &decoded, be_exec_version);

    const auto& decoded_variant = assert_cast<const ColumnVariantV2&>(*decoded);
    if (decoded_variant.size() != saved_rows) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 saved row count {} does not match decoded row count {}",
                saved_rows, decoded_variant.size()));
    }
    if (is_const) {
        ColumnPtr decoded_data = std::move(decoded);
        *column = ColumnConst::create(std::move(decoded_data), logical_rows);
    } else {
        destination->_adopt_state_from(assert_cast<ColumnVariantV2&>(*decoded));
    }
    return buf;
}

const char* DataTypeVariantV2SerDe::deserialize(const char* buf, const char* end,
                                                MutableColumnPtr* column, int be_exec_version) {
    if (buf == nullptr || end == nullptr || end < buf) {
        throw Exception(Status::Corruption("ColumnVariantV2 has an invalid wire buffer"));
    }

    ShreddedWireCursor cursor(buf, static_cast<size_t>(end - buf));
    const auto header = read_serialized_column_header(cursor, "column header");
    const char* const representation_position = cursor.position();
    const auto representation =
            static_cast<VariantV2WireRepresentation>(cursor.read<uint8_t>("representation header"));

    MutableColumnPtr decoded;
    const char* result = nullptr;
    if (representation == VariantV2WireRepresentation::ENCODED ||
        representation == VariantV2WireRepresentation::TYPED_SCALAR) {
        result = deserialize_non_shredded_payload(representation_position, &decoded,
                                                  be_exec_version);
        if (result > end) {
            throw Exception(
                    Status::Corruption("ColumnVariantV2 payload exceeds the provided wire buffer"));
        }
    } else if (representation == VariantV2WireRepresentation::SHREDDED) {
        const auto wire_version = cursor.read<uint8_t>("shredded wire version");
        if (wire_version != SHREDDED_WIRE_VERSION) {
            throw Exception(Status::Corruption(
                    "Unsupported shredded ColumnVariantV2 wire version {}", wire_version));
        }
        const auto payload_size = cursor.read<uint64_t>("shredded payload length");
        const auto payload = cursor.read_bytes(payload_size, "shredded payload");
        result = cursor.position();

        ShreddedWireCursor payload_cursor(payload.data(), payload.size());
        const auto residual_size = payload_cursor.read<uint64_t>("residual payload length");
        const auto residual_payload = payload_cursor.read_bytes(residual_size, "residual payload");

        auto residual = ColumnVariantV2::create();
        const char* const residual_end = deserialize_encoded_payload(
                residual_payload.data(), residual.get(), be_exec_version);
        if (residual_end != residual_payload.data() + residual_payload.size()) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 residual length does not match its framing"));
        }
        if (residual->size() != header.saved_rows) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 residual row count {} does not match saved row "
                    "count {}",
                    residual->size(), header.saved_rows));
        }

        const auto field_count = payload_cursor.read<uint32_t>("shredded field count");
        if (field_count == 0) {
            throw Exception(
                    Status::Corruption("Shredded ColumnVariantV2 wire payload has no fields"));
        }
        constexpr size_t MINIMUM_FIELD_WIRE_BYTES =
                sizeof(uint32_t) * 2 + sizeof(uint8_t) * 2 + sizeof(uint64_t) * 2 + sizeof(uint8_t);
        if (header.saved_rows > std::numeric_limits<size_t>::max() - MINIMUM_FIELD_WIRE_BYTES ||
            field_count >
                    payload_cursor.remaining() / (MINIMUM_FIELD_WIRE_BYTES + header.saved_rows)) {
            throw Exception(Status::Corruption(
                    "Shredded ColumnVariantV2 field count {} exceeds the payload boundary",
                    field_count));
        }
        ColumnVariantV2::ShreddedFields fields;
        fields.reserve(field_count);
        for (uint32_t field_index = 0; field_index < field_count; ++field_index) {
            const PathInData* previous = fields.empty() ? nullptr : &fields.back().path;
            auto path = deserialize_shredded_path(payload_cursor, field_index, previous);
            auto presence = deserialize_shredded_presence(payload_cursor, header.saved_rows);
            const auto child_size = payload_cursor.read<uint64_t>("child payload length");
            const auto child_payload = payload_cursor.read_bytes(child_size, "child payload");
            ShreddedWireCursor child_header(child_payload.data(), child_payload.size());
            const auto child_representation = static_cast<VariantV2WireRepresentation>(
                    child_header.read<uint8_t>("child representation"));
            if (child_representation != VariantV2WireRepresentation::ENCODED &&
                child_representation != VariantV2WireRepresentation::TYPED_SCALAR) {
                throw Exception(Status::Corruption(
                        "Unsupported shredded ColumnVariantV2 child representation {}",
                        static_cast<uint8_t>(child_representation)));
            }

            MutableColumnPtr values;
            const char* const child_end = deserialize_non_shredded_payload(
                    child_payload.data(), &values, be_exec_version);
            if (child_end != child_payload.data() + child_payload.size()) {
                throw Exception(Status::Corruption(
                        "Shredded ColumnVariantV2 child length does not match its framing"));
            }
            if (values->size() != header.saved_rows) {
                throw Exception(Status::Corruption(
                        "Shredded ColumnVariantV2 child row count {} does not match saved row "
                        "count {}",
                        values->size(), header.saved_rows));
            }
            fields.emplace_back(std::move(path), std::move(values), std::move(presence));
        }
        payload_cursor.require_empty("payload");
        validate_shredded_wire_fields(*residual, fields);
        decoded = ColumnVariantV2::_create_shredded_from_valid_parts(std::move(residual),
                                                                     std::move(fields), true);
    } else {
        throw Exception(Status::Corruption("Unknown ColumnVariantV2 representation header {}",
                                           static_cast<uint8_t>(representation)));
    }

    const auto& decoded_variant = assert_cast<const ColumnVariantV2&>(*decoded);
    if (decoded_variant.size() != header.saved_rows) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 saved row count {} does not match decoded row count {}",
                header.saved_rows, decoded_variant.size()));
    }
    if (header.is_const) {
        ColumnPtr decoded_data = std::move(decoded);
        *column = ColumnConst::create(std::move(decoded_data), header.logical_rows);
    } else {
        auto* destination = assert_cast<ColumnVariantV2*>(column->get());
        destination->_adopt_state_from(assert_cast<ColumnVariantV2&>(*decoded));
    }
    return result;
}

std::string DataTypeVariantV2SerDe::get_name() const {
    return "Variant";
}

Status DataTypeVariantV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t row = checked_row(row_num);
        preflight_json(column, row, row + 1, options);
        visit_variant_v2_values(
                column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) { write_json_value(value, bw, options); });
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                        int64_t end_idx, BufferWritable& bw,
                                                        FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t start = checked_row(start_idx);
        const size_t end = checked_row(end_idx);
        preflight_json(column, start, end, options);
        visit_variant_v2_values(
                column, start, end, {}, [](size_t) {},
                [&](size_t row, VariantRef value) {
                    if (row != start) {
                        bw.write(options.field_delim.data(), options.field_delim.size());
                    }
                    write_json_value(value, bw, options);
                });
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_pb(const IColumn& column, PValues&, int64_t,
                                                  int64_t) const {
    return Status::NotSupported("write_column_to_pb with type " + column.get_name());
}

Status DataTypeVariantV2SerDe::read_column_from_pb(IColumn& column, const PValues&) const {
    return Status::NotSupported("read_column_from_pb with type " + column.get_name());
}

Status DataTypeVariantV2SerDe::read_column_from_arrow(IColumn& column, const arrow::Array*, int64_t,
                                                      int64_t, const cctz::time_zone&) const {
    return Status::Error(ErrorCode::NOT_IMPLEMENTED_ERROR,
                         "read_column_from_arrow with type " + column.get_name());
}

namespace {

ColumnVariantV2& destination(IColumn& column) {
    auto* result = check_and_get_column<ColumnVariantV2>(column);
    if (result == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 SerDe destination requires ColumnVariantV2, got {}",
                        column.get_name());
    }
    return *result;
}

void require_jsonb_write(bool written, const char* operation) {
    if (!written) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "JSONB writer rejected {}", operation);
    }
}

} // namespace

Status DataTypeVariantV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions&) const {
    RETURN_IF_CATCH_EXCEPTION({
        JsonStringToVariantEncoder encoder;
        encoder.add_json({slice.data, slice.size});
        VariantBatchBuilder block = encoder.finish_batch();
        destination(column).insert_encoded_batch(block);
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                                             const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        if (slice.size != 0 && slice.data == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant CSV input has a null data pointer");
        }
        if (slice.size != 0 && options.escape_char != 0) {
            escape_string_for_csv(slice.data, &slice.size, options.escape_char, options.quote_char);
        }
        VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = 1});
        auto row = builder.begin_row();
        row.add_string({slice.data, slice.size});
        row.finish();
        VariantBatchBuilder block = builder.finish_batch();
        destination(column).insert_encoded_batch(block);
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                   std::vector<Slice>& slices,
                                                                   uint64_t* num_deserialized,
                                                                   const FormatOptions&) const {
    if (num_deserialized == nullptr) {
        return Status::InvalidArgument("Variant JSON deserialized counter is null");
    }
    if (slices.size() > std::numeric_limits<uint64_t>::max() - *num_deserialized) {
        return Status::InvalidArgument("Variant JSON deserialized counter overflows");
    }
    RETURN_IF_CATCH_EXCEPTION({
        ColumnVariantV2& result = destination(column);
        if (slices.empty()) {
            return Status::OK();
        }
        JsonStringToVariantEncoder encoder;
        for (const Slice& slice : slices) {
            encoder.add_json({slice.data, slice.size});
        }
        VariantBatchBuilder block = encoder.finish_batch();
        result.insert_encoded_batch(block);
        *num_deserialized += slices.size();
    });
    return Status::OK();
}

void DataTypeVariantV2SerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena&, int32_t col_id, int64_t row_num,
                                                     const FormatOptions& options) const {
    const size_t row = checked_row(row_num);
    JsonbWriter document;
    visit_variant_v2_values(
            column, row, row + 1, {}, [](size_t) {},
            [&](size_t, VariantRef value) {
                variant_to_jsonb(value, document, {.timezone = options.timezone});
            });
    // DataTypeNullableSerDe pre-writes the key for a non-null nested value. A direct row-store call
    // does not. writeKey() is side-effect free when the writer is already waiting for a value, so
    // attempting it supports both call shapes; writeStartBinary() remains the state validation.
    static_cast<void>(result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id)));
    require_jsonb_write(result.writeStartBinary(), "binary start");
    require_jsonb_write(
            result.writeBinary(document.getOutput()->getBuffer(), document.getOutput()->getSize()),
            "binary payload");
    require_jsonb_write(result.writeEndBinary(), "binary end");
}

void DataTypeVariantV2SerDe::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    if (arg == nullptr || !arg->isBinary()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 row-store value must be a binary JSONB document");
    }
    const auto* binary = arg->unpack<JsonbBinaryVal>();
    JsonbToVariantEncoder encoder;
    encoder.add_jsonb({binary->getBlob(), binary->getBlobLen()});
    VariantBatchBuilder block = encoder.finish_batch();
    destination(column).insert_encoded_batch(block);
}

namespace {

DorisVector<size_t> json_lengths(const IColumn& column, size_t start, size_t end,
                                 const NullMap* null_map,
                                 const DataTypeSerDe::FormatOptions& options) {
    if (start > end || end > column.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant row range [{}, {}) exceeds column size {}", start, end,
                        column.size());
    }
    DorisVector<size_t> lengths(end - start, 0);
    visit_variant_v2_values(
            column, start, end, forced_nulls(null_map), [](size_t) {},
            [&](size_t row, VariantRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
                lengths[row - start] = writer.count;
            });
    return lengths;
}

template <typename Builder>
Status write_arrow(const IColumn& column, const NullMap* null_map, Builder& builder, size_t start,
                   size_t end, const DataTypeSerDe::FormatOptions& options) {
    const DorisVector<size_t> lengths = json_lengths(column, start, end, null_map, options);
    const size_t maximum = lengths.empty() ? 0 : *std::ranges::max_element(lengths);
    if (maximum > static_cast<size_t>(std::numeric_limits<typename Builder::offset_type>::max())) {
        return Status::InvalidArgument("Variant JSON value exceeds Arrow offset range");
    }
    DorisVector<char> rendered(maximum);
    Status status = Status::OK();
    visit_variant_v2_values(
            column, start, end, forced_nulls(null_map),
            [&](size_t) {
                if (status.ok()) {
                    status = checkArrowStatus(builder.AppendNull(), column, builder);
                }
            },
            [&](size_t row, VariantRef value) {
                if (!status.ok()) {
                    return;
                }
                FixedWriter writer {.destination = rendered.data(),
                                    .capacity = lengths[row - start]};
                write_json_value(value, writer, options);
                DCHECK_EQ(writer.written, lengths[row - start]);
                status = checkArrowStatus(
                        builder.Append(rendered.data(),
                                       cast_set<typename Builder::offset_type>(writer.written)),
                        column, builder);
            });
    return status;
}

} // namespace

void DataTypeVariantV2SerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                       const FormatOptions& options) const {
    visit_variant_v2_values(
            column, row_num, row_num + 1, {}, [](size_t) {},
            [&](size_t, VariantRef value) {
                if (_nesting_level > 1) {
                    write_json_value(value, bw, options);
                } else {
                    write_sql_value(value, bw, options);
                }
            });
}

Status DataTypeVariantV2SerDe::write_column_to_mysql_binary(const IColumn& column,
                                                            MysqlRowBinaryBuffer& row_buffer,
                                                            int64_t row_idx, bool col_const,
                                                            const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t row = col_const ? 0 : checked_row(row_idx);
        CountingWriter counter;
        visit_variant_v2_values(
                column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) { write_sql_value(value, counter, options); });
        const size_t rendered_size = counter.count;
        DorisVector<char> rendered(rendered_size == 0 ? 1 : rendered_size);
        visit_variant_v2_values(
                column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) {
                    FixedWriter writer {.destination = rendered.data(), .capacity = rendered_size};
                    write_sql_value(value, writer, options);
                    DCHECK_EQ(writer.written, rendered_size);
                });
        if (row_buffer.push_string(rendered.data(), rendered_size) != 0) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Failed to pack Variant MySQL buffer");
        }
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    if (array_builder == nullptr) {
        return Status::InvalidArgument("Variant Arrow builder is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        FormatOptions options;
        options.timezone = &ctz;
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        if (array_builder->type()->id() == arrow::Type::STRING) {
            return write_arrow(column, null_map, assert_cast<arrow::StringBuilder&>(*array_builder),
                               first, last, options);
        }
        if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
            return write_arrow(column, null_map,
                               assert_cast<arrow::LargeStringBuilder&>(*array_builder), first, last,
                               options);
        }
        return Status::InvalidArgument("Unsupported arrow type for variant column: {}",
                                       array_builder->type()->name());
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_orc(const std::string&, const IColumn& column,
                                                   const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end, Arena& arena,
                                                   const FormatOptions& options) const {
    auto* batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    if (batch == nullptr) {
        return Status::InvalidArgument("Variant ORC output requires StringVectorBatch");
    }
    RETURN_IF_CATCH_EXCEPTION({
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        const DorisVector<size_t> lengths = json_lengths(column, first, last, null_map, options);
        size_t total_size = 0;
        for (size_t length : lengths) {
            if (length > std::numeric_limits<size_t>::max() - total_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant ORC output is too large");
            }
            total_size += length;
        }
        char* output = total_size == 0 ? nullptr : arena.alloc(total_size);
        size_t offset = 0;
        batch->hasNulls = null_map != nullptr;
        visit_variant_v2_values(
                column, first, last, forced_nulls(null_map),
                [&](size_t row) {
                    batch->notNull[row] = 0;
                    batch->data[row] = nullptr;
                    batch->length[row] = 0;
                },
                [&](size_t row, VariantRef value) {
                    batch->notNull[row] = 1;
                    batch->data[row] = output + offset;
                    FixedWriter writer {.destination = output + offset,
                                        .capacity = lengths[row - first]};
                    write_json_value(value, writer, options);
                    DCHECK_EQ(writer.written, lengths[row - first]);
                    batch->length[row] = cast_set<int64_t>(writer.written);
                    offset += writer.written;
                });
        DCHECK_EQ(offset, total_size);
        batch->numElements = last - first;
    });
    return Status::OK();
}

} // namespace doris
