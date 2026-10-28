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

#include <arrow/array/array_binary.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/extension_type.h>
#include <arrow/type_traits.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <orc/Vector.hh>
#include <span>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_payload_validator.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"

namespace doris {
namespace {

// ColumnVariantV2 already validates its encoded dictionaries and value structure. Only walk
// the selected row here: validating every unused dictionary key per row is quadratic for
// shared dictionaries, including when a nested ARRAY invokes this writer on one-row slices.
Status append_flight_variant_value(VariantRef value, VariantBatchBuilder::Row& output,
                                   size_t depth = 0) {
    const auto basic_type = value.basic_type();
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        return Status::NotSupported(
                "Native Arrow Variant nesting exceeds {}; "
                "cast the result to STRING for text output",
                VARIANT_MAX_NESTING_DEPTH);
    }
    if (value.value_size() != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Native Arrow Variant contains trailing value bytes");
    }
    if (basic_type == VariantBasicType::OBJECT) {
        auto object = output.start_object();
        auto fields = value.object_view();
        for (uint32_t i = 0; i < fields.size(); ++i) {
            uint32_t field_id;
            auto child = fields.value_at(i, &field_id);
            object.add_key(value.metadata.key_at(field_id));
            RETURN_IF_ERROR(append_flight_variant_value(child, output, depth + 1));
        }
        object.finish();
    } else if (basic_type == VariantBasicType::ARRAY) {
        auto array = output.start_array();
        for (uint32_t i = 0; i < value.num_elements(); ++i) {
            RETURN_IF_ERROR(append_flight_variant_value(value.array_at(i), output, depth + 1));
        }
        array.finish();
    } else {
        // Primitives never reference dictionary keys. Reuse physical import to retain widths,
        // decimal scales and non-JSON types; canonical equality encoding normalizes those away.
        static constexpr char empty_metadata[] = {0x11, 0, 0};
        value.metadata = {.data = empty_metadata, .size = sizeof(empty_metadata)};
        output.add_value(value);
    }
    return Status::OK();
}

// The dictionary that a writer gives each Variant row; see with_row_metadata().
enum class VariantRowMetadata {
    // Flight sends every row with a dictionary of only the keys it uses.
    OWN_KEYS,
    // Table writers keep the dictionary a row shares with its batch unless it is more than
    // SHARED_METADATA_SLACK bytes larger than the row's value.
    BOUNDED,
};

// Rows encoded together, such as the rows of one parse_json() call, share one dictionary of all
// their keys, and a table writer copies the dictionary into every row. 4064 rows with a distinct
// 250-byte key each share a 1 MiB dictionary, so copying it costs 4 GiB, past the 2 GiB that an
// Arrow binary array holds. A row whose dictionary exceeds its value by more than this slack gets
// its own keys instead, which bounds the copied metadata by the value bytes plus 1 KiB a row. A
// smaller dictionary is kept: the rows of a batch then carry the same metadata, which Parquet
// dictionary-encodes.
constexpr size_t SHARED_METADATA_SLACK = 1024;

// Re-encodes `value` with a dictionary of only the keys it uses, kept alive by `owner`, when
// `policy` asks for that and the dictionary holds keys the row does not use. For example, under
// BOUNDED, {"a": 1} keeps a shared dictionary [a, b] but gets [a] when the dictionary also holds
// 4000 other keys.
Status with_row_metadata(VariantRef& value, VariantRowMetadata policy,
                         std::optional<VariantBatchBuilder>& owner) {
    const auto keys = value.metadata.dict_size();
    // An empty dictionary or an object using every key already has row-local metadata.
    if (keys == 0 ||
        (value.basic_type() == VariantBasicType::OBJECT && value.num_elements() == keys)) {
        return Status::OK();
    }
    if (policy == VariantRowMetadata::BOUNDED &&
        value.metadata.size <= value.value.size + SHARED_METADATA_SLACK) {
        return Status::OK();
    }
    VariantBatchBuilder encoder;
    auto row = encoder.begin_row();
    RETURN_IF_ERROR(append_flight_variant_value(value, row));
    row.finish();
    owner.emplace(encoder.finish_batch());
    value = owner->value_at(0);
    return Status::OK();
}

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
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Variant V2 typed identity {}",
                        static_cast<int32_t>(primitive));
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

void validate_binary_variant_primitive(VariantPrimitiveId primitive_id) {
    switch (primitive_id) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
    case VariantPrimitiveId::DOUBLE:
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
    case VariantPrimitiveId::DATE:
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::FLOAT:
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::STRING:
    case VariantPrimitiveId::UUID:
        return;
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Binary Variant V2 Arrow encoding does not support primitive id {}",
                        static_cast<uint8_t>(primitive_id));
    }
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Binary Variant V2 Arrow encoding does not support unknown primitive id {}",
                    static_cast<uint8_t>(primitive_id));
}

void validate_binary_variant_value(VariantRef value, uint32_t depth = 0) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION, "Variant value exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant value has {} trailing bytes after the encoded value",
                        value.value.size - encoded_size);
    }

    switch (value.basic_type()) {
    case VariantBasicType::PRIMITIVE:
        validate_binary_variant_primitive(value.primitive_id());
        return;
    case VariantBasicType::SHORT_STRING:
        return;
    case VariantBasicType::OBJECT:
        for (uint32_t i = 0; i < value.num_elements(); ++i) {
            uint32_t field_id = 0;
            VariantRef child = value.object_value_at(i, &field_id);
            value.metadata.key_at(field_id);
            validate_binary_variant_value(child, depth + 1);
        }
        return;
    case VariantBasicType::ARRAY:
        for (uint32_t i = 0; i < value.num_elements(); ++i) {
            validate_binary_variant_value(value.array_at(i), depth + 1);
        }
        return;
    }
}

void require_variant_arrow_status(const arrow::Status& status) {
    if (!status.ok()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Variant V2 Arrow append failed: {}",
                        status.ToString());
    }
}

Status write_binary_variant_arrow(const IColumn& column, const NullMap* null_map,
                                  arrow::StructBuilder& builder, size_t start, size_t end) {
    // StructBuilder::type() returns a shared_ptr by value. Keep that owner alive while using the
    // cast reference; otherwise the reference would dangle as soon as the temporary is destroyed.
    const auto builder_type = builder.type();
    const auto& struct_type = assert_cast<const arrow::StructType&>(*builder_type);
    if (struct_type.num_fields() != 2 || struct_type.field(0)->name() != "value" ||
        struct_type.field(1)->name() != "metadata" ||
        struct_type.field(0)->type()->id() != arrow::Type::BINARY ||
        struct_type.field(1)->type()->id() != arrow::Type::BINARY) {
        return Status::InvalidArgument(
                "Binary Variant V2 Arrow type must be "
                "struct<value: binary, metadata: binary>, got {}",
                struct_type.ToString());
    }
    auto* value_builder = dynamic_cast<arrow::BinaryBuilder*>(builder.field_builder(0));
    auto* metadata_builder = dynamic_cast<arrow::BinaryBuilder*>(builder.field_builder(1));
    if (value_builder == nullptr || metadata_builder == nullptr) {
        return Status::InvalidArgument("Binary Variant V2 Arrow child builders must be binary");
    }

    // Consumers of the binary Variant V2 Arrow representation copy these two buffers without
    // inspecting them. Validate once at the serialization boundary so a writer cannot commit
    // malformed or unsupported bytes.
    const auto outer_nulls = forced_nulls(null_map);
    visit_variant_v2_values(
            column, start, end, outer_nulls,
            [&](size_t) { require_variant_arrow_status(builder.AppendNull()); },
            [&](size_t row, VariantRef value) {
                std::optional<VariantBatchBuilder> own_keys;
                try {
                    // Check and validate the row as it is written: a row given its own keys is
                    // validated against them instead of the dictionary its batch shares.
                    THROW_IF_ERROR(with_row_metadata(value, VariantRowMetadata::BOUNDED, own_keys));
                    constexpr size_t BINARY_VARIANT_SIZE_LIMIT = 128 * 1024 * 1024;
                    if (value.value.size > BINARY_VARIANT_SIZE_LIMIT ||
                        value.metadata.size > BINARY_VARIANT_SIZE_LIMIT) {
                        throw Exception(ErrorCode::INVALID_ARGUMENT,
                                        "exceeds the 128 MiB value/metadata limit");
                    }
                    value.metadata.validate();
                    validate_binary_variant_value(value);
                } catch (const Exception& e) {
                    throw Exception(e.code(), "Binary Variant V2 row {} is incompatible: {}", row,
                                    e.what());
                }
                require_variant_arrow_status(builder.Append());
                require_variant_arrow_status(
                        value_builder->Append(reinterpret_cast<const uint8_t*>(value.value.data),
                                              cast_set<int32_t, size_t, false>(value.value.size)));
                require_variant_arrow_status(metadata_builder->Append(
                        reinterpret_cast<const uint8_t*>(value.metadata.data),
                        cast_set<int32_t, size_t, false>(value.metadata.size)));
            });
    return Status::OK();
}

} // namespace

DataTypeVariantV2SerDe::DataTypeVariantV2SerDe(int nesting_level) : DataTypeSerDe(nesting_level) {}

int64_t DataTypeVariantV2SerDe::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                  int be_exec_version) {
    const auto& variant = get_variant_v2_column(column).serialization_column();
    int64_t size = sizeof(bool) + sizeof(size_t) * 2 + sizeof(bool);
    if (variant.is_typed()) {
        const DataTypePtr nullable_type = make_nullable(variant._typed_type);
        return size + VARIANT_V2_TYPE_META_BYTES +
               nullable_type->get_uncompressed_serialized_bytes(*variant._typed, be_exec_version);
    }
    const DataTypeString string_type;
    size += string_type.get_uncompressed_serialized_bytes(*variant._metadatas, be_exec_version);
    size += sizeof(size_t) + variant._meta_ids->size() * sizeof(uint32_t);
    size += string_type.get_uncompressed_serialized_bytes(*variant._values, be_exec_version);
    return size;
}

char* DataTypeVariantV2SerDe::serialize(const IColumn& column, char* buf, int be_exec_version) {
    const IColumn* physical = &column;
    size_t saved_rows = 0;
    buf = serialize_const_flag_and_row_num(&physical, buf, &saved_rows);
    const auto& variant = assert_cast<const ColumnVariantV2&>(*physical).serialization_column();
    DCHECK_EQ(variant.size(), saved_rows);
    unaligned_store<bool>(buf, variant.is_typed());
    buf += sizeof(bool);
    if (variant.is_typed()) {
        write_variant_v2_type(variant._typed_type, buf);
        return make_nullable(variant._typed_type)->serialize(*variant._typed, buf, be_exec_version);
    }
    const DataTypeString string_type;
    buf = string_type.serialize(*variant._metadatas, buf, be_exec_version);
    buf = serialize_meta_ids(*variant._meta_ids, buf);
    return string_type.serialize(*variant._values, buf, be_exec_version);
}

const char* DataTypeVariantV2SerDe::deserialize(const char* buf, MutableColumnPtr* column,
                                                int be_exec_version) {
    auto* destination = assert_cast<ColumnVariantV2*>(column->get());
    size_t saved_rows = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &saved_rows);
    const bool typed = unaligned_load<bool>(buf);
    buf += sizeof(bool);

    ColumnVariantV2::MutablePtr decoded;
    if (typed) {
        DataTypePtr type = read_variant_v2_type(buf);
        const DataTypePtr nullable_type = make_nullable(type);
        MutableColumnPtr typed_column = nullable_type->create_column();
        buf = nullable_type->deserialize(buf, &typed_column, be_exec_version);
        decoded = ColumnVariantV2::create_typed(std::move(typed_column), std::move(type));
    } else {
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
                    "ColumnVariantV2 metadata id count {} does not match value count {}",
                    ids.size(), values->size()));
        }
        for (uint32_t id : ids) {
            if (id >= metadatas->size()) {
                throw Exception(Status::Corruption(
                        "ColumnVariantV2 metadata id {} exceeds metadata count {}", id,
                        metadatas->size()));
            }
        }
        decoded = ColumnVariantV2::create();
        static_cast<ColumnString::Ptr&>(decoded->_metadatas) =
                ColumnString::cast_to_column_ptr(assert_cast<const ColumnString*>(metadatas.get()));
        static_cast<MetaIdsColumn::Ptr&>(decoded->_meta_ids) = MetaIdsColumn::cast_to_column_ptr(
                assert_cast<const MetaIdsColumn*>(meta_ids.get()));
        static_cast<ColumnString::Ptr&>(decoded->_values) =
                ColumnString::cast_to_column_ptr(assert_cast<const ColumnString*>(values.get()));
        decoded->sanity_check();
    }
    if (decoded->size() != saved_rows) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 saved row count {} does not match decoded row count {}",
                saved_rows, decoded->size()));
    }
    destination->_adopt_state_from(*decoded);
    return buf;
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

bool DataTypeVariantV2SerDe::is_native_arrow_type(const arrow::DataType& type) {
    if (type.id() == arrow::Type::EXTENSION) {
        const auto& extension = static_cast<const arrow::ExtensionType&>(type);
        return extension.extension_name() == "arrow.parquet.variant" &&
               is_native_arrow_type(*extension.storage_type());
    }
    return type.id() == arrow::Type::STRUCT && type.num_fields() == 2 &&
           type.field(0)->name() == "metadata" && type.field(1)->name() == "value" &&
           type.field(0)->type()->id() == arrow::Type::BINARY &&
           type.field(1)->type()->id() == arrow::Type::BINARY;
}

namespace {

// The value of a Variant JSON null: a primitive header whose primitive id is NULL. It references no
// metadata key, so any valid metadata blob can accompany it.
constexpr std::array<char, 1> VARIANT_NULL_VALUE {0};

bool is_arrow_binary_type(const arrow::DataType& type) {
    return type.id() == arrow::Type::BINARY || type.id() == arrow::Type::LARGE_BINARY;
}

bool is_arrow_text_type(const arrow::DataType& type) {
    return is_arrow_binary_type(type) || type.id() == arrow::Type::STRING ||
           type.id() == arrow::Type::LARGE_STRING;
}

// A metadata child that holds one shared dictionary per batch, e.g. dictionary<int32, binary>
// with a single entry and every index 0, costs a few bytes per row instead of the whole blob.
bool is_dictionary_metadata_type(const arrow::DataType& type) {
    if (type.id() != arrow::Type::DICTIONARY) {
        return false;
    }
    const auto& dictionary = static_cast<const arrow::DictionaryType&>(type);
    return arrow::is_integer(dictionary.index_type()->id()) &&
           is_arrow_binary_type(*dictionary.value_type());
}

// struct<metadata, value> in either field order. Producers that build the struct by name, such as
// pyarrow from a dict, do not keep the Parquet field order.
bool is_encoded_variant_struct_type(const arrow::DataType& type) {
    if (type.id() != arrow::Type::STRUCT || type.num_fields() != 2) {
        return false;
    }
    const auto& struct_type = static_cast<const arrow::StructType&>(type);
    const auto metadata = struct_type.GetFieldByName("metadata");
    const auto value = struct_type.GetFieldByName("value");
    return metadata != nullptr && value != nullptr &&
           (is_arrow_binary_type(*metadata->type()) ||
            is_dictionary_metadata_type(*metadata->type())) &&
           is_arrow_binary_type(*value->type());
}

template <typename Fn>
void with_binary_array(const arrow::Array& array, Fn&& fn) {
    switch (array.type_id()) {
    case arrow::Type::BINARY:
    case arrow::Type::STRING:
        fn(static_cast<const arrow::BinaryArray&>(array));
        return;
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::LARGE_STRING:
        fn(static_cast<const arrow::LargeBinaryArray&>(array));
        return;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Expected an Arrow binary array, got {}",
                        array.type()->ToString());
    }
}

// Arrow may omit the data buffer of a binary array whose values are all empty. Such rows are
// rejected by the Variant validation that follows, so they must reach it rather than crash here.
template <typename ArrowBinaryArray>
const char* binary_data(const ArrowBinaryArray& array) {
    return array.value_data() == nullptr
                   ? nullptr
                   : reinterpret_cast<const char*>(array.value_data()->data());
}

uint32_t checked_column_bytes(uint64_t bytes, std::string_view what) {
    if (bytes > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant Arrow {} bytes {} exceed the 4 GiB column limit", what, bytes);
    }
    return static_cast<uint32_t>(bytes);
}

// Value bytes of every row of a validated struct slice, laid out for insert_encoded_rows. Without
// NULL struct rows the values are one contiguous run of the Arrow buffer and are borrowed. A NULL
// struct row becomes Variant null, so its bytes must be spliced in; e.g. rows {v0, NULL, v2}
// become v0 | 0x00 | v2 even when the hidden child row holds bytes.
struct EncodedVariantValues {
    DorisVector<uint32_t> offsets;
    DorisVector<char> spliced;
    StringRef bytes;
};

template <typename ValueArray>
EncodedVariantValues encoded_variant_values(const arrow::StructArray& storage,
                                            const ValueArray& values) {
    const int64_t rows = storage.length();
    EncodedVariantValues result;
    result.offsets.resize(rows + 1);
    result.offsets[0] = 0;
    uint64_t total = 0;
    for (int64_t row = 0; row < rows; ++row) {
        if (storage.IsNull(row)) {
            total += VARIANT_NULL_VALUE.size();
        } else {
            if (values.IsNull(row)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Native Variant has a NULL value child at row {}", row);
            }
            total += values.value_length(row);
        }
        result.offsets[row + 1] = checked_column_bytes(total, "value");
    }
    if (storage.null_count() == 0) {
        const char* data = binary_data(values);
        result.bytes = {data == nullptr ? nullptr : data + values.value_offset(0), total};
        return result;
    }
    result.spliced.resize(total);
    for (int64_t row = 0; row < rows; ++row) {
        char* output = result.spliced.data() + result.offsets[row];
        if (storage.IsNull(row)) {
            std::memcpy(output, VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size());
        } else {
            const auto value = values.GetView(row);
            std::memcpy(output, value.data(), value.size());
        }
    }
    result.bytes = {result.spliced.data(), result.spliced.size()};
    return result;
}

// Rows of a validated struct slice with a plain binary metadata child. The appender validates the
// rows and appends them atomically, and keeps a batch whose rows repeat one metadata blob on the
// single-metadata form without per-row ids.
template <typename MetadataArray, typename ValueArray>
void read_variant_rows(ColumnVariantV2& result, const arrow::StructArray& storage,
                       const MetadataArray& metadata, const ValueArray& values) {
    const int64_t rows = storage.length();
    DorisVector<VariantRef> refs(rows);
    std::optional<VariantMetadataRef> first_metadata;
    for (int64_t row = 0; row < rows; ++row) {
        if (storage.IsNull(row)) {
            refs[row].value = {VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size()};
            continue;
        }
        if (metadata.IsNull(row) || values.IsNull(row)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Native Variant has a NULL metadata/value child at row {}", row);
        }
        const auto meta = metadata.GetView(row);
        const auto value = values.GetView(row);
        refs[row] = {.metadata = {.data = meta.data(), .size = meta.size()},
                     .value = {value.data(), value.size()}};
        if (!first_metadata.has_value()) {
            first_metadata = refs[row].metadata;
        }
    }
    // A NULL row borrows a row's metadata so that it does not add the empty metadata as a second
    // distinct blob, which would put the whole batch on per-row metadata ids.
    const VariantMetadataRef null_metadata = first_metadata.value_or(VariantMetadataRef {
            .data = VARIANT_EMPTY_METADATA.data(), .size = VARIANT_EMPTY_METADATA.size()});
    for (int64_t row = 0; row < rows; ++row) {
        if (storage.IsNull(row)) {
            refs[row].metadata = null_metadata;
        }
    }
    result.create_encoded_rows_appender().append(refs);
}

// Rows of a validated struct slice whose metadata child is dictionary encoded. The dictionary maps
// directly onto insert_encoded_rows: its entries are the metadata blobs and its indices the
// metadata ids. When every row uses one entry, e.g. a producer that sends one dictionary entry per
// batch, only that entry is passed and the rows take the single-metadata form, so no row's
// metadata is copied, hashed or compared.
template <typename DictionaryValues, typename ValueArray>
void read_dictionary_metadata_variant_rows(ColumnVariantV2& result,
                                           const arrow::StructArray& storage,
                                           const arrow::DictionaryArray& metadata,
                                           const DictionaryValues& dictionary,
                                           const ValueArray& values) {
    const int64_t rows = storage.length();
    DorisVector<uint32_t> meta_ids(rows);
    std::optional<uint32_t> first_id;
    bool shared_id = true;
    for (int64_t row = 0; row < rows; ++row) {
        if (storage.IsNull(row)) {
            continue;
        }
        if (metadata.IsNull(row)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Native Variant has a NULL metadata dictionary index at row {}", row);
        }
        // ValidateFull() has checked every index against the dictionary length.
        const auto id = static_cast<uint32_t>(metadata.GetValueIndex(row));
        meta_ids[row] = id;
        if (!first_id.has_value()) {
            first_id = id;
        } else if (id != *first_id) {
            shared_id = false;
        }
    }
    const EncodedVariantValues encoded_values = encoded_variant_values(storage, values);
    ColumnVariantV2::EncodedDataView view {.metadata_bytes = {},
                                           .metadata_offsets = {},
                                           .meta_ids = {},
                                           .value_bytes = encoded_values.bytes,
                                           .value_offsets = encoded_values.offsets};
    if (!first_id.has_value() || shared_id) {
        const std::string_view entry = first_id.has_value()
                                               ? dictionary.GetView(*first_id)
                                               : std::string_view(VARIANT_EMPTY_METADATA.data(),
                                                                  VARIANT_EMPTY_METADATA.size());
        const std::array<uint32_t, 2> metadata_offsets {
                0, checked_column_bytes(entry.size(), "metadata")};
        view.metadata_bytes = {entry.data(), entry.size()};
        view.metadata_offsets = metadata_offsets;
        result.insert_encoded_rows(view);
        return;
    }
    // A NULL row's Variant null references no key, so any entry serves as its metadata.
    for (int64_t row = 0; row < rows; ++row) {
        if (storage.IsNull(row)) {
            meta_ids[row] = *first_id;
        }
    }
    // Several entries: insert_encoded_rows validates every entry, used or not, before appending.
    // Entries are consecutive in the dictionary's data buffer and are borrowed as one run.
    const int64_t entries = dictionary.length();
    DorisVector<uint32_t> metadata_offsets(entries + 1);
    const int64_t base = dictionary.value_offset(0);
    for (int64_t entry = 0; entry <= entries; ++entry) {
        metadata_offsets[entry] =
                checked_column_bytes(dictionary.value_offset(entry) - base, "metadata");
    }
    const char* data = binary_data(dictionary);
    view.metadata_bytes = {data == nullptr ? nullptr : data + base, metadata_offsets.back()};
    view.metadata_offsets = metadata_offsets;
    view.meta_ids = meta_ids;
    result.insert_encoded_rows(view);
}

void read_encoded_variant_arrow(ColumnVariantV2& result, const arrow::StructArray& storage) {
    const arrow::Array& metadata = *storage.GetFieldByName("metadata");
    const arrow::Array& values = *storage.GetFieldByName("value");
    if (metadata.type_id() == arrow::Type::DICTIONARY) {
        const auto& dictionary_metadata = static_cast<const arrow::DictionaryArray&>(metadata);
        with_binary_array(*dictionary_metadata.dictionary(), [&](const auto& dictionary) {
            with_binary_array(values, [&](const auto& value_array) {
                read_dictionary_metadata_variant_rows(result, storage, dictionary_metadata,
                                                      dictionary, value_array);
            });
        });
        return;
    }
    with_binary_array(metadata, [&](const auto& metadata_array) {
        with_binary_array(values, [&](const auto& value_array) {
            read_variant_rows(result, storage, metadata_array, value_array);
        });
    });
}

// One JSON document per row, parsed with the same options as parse_to_variant and JSON loads. A
// NULL row is JSON null, the value a NOT NULL Variant column keeps for it.
void read_json_variant_arrow(ColumnVariantV2& result, const arrow::Array& array) {
    JsonStringToVariantEncoder encoder;
    static constexpr std::string_view json_null = "null";
    with_binary_array(array, [&](const auto& text) {
        for (int64_t row = 0; row < text.length(); ++row) {
            const std::string_view json = text.IsNull(row) ? json_null : text.GetView(row);
            encoder.add_json({json.data(), json.size()});
        }
    });
    result.insert_encoded_batch(encoder.finish_batch());
}

} // namespace

bool DataTypeVariantV2SerDe::is_arrow_input_type(const arrow::DataType& type) {
    if (type.id() == arrow::Type::EXTENSION) {
        const auto& extension = static_cast<const arrow::ExtensionType&>(type);
        return extension.extension_name() == "arrow.parquet.variant" &&
               is_encoded_variant_struct_type(*extension.storage_type());
    }
    return is_encoded_variant_struct_type(type) || is_arrow_text_type(type);
}

Status DataTypeVariantV2SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* array,
                                                      int64_t start, int64_t end,
                                                      const cctz::time_zone&) const {
    if (array == nullptr) {
        return Status::InvalidArgument("Native Variant Arrow array is null");
    }
    if (start < 0 || end < start || end > array->length()) {
        return Status::InvalidArgument("Invalid native Variant Arrow row range [{}, {})", start,
                                       end);
    }
    if (!is_arrow_input_type(*array->type())) {
        return Status::InvalidArgument(
                "Variant Arrow input must be JSON text (utf8, large_utf8, binary or large_binary) "
                "or struct<metadata: binary, value: binary> (optionally with the "
                "arrow.parquet.variant extension, large_binary children or a dictionary-encoded "
                "metadata child), got {}",
                array->type()->ToString());
    }
    if (start == end) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION({
        if (array->type_id() == arrow::Type::EXTENSION) {
            array = static_cast<const arrow::ExtensionArray*>(array)->storage().get();
        }
        // Nested ARRAY readers invoke this on row slices; validating the entire child array
        // each time would make a batch with many small arrays quadratic.
        const auto slice = array->Slice(start, end - start);
        const auto validation = slice->ValidateFull();
        if (!validation.ok()) {
            return Status::InvalidArgument("Invalid native Variant Arrow array: {}",
                                           validation.ToString());
        }
        // A driver or load client supplies untrusted bytes. Every path validates the complete
        // slice before mutating the destination, and copies the bytes so the column survives
        // Arrow batch release.
        auto& result = assert_cast<ColumnVariantV2&>(column);
        if (slice->type_id() == arrow::Type::STRUCT) {
            read_encoded_variant_arrow(result, assert_cast<const arrow::StructArray&>(*slice));
        } else {
            read_json_variant_arrow(result, *slice);
        }
    });
    return Status::OK();
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

// Most JSON text read here was written by a Variant serde, for example into an aggregate state, so
// it must read back every value a Variant can hold, including keys longer than the parse limit. A
// CAST from a JSON string to ARRAY<VARIANT> or STRUCT also passes the elements it splits out.
JsonToVariantOptions serde_json_options() {
    return {.max_json_key_length = std::numeric_limits<uint32_t>::max(),
            .throw_on_invalid_json = false};
}

} // namespace

Status DataTypeVariantV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions&) const {
    RETURN_IF_CATCH_EXCEPTION({
        JsonStringToVariantEncoder encoder(serde_json_options());
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
        JsonStringToVariantEncoder encoder(serde_json_options());
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

// to_json and CAST(<complex type> AS JSON) write Variant values here, with the same session time zone
// as CAST(<variant> AS JSON).
Status DataTypeVariantV2SerDe::serialize_column_to_jsonb(const IColumn& from_column,
                                                         int64_t row_num, JsonbWriter& writer,
                                                         const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t row = checked_row(row_num);
        JsonbWriter document;
        visit_variant_v2_values(
                from_column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) {
                    variant_to_jsonb(value, document, {.timezone = options.timezone});
                });
        const JsonbDocument* doc = nullptr;
        THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(
                document.getOutput()->getBuffer(), document.getOutput()->getSize(), &doc));
        require_jsonb_write(writer.writeValue(doc->getValue()), "Variant value");
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::serialize_column_to_jsonb_vector(
        const IColumn& from_column, ColumnString& to_column, const FormatOptions& options,
        const NullMap::value_type* source_null_map) const {
    RETURN_IF_CATCH_EXCEPTION({
        JsonbWriter writer;
        visit_variant_v2_values(
                from_column, 0, from_column.size(),
                source_null_map
                        ? std::span<const NullMap::value_type>(source_null_map, from_column.size())
                        : std::span<const NullMap::value_type>(),
                [&](size_t) { to_column.insert_default(); },
                [&](size_t, VariantRef value) {
                    variant_to_jsonb(value, writer, {.timezone = options.timezone});
                    to_column.insert_data(writer.getOutput()->getBuffer(),
                                          writer.getOutput()->getSize());
                });
    });
    return Status::OK();
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

Status write_parquet_variant_arrow(const IColumn& column, const NullMap* null_map,
                                   arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                   VariantRowMetadata row_metadata) {
    if (start < 0 || end < start) {
        return Status::InvalidArgument("Invalid Variant Arrow row range [{}, {})", start, end);
    }
    if (array_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::InvalidArgument("Variant Arrow writer requires a struct builder, got {}",
                                       array_builder->type()->ToString());
    }
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    const auto type = std::static_pointer_cast<arrow::StructType>(builder.type());
    if (type->num_fields() != 2 || type->field(0)->name() != "metadata" ||
        type->field(1)->name() != "value" || type->field(0)->type()->id() != arrow::Type::BINARY ||
        type->field(1)->type()->id() != arrow::Type::BINARY) {
        return Status::InvalidArgument(
                "Variant Arrow writer requires struct<metadata: binary, value: binary>, got {}",
                type->ToString());
    }
    auto& metadata_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(0));
    auto& value_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(1));
    Status status = Status::OK();
    visit_variant_v2_values(
            column, start, end, forced_nulls(null_map),
            [&](size_t) {
                if (status.ok()) {
                    status = checkArrowStatus(builder.AppendNull(), column, builder);
                }
            },
            [&](size_t, VariantRef value) {
                if (!status.ok()) {
                    return;
                }
                std::optional<VariantBatchBuilder> own_keys;
                status = with_row_metadata(value, row_metadata, own_keys);
                if (!status.ok()) {
                    return;
                }
                if (value.metadata.size > std::numeric_limits<int32_t>::max() ||
                    value.value.size > std::numeric_limits<int32_t>::max()) {
                    status = Status::InvalidArgument(
                            "Variant Arrow metadata/value exceeds Arrow binary size limit");
                    return;
                }
                status = checkArrowStatus(builder.Append(), column, builder);
                if (status.ok()) {
                    status = checkArrowStatus(
                            metadata_builder.Append(value.metadata.data,
                                                    cast_set<int32_t>(value.metadata.size)),
                            column, metadata_builder);
                }
                if (status.ok()) {
                    status = checkArrowStatus(
                            value_builder.Append(value.value.data,
                                                 cast_set<int32_t>(value.value.size)),
                            column, value_builder);
                }
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
        if (is_native_arrow_type(*array_builder->type())) {
            return write_parquet_variant_arrow(column, null_map, array_builder, start, end,
                                               VariantRowMetadata::OWN_KEYS);
        }
        if (array_builder->type()->id() == arrow::Type::STRING) {
            return write_arrow(column, null_map, assert_cast<arrow::StringBuilder&>(*array_builder),
                               first, last, options);
        }
        if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
            return write_arrow(column, null_map,
                               assert_cast<arrow::LargeStringBuilder&>(*array_builder), first, last,
                               options);
        }
        if (array_builder->type()->id() == arrow::Type::STRUCT) {
            return write_binary_variant_arrow(column, null_map,
                                              assert_cast<arrow::StructBuilder&>(*array_builder),
                                              first, last);
        }
        return Status::InvalidArgument("Unsupported arrow type for variant column: {}",
                                       array_builder->type()->name());
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_paimon_arrow(
        const std::shared_ptr<const IDataType>&, const IColumn& column, const NullMap* null_map,
        const std::shared_ptr<arrow::Field>&, arrow::ArrayBuilder* array_builder, int64_t start,
        int64_t end, const cctz::time_zone&) const {
    // Paimon owns the value/metadata order and validation; Flight uses metadata/value.
    // Keep protocol dispatch explicit so changes to Flight cannot intercept table writes.
    if (array_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::InvalidArgument("Paimon Variant requires a struct builder");
    }
    RETURN_IF_CATCH_EXCEPTION({
        return write_binary_variant_arrow(column, null_map,
                                          assert_cast<arrow::StructBuilder&>(*array_builder),
                                          checked_row(start), checked_row(end));
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_iceberg_arrow(
        const std::shared_ptr<const IDataType>&, const IColumn& column, const NullMap* null_map,
        const std::shared_ptr<arrow::Field>&, arrow::ArrayBuilder* array_builder, int64_t start,
        int64_t end, const cctz::time_zone&) const {
    // A row keeps a small dictionary shared with its batch and gets its own keys instead of a
    // large one; Flight compacts each transmitted row.
    RETURN_IF_CATCH_EXCEPTION({
        return write_parquet_variant_arrow(column, null_map, array_builder, start, end,
                                           VariantRowMetadata::BOUNDED);
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
