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

#include "format/parquet/parquet_variant_reader.h"

#include <algorithm>
#include <cstring>
#include <deque>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

#include "core/column/column_variant.h"
#include "core/data_type/data_type_decimal.h"
#include "core/value/jsonb_value.h"
#include "exec/common/variant_util.h"

namespace doris::parquet {

std::string format_variant_uuid(const uint8_t* ptr) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string uuid;
    uuid.reserve(36);
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            uuid.push_back('-');
        }
        uuid.push_back(hex[ptr[i] >> 4]);
        uuid.push_back(hex[ptr[i] & 0x0f]);
    }
    return uuid;
}

namespace {

struct VariantMetadata {
    std::vector<std::string> dictionary;
};

struct VariantObjectLayout {
    std::vector<uint64_t> field_ids;
    std::vector<uint64_t> field_offsets;
    std::vector<uint64_t> field_ends;
    const uint8_t* fields = nullptr;
    uint64_t total_size = 0;
};

struct VariantArrayLayout {
    std::vector<uint64_t> field_offsets;
    const uint8_t* fields = nullptr;
    uint64_t total_size = 0;
};

uint64_t read_unsigned_le(const uint8_t* ptr, int size) {
    uint64_t value = 0;
    for (int i = 0; i < size; ++i) {
        value |= static_cast<uint64_t>(ptr[i]) << (i * 8);
    }
    return value;
}

int64_t read_signed_le(const uint8_t* ptr, int size) {
    uint64_t value = read_unsigned_le(ptr, size);
    if (size < 8) {
        uint64_t sign_bit = uint64_t {1} << (size * 8 - 1);
        if ((value & sign_bit) != 0) {
            uint64_t mask = ~((uint64_t {1} << (size * 8)) - 1);
            value |= mask;
        }
    }
    return static_cast<int64_t>(value);
}

__int128 read_signed_int128_le(const uint8_t* ptr) {
    unsigned __int128 unsigned_value = 0;
    for (int i = 15; i >= 0; --i) {
        unsigned_value <<= 8;
        unsigned_value |= ptr[i];
    }
    static constexpr unsigned __int128 sign_bit = static_cast<unsigned __int128>(1) << 127;
    if ((unsigned_value & sign_bit) == 0) {
        return static_cast<__int128>(unsigned_value);
    }
    static constexpr __int128 signed_half_range = static_cast<__int128>(1) << 126;
    return (static_cast<__int128>(unsigned_value & (sign_bit - 1)) - signed_half_range) -
           signed_half_range;
}

Status require_available(const uint8_t* ptr, const uint8_t* end, size_t size,
                         std::string_view context) {
    if (ptr > end) {
        return Status::Corruption("Invalid Parquet VARIANT {} encoding", context);
    }
    if (size > static_cast<size_t>(end - ptr)) {
        return Status::Corruption("Invalid Parquet VARIANT {} encoding", context);
    }
    return Status::OK();
}

Status require_available_entries(const uint8_t* ptr, const uint8_t* end, uint64_t entries,
                                 size_t entry_size, std::string_view context) {
    if (entries > std::numeric_limits<size_t>::max() / entry_size) {
        return Status::Corruption("Invalid Parquet VARIANT {} encoding", context);
    }
    return require_available(ptr, end, static_cast<size_t>(entries) * entry_size, context);
}

bool variant_string_less(std::string_view lhs, std::string_view rhs) {
    return std::lexicographical_compare(
            lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](char left, char right) {
                return static_cast<unsigned char>(left) < static_cast<unsigned char>(right);
            });
}

bool is_valid_utf8(std::string_view value) {
    const auto* data = reinterpret_cast<const uint8_t*>(value.data());
    const auto* end = data + value.size();
    while (data < end) {
        const uint8_t first = *data++;
        if (first <= 0x7f) {
            continue;
        }

        uint32_t code_point = 0;
        size_t continuation_bytes = 0;
        if (first >= 0xc2 && first <= 0xdf) {
            code_point = first & 0x1f;
            continuation_bytes = 1;
        } else if (first >= 0xe0 && first <= 0xef) {
            code_point = first & 0x0f;
            continuation_bytes = 2;
        } else if (first >= 0xf0 && first <= 0xf4) {
            code_point = first & 0x07;
            continuation_bytes = 3;
        } else {
            return false;
        }

        if (static_cast<size_t>(end - data) < continuation_bytes) {
            return false;
        }
        for (size_t i = 0; i < continuation_bytes; ++i) {
            const uint8_t byte = *data++;
            if ((byte & 0xc0) != 0x80) {
                return false;
            }
            code_point = (code_point << 6) | (byte & 0x3f);
        }

        if ((continuation_bytes == 2 && code_point < 0x800) ||
            (continuation_bytes == 3 && code_point < 0x10000) ||
            (code_point >= 0xd800 && code_point <= 0xdfff) || code_point > 0x10ffff) {
            return false;
        }
    }
    return true;
}

Status require_valid_utf8(std::string_view value, std::string_view context) {
    if (!is_valid_utf8(value)) {
        return Status::Corruption("Invalid Parquet VARIANT {} UTF-8 string", context);
    }
    return Status::OK();
}

Status validate_array_field_offsets(const std::vector<uint64_t>& field_offsets, uint64_t total_size,
                                    std::string_view context) {
    if (field_offsets.empty() || field_offsets.front() != 0) {
        return Status::Corruption("Invalid Parquet VARIANT {} field offsets", context);
    }
    for (size_t i = 0; i < field_offsets.size(); ++i) {
        if (field_offsets[i] > total_size) {
            return Status::Corruption("Invalid Parquet VARIANT {} field offset {}", context,
                                      field_offsets[i]);
        }
        if (i > 0 && field_offsets[i] < field_offsets[i - 1]) {
            return Status::Corruption("Invalid Parquet VARIANT {} field offsets", context);
        }
    }
    return Status::OK();
}

Status compute_object_field_ends(const std::vector<uint64_t>& field_offsets, uint64_t total_size,
                                 std::vector<uint64_t>* field_ends) {
    if (field_offsets.empty()) {
        return Status::Corruption("Invalid Parquet VARIANT object field offsets");
    }
    size_t num_elements = field_offsets.size() - 1;
    if (num_elements == 0) {
        if (total_size != 0) {
            return Status::Corruption("Invalid Parquet VARIANT object field offsets");
        }
        return Status::OK();
    }

    std::vector<std::pair<uint64_t, size_t>> physical_offsets;
    physical_offsets.reserve(num_elements);
    for (size_t i = 0; i < num_elements; ++i) {
        if (field_offsets[i] >= total_size) {
            return Status::Corruption("Invalid Parquet VARIANT object field offset {}",
                                      field_offsets[i]);
        }
        physical_offsets.emplace_back(field_offsets[i], i);
    }
    std::sort(physical_offsets.begin(), physical_offsets.end());
    if (physical_offsets.front().first != 0) {
        return Status::Corruption("Invalid Parquet VARIANT object field offsets");
    }

    field_ends->assign(num_elements, 0);
    for (size_t i = 0; i < physical_offsets.size(); ++i) {
        if (i > 0 && physical_offsets[i].first == physical_offsets[i - 1].first) {
            return Status::Corruption("Invalid Parquet VARIANT object field offsets");
        }
        uint64_t child_end =
                i + 1 < physical_offsets.size() ? physical_offsets[i + 1].first : total_size;
        (*field_ends)[physical_offsets[i].second] = child_end;
    }
    return Status::OK();
}

void append_json_string(std::string_view value, std::string* json, bool escape_non_ascii = false) {
    json->push_back('"');
    static constexpr char hex[] = "0123456789abcdef";
    for (unsigned char c : value) {
        switch (c) {
        case '"':
            json->append("\\\"");
            break;
        case '\\':
            json->append("\\\\");
            break;
        case '\b':
            json->append("\\b");
            break;
        case '\f':
            json->append("\\f");
            break;
        case '\n':
            json->append("\\n");
            break;
        case '\r':
            json->append("\\r");
            break;
        case '\t':
            json->append("\\t");
            break;
        default:
            if (c < 0x20 || (escape_non_ascii && c >= 0x80)) {
                json->append("\\u00");
                json->push_back(hex[c >> 4]);
                json->push_back(hex[c & 0x0f]);
            } else {
                json->push_back(static_cast<char>(c));
            }
            break;
        }
    }
    json->push_back('"');
}

template <typename T>
Status append_floating_json(T value, std::string* json) {
    std::ostringstream oss;
    oss << std::setprecision(std::numeric_limits<T>::max_digits10) << value;
    json->append(oss.str());
    return Status::OK();
}

std::string int128_to_string(__int128 value) {
    if (value == 0) {
        return "0";
    }
    bool negative = value < 0;
    unsigned __int128 unsigned_value = negative ? static_cast<unsigned __int128>(-(value + 1)) + 1
                                                : static_cast<unsigned __int128>(value);
    std::string digits;
    while (unsigned_value > 0) {
        digits.push_back(static_cast<char>('0' + unsigned_value % 10));
        unsigned_value /= 10;
    }
    if (negative) {
        digits.push_back('-');
    }
    std::reverse(digits.begin(), digits.end());
    return digits;
}

void append_decimal_json(__int128 unscaled, int scale, std::string* json) {
    std::string value = int128_to_string(unscaled);
    bool negative = !value.empty() && value[0] == '-';
    std::string digits = negative ? value.substr(1) : value;
    if (scale == 0) {
        json->append(value);
        return;
    }
    if (scale > 0) {
        if (digits.size() <= static_cast<size_t>(scale)) {
            digits.insert(0, static_cast<size_t>(scale) + 1 - digits.size(), '0');
        }
        digits.insert(digits.end() - scale, '.');
        if (negative) {
            json->push_back('-');
        }
        json->append(digits);
        return;
    }
    if (negative) {
        json->push_back('-');
    }
    json->append(digits);
    json->append(static_cast<size_t>(-scale), '0');
}

Status decode_primitive(uint8_t primitive_header, const uint8_t* ptr, const uint8_t* end,
                        std::string* json, const uint8_t** next);
Status decode_value(const uint8_t* ptr, const uint8_t* end, const VariantMetadata& metadata,
                    std::string* json, const uint8_t** next);

void append_uuid_json(const uint8_t* ptr, std::string* json) {
    json->push_back('"');
    json->append(format_variant_uuid(ptr));
    json->push_back('"');
}

Status make_jsonb_field(std::string_view json, FieldWithDataType* value) {
    JsonBinaryValue jsonb_value;
    RETURN_IF_ERROR(jsonb_value.from_json_string(json.data(), json.size()));
    value->field =
            Field::create_field<TYPE_JSONB>(JsonbField(jsonb_value.value(), jsonb_value.size()));
    value->base_scalar_type_id = TYPE_JSONB;
    value->num_dimensions = 0;
    value->precision = 0;
    value->scale = 0;
    return Status::OK();
}

std::string make_null_array_json(size_t elements) {
    std::string json = "[";
    for (size_t i = 0; i < elements; ++i) {
        if (i != 0) {
            json.push_back(',');
        }
        json.append("null");
    }
    json.push_back(']');
    return json;
}

Status insert_empty_object_marker(const PathInData& path, VariantMap* values) {
    FieldWithDataType value;
    RETURN_IF_ERROR(make_jsonb_field("{}", &value));
    (*values)[path] = std::move(value);
    return Status::OK();
}

Status parse_json_to_variant_map(std::string_view json, const PathInData& prefix,
                                 VariantMap* values) {
    auto parsed_column = ColumnVariant::create(0, false);
    ParseConfig parse_config;
    StringRef json_ref(json.data(), json.size());
    RETURN_IF_CATCH_EXCEPTION(
            variant_util::parse_json_to_variant(*parsed_column, json_ref, nullptr, parse_config));
    Field parsed = (*parsed_column)[0];
    if (parsed.is_null()) {
        (*values)[prefix] = FieldWithDataType {.field = Field()};
        return Status::OK();
    }

    PathInDataBuilder path;
    path.append(prefix.get_parts(), false);
    for (auto& [parsed_path, value] : parsed.get<TYPE_VARIANT>()) {
        path.append(parsed_path.get_parts(), false);
        (*values)[path.build()] = std::move(value);
        for (size_t i = 0; i < parsed_path.get_parts().size(); ++i) {
            path.pop_back();
        }
    }
    return Status::OK();
}

void fill_field_type_info(FieldWithDataType* value) {
    FieldInfo info;
    variant_util::get_field_info(value->field, &info);
    value->base_scalar_type_id = info.scalar_type_id;
    value->num_dimensions = static_cast<uint8_t>(info.num_dimensions);
    value->precision = info.precision;
    value->scale = info.scale;
}

template <PrimitiveType Primitive>
void set_primitive_variant_field(const typename PrimitiveTypeTraits<Primitive>::CppType& data,
                                 FieldWithDataType* value) {
    value->field = Field::create_field<Primitive>(data);
    fill_field_type_info(value);
}

Status read_decimal_primitive_field(uint8_t primitive_header, const uint8_t* ptr,
                                    const uint8_t* end, FieldWithDataType* value,
                                    const uint8_t** next) {
    int value_size = 16;
    if (primitive_header == 8) {
        value_size = 4;
    } else if (primitive_header == 9) {
        value_size = 8;
    }
    RETURN_IF_ERROR(require_available(ptr, end, 1 + value_size, "decimal value"));
    int scale = static_cast<int8_t>(*ptr++);
    if (scale < 0 || scale > BeConsts::MAX_DECIMAL128_PRECISION) {
        return Status::Corruption("Invalid Parquet VARIANT decimal scale {}", scale);
    }

    if (primitive_header == 8) {
        set_primitive_variant_field<TYPE_DECIMAL32>(
                Decimal32(static_cast<Int32>(read_signed_le(ptr, value_size))), value);
        value->precision = BeConsts::MAX_DECIMAL32_PRECISION;
    } else if (primitive_header == 9) {
        set_primitive_variant_field<TYPE_DECIMAL64>(
                Decimal64(static_cast<Int64>(read_signed_le(ptr, value_size))), value);
        value->precision = BeConsts::MAX_DECIMAL64_PRECISION;
    } else {
        set_primitive_variant_field<TYPE_DECIMAL128I>(Decimal128V3(read_signed_int128_le(ptr)),
                                                      value);
        value->precision = BeConsts::MAX_DECIMAL128_PRECISION;
    }
    value->scale = scale;
    *next = ptr + value_size;
    return Status::OK();
}

Status read_integral_primitive_field(uint8_t primitive_header, const uint8_t* ptr,
                                     const uint8_t* end, FieldWithDataType* value,
                                     const uint8_t** next) {
    int value_size = 8;
    if (primitive_header == 3) {
        value_size = 1;
    } else if (primitive_header == 4) {
        value_size = 2;
    } else if (primitive_header == 5 || primitive_header == 11) {
        value_size = 4;
    }
    RETURN_IF_ERROR(require_available(ptr, end, value_size, "integer value"));
    const auto data = static_cast<Int64>(read_signed_le(ptr, value_size));

    switch (primitive_header) {
    case 3:
        set_primitive_variant_field<TYPE_TINYINT>(static_cast<Int8>(data), value);
        break;
    case 4:
        set_primitive_variant_field<TYPE_SMALLINT>(static_cast<Int16>(data), value);
        break;
    case 5:
        set_primitive_variant_field<TYPE_INT>(static_cast<Int32>(data), value);
        break;
    case 6:
    case 11:
    case 12:
    case 13:
    case 17:
        set_primitive_variant_field<TYPE_BIGINT>(data, value);
        break;
    case 18:
    case 19:
        set_primitive_variant_field<TYPE_BIGINT>(data / 1000, value);
        break;
    default:
        return Status::Corruption("Unsupported Parquet VARIANT primitive header {}",
                                  primitive_header);
    }
    *next = ptr + value_size;
    return Status::OK();
}

Status read_floating_primitive_field(uint8_t primitive_header, const uint8_t* ptr,
                                     const uint8_t* end, FieldWithDataType* value,
                                     const uint8_t** next) {
    if (primitive_header == 14) {
        RETURN_IF_ERROR(require_available(ptr, end, 4, "float value"));
        auto bits = static_cast<uint32_t>(read_unsigned_le(ptr, 4));
        float data;
        std::memcpy(&data, &bits, sizeof(data));
        set_primitive_variant_field<TYPE_FLOAT>(data, value);
        *next = ptr + 4;
        return Status::OK();
    }

    DCHECK_EQ(primitive_header, 7);
    RETURN_IF_ERROR(require_available(ptr, end, 8, "double value"));
    uint64_t bits = read_unsigned_le(ptr, 8);
    double data;
    std::memcpy(&data, &bits, sizeof(data));
    set_primitive_variant_field<TYPE_DOUBLE>(data, value);
    *next = ptr + 8;
    return Status::OK();
}

Status read_binary_primitive_field(const uint8_t* ptr, const uint8_t* end, FieldWithDataType* value,
                                   std::deque<std::string>* string_values, const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 4, "binary length"));
    uint64_t size = read_unsigned_le(ptr, 4);
    ptr += 4;
    RETURN_IF_ERROR(require_available(ptr, end, size, "binary value"));
    string_values->emplace_back(reinterpret_cast<const char*>(ptr), static_cast<size_t>(size));
    value->field = Field::create_field<TYPE_VARBINARY>(StringView(string_values->back()));
    fill_field_type_info(value);
    *next = ptr + size;
    return Status::OK();
}

Status read_string_primitive_field(const uint8_t* ptr, const uint8_t* end, FieldWithDataType* value,
                                   const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 4, "binary or string length"));
    uint64_t size = read_unsigned_le(ptr, 4);
    ptr += 4;
    RETURN_IF_ERROR(require_available(ptr, end, size, "string value"));
    std::string_view data(reinterpret_cast<const char*>(ptr), static_cast<size_t>(size));
    RETURN_IF_ERROR(require_valid_utf8(data, "string value"));
    value->field = Field::create_field<TYPE_STRING>(String(data));
    fill_field_type_info(value);
    *next = ptr + size;
    return Status::OK();
}

Status read_uuid_primitive_field(const uint8_t* ptr, const uint8_t* end, FieldWithDataType* value,
                                 const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 16, "uuid value"));
    value->field = Field::create_field<TYPE_STRING>(format_variant_uuid(ptr));
    fill_field_type_info(value);
    *next = ptr + 16;
    return Status::OK();
}

Status read_array_layout(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                         VariantArrayLayout* layout) {
    int field_offset_size = (value_header & 0x03) + 1;
    int num_elements_size = (value_header & 0x04) != 0 ? 4 : 1;

    RETURN_IF_ERROR(require_available(ptr, end, num_elements_size, "array element count"));
    uint64_t num_elements = read_unsigned_le(ptr, num_elements_size);
    ptr += num_elements_size;

    RETURN_IF_ERROR(require_available_entries(ptr, end, num_elements + 1, field_offset_size,
                                              "array field offsets"));
    layout->field_offsets.resize(num_elements + 1);
    for (uint64_t i = 0; i <= num_elements; ++i) {
        layout->field_offsets[i] = read_unsigned_le(ptr, field_offset_size);
        ptr += field_offset_size;
    }

    layout->total_size = layout->field_offsets.back();
    layout->fields = ptr;
    RETURN_IF_ERROR(
            require_available(layout->fields, end, layout->total_size, "array field values"));
    RETURN_IF_ERROR(
            validate_array_field_offsets(layout->field_offsets, layout->total_size, "array"));
    return Status::OK();
}

Status read_object_layout(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                          const VariantMetadata& metadata, VariantObjectLayout* layout) {
    int field_offset_size = (value_header & 0x03) + 1;
    int field_id_size = ((value_header >> 2) & 0x03) + 1;
    int num_elements_size = (value_header & 0x10) != 0 ? 4 : 1;

    RETURN_IF_ERROR(require_available(ptr, end, num_elements_size, "object element count"));
    uint64_t num_elements = read_unsigned_le(ptr, num_elements_size);
    ptr += num_elements_size;

    RETURN_IF_ERROR(
            require_available_entries(ptr, end, num_elements, field_id_size, "object field ids"));
    layout->field_ids.resize(num_elements);
    for (uint64_t i = 0; i < num_elements; ++i) {
        layout->field_ids[i] = read_unsigned_le(ptr, field_id_size);
        ptr += field_id_size;
        if (layout->field_ids[i] >= metadata.dictionary.size()) {
            return Status::Corruption("Invalid Parquet VARIANT object field id {}",
                                      layout->field_ids[i]);
        }
        if (i > 0 && !variant_string_less(metadata.dictionary[layout->field_ids[i - 1]],
                                          metadata.dictionary[layout->field_ids[i]])) {
            return Status::Corruption("Invalid Parquet VARIANT object field names");
        }
    }

    RETURN_IF_ERROR(require_available_entries(ptr, end, num_elements + 1, field_offset_size,
                                              "object field offsets"));
    layout->field_offsets.resize(num_elements + 1);
    for (uint64_t i = 0; i <= num_elements; ++i) {
        layout->field_offsets[i] = read_unsigned_le(ptr, field_offset_size);
        ptr += field_offset_size;
    }

    layout->total_size = layout->field_offsets.back();
    layout->fields = ptr;
    RETURN_IF_ERROR(
            require_available(layout->fields, end, layout->total_size, "object field values"));
    RETURN_IF_ERROR(compute_object_field_ends(layout->field_offsets, layout->total_size,
                                              &layout->field_ends));
    return Status::OK();
}

Status decode_value_to_variant_map(const uint8_t* ptr, const uint8_t* end,
                                   const VariantMetadata& metadata, PathInDataBuilder* path,
                                   VariantMap* values, std::deque<std::string>* string_values,
                                   const uint8_t** next);

Status decode_primitive_to_variant_map(uint8_t primitive_header, const uint8_t* ptr,
                                       const uint8_t* end, const VariantMetadata&,
                                       PathInDataBuilder* path, VariantMap* values,
                                       std::deque<std::string>* string_values,
                                       const uint8_t** next) {
    FieldWithDataType value;
    switch (primitive_header) {
    case 0:
        value.field = Field();
        value.base_scalar_type_id = INVALID_TYPE;
        *next = ptr;
        break;
    case 1:
        set_primitive_variant_field<TYPE_BOOLEAN>(true, &value);
        *next = ptr;
        break;
    case 2:
        set_primitive_variant_field<TYPE_BOOLEAN>(false, &value);
        *next = ptr;
        break;
    case 3:
    case 4:
    case 5:
    case 6:
    case 11:
    case 12:
    case 13:
    case 17:
    case 18:
    case 19:
        RETURN_IF_ERROR(read_integral_primitive_field(primitive_header, ptr, end, &value, next));
        break;
    case 7:
    case 14:
        RETURN_IF_ERROR(read_floating_primitive_field(primitive_header, ptr, end, &value, next));
        break;
    case 8:
    case 9:
    case 10:
        RETURN_IF_ERROR(read_decimal_primitive_field(primitive_header, ptr, end, &value, next));
        break;
    case 15:
        RETURN_IF_ERROR(read_binary_primitive_field(ptr, end, &value, string_values, next));
        break;
    case 16:
        RETURN_IF_ERROR(read_string_primitive_field(ptr, end, &value, next));
        break;
    case 20:
        RETURN_IF_ERROR(read_uuid_primitive_field(ptr, end, &value, next));
        break;
    default:
        return Status::Corruption("Unsupported Parquet VARIANT primitive header {}",
                                  primitive_header);
    }
    (*values)[path->build()] = std::move(value);
    return Status::OK();
}

Status decode_object_to_variant_map(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                                    const VariantMetadata& metadata, PathInDataBuilder* path,
                                    VariantMap* values, std::deque<std::string>* string_values,
                                    const uint8_t** next) {
    VariantObjectLayout layout;
    RETURN_IF_ERROR(read_object_layout(value_header, ptr, end, metadata, &layout));

    if (layout.field_ids.empty()) {
        RETURN_IF_ERROR(insert_empty_object_marker(path->build(), values));
    }

    for (uint64_t i = 0; i < layout.field_ids.size(); ++i) {
        const uint8_t* child_begin = layout.fields + layout.field_offsets[i];
        const uint8_t* child_end = layout.fields + layout.field_ends[i];
        const uint8_t* child_next = nullptr;
        path->append(metadata.dictionary[layout.field_ids[i]], false);
        RETURN_IF_ERROR(decode_value_to_variant_map(child_begin, child_end, metadata, path, values,
                                                    string_values, &child_next));
        path->pop_back();
        if (child_next != child_end) {
            return Status::Corruption("Invalid Parquet VARIANT object child value length");
        }
    }
    *next = layout.fields + layout.total_size;
    return Status::OK();
}

void move_variant_map_to_field(VariantMap&& element_values, FieldWithDataType* value) {
    if (element_values.size() == 1 && element_values.begin()->first.empty()) {
        *value = std::move(element_values.begin()->second);
        return;
    }
    value->field = Field::create_field<TYPE_VARIANT>(std::move(element_values));
    fill_field_type_info(value);
}

Status decode_array_element_to_field(const uint8_t* ptr, const uint8_t* end,
                                     const VariantMetadata& metadata, FieldWithDataType* value,
                                     std::deque<std::string>* string_values, const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 1, "array child value"));
    const uint8_t value_metadata = *ptr++;
    const uint8_t basic_type = value_metadata & 0x03;
    const uint8_t value_header = value_metadata >> 2;

    if (basic_type == 0) {
        VariantMap element_values;
        PathInDataBuilder element_path;
        RETURN_IF_ERROR(decode_primitive_to_variant_map(value_header, ptr, end, metadata,
                                                        &element_path, &element_values,
                                                        string_values, next));
        move_variant_map_to_field(std::move(element_values), value);
        return Status::OK();
    }

    if (basic_type == 1) {
        const size_t size = value_header;
        RETURN_IF_ERROR(require_available(ptr, end, size, "short string value"));
        std::string_view data(reinterpret_cast<const char*>(ptr), size);
        RETURN_IF_ERROR(require_valid_utf8(data, "short string value"));
        value->field = Field::create_field<TYPE_STRING>(String(data));
        fill_field_type_info(value);
        *next = ptr + size;
        return Status::OK();
    }

    if (basic_type == 2 || basic_type == 3) {
        VariantMap element_values;
        PathInDataBuilder element_path;
        RETURN_IF_ERROR(decode_value_to_variant_map(ptr - 1, end, metadata, &element_path,
                                                    &element_values, string_values, next));
        move_variant_map_to_field(std::move(element_values), value);
        return Status::OK();
    }

    std::string json;
    RETURN_IF_ERROR(decode_value(ptr - 1, end, metadata, &json, next));
    VariantMap element_values;
    RETURN_IF_ERROR(parse_json_to_variant_map(json, PathInData(), &element_values));
    move_variant_map_to_field(std::move(element_values), value);
    return Status::OK();
}

Status decode_array_to_variant_map(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                                   const VariantMetadata& metadata, PathInDataBuilder* path,
                                   VariantMap* values, std::deque<std::string>* string_values,
                                   const uint8_t** next) {
    VariantArrayLayout layout;
    RETURN_IF_ERROR(read_array_layout(value_header, ptr, end, &layout));

    Array array;
    array.reserve(layout.field_offsets.size() - 1);
    for (uint64_t i = 0; i + 1 < layout.field_offsets.size(); ++i) {
        const uint8_t* child_begin = layout.fields + layout.field_offsets[i];
        const uint8_t* child_end = layout.fields + layout.field_offsets[i + 1];
        const uint8_t* child_next = nullptr;
        FieldWithDataType child;
        RETURN_IF_ERROR(decode_array_element_to_field(child_begin, child_end, metadata, &child,
                                                      string_values, &child_next));
        if (child_next != child_end) {
            return Status::Corruption("Invalid Parquet VARIANT array child value length");
        }
        array.push_back(std::move(child.field));
    }

    FieldWithDataType value;
    const size_t elements = array.size();
    value.field = Field::create_field<TYPE_ARRAY>(std::move(array));
    fill_field_type_info(&value);
    if (value.base_scalar_type_id == INVALID_TYPE) {
        RETURN_IF_ERROR(make_jsonb_field(make_null_array_json(elements), &value));
    }
    (*values)[path->build()] = std::move(value);
    *next = layout.fields + layout.total_size;
    return Status::OK();
}

Status decode_value_to_variant_map(const uint8_t* ptr, const uint8_t* end,
                                   const VariantMetadata& metadata, PathInDataBuilder* path,
                                   VariantMap* values, std::deque<std::string>* string_values,
                                   const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 1, "value"));
    uint8_t value_metadata = *ptr++;
    uint8_t basic_type = value_metadata & 0x03;
    uint8_t value_header = value_metadata >> 2;

    switch (basic_type) {
    case 0:
        return decode_primitive_to_variant_map(value_header, ptr, end, metadata, path, values,
                                               string_values, next);
    case 2:
        return decode_object_to_variant_map(value_header, ptr, end, metadata, path, values,
                                            string_values, next);
    case 1:
        [[fallthrough]];
    case 3: {
        if (basic_type == 3) {
            Status array_st = decode_array_to_variant_map(value_header, ptr, end, metadata, path,
                                                          values, string_values, next);
            if (array_st.ok()) {
                return array_st;
            }
            if (!array_st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
                return array_st;
            }
        }
        std::string json;
        RETURN_IF_ERROR(decode_value(ptr - 1, end, metadata, &json, next));
        return parse_json_to_variant_map(json, path->build(), values);
    }
    default:
        return Status::Corruption("Unsupported Parquet VARIANT basic type {}", basic_type);
    }
}

Status decode_metadata(const StringRef& metadata, VariantMetadata* result) {
    const auto* ptr = reinterpret_cast<const uint8_t*>(metadata.data);
    const auto* end = ptr + metadata.size;
    RETURN_IF_ERROR(require_available(ptr, end, 1, "metadata"));
    uint8_t header = *ptr++;
    uint8_t version = header & 0x0f;
    if (version != 1) {
        return Status::Corruption("Unsupported Parquet VARIANT metadata version {}", version);
    }
    if ((header & 0x20) != 0) {
        return Status::Corruption("Invalid Parquet VARIANT metadata header {}", header);
    }
    const bool sorted_strings = (header & 0x10) != 0;
    int offset_size = ((header >> 6) & 0x03) + 1;
    RETURN_IF_ERROR(require_available(ptr, end, offset_size, "metadata dictionary size"));
    uint64_t dictionary_size = read_unsigned_le(ptr, offset_size);
    ptr += offset_size;

    RETURN_IF_ERROR(require_available_entries(ptr, end, dictionary_size + 1, offset_size,
                                              "metadata dictionary offsets"));
    std::vector<uint64_t> offsets(dictionary_size + 1);
    for (uint64_t i = 0; i <= dictionary_size; ++i) {
        offsets[i] = read_unsigned_le(ptr, offset_size);
        ptr += offset_size;
        if (i > 0 && offsets[i] < offsets[i - 1]) {
            return Status::Corruption("Invalid Parquet VARIANT metadata dictionary offsets");
        }
    }
    if (offsets.front() != 0) {
        return Status::Corruption("Invalid Parquet VARIANT metadata dictionary offsets");
    }

    RETURN_IF_ERROR(require_available(ptr, end, offsets.back(), "metadata dictionary bytes"));
    if (ptr + offsets.back() != end) {
        return Status::Corruption("Invalid Parquet VARIANT metadata dictionary bytes");
    }
    result->dictionary.clear();
    result->dictionary.reserve(dictionary_size);
    for (uint64_t i = 0; i < dictionary_size; ++i) {
        std::string entry(reinterpret_cast<const char*>(ptr + offsets[i]),
                          offsets[i + 1] - offsets[i]);
        RETURN_IF_ERROR(require_valid_utf8(entry, "metadata dictionary"));
        if (sorted_strings && !result->dictionary.empty() &&
            !variant_string_less(result->dictionary.back(), entry)) {
            return Status::Corruption("Invalid Parquet VARIANT sorted metadata dictionary key");
        }
        result->dictionary.emplace_back(std::move(entry));
    }
    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity, readability-function-size): VARIANT primitive tags are a compact spec switch.
Status decode_primitive(uint8_t primitive_header, const uint8_t* ptr, const uint8_t* end,
                        std::string* json, const uint8_t** next) {
    switch (primitive_header) {
    case 0:
        json->append("null");
        *next = ptr;
        return Status::OK();
    case 1:
        json->append("true");
        *next = ptr;
        return Status::OK();
    case 2:
        json->append("false");
        *next = ptr;
        return Status::OK();
    case 3:
        RETURN_IF_ERROR(require_available(ptr, end, 1, "int8 value"));
        json->append(std::to_string(static_cast<int8_t>(*ptr)));
        *next = ptr + 1;
        return Status::OK();
    case 4:
        RETURN_IF_ERROR(require_available(ptr, end, 2, "int16 value"));
        json->append(std::to_string(read_signed_le(ptr, 2)));
        *next = ptr + 2;
        return Status::OK();
    case 5:
        RETURN_IF_ERROR(require_available(ptr, end, 4, "int32 value"));
        json->append(std::to_string(read_signed_le(ptr, 4)));
        *next = ptr + 4;
        return Status::OK();
    case 6:
        RETURN_IF_ERROR(require_available(ptr, end, 8, "int64 value"));
        json->append(std::to_string(read_signed_le(ptr, 8)));
        *next = ptr + 8;
        return Status::OK();
    case 7: {
        RETURN_IF_ERROR(require_available(ptr, end, 8, "double value"));
        uint64_t bits = read_unsigned_le(ptr, 8);
        double value;
        std::memcpy(&value, &bits, sizeof(value));
        RETURN_IF_ERROR(append_floating_json(value, json));
        *next = ptr + 8;
        return Status::OK();
    }
    case 8:
    case 9:
    case 10: {
        int value_size = 16;
        if (primitive_header == 8) {
            value_size = 4;
        } else if (primitive_header == 9) {
            value_size = 8;
        }
        RETURN_IF_ERROR(require_available(ptr, end, 1 + value_size, "decimal value"));
        int scale = static_cast<int8_t>(*ptr++);
        if (scale < 0 || scale > 38) {
            return Status::Corruption("Invalid Parquet VARIANT decimal scale {}", scale);
        }
        __int128 unscaled = 0;
        if (value_size == 16) {
            unscaled = read_signed_int128_le(ptr);
        } else {
            unscaled = read_signed_le(ptr, value_size);
        }
        append_decimal_json(unscaled, scale, json);
        *next = ptr + value_size;
        return Status::OK();
    }
    case 11:
        RETURN_IF_ERROR(require_available(ptr, end, 4, "date value"));
        json->append(std::to_string(read_signed_le(ptr, 4)));
        *next = ptr + 4;
        return Status::OK();
    case 12:
    case 13:
    case 17:
        RETURN_IF_ERROR(require_available(ptr, end, 8, "time or timestamp value"));
        json->append(std::to_string(read_signed_le(ptr, 8)));
        *next = ptr + 8;
        return Status::OK();
    case 18:
    case 19:
        RETURN_IF_ERROR(require_available(ptr, end, 8, "nanosecond timestamp value"));
        json->append(std::to_string(read_signed_le(ptr, 8) / 1000));
        *next = ptr + 8;
        return Status::OK();
    case 14: {
        RETURN_IF_ERROR(require_available(ptr, end, 4, "float value"));
        auto bits = static_cast<uint32_t>(read_unsigned_le(ptr, 4));
        float value;
        std::memcpy(&value, &bits, sizeof(value));
        RETURN_IF_ERROR(append_floating_json(value, json));
        *next = ptr + 4;
        return Status::OK();
    }
    case 15: {
        RETURN_IF_ERROR(require_available(ptr, end, 4, "binary length"));
        uint64_t size = read_unsigned_le(ptr, 4);
        ptr += 4;
        RETURN_IF_ERROR(require_available(ptr, end, size, "binary value"));
        std::string_view value(reinterpret_cast<const char*>(ptr), static_cast<size_t>(size));
        append_json_string(value, json, true);
        *next = ptr + size;
        return Status::OK();
    }
    case 16: {
        RETURN_IF_ERROR(require_available(ptr, end, 4, "binary or string length"));
        uint64_t size = read_unsigned_le(ptr, 4);
        ptr += 4;
        RETURN_IF_ERROR(require_available(ptr, end, size, "string value"));
        std::string_view value(reinterpret_cast<const char*>(ptr), static_cast<size_t>(size));
        RETURN_IF_ERROR(require_valid_utf8(value, "string value"));
        append_json_string(value, json);
        *next = ptr + size;
        return Status::OK();
    }
    case 20:
        RETURN_IF_ERROR(require_available(ptr, end, 16, "uuid value"));
        append_uuid_json(ptr, json);
        *next = ptr + 16;
        return Status::OK();
    default:
        return Status::Corruption("Unsupported Parquet VARIANT primitive header {}",
                                  primitive_header);
    }
}

Status decode_object(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                     const VariantMetadata& metadata, std::string* json, const uint8_t** next) {
    int field_offset_size = (value_header & 0x03) + 1;
    int field_id_size = ((value_header >> 2) & 0x03) + 1;
    int num_elements_size = (value_header & 0x10) != 0 ? 4 : 1;

    RETURN_IF_ERROR(require_available(ptr, end, num_elements_size, "object element count"));
    uint64_t num_elements = read_unsigned_le(ptr, num_elements_size);
    ptr += num_elements_size;

    RETURN_IF_ERROR(
            require_available_entries(ptr, end, num_elements, field_id_size, "object field ids"));
    std::vector<uint64_t> field_ids(num_elements);
    for (uint64_t i = 0; i < num_elements; ++i) {
        field_ids[i] = read_unsigned_le(ptr, field_id_size);
        ptr += field_id_size;
        if (field_ids[i] >= metadata.dictionary.size()) {
            return Status::Corruption("Invalid Parquet VARIANT object field id {}", field_ids[i]);
        }
        if (i > 0 && !variant_string_less(metadata.dictionary[field_ids[i - 1]],
                                          metadata.dictionary[field_ids[i]])) {
            return Status::Corruption("Invalid Parquet VARIANT object field names");
        }
    }

    RETURN_IF_ERROR(require_available_entries(ptr, end, num_elements + 1, field_offset_size,
                                              "object field offsets"));
    std::vector<uint64_t> field_offsets(num_elements + 1);
    for (uint64_t i = 0; i <= num_elements; ++i) {
        field_offsets[i] = read_unsigned_le(ptr, field_offset_size);
        ptr += field_offset_size;
    }

    uint64_t total_size = field_offsets.back();
    const uint8_t* fields = ptr;
    RETURN_IF_ERROR(require_available(fields, end, total_size, "object field values"));
    std::vector<uint64_t> field_ends;
    RETURN_IF_ERROR(compute_object_field_ends(field_offsets, total_size, &field_ends));

    json->push_back('{');
    for (uint64_t i = 0; i < num_elements; ++i) {
        if (i != 0) {
            json->push_back(',');
        }
        append_json_string(metadata.dictionary[field_ids[i]], json);
        json->push_back(':');
        const uint8_t* child_begin = fields + field_offsets[i];
        const uint8_t* child_end = fields + field_ends[i];
        const uint8_t* child_next = nullptr;
        RETURN_IF_ERROR(decode_value(child_begin, child_end, metadata, json, &child_next));
        if (child_next != child_end) {
            return Status::Corruption("Invalid Parquet VARIANT object child value length");
        }
    }
    json->push_back('}');
    *next = fields + total_size;
    return Status::OK();
}

Status decode_array(uint8_t value_header, const uint8_t* ptr, const uint8_t* end,
                    const VariantMetadata& metadata, std::string* json, const uint8_t** next) {
    VariantArrayLayout layout;
    RETURN_IF_ERROR(read_array_layout(value_header, ptr, end, &layout));

    json->push_back('[');
    for (uint64_t i = 0; i + 1 < layout.field_offsets.size(); ++i) {
        if (i != 0) {
            json->push_back(',');
        }
        const uint8_t* child_begin = layout.fields + layout.field_offsets[i];
        const uint8_t* child_end = layout.fields + layout.field_offsets[i + 1];
        const uint8_t* child_next = nullptr;
        RETURN_IF_ERROR(decode_value(child_begin, child_end, metadata, json, &child_next));
        if (child_next != child_end) {
            return Status::Corruption("Invalid Parquet VARIANT array child value length");
        }
    }
    json->push_back(']');
    *next = layout.fields + layout.total_size;
    return Status::OK();
}

Status decode_value(const uint8_t* ptr, const uint8_t* end, const VariantMetadata& metadata,
                    std::string* json, const uint8_t** next) {
    RETURN_IF_ERROR(require_available(ptr, end, 1, "value"));
    uint8_t value_metadata = *ptr++;
    uint8_t basic_type = value_metadata & 0x03;
    uint8_t value_header = value_metadata >> 2;

    switch (basic_type) {
    case 0:
        return decode_primitive(value_header, ptr, end, json, next);
    case 1: {
        size_t size = value_header;
        RETURN_IF_ERROR(require_available(ptr, end, size, "short string value"));
        std::string_view value(reinterpret_cast<const char*>(ptr), static_cast<size_t>(size));
        RETURN_IF_ERROR(require_valid_utf8(value, "short string value"));
        append_json_string(value, json);
        *next = ptr + size;
        return Status::OK();
    }
    case 2:
        return decode_object(value_header, ptr, end, metadata, json, next);
    case 3:
        return decode_array(value_header, ptr, end, metadata, json, next);
    default:
        return Status::Corruption("Unsupported Parquet VARIANT basic type {}", basic_type);
    }
}

} // namespace

Status decode_variant_to_json(const StringRef& metadata, const StringRef& value,
                              std::string* json) {
    VariantMetadata decoded_metadata;
    RETURN_IF_ERROR(decode_metadata(metadata, &decoded_metadata));
    json->clear();
    const auto* ptr = reinterpret_cast<const uint8_t*>(value.data);
    const auto* end = ptr + value.size;
    const uint8_t* next = nullptr;
    RETURN_IF_ERROR(decode_value(ptr, end, decoded_metadata, json, &next));
    if (next != end) {
        return Status::Corruption("Invalid Parquet VARIANT value has {} trailing bytes",
                                  end - next);
    }
    return Status::OK();
}

Status decode_variant_to_variant_map(const StringRef& metadata, const StringRef& value,
                                     const PathInData& prefix, VariantMap* values,
                                     std::deque<std::string>* string_values) {
    VariantMetadata decoded_metadata;
    RETURN_IF_ERROR(decode_metadata(metadata, &decoded_metadata));
    const auto* ptr = reinterpret_cast<const uint8_t*>(value.data);
    const auto* end = ptr + value.size;
    const uint8_t* next = nullptr;
    PathInDataBuilder path;
    path.append(prefix.get_parts(), false);
    RETURN_IF_ERROR(decode_value_to_variant_map(ptr, end, decoded_metadata, &path, values,
                                                string_values, &next));
    if (next != end) {
        return Status::Corruption("Invalid Parquet VARIANT value has {} trailing bytes",
                                  end - next);
    }
    return Status::OK();
}

} // namespace doris::parquet
