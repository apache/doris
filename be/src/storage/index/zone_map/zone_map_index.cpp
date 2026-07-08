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

#include "storage/index/zone_map/zone_map_index.h"

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>

#include "common/cast_set.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/packed_int128.h"
#include "core/string_ref.h"
#include "core/uint24.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "storage/index/indexed_column_reader.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/olap_common.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/page_pointer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/unaligned.h"

namespace doris {
struct uint24_t;

namespace segment_v2 {
namespace {

constexpr uint32_t ZONE_MAP_NATIVE_ENTRY_VERSION = 1;
constexpr uint32_t ZONE_MAP_NATIVE_PAGE_FORMAT_VERSION = 1;
constexpr uint32_t ZONE_MAP_NATIVE_OFFSET_WIDTH = 4;

template <typename T>
void append_as_little_endian(std::string* dst, T value) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>);
    if constexpr (sizeof(T) == 1) {
        dst->push_back(static_cast<char>(value));
    } else if constexpr (sizeof(T) == 2) {
        uint8_t buf[sizeof(uint16_t)];
        uint16_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        encode_fixed16_le(buf, bits);
        dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
    } else if constexpr (sizeof(T) == 4) {
        uint8_t buf[sizeof(uint32_t)];
        uint32_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        encode_fixed32_le(buf, bits);
        dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
    } else if constexpr (sizeof(T) == 8) {
        uint8_t buf[sizeof(uint64_t)];
        uint64_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        encode_fixed64_le(buf, bits);
        dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
    }
}

template <typename T>
T read_little_endian(const char* data) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>);
    if constexpr (sizeof(T) == 1) {
        return static_cast<T>(*reinterpret_cast<const uint8_t*>(data));
    } else if constexpr (sizeof(T) == 2) {
        uint16_t bits = decode_fixed16_le(reinterpret_cast<const uint8_t*>(data));
        T value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    } else if constexpr (sizeof(T) == 4) {
        uint32_t bits = decode_fixed32_le(reinterpret_cast<const uint8_t*>(data));
        T value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    } else {
        uint64_t bits = decode_fixed64_le(reinterpret_cast<const uint8_t*>(data));
        T value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    }
}

void append_int128_le(std::string* dst, Int128 value) {
    uint8_t buf[sizeof(uint128_t)];
    uint128_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    encode_fixed128_le(buf, bits);
    dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
}

Int128 read_int128_le(const char* data) {
    uint128_t bits = decode_fixed128_le(reinterpret_cast<const uint8_t*>(data));
    Int128 value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

void append_int256_le(std::string* dst, const wide::Int256& value) {
    for (size_t i = 0; i < wide::Int256::item_count; ++i) {
        uint8_t buf[sizeof(uint64_t)];
        encode_fixed64_le(buf, value.items[wide::Int256::_impl::little(i)]);
        dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
    }
}

wide::Int256 read_int256_le(const char* data) {
    wide::Int256 value;
    for (size_t i = 0; i < wide::Int256::item_count; ++i) {
        value.items[wide::Int256::_impl::little(i)] =
                decode_fixed64_le(reinterpret_cast<const uint8_t*>(data + i * sizeof(uint64_t)));
    }
    return value;
}

void append_uint24_le(std::string* dst, uint24_t value) {
    const uint32_t raw = static_cast<uint32_t>(value);
    dst->push_back(static_cast<char>(raw));
    dst->push_back(static_cast<char>(raw >> 8));
    dst->push_back(static_cast<char>(raw >> 16));
}

uint24_t read_uint24_le(const char* data) {
    uint32_t raw = static_cast<uint8_t>(data[0]);
    raw |= static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 8;
    raw |= static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 16;
    return uint24_t(raw);
}

template <PrimitiveType Type>
constexpr uint32_t native_fixed_width() {
    if constexpr (Type == TYPE_DATE) {
        return 3;
    } else if constexpr (Type == TYPE_DECIMALV2) {
        return sizeof(int64_t) + sizeof(int32_t);
    } else {
        return sizeof(typename PrimitiveTypeTraits<Type>::StorageFieldType);
    }
}

template <PrimitiveType Type>
void append_native_value(const Field& field, std::string* dst) {
    if constexpr (Type == TYPE_DATE) {
        append_uint24_le(dst, uint24_t(field.get<Type>().to_olap_date()));
    } else if constexpr (Type == TYPE_DATETIME) {
        append_as_little_endian<uint64_t>(dst, field.get<Type>().to_olap_datetime());
    } else if constexpr (Type == TYPE_DATEV2) {
        append_as_little_endian<uint32_t>(
                dst, static_cast<uint32_t>(field.get<Type>().to_date_int_val()));
    } else if constexpr (Type == TYPE_DATETIMEV2) {
        append_as_little_endian<uint64_t>(dst, field.get<Type>().to_date_int_val());
    } else if constexpr (Type == TYPE_TIMESTAMPTZ) {
        append_as_little_endian<uint64_t>(dst, field.get<Type>().to_date_int_val());
    } else if constexpr (Type == TYPE_DECIMALV2) {
        const auto& value = field.get<Type>();
        append_as_little_endian<int64_t>(dst, value.int_value());
        append_as_little_endian<int32_t>(dst, value.frac_value());
    } else if constexpr (Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                         Type == TYPE_DECIMAL128I || Type == TYPE_DECIMAL256) {
        const auto& value = field.get<Type>().value;
        if constexpr (Type == TYPE_DECIMAL128I) {
            append_int128_le(dst, value);
        } else if constexpr (Type == TYPE_DECIMAL256) {
            append_int256_le(dst, value);
        } else {
            append_as_little_endian(dst, value);
        }
    } else if constexpr (Type == TYPE_LARGEINT) {
        append_int128_le(dst, field.get<Type>());
    } else if constexpr (Type == TYPE_IPV6) {
        uint8_t buf[sizeof(uint128_t)];
        encode_fixed128_le(buf, field.get<Type>());
        dst->append(reinterpret_cast<const char*>(buf), sizeof(buf));
    } else if constexpr (is_string_type(Type)) {
        const auto& value = field.get<Type>();
        dst->append(value.data(), value.size());
    } else {
        append_as_little_endian(dst, field.get<Type>());
    }
}

template <PrimitiveType Type>
Status decode_native_value(std::string_view value, Field* field) {
    DORIS_CHECK(field != nullptr);
    if constexpr (is_string_type(Type)) {
        *field = Field::create_field<Type>(String(value.data(), value.size()));
        return Status::OK();
    } else {
        if (value.size() != native_fixed_width<Type>()) {
            return Status::Corruption("invalid zone map native value size {}, expected {}",
                                      value.size(), native_fixed_width<Type>());
        }
        const char* data = value.data();
        if constexpr (Type == TYPE_DATE) {
            *field = Field::create_field_from_olap_value<Type>(read_uint24_le(data));
        } else if constexpr (Type == TYPE_DATETIME) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<uint64_t>(data));
        } else if constexpr (Type == TYPE_DATEV2) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<uint32_t>(data));
        } else if constexpr (Type == TYPE_DATETIMEV2) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<uint64_t>(data));
        } else if constexpr (Type == TYPE_TIMESTAMPTZ) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<uint64_t>(data));
        } else if constexpr (Type == TYPE_DECIMALV2) {
            decimal12_t decimal {read_little_endian<int64_t>(data),
                                 read_little_endian<int32_t>(data + sizeof(int64_t))};
            *field = Field::create_field_from_olap_value<Type>(decimal);
        } else if constexpr (Type == TYPE_DECIMAL32) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<Int32>(data));
        } else if constexpr (Type == TYPE_DECIMAL64) {
            *field = Field::create_field_from_olap_value<Type>(read_little_endian<Int64>(data));
        } else if constexpr (Type == TYPE_DECIMAL128I) {
            *field = Field::create_field_from_olap_value<Type>(read_int128_le(data));
        } else if constexpr (Type == TYPE_DECIMAL256) {
            *field = Field::create_field_from_olap_value<Type>(read_int256_le(data));
        } else if constexpr (Type == TYPE_LARGEINT) {
            *field = Field::create_field_from_olap_value<Type>(read_int128_le(data));
        } else if constexpr (Type == TYPE_IPV6) {
            *field = Field::create_field_from_olap_value<Type>(
                    decode_fixed128_le(reinterpret_cast<const uint8_t*>(data)));
        } else {
            *field = Field::create_field_from_olap_value<Type>(
                    read_little_endian<typename PrimitiveTypeTraits<Type>::StorageFieldType>(data));
        }
        return Status::OK();
    }
}

template <PrimitiveType Type>
void append_value_array_value(const Field& field, ZoneMapValueArrayPB* values) {
    DORIS_CHECK(values != nullptr);
    if constexpr (is_string_type(Type)) {
        if (!values->has_layout()) {
            values->set_layout(ZoneMapValueArrayPB::VAR_BINARY);
            values->set_offset_width(ZONE_MAP_NATIVE_OFFSET_WIDTH);
            put_fixed32_le(values->mutable_offsets(), 0);
        }
        const auto& value = field.get<Type>();
        values->mutable_values()->append(value.data(), value.size());
        put_fixed32_le(values->mutable_offsets(), cast_set<uint32_t>(values->values().size()));
    } else {
        if (!values->has_layout()) {
            values->set_layout(ZoneMapValueArrayPB::FIXED_WIDTH);
            values->set_fixed_width(native_fixed_width<Type>());
        }
        append_native_value<Type>(field, values->mutable_fixed_values());
    }
}

template <PrimitiveType Type>
void init_empty_value_array(ZoneMapValueArrayPB* values) {
    DORIS_CHECK(values != nullptr);
    if constexpr (is_string_type(Type)) {
        values->set_layout(ZoneMapValueArrayPB::VAR_BINARY);
        values->set_offset_width(ZONE_MAP_NATIVE_OFFSET_WIDTH);
        put_fixed32_le(values->mutable_offsets(), 0);
    } else {
        values->set_layout(ZoneMapValueArrayPB::FIXED_WIDTH);
        values->set_fixed_width(native_fixed_width<Type>());
    }
}

void set_bitmap_bit(std::string* bitmap, uint32_t index, bool value) {
    if (value) {
        (*bitmap)[index / 8] =
                static_cast<char>(static_cast<uint8_t>((*bitmap)[index / 8]) | (1U << (index % 8)));
    }
}

bool get_bitmap_bit(const std::string& bitmap, uint32_t index) {
    return (static_cast<uint8_t>(bitmap[index / 8]) & (1U << (index % 8))) != 0;
}

Status validate_bitmap(const std::string& bitmap, uint32_t num_pages, std::string_view name) {
    const size_t min_size = (num_pages + 7) / 8;
    if (bitmap.size() < min_size) {
        return Status::Corruption("zone map {} bitmap too small: {}, expected at least {}", name,
                                  bitmap.size(), min_size);
    }
    return Status::OK();
}

bool has_present_value(const ZoneMap& zone_map) {
    return zone_map.has_not_null && !zone_map.pass_all;
}

template <PrimitiveType Type>
Status apply_special_float_values(ZoneMap* zone_map) {
    DORIS_CHECK(zone_map != nullptr);
    if (!zone_map->has_not_null) {
        return Status::OK();
    }
    if constexpr (Type == TYPE_FLOAT || Type == TYPE_DOUBLE) {
        using CppType = typename PrimitiveTypeTraits<Type>::CppType;
        if (zone_map->has_negative_inf) {
            zone_map->min_value =
                    Field::create_field<Type>(-std::numeric_limits<CppType>::infinity());
        }
        if (zone_map->has_nan) {
            zone_map->max_value =
                    Field::create_field<Type>(std::numeric_limits<CppType>::quiet_NaN());
        } else if (zone_map->has_positive_inf) {
            zone_map->max_value =
                    Field::create_field<Type>(std::numeric_limits<CppType>::infinity());
        }
    } else {
        if (zone_map->has_negative_inf) {
            return Status::InternalError("invalid zone map with negative Infinity");
        }
        if (zone_map->has_nan) {
            return Status::InternalError("invalid zone map with NaN");
        }
        if (zone_map->has_positive_inf) {
            return Status::InternalError("invalid zone map with positive Infinity");
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
Status validate_value_array(const ZoneMapValueArrayPB& values, uint32_t value_count,
                            std::string_view name) {
    if constexpr (is_string_type(Type)) {
        if (values.layout() != ZoneMapValueArrayPB::VAR_BINARY) {
            return Status::Corruption("zone map {} values use unsupported layout {}", name,
                                      values.layout());
        }
        if (values.offset_width() != ZONE_MAP_NATIVE_OFFSET_WIDTH) {
            return Status::Corruption("zone map {} values use unsupported offset width {}", name,
                                      values.offset_width());
        }
        const size_t expected_offsets_size =
                (static_cast<size_t>(value_count) + 1) * ZONE_MAP_NATIVE_OFFSET_WIDTH;
        if (values.offsets().size() != expected_offsets_size) {
            return Status::Corruption("zone map {} offsets size {}, expected {}", name,
                                      values.offsets().size(), expected_offsets_size);
        }
        uint32_t previous_offset = 0;
        for (uint32_t i = 0; i <= value_count; ++i) {
            uint32_t offset = decode_fixed32_le(reinterpret_cast<const uint8_t*>(
                    values.offsets().data() + i * ZONE_MAP_NATIVE_OFFSET_WIDTH));
            if (i == 0 && offset != 0) {
                return Status::Corruption("zone map {} first offset must be zero", name);
            }
            if (offset < previous_offset) {
                return Status::Corruption("zone map {} offsets are not monotonic", name);
            }
            if (offset > values.values().size()) {
                return Status::Corruption("zone map {} offset {} exceeds values size {}", name,
                                          offset, values.values().size());
            }
            previous_offset = offset;
        }
        if (previous_offset != values.values().size()) {
            return Status::Corruption("zone map {} last offset {} does not match values size {}",
                                      name, previous_offset, values.values().size());
        }
    } else {
        if (values.layout() != ZoneMapValueArrayPB::FIXED_WIDTH) {
            return Status::Corruption("zone map {} values use unsupported layout {}", name,
                                      values.layout());
        }
        if (values.fixed_width() != native_fixed_width<Type>()) {
            return Status::Corruption("zone map {} fixed width {}, expected {}", name,
                                      values.fixed_width(), native_fixed_width<Type>());
        }
        const size_t expected_values_size =
                static_cast<size_t>(value_count) * native_fixed_width<Type>();
        if (values.fixed_values().size() != expected_values_size) {
            return Status::Corruption("zone map {} fixed values size {}, expected {}", name,
                                      values.fixed_values().size(), expected_values_size);
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
Status decode_value_array_value(const ZoneMapValueArrayPB& values, uint32_t index, Field* field) {
    if constexpr (is_string_type(Type)) {
        const char* offsets = values.offsets().data();
        const uint32_t start = decode_fixed32_le(
                reinterpret_cast<const uint8_t*>(offsets + index * ZONE_MAP_NATIVE_OFFSET_WIDTH));
        const uint32_t end = decode_fixed32_le(reinterpret_cast<const uint8_t*>(
                offsets + (index + 1) * ZONE_MAP_NATIVE_OFFSET_WIDTH));
        return decode_native_value<Type>(
                std::string_view(values.values().data() + start, static_cast<size_t>(end - start)),
                field);
    } else {
        const uint32_t width = native_fixed_width<Type>();
        return decode_native_value<Type>(
                std::string_view(values.fixed_values().data() + static_cast<size_t>(index) * width,
                                 width),
                field);
    }
}

template <PrimitiveType Type>
Status build_zone_map_pages_pb(const std::vector<ZoneMap>& zone_maps, ZoneMapPagesPB* pages) {
    DORIS_CHECK(pages != nullptr);
    const auto num_pages = cast_set<uint32_t>(zone_maps.size());
    const size_t bitmap_size = (num_pages + 7) / 8;
    pages->set_num_pages(num_pages);
    pages->set_entry_version(ZONE_MAP_NATIVE_ENTRY_VERSION);
    pages->set_has_null_bitmap(std::string(bitmap_size, '\0'));
    pages->set_has_not_null_bitmap(std::string(bitmap_size, '\0'));
    pages->set_pass_all_bitmap(std::string(bitmap_size, '\0'));
    pages->set_has_positive_inf_bitmap(std::string(bitmap_size, '\0'));
    pages->set_has_negative_inf_bitmap(std::string(bitmap_size, '\0'));
    pages->set_has_nan_bitmap(std::string(bitmap_size, '\0'));
    init_empty_value_array<Type>(pages->mutable_min_values());
    init_empty_value_array<Type>(pages->mutable_max_values());

    for (uint32_t i = 0; i < num_pages; ++i) {
        const auto& zone_map = zone_maps[i];
        set_bitmap_bit(pages->mutable_has_null_bitmap(), i, zone_map.has_null);
        set_bitmap_bit(pages->mutable_has_not_null_bitmap(), i, zone_map.has_not_null);
        set_bitmap_bit(pages->mutable_pass_all_bitmap(), i, zone_map.pass_all);
        set_bitmap_bit(pages->mutable_has_positive_inf_bitmap(), i, zone_map.has_positive_inf);
        set_bitmap_bit(pages->mutable_has_negative_inf_bitmap(), i, zone_map.has_negative_inf);
        set_bitmap_bit(pages->mutable_has_nan_bitmap(), i, zone_map.has_nan);
        if (has_present_value(zone_map)) {
            append_value_array_value<Type>(zone_map.min_value, pages->mutable_min_values());
            append_value_array_value<Type>(zone_map.max_value, pages->mutable_max_values());
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
Status decode_zone_map_pages_pb(const ZoneMapPagesPB& pages, std::vector<ZoneMap>* zone_maps) {
    DORIS_CHECK(zone_maps != nullptr);
    if (pages.entry_version() != ZONE_MAP_NATIVE_ENTRY_VERSION) {
        return Status::Corruption("unsupported zone map native entry version {}",
                                  pages.entry_version());
    }
    if (!pages.has_has_null_bitmap() || !pages.has_has_not_null_bitmap() ||
        !pages.has_pass_all_bitmap() || !pages.has_has_positive_inf_bitmap() ||
        !pages.has_has_negative_inf_bitmap() || !pages.has_has_nan_bitmap()) {
        return Status::Corruption("zone map native page bitmaps are incomplete");
    }
    if (!pages.has_min_values() || !pages.has_max_values()) {
        return Status::Corruption("zone map native page values are incomplete");
    }

    const uint32_t num_pages = pages.num_pages();
    RETURN_IF_ERROR(validate_bitmap(pages.has_null_bitmap(), num_pages, "has_null"));
    RETURN_IF_ERROR(validate_bitmap(pages.has_not_null_bitmap(), num_pages, "has_not_null"));
    RETURN_IF_ERROR(validate_bitmap(pages.pass_all_bitmap(), num_pages, "pass_all"));
    RETURN_IF_ERROR(
            validate_bitmap(pages.has_positive_inf_bitmap(), num_pages, "has_positive_inf"));
    RETURN_IF_ERROR(
            validate_bitmap(pages.has_negative_inf_bitmap(), num_pages, "has_negative_inf"));
    RETURN_IF_ERROR(validate_bitmap(pages.has_nan_bitmap(), num_pages, "has_nan"));

    uint32_t value_count = 0;
    for (uint32_t i = 0; i < num_pages; ++i) {
        if (get_bitmap_bit(pages.has_not_null_bitmap(), i) &&
            !get_bitmap_bit(pages.pass_all_bitmap(), i)) {
            ++value_count;
        }
    }
    RETURN_IF_ERROR(validate_value_array<Type>(pages.min_values(), value_count, "min"));
    RETURN_IF_ERROR(validate_value_array<Type>(pages.max_values(), value_count, "max"));

    zone_maps->resize(num_pages);
    uint32_t value_index = 0;
    for (uint32_t i = 0; i < num_pages; ++i) {
        auto& zone_map = (*zone_maps)[i];
        zone_map.has_null = get_bitmap_bit(pages.has_null_bitmap(), i);
        zone_map.has_not_null = get_bitmap_bit(pages.has_not_null_bitmap(), i);
        zone_map.pass_all = get_bitmap_bit(pages.pass_all_bitmap(), i);
        zone_map.has_positive_inf = get_bitmap_bit(pages.has_positive_inf_bitmap(), i);
        zone_map.has_negative_inf = get_bitmap_bit(pages.has_negative_inf_bitmap(), i);
        zone_map.has_nan = get_bitmap_bit(pages.has_nan_bitmap(), i);
        if (has_present_value(zone_map)) {
            RETURN_IF_ERROR(decode_value_array_value<Type>(pages.min_values(), value_index,
                                                           &zone_map.min_value));
            RETURN_IF_ERROR(decode_value_array_value<Type>(pages.max_values(), value_index,
                                                           &zone_map.max_value));
            ++value_index;
        }
        RETURN_IF_ERROR(apply_special_float_values<Type>(&zone_map));
    }
    return Status::OK();
}

#define APPLY_FOR_ZONEMAP_NATIVE_PRIMITIVE(M) \
    M(TYPE_BOOLEAN)                           \
    M(TYPE_TINYINT)                           \
    M(TYPE_SMALLINT)                          \
    M(TYPE_INT)                               \
    M(TYPE_BIGINT)                            \
    M(TYPE_LARGEINT)                          \
    M(TYPE_FLOAT)                             \
    M(TYPE_DOUBLE)                            \
    M(TYPE_CHAR)                              \
    M(TYPE_DATE)                              \
    M(TYPE_DATETIME)                          \
    M(TYPE_DATEV2)                            \
    M(TYPE_DATETIMEV2)                        \
    M(TYPE_TIMESTAMPTZ)                       \
    M(TYPE_IPV4)                              \
    M(TYPE_IPV6)                              \
    M(TYPE_VARCHAR)                           \
    M(TYPE_STRING)                            \
    M(TYPE_DECIMALV2)                         \
    M(TYPE_DECIMAL32)                         \
    M(TYPE_DECIMAL64)                         \
    M(TYPE_DECIMAL128I)                       \
    M(TYPE_DECIMAL256)

template <typename Callback>
Status dispatch_zonemap_native_type(PrimitiveType type, Callback&& callback) {
    switch (type) {
#define M(NAME) \
    case NAME:  \
        return callback.template operator()<NAME>();
        APPLY_FOR_ZONEMAP_NATIVE_PRIMITIVE(M)
#undef M
    default:
        return Status::InvalidArgument("invalid zone map native type {}", type_to_string(type));
    }
}

} // namespace

Status ZoneMap::from_proto(const ZoneMapPB& zone_map, const DataTypePtr& data_type,
                           ZoneMap& zone_map_info) {
    zone_map_info.has_null = zone_map.has_null();
    zone_map_info.has_not_null = zone_map.has_not_null();
    zone_map_info.pass_all = zone_map.pass_all();
    zone_map_info.has_negative_inf = zone_map.has_negative_inf();
    zone_map_info.has_positive_inf = zone_map.has_positive_inf();
    zone_map_info.has_nan = zone_map.has_nan();

    auto field_type = data_type->get_storage_field_type();
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        if (zone_map.has_negative_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_neg_inf = -std::numeric_limits<float>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_FLOAT>(float_neg_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_neg_inf = -std::numeric_limits<double>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_DOUBLE>(double_neg_inf);
            } else {
                return Status::InternalError("invalid zone map with negative Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                RETURN_IF_ERROR(data_type->get_serde()->from_zonemap_string(
                        zone_map.min(), zone_map_info.min_value));
            }
        }

        if (zone_map.has_nan()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_nan = std::numeric_limits<float>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_nan);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_nan = std::numeric_limits<double>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_nan);
            } else {
                return Status::InternalError("invalid zone map with NaN");
            }
        } else if (zone_map.has_positive_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_pos_inf = std::numeric_limits<float>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_pos_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_pos_inf = std::numeric_limits<double>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_pos_inf);
            } else {
                return Status::InternalError("invalid zone map with positive Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                RETURN_IF_ERROR(data_type->get_serde()->from_zonemap_string(
                        zone_map.max(), zone_map_info.max_value));
            }
        }
    }
    return Status::OK();
}

void ZoneMap::to_entry_pb_native(ZoneMapEntryPB* dst, const DataTypePtr& data_type) const {
    DORIS_CHECK(dst != nullptr);
    if (pass_all || !has_not_null) {
        dst->set_min("");
        dst->set_max("");
    } else {
        std::string min;
        std::string max;
        static_cast<void>(dispatch_zonemap_native_type(
                data_type->get_primitive_type(), [&]<PrimitiveType Type>() -> Status {
                    append_native_value<Type>(min_value, &min);
                    append_native_value<Type>(max_value, &max);
                    return Status::OK();
                }));
        dst->set_min(std::move(min));
        dst->set_max(std::move(max));
    }
    dst->set_has_null(has_null);
    dst->set_has_not_null(has_not_null);
    dst->set_pass_all(pass_all);
    dst->set_has_positive_inf(has_positive_inf);
    dst->set_has_negative_inf(has_negative_inf);
    dst->set_has_nan(has_nan);
}

Status ZoneMap::from_entry_pb_native(const ZoneMapEntryPB& zone_map, const DataTypePtr& data_type,
                                     ZoneMap& zone_map_info) {
    zone_map_info.has_null = zone_map.has_null();
    zone_map_info.has_not_null = zone_map.has_not_null();
    zone_map_info.pass_all = zone_map.pass_all();
    zone_map_info.has_negative_inf = zone_map.has_negative_inf();
    zone_map_info.has_positive_inf = zone_map.has_positive_inf();
    zone_map_info.has_nan = zone_map.has_nan();

    auto field_type = data_type->get_storage_field_type();
    if (zone_map.has_not_null()) {
        if (zone_map.has_negative_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_neg_inf = -std::numeric_limits<float>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_FLOAT>(float_neg_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_neg_inf = -std::numeric_limits<double>::infinity();
                zone_map_info.min_value = Field::create_field<TYPE_DOUBLE>(double_neg_inf);
            } else {
                return Status::InternalError("invalid zone map with negative Infinity");
            }
        } else if (!zone_map_info.pass_all) {
            if (!zone_map.has_min()) {
                return Status::Corruption("zone map native min is missing");
            }
            RETURN_IF_ERROR(dispatch_zonemap_native_type(
                    data_type->get_primitive_type(), [&]<PrimitiveType Type>() -> Status {
                        return decode_native_value<Type>(zone_map.min(), &zone_map_info.min_value);
                    }));
        }

        if (zone_map.has_nan()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_nan = std::numeric_limits<float>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_nan);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_nan = std::numeric_limits<double>::quiet_NaN();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_nan);
            } else {
                return Status::InternalError("invalid zone map with NaN");
            }
        } else if (zone_map.has_positive_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_pos_inf = std::numeric_limits<float>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_FLOAT>(float_pos_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_pos_inf = std::numeric_limits<double>::infinity();
                zone_map_info.max_value = Field::create_field<TYPE_DOUBLE>(double_pos_inf);
            } else {
                return Status::InternalError("invalid zone map with positive Infinity");
            }
        } else if (!zone_map_info.pass_all) {
            if (!zone_map.has_max()) {
                return Status::Corruption("zone map native max is missing");
            }
            RETURN_IF_ERROR(dispatch_zonemap_native_type(
                    data_type->get_primitive_type(), [&]<PrimitiveType Type>() -> Status {
                        return decode_native_value<Type>(zone_map.max(), &zone_map_info.max_value);
                    }));
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
TypedZoneMapIndexWriter<Type>::TypedZoneMapIndexWriter(DataTypePtr&& data_type)
        : _data_type(std::move(data_type)) {
    _page_zone_map.min_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _page_zone_map.max_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.min_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.max_value =
            doris::Field::create_field<Type>(typename PrimitiveTypeTraits<Type>::CppType());
    _reset_zone_map(&_page_zone_map);
    _reset_zone_map(&_segment_zone_map);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::_update_page_zonemap(const ValType& min_value,
                                                         const ValType& max_value) {
    // Hot path: compare/store using raw CppType to avoid Field temporaries.
    // For string types, truncate to MAX_ZONE_MAP_INDEX_SIZE (matching the old
    // Field-based path) and copy bytes into _page_{min,max}_storage so the
    // StringRef stays valid across add_values() calls.
    if constexpr (is_string_type(Type)) {
        auto truncate_into = [](const StringRef& src, std::string& dst) {
            auto sz = std::min<size_t>(src.size, MAX_ZONE_MAP_INDEX_SIZE);
            dst.assign(src.data, sz);
            return StringRef(dst.data(), dst.size());
        };
        StringRef min_t(min_value.data, std::min<size_t>(min_value.size, MAX_ZONE_MAP_INDEX_SIZE));
        StringRef max_t(max_value.data, std::min<size_t>(max_value.size, MAX_ZONE_MAP_INDEX_SIZE));
        if (!_page_zone_map.has_not_null || min_t < _page_min) {
            _page_min = truncate_into(min_value, _page_min_storage);
        }
        if (!_page_zone_map.has_not_null || _page_max < max_t) {
            _page_max = truncate_into(max_value, _page_max_storage);
        }
    } else {
        if (!_page_zone_map.has_not_null || min_value < _page_min) {
            _page_min = min_value;
        }
        if (!_page_zone_map.has_not_null || max_value > _page_max) {
            _page_max = max_value;
        }
    }
    _page_zone_map.has_not_null = true;
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::_materialize_page_minmax() {
    if (!_page_zone_map.has_not_null) {
        return;
    }
    _page_zone_map.min_value = doris::Field::create_field_from_olap_value<Type>(_page_min);
    _page_zone_map.max_value = doris::Field::create_field_from_olap_value<Type>(_page_max);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::add_values(const void* values, size_t count) {
    if (count == 0) {
        return;
    }
    const auto* vals = reinterpret_cast<const ValType*>(values);
    if constexpr (Type == TYPE_FLOAT || Type == TYPE_DOUBLE) {
        ValType min = std::numeric_limits<ValType>::max();
        ValType max = std::numeric_limits<ValType>::lowest();
        for (size_t i = 0; i < count; ++i) {
            if (std::isnan(vals[i])) {
                _page_zone_map.has_nan = true;
            } else if (vals[i] == std::numeric_limits<ValType>::infinity()) {
                _page_zone_map.has_positive_inf = true;
            } else if (vals[i] == -std::numeric_limits<ValType>::infinity()) {
                _page_zone_map.has_negative_inf = true;
            } else {
                if (vals[i] < min) {
                    min = vals[i];
                }
                if (vals[i] > max) {
                    max = vals[i];
                }
            }
        }
        _update_page_zonemap(min, max);
    } else {
        auto [min, max] = std::minmax_element(vals, vals + count);
        _update_page_zonemap(unaligned_load<ValType>(min), unaligned_load<ValType>(max));
    }
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::modify_index_before_flush(
        struct doris::segment_v2::ZoneMap& zone_map) {
    // Only varchar/string filed need modify zone map index when zone map max_value
    // For varchar/string type, the zone map buffer is truncated at MAX_ZONE_MAP_INDEX_SIZE (512 bytes).
    // When a string value is longer than 512 bytes, only the first 512 bytes are stored.
    //
    // Truncating the max value creates a correctness problem: the truncated max is now smaller than the actual max.
    // This means the zone map could incorrectly skip pages that actually contain matching rows.
    //
    // So here we add one for the last byte if the max value is truncated, which makes the truncated max value
    // slightly larger than any real string that shares the same 512-byte prefix, ensuring no false negatives —
    // the zone map will never incorrectly skip a page that contains matching data.
    //
    // In UTF8 encoding, here do not appear 0xff in last byte
    if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING) {
        auto& str = zone_map.max_value.get<Type>();
        if (str.size() == MAX_ZONE_MAP_INDEX_SIZE) {
            str[str.size() - 1] += 1;
        }
    }
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::invalid_page_zone_map() {
    _page_zone_map.pass_all = true;
}

template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::flush() {
    // Materialize the running CppType min/max into the Field-typed page zone map
    // before merging into the segment zone map / serializing to proto.
    _materialize_page_minmax();

    // Update segment zone map.
    if (_page_zone_map.has_not_null) {
        if (!_segment_zone_map.has_not_null ||
            _segment_zone_map.min_value.get<Type>() > _page_zone_map.min_value.get<Type>()) {
            _segment_zone_map.min_value = _page_zone_map.min_value;
        }
        if (!_segment_zone_map.has_not_null ||
            _segment_zone_map.max_value.get<Type>() < _page_zone_map.max_value.get<Type>()) {
            _segment_zone_map.max_value = _page_zone_map.max_value;
        }
    }
    if (_page_zone_map.has_null) {
        _segment_zone_map.has_null = true;
    }
    if (_page_zone_map.has_not_null) {
        _segment_zone_map.has_not_null = true;
    }
    if (_page_zone_map.has_positive_inf) {
        _segment_zone_map.has_positive_inf = true;
    }
    if (_page_zone_map.has_negative_inf) {
        _segment_zone_map.has_negative_inf = true;
    }
    if (_page_zone_map.has_nan) {
        _segment_zone_map.has_nan = true;
    }

    modify_index_before_flush(_page_zone_map);
    _page_zone_maps.push_back(_page_zone_map);
    _reset_zone_map(&_page_zone_map);

    _estimated_size += sizeof(ZoneMap);
    return Status::OK();
}

template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::finish(io::FileWriter* file_writer,
                                             ColumnIndexMetaPB* index_meta) {
    index_meta->set_type(ZONE_MAP_INDEX);
    ZoneMapIndexPB* meta = index_meta->mutable_zone_map_index();
    // store segment zone map
    modify_index_before_flush(_segment_zone_map);
    _segment_zone_map.to_entry_pb_native(meta->mutable_segment_zone_map_v2(), _data_type);

    ZoneMapPagesPB pages;
    RETURN_IF_ERROR(build_zone_map_pages_pb<Type>(_page_zone_maps, &pages));
    std::string serialized_pages;
    if (!pages.SerializeToString(&serialized_pages)) {
        return Status::InternalError("serialize zone map pages failed");
    }

    PagePointer page_pointer(file_writer->bytes_appended(),
                             cast_set<uint32_t>(serialized_pages.size()));
    Slice pages_slice(serialized_pages);
    RETURN_IF_ERROR(file_writer->append(pages_slice));

    ZoneMapPageIndexPB* page_index = meta->mutable_page_zone_maps_v2();
    page_pointer.to_proto(page_index->mutable_data());
    page_index->set_num_pages(cast_set<uint32_t>(_page_zone_maps.size()));
    page_index->set_format_version(ZONE_MAP_NATIVE_PAGE_FORMAT_VERSION);
    return Status::OK();
}

Status ZoneMapIndexReader::load(bool use_page_cache, bool kept_in_memory,
                                OlapReaderStatistics* index_load_stats,
                                const io::IOContext* io_ctx) {
    // TODO yyq: implement a new once flag to avoid status construct.
    return _load_once.call([this, use_page_cache, kept_in_memory, index_load_stats, io_ctx] {
        return _load(use_page_cache, kept_in_memory, std::move(_page_zone_maps_meta),
                     std::move(_page_zone_maps_v2_meta), index_load_stats, io_ctx);
    });
}

Status ZoneMapIndexReader::_load(bool use_page_cache, bool kept_in_memory,
                                 std::unique_ptr<IndexedColumnMetaPB> page_zone_maps_meta,
                                 std::unique_ptr<ZoneMapPageIndexPB> page_zone_maps_v2_meta,
                                 OlapReaderStatistics* index_load_stats,
                                 const io::IOContext* io_ctx) {
    if (page_zone_maps_v2_meta != nullptr) {
        return _load_v2(std::move(page_zone_maps_v2_meta), index_load_stats, io_ctx);
    }
    if (page_zone_maps_meta != nullptr) {
        return _load_legacy(use_page_cache, kept_in_memory, std::move(page_zone_maps_meta),
                            index_load_stats, io_ctx);
    }
    _num_pages = 0;
    update_metadata_size();
    return Status::OK();
}

Status ZoneMapIndexReader::_load_legacy(bool use_page_cache, bool kept_in_memory,
                                        std::unique_ptr<IndexedColumnMetaPB> page_zone_maps_meta,
                                        OlapReaderStatistics* index_load_stats,
                                        const io::IOContext* io_ctx) {
    IndexedColumnReader reader(_file_reader, *page_zone_maps_meta);
    RETURN_IF_ERROR(reader.load(use_page_cache, kept_in_memory, index_load_stats, io_ctx));
    IndexedColumnIterator iter(&reader, index_load_stats, io_ctx);

    _page_zone_maps.resize(reader.num_values());
    _num_pages = _page_zone_maps.size();

    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        size_t num_to_read = 1;
        // The type of reader is FieldType::OLAP_FIELD_TYPE_BITMAP.
        // ColumnBitmap will be created when using FieldType::OLAP_FIELD_TYPE_BITMAP.
        // But what we need actually is ColumnString.
        MutableColumnPtr column = ColumnString::create();

        RETURN_IF_ERROR(iter.seek_to_ordinal(i));
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter.next_batch(&num_read, column));
        DCHECK(num_to_read == num_read);

        if (!_page_zone_maps[i].ParseFromArray(column->get_data_at(0).data,
                                               cast_set<int>(column->get_data_at(0).size))) {
            return Status::Corruption("Failed to parse zone map");
        }
        _pb_meta_size += _page_zone_maps[i].ByteSizeLong();
    }

    if (_data_type != nullptr) {
        _page_zone_map_infos.resize(_page_zone_maps.size());
        for (size_t i = 0; i < _page_zone_maps.size(); ++i) {
            RETURN_IF_ERROR(
                    ZoneMap::from_proto(_page_zone_maps[i], _data_type, _page_zone_map_infos[i]));
        }
    }

    update_metadata_size();
    return Status::OK();
}

Status ZoneMapIndexReader::_load_v2(std::unique_ptr<ZoneMapPageIndexPB> page_zone_maps_v2_meta,
                                    OlapReaderStatistics* index_load_stats,
                                    const io::IOContext* io_ctx) {
    (void)index_load_stats;
    DORIS_CHECK(_data_type != nullptr);
    if (page_zone_maps_v2_meta->format_version() != ZONE_MAP_NATIVE_PAGE_FORMAT_VERSION) {
        return Status::Corruption("unsupported zone map native page format version {}",
                                  page_zone_maps_v2_meta->format_version());
    }
    if (!page_zone_maps_v2_meta->has_data()) {
        return Status::Corruption("zone map native page payload is missing");
    }
    const PagePointer page_pointer(page_zone_maps_v2_meta->data());
    std::string payload(page_pointer.size, '\0');
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(
            page_pointer.offset, Slice(payload.data(), payload.size()), &bytes_read, io_ctx));
    if (bytes_read != page_pointer.size) {
        return Status::Corruption("short read zone map native page payload: expect {}, got {}",
                                  page_pointer.size, bytes_read);
    }

    ZoneMapPagesPB pages;
    if (!pages.ParseFromArray(payload.data(), cast_set<int>(payload.size()))) {
        return Status::Corruption("failed to parse zone map native page payload");
    }
    if (page_zone_maps_v2_meta->num_pages() != pages.num_pages()) {
        return Status::Corruption("zone map native page count mismatch: meta {}, payload {}",
                                  page_zone_maps_v2_meta->num_pages(), pages.num_pages());
    }
    RETURN_IF_ERROR(dispatch_zonemap_native_type(
            _data_type->get_primitive_type(), [&]<PrimitiveType Type>() -> Status {
                return decode_zone_map_pages_pb<Type>(pages, &_page_zone_map_infos);
            }));
    _num_pages = _page_zone_map_infos.size();
    _pb_meta_size += pages.ByteSizeLong();
    update_metadata_size();
    return Status::OK();
}

const std::vector<ZoneMapPB>& ZoneMapIndexReader::page_zone_maps() const {
    if (_page_zone_maps.empty() && !_page_zone_map_infos.empty() && _data_type != nullptr) {
        _page_zone_maps.resize(_page_zone_map_infos.size());
        for (size_t i = 0; i < _page_zone_map_infos.size(); ++i) {
            _page_zone_map_infos[i].to_proto(&_page_zone_maps[i], _data_type);
        }
    }
    return _page_zone_maps;
}

int64_t ZoneMapIndexReader::get_metadata_size() const {
    return sizeof(ZoneMapIndexReader) + _pb_meta_size;
}

ZoneMapIndexReader::~ZoneMapIndexReader() = default;
#define APPLY_FOR_PRIMITITYPE(M) \
    M(TYPE_TINYINT)              \
    M(TYPE_SMALLINT)             \
    M(TYPE_INT)                  \
    M(TYPE_BIGINT)               \
    M(TYPE_LARGEINT)             \
    M(TYPE_FLOAT)                \
    M(TYPE_DOUBLE)               \
    M(TYPE_CHAR)                 \
    M(TYPE_DATE)                 \
    M(TYPE_DATETIME)             \
    M(TYPE_DATEV2)               \
    M(TYPE_DATETIMEV2)           \
    M(TYPE_TIMESTAMPTZ)          \
    M(TYPE_IPV4)                 \
    M(TYPE_IPV6)                 \
    M(TYPE_VARCHAR)              \
    M(TYPE_STRING)               \
    M(TYPE_DECIMAL32)            \
    M(TYPE_DECIMAL64)            \
    M(TYPE_DECIMAL128I)          \
    M(TYPE_DECIMAL256)

Status ZoneMapIndexWriter::create(DataTypePtr data_type, const TabletColumn* column,
                                  std::unique_ptr<ZoneMapIndexWriter>& res) {
    switch (column->type()) {
#define M(NAME)                                                             \
    case FieldType::OLAP_FIELD_##NAME: {                                    \
        res.reset(new TypedZoneMapIndexWriter<NAME>(std::move(data_type))); \
        return Status::OK();                                                \
    }
        APPLY_FOR_PRIMITITYPE(M)
#undef M
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_DECIMALV2>(std::move(data_type)));
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_BOOLEAN>(std::move(data_type)));
        return Status::OK();
    }
    default:
        return Status::InvalidArgument("Invalid type!");
    }
}
} // namespace segment_v2
} // namespace doris
