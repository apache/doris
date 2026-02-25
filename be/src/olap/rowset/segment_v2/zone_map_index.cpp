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

#include "olap/rowset/segment_v2/zone_map_index.h"

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <type_traits>

#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/common/unaligned.h"
#include "vec/data_types/data_type.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
#include "common/compile_check_begin.h"
struct uint24_t;

namespace segment_v2 {
Status ZoneMap::from_proto(const ZoneMapPB& zone_map, const vectorized::DataTypePtr& data_type,
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
                zone_map_info.min_value =
                        vectorized::Field::create_field<TYPE_FLOAT>(float_neg_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_neg_inf = -std::numeric_limits<double>::infinity();
                zone_map_info.min_value =
                        vectorized::Field::create_field<TYPE_DOUBLE>(double_neg_inf);
            } else {
                return Status::InternalError("invalid zone map with negative Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                vectorized::DataTypeSerDe::FormatOptions opt;
                opt.ignore_scale = true;
                RETURN_IF_ERROR(data_type->get_serde()->from_olap_string(
                        zone_map.min(), zone_map_info.min_value, opt));
            }
        }

        if (zone_map.has_nan()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_nan = std::numeric_limits<float>::quiet_NaN();
                zone_map_info.max_value = vectorized::Field::create_field<TYPE_FLOAT>(float_nan);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_nan = std::numeric_limits<double>::quiet_NaN();
                zone_map_info.max_value = vectorized::Field::create_field<TYPE_DOUBLE>(double_nan);
            } else {
                return Status::InternalError("invalid zone map with NaN");
            }
        } else if (zone_map.has_positive_inf()) {
            if (FieldType::OLAP_FIELD_TYPE_FLOAT == field_type) {
                static auto constexpr float_pos_inf = std::numeric_limits<float>::infinity();
                zone_map_info.max_value =
                        vectorized::Field::create_field<TYPE_FLOAT>(float_pos_inf);
            } else if (FieldType::OLAP_FIELD_TYPE_DOUBLE == field_type) {
                static auto constexpr double_pos_inf = std::numeric_limits<double>::infinity();
                zone_map_info.max_value =
                        vectorized::Field::create_field<TYPE_DOUBLE>(double_pos_inf);
            } else {
                return Status::InternalError("invalid zone map with positive Infinity");
            }
        } else {
            if (!zone_map_info.pass_all) {
                vectorized::DataTypeSerDe::FormatOptions opt;
                opt.ignore_scale = true;
                RETURN_IF_ERROR(data_type->get_serde()->from_olap_string(
                        zone_map.max(), zone_map_info.max_value, opt));
            }
        }
    }
    return Status::OK();
}

template <PrimitiveType Type>
TypedZoneMapIndexWriter<Type>::TypedZoneMapIndexWriter(vectorized::DataTypePtr&& data_type)
        : _data_type(std::move(data_type)) {
    _page_zone_map.min_value = doris::vectorized::Field::create_field<Type>(
            typename PrimitiveTypeTraits<Type>::CppType());
    _page_zone_map.max_value = doris::vectorized::Field::create_field<Type>(
            typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.min_value = doris::vectorized::Field::create_field<Type>(
            typename PrimitiveTypeTraits<Type>::CppType());
    _segment_zone_map.max_value = doris::vectorized::Field::create_field<Type>(
            typename PrimitiveTypeTraits<Type>::CppType());
    _reset_zone_map(&_page_zone_map);
    _reset_zone_map(&_segment_zone_map);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::_update_page_zonemap(const ValType& min_value,
                                                         const ValType& max_value) {
    auto min_field = doris::vectorized::Field::create_field_from_olap_value<Type>(min_value);
    auto max_field = doris::vectorized::Field::create_field_from_olap_value<Type>(max_value);
    if (!_page_zone_map.has_not_null || min_field < _page_zone_map.min_value) {
        _page_zone_map.min_value = std::move(min_field);
    }
    if (!_page_zone_map.has_not_null || max_field > _page_zone_map.max_value) {
        _page_zone_map.max_value = std::move(max_field);
    }
    _page_zone_map.has_not_null = true;
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
    // Update segment zone map.
    if (!_segment_zone_map.has_not_null ||
        _segment_zone_map.min_value.get<Type>() > _page_zone_map.min_value.get<Type>()) {
        _segment_zone_map.min_value = _page_zone_map.min_value;
    }
    if (!_segment_zone_map.has_not_null ||
        _segment_zone_map.max_value.get<Type>() < _page_zone_map.max_value.get<Type>()) {
        _segment_zone_map.max_value = _page_zone_map.max_value;
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

    ZoneMapPB zone_map_pb;
    modify_index_before_flush(_page_zone_map);
    _page_zone_map.to_proto(&zone_map_pb, _data_type);
    _reset_zone_map(&_page_zone_map);

    std::string serialized_zone_map;
    bool ret = zone_map_pb.SerializeToString(&serialized_zone_map);
    if (!ret) {
        return Status::InternalError("serialize zone map failed");
    }
    _estimated_size += serialized_zone_map.size() + sizeof(uint32_t);
    _values.push_back(std::move(serialized_zone_map));
    return Status::OK();
}

template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::finish(io::FileWriter* file_writer,
                                             ColumnIndexMetaPB* index_meta) {
    index_meta->set_type(ZONE_MAP_INDEX);
    ZoneMapIndexPB* meta = index_meta->mutable_zone_map_index();
    // store segment zone map
    modify_index_before_flush(_segment_zone_map);
    _segment_zone_map.to_proto(meta->mutable_segment_zone_map(), _data_type);

    // write out zone map for each data pages
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BITMAP>();
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = EncodingInfo::get_default_encoding(type_info->type(), {}, false);
    options.compression = NO_COMPRESSION; // currently not compressed

    IndexedColumnWriter writer(options, type_info, file_writer);
    RETURN_IF_ERROR(writer.init());

    for (auto& value : _values) {
        Slice value_slice(value);
        RETURN_IF_ERROR(writer.add(&value_slice));
    }
    return writer.finish(meta->mutable_page_zone_maps());
}

Status ZoneMapIndexReader::load(bool use_page_cache, bool kept_in_memory,
                                OlapReaderStatistics* index_load_stats) {
    // TODO yyq: implement a new once flag to avoid status construct.
    return _load_once.call([this, use_page_cache, kept_in_memory, index_load_stats] {
        return _load(use_page_cache, kept_in_memory, std::move(_page_zone_maps_meta),
                     index_load_stats);
    });
}

Status ZoneMapIndexReader::_load(bool use_page_cache, bool kept_in_memory,
                                 std::unique_ptr<IndexedColumnMetaPB> page_zone_maps_meta,
                                 OlapReaderStatistics* index_load_stats) {
    IndexedColumnReader reader(_file_reader, *page_zone_maps_meta);
    RETURN_IF_ERROR(reader.load(use_page_cache, kept_in_memory, index_load_stats));
    IndexedColumnIterator iter(&reader, index_load_stats);

    _page_zone_maps.resize(reader.num_values());

    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        size_t num_to_read = 1;
        // The type of reader is FieldType::OLAP_FIELD_TYPE_BITMAP.
        // ColumnBitmap will be created when using FieldType::OLAP_FIELD_TYPE_BITMAP.
        // But what we need actually is ColumnString.
        vectorized::MutableColumnPtr column = vectorized::ColumnString::create();

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

    update_metadata_size();
    return Status::OK();
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

Status ZoneMapIndexWriter::create(vectorized::DataTypePtr data_type, Field* field,
                                  std::unique_ptr<ZoneMapIndexWriter>& res) {
    switch (field->type()) {
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
#include "common/compile_check_end.h"
} // namespace doris
