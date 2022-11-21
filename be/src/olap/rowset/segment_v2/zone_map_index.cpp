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

#include "olap/column_block.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"

namespace doris {

namespace segment_v2 {

template <PrimitiveType Type>
TypedZoneMapIndexWriter<Type>::TypedZoneMapIndexWriter(Field* field) : _field(field) {
    _page_zone_map.min_value = _field->allocate_zone_map_value(&_pool);
    _page_zone_map.max_value = _field->allocate_zone_map_value(&_pool);
    _reset_zone_map(&_page_zone_map);
    _segment_zone_map.min_value = _field->allocate_zone_map_value(&_pool);
    _segment_zone_map.max_value = _field->allocate_zone_map_value(&_pool);
    _reset_zone_map(&_segment_zone_map);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::add_values(const void* values, size_t count) {
    if (count > 0) {
        _page_zone_map.has_not_null = true;
    }
    using ValType =
            std::conditional_t<Type == TYPE_DATE, uint24_t,
                               typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType>;
    const ValType* vals = reinterpret_cast<const ValType*>(values);
    auto [min, max] = std::minmax_element(vals, vals + count);
    if (unaligned_load<ValType>(min) < unaligned_load<ValType>(_page_zone_map.min_value)) {
        _field->type_info()->direct_copy_may_cut(_page_zone_map.min_value,
                                                 reinterpret_cast<const void*>(min));
    }
    if (unaligned_load<ValType>(max) > unaligned_load<ValType>(_page_zone_map.max_value)) {
        _field->type_info()->direct_copy_may_cut(_page_zone_map.max_value,
                                                 reinterpret_cast<const void*>(max));
    }
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::moidfy_index_before_flush(
        struct doris::segment_v2::ZoneMap& zone_map) {
    _field->modify_zone_map_index(zone_map.max_value);
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::reset_page_zone_map() {
    _page_zone_map.pass_all = true;
}

template <PrimitiveType Type>
void TypedZoneMapIndexWriter<Type>::reset_segment_zone_map() {
    _segment_zone_map.pass_all = true;
}

template <PrimitiveType Type>
Status TypedZoneMapIndexWriter<Type>::flush() {
    // Update segment zone map.
    if (_field->compare(_segment_zone_map.min_value, _page_zone_map.min_value) > 0) {
        _field->type_info()->direct_copy(_segment_zone_map.min_value, _page_zone_map.min_value);
    }
    if (_field->compare(_segment_zone_map.max_value, _page_zone_map.max_value) < 0) {
        _field->type_info()->direct_copy(_segment_zone_map.max_value, _page_zone_map.max_value);
    }
    if (_page_zone_map.has_null) {
        _segment_zone_map.has_null = true;
    }
    if (_page_zone_map.has_not_null) {
        _segment_zone_map.has_not_null = true;
    }

    ZoneMapPB zone_map_pb;
    moidfy_index_before_flush(_page_zone_map);
    _page_zone_map.to_proto(&zone_map_pb, _field);
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
    moidfy_index_before_flush(_segment_zone_map);
    _segment_zone_map.to_proto(meta->mutable_segment_zone_map(), _field);

    // write out zone map for each data pages
    const auto* type_info = get_scalar_type_info<OLAP_FIELD_TYPE_OBJECT>();
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = EncodingInfo::get_default_encoding(type_info, false);
    options.compression = NO_COMPRESSION; // currently not compressed

    IndexedColumnWriter writer(options, type_info, file_writer);
    RETURN_IF_ERROR(writer.init());

    for (auto& value : _values) {
        Slice value_slice(value);
        RETURN_IF_ERROR(writer.add(&value_slice));
    }
    return writer.finish(meta->mutable_page_zone_maps());
}

Status ZoneMapIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    IndexedColumnReader reader(_file_reader, _index_meta->page_zone_maps());
    RETURN_IF_ERROR(reader.load(use_page_cache, kept_in_memory));
    IndexedColumnIterator iter(&reader);

    MemPool pool;
    _page_zone_maps.resize(reader.num_values());

    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        size_t num_to_read = 1;
        std::unique_ptr<ColumnVectorBatch> cvb;
        RETURN_IF_ERROR(
                ColumnVectorBatch::create(num_to_read, false, reader.type_info(), nullptr, &cvb));
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);

        RETURN_IF_ERROR(iter.seek_to_ordinal(i));
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter.next_batch(&num_read, &column_block_view));
        DCHECK(num_to_read == num_read);

        Slice* value = reinterpret_cast<Slice*>(cvb->data());
        if (!_page_zone_maps[i].ParseFromArray(value->data, value->size)) {
            return Status::Corruption("Failed to parse zone map");
        }
        pool.clear();
    }
    return Status::OK();
}

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
    M(TYPE_VARCHAR)              \
    M(TYPE_STRING)               \
    M(TYPE_DECIMAL32)            \
    M(TYPE_DECIMAL64)            \
    M(TYPE_DECIMAL128I)

Status ZoneMapIndexWriter::create(Field* field, std::unique_ptr<ZoneMapIndexWriter>& res) {
    switch (field->type()) {
#define M(NAME)                                              \
    case OLAP_FIELD_##NAME: {                                \
        res.reset(new TypedZoneMapIndexWriter<NAME>(field)); \
        return Status::OK();                                 \
    }
        APPLY_FOR_PRIMITITYPE(M)
#undef M
    case OLAP_FIELD_TYPE_DECIMAL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_DECIMALV2>(field));
        return Status::OK();
    }
    case OLAP_FIELD_TYPE_BOOL: {
        res.reset(new TypedZoneMapIndexWriter<TYPE_BOOLEAN>(field));
        return Status::OK();
    }
    default:
        return Status::InvalidArgument("Invalid type!");
    }
}
} // namespace segment_v2
} // namespace doris
