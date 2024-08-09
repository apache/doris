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

#include "vec/olap/olap_data_convertor.h"

#include <memory>
#include <new>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "olap/hll.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/decimalv2_value.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

OlapBlockDataConvertor::OlapBlockDataConvertor(const TabletSchema* tablet_schema) {
    assert(tablet_schema);
    const auto& columns = tablet_schema->columns();
    for (const auto& col : columns) {
        _convertors.emplace_back(create_olap_column_data_convertor(*col));
    }
}

OlapBlockDataConvertor::OlapBlockDataConvertor(const TabletSchema* tablet_schema,
                                               const std::vector<uint32_t>& col_ids) {
    assert(tablet_schema);
    for (const auto& id : col_ids) {
        const auto& col = tablet_schema->column(id);
        _convertors.emplace_back(create_olap_column_data_convertor(col));
    }
}

void OlapBlockDataConvertor::add_column_data_convertor(const TabletColumn& column) {
    _convertors.emplace_back(create_olap_column_data_convertor(column));
}

OlapBlockDataConvertor::OlapColumnDataConvertorBaseUPtr
OlapBlockDataConvertor::create_map_convertor(const TabletColumn& column) {
    const auto& key_column = column.get_sub_column(0);
    const auto& value_column = column.get_sub_column(1);
    return std::make_unique<OlapColumnDataConvertorMap>(key_column, value_column);
}

OlapBlockDataConvertor::OlapColumnDataConvertorBaseUPtr
OlapBlockDataConvertor::create_array_convertor(const TabletColumn& column) {
    const auto& sub_column = column.get_sub_column(0);
    return std::make_unique<OlapColumnDataConvertorArray>(
            create_olap_column_data_convertor(sub_column));
}

OlapBlockDataConvertor::OlapColumnDataConvertorBaseUPtr
OlapBlockDataConvertor::create_agg_state_convertor(const TabletColumn& column) {
    auto data_type = DataTypeFactory::instance().create_data_type(column);
    const auto* agg_state_type = assert_cast<const vectorized::DataTypeAggState*>(data_type.get());
    auto type = agg_state_type->get_serialized_type()->get_type_as_type_descriptor().type;

    // Terialized type of most functions is string, and some of them are fixed object.
    // Finally, the serialized type of some special functions is bitmap/array/map...
    if (type == PrimitiveType::TYPE_STRING) {
        return std::make_unique<OlapColumnDataConvertorVarChar>(false);
    } else if (type == PrimitiveType::TYPE_OBJECT) {
        return std::make_unique<OlapColumnDataConvertorBitMap>();
    } else if (type == PrimitiveType::INVALID_TYPE) {
        // INVALID_TYPE means function's serialized type is fixed object
        return std::make_unique<OlapColumnDataConvertorAggState>();
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "OLAP_FIELD_TYPE_AGG_STATE meet unsupported type: {}",
                        agg_state_type->get_name());
    }
}

OlapBlockDataConvertor::OlapColumnDataConvertorBaseUPtr
OlapBlockDataConvertor::create_olap_column_data_convertor(const TabletColumn& column) {
    switch (column.type()) {
    case FieldType::OLAP_FIELD_TYPE_OBJECT: {
        return std::make_unique<OlapColumnDataConvertorBitMap>();
    }
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE: {
        return std::make_unique<OlapColumnDataConvertorQuantileState>();
    }
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE: {
        return create_agg_state_convertor(column);
    }
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        return std::make_unique<OlapColumnDataConvertorHLL>();
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<OlapColumnDataConvertorChar>(column.length());
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        return std::make_unique<OlapColumnDataConvertorVarChar>(false);
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        return std::make_unique<OlapColumnDataConvertorVarChar>(true);
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        return std::make_unique<OlapColumnDataConvertorDate>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        return std::make_unique<OlapColumnDataConvertorDateTime>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        return std::make_unique<OlapColumnDataConvertorDateV2>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return std::make_unique<OlapColumnDataConvertorDateTimeV2>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return std::make_unique<OlapColumnDataConvertorDecimal>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        return std::make_unique<OlapColumnDataConvertorDecimalV3<Decimal32>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        return std::make_unique<OlapColumnDataConvertorDecimalV3<Decimal64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        return std::make_unique<OlapColumnDataConvertorDecimalV3<Decimal128V3>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        return std::make_unique<OlapColumnDataConvertorDecimalV3<Decimal256>>();
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        return std::make_unique<OlapColumnDataConvertorVarChar>(true);
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::UInt8>>();
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Int8>>();
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Int16>>();
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Int32>>();
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Int64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Int128>>();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::IPv4>>();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::IPv6>>();
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Float32>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Float64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_VARIANT: {
        return std::make_unique<OlapColumnDataConvertorVariant>();
    }
    case FieldType::OLAP_FIELD_TYPE_STRUCT: {
        std::vector<OlapColumnDataConvertorBaseUPtr> sub_convertors;
        for (uint32_t i = 0; i < column.get_subtype_count(); i++) {
            const TabletColumn& sub_column = column.get_sub_column(i);
            sub_convertors.emplace_back(create_olap_column_data_convertor(sub_column));
        }
        return std::make_unique<OlapColumnDataConvertorStruct>(sub_convertors);
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        return create_array_convertor(column);
    }
    case FieldType::OLAP_FIELD_TYPE_MAP: {
        return create_map_convertor(column);
    }
    default: {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid type in olap data convertor: {}",
                        int(column.type()));
    }
    }
}

void OlapBlockDataConvertor::set_source_content(const vectorized::Block* block, size_t row_pos,
                                                size_t num_rows) {
    DCHECK(block && num_rows > 0 && row_pos + num_rows <= block->rows() &&
           block->columns() == _convertors.size());
    size_t cid = 0;
    for (const auto& typed_column : *block) {
        if (typed_column.column->size() != block->rows()) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "input invalid block, block={}",
                            block->dump_structure());
        }
        _convertors[cid]->set_source_column(typed_column, row_pos, num_rows);
        ++cid;
    }
}

Status OlapBlockDataConvertor::set_source_content_with_specifid_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows, uint32_t cid) {
    DCHECK(num_rows > 0);
    DCHECK(row_pos + num_rows <= typed_column.column->size());
    DCHECK(cid < _convertors.size());
    RETURN_IF_CATCH_EXCEPTION(
            { _convertors[cid]->set_source_column(typed_column, row_pos, num_rows); });
    return Status::OK();
}

Status OlapBlockDataConvertor::set_source_content_with_specifid_columns(
        const vectorized::Block* block, size_t row_pos, size_t num_rows,
        std::vector<uint32_t> cids) {
    DCHECK(block != nullptr);
    DCHECK(num_rows > 0);
    DCHECK(row_pos + num_rows <= block->rows());
    RETURN_IF_CATCH_EXCEPTION({
        for (auto i : cids) {
            DCHECK(i < _convertors.size());
            _convertors[i]->set_source_column(block->get_by_position(i), row_pos, num_rows);
        }
    });
    return Status::OK();
}

void OlapBlockDataConvertor::clear_source_content() {
    for (auto& convertor : _convertors) {
        convertor->clear_source_column();
    }
}

std::pair<Status, IOlapColumnDataAccessor*> OlapBlockDataConvertor::convert_column_data(
        size_t cid) {
    assert(cid < _convertors.size());
    auto convert_func = [&]() -> Status {
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_convertors[cid]->convert_to_olap());
        return Status::OK();
    };
    auto status = convert_func();
    return {status, _convertors[cid].get()};
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorBase
void OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    DCHECK(row_pos + num_rows <= typed_column.column->size())
            << "row_pos=" << row_pos << ", num_rows=" << num_rows
            << ", typed_column.column->size()=" << typed_column.column->size();
    _typed_column = typed_column;
    _row_pos = row_pos;
    _num_rows = num_rows;
    if (_typed_column.column->is_nullable()) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        _nullmap = nullable_column->get_null_map_data().data();
    }
}

void OlapBlockDataConvertor::OlapColumnDataConvertorBase::clear_source_column() {
    // just to reduce the source column's ref count to 1
    _typed_column.column = nullptr;
    _nullmap = nullptr;
}

// Obtain the converted nullmap with an offset of _row_pos.
// This should be called only in SegmentWriter and `get_data_at` in Convertor.
// If you want to access origin nullmap without offset, use `_nullmap` directly.
const UInt8* OlapBlockDataConvertor::OlapColumnDataConvertorBase::get_nullmap() const {
    assert(_typed_column.column);
    return _nullmap ? _nullmap + _row_pos : nullptr;
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorObject
void OlapBlockDataConvertor::OlapColumnDataConvertorObject::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    _raw_data.clear();
    _slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorObject::get_data() const {
    return _slice.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorObject::get_data_at(
        size_t offset) const {
    UInt8 null_flag = 0;
    if (get_nullmap()) {
        null_flag = get_nullmap()[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorBitMap::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnBitmap* column_bitmap = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_bitmap = assert_cast<const vectorized::ColumnBitmap*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_bitmap = assert_cast<const vectorized::ColumnBitmap*>(_typed_column.column.get());
    }

    assert(column_bitmap);
    BitmapValue* bitmap_value =
            const_cast<BitmapValue*>(column_bitmap->get_data().data() + _row_pos);
    BitmapValue* bitmap_value_cur = bitmap_value;
    BitmapValue* bitmap_value_end = bitmap_value_cur + _num_rows;

    size_t total_size = 0;
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (bitmap_value_cur != bitmap_value_end) {
            if (!*nullmap_cur) {
                total_size += bitmap_value_cur->getSizeInBytes();
            }
            ++nullmap_cur;
            ++bitmap_value_cur;
        }
    } else {
        while (bitmap_value_cur != bitmap_value_end) {
            total_size += bitmap_value_cur->getSizeInBytes();
            ++bitmap_value_cur;
        }
    }
    _raw_data.resize(total_size);

    bitmap_value_cur = bitmap_value;
    size_t slice_size;
    char* raw_data = _raw_data.data();
    Slice* slice = _slice.data();
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (bitmap_value_cur != bitmap_value_end) {
            if (!*nullmap_cur) {
                slice_size = bitmap_value_cur->getSizeInBytes();
                bitmap_value_cur->write_to(raw_data);

                slice->data = raw_data;
                slice->size = slice_size;
                raw_data += slice_size;
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++bitmap_value_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (bitmap_value_cur != bitmap_value_end) {
            slice_size = bitmap_value_cur->getSizeInBytes();
            bitmap_value_cur->write_to(raw_data);

            slice->data = raw_data;
            slice->size = slice_size;
            raw_data += slice_size;

            ++slice;
            ++bitmap_value_cur;
        }
        assert(slice == _slice.get_end_ptr());
    }
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorQuantileState::convert_to_olap() {
    assert(_typed_column.column);

    const vectorized::ColumnQuantileState* column_quantile_state = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_quantile_state = assert_cast<const vectorized::ColumnQuantileState*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_quantile_state =
                assert_cast<const vectorized::ColumnQuantileState*>(_typed_column.column.get());
    }

    assert(column_quantile_state);
    QuantileState* quantile_state =
            const_cast<QuantileState*>(column_quantile_state->get_data().data() + _row_pos);
    QuantileState* quantile_state_cur = quantile_state;
    QuantileState* quantile_state_end = quantile_state_cur + _num_rows;

    size_t total_size = 0;
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (quantile_state_cur != quantile_state_end) {
            if (!*nullmap_cur) {
                total_size += quantile_state_cur->get_serialized_size();
            }
            ++nullmap_cur;
            ++quantile_state_cur;
        }
    } else {
        while (quantile_state_cur != quantile_state_end) {
            total_size += quantile_state_cur->get_serialized_size();
            ++quantile_state_cur;
        }
    }
    _raw_data.resize(total_size);

    quantile_state_cur = quantile_state;
    size_t slice_size;
    char* raw_data = _raw_data.data();
    Slice* slice = _slice.data();
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (quantile_state_cur != quantile_state_end) {
            if (!*nullmap_cur) {
                slice_size = quantile_state_cur->get_serialized_size();
                quantile_state_cur->serialize((uint8_t*)raw_data);

                slice->data = raw_data;
                slice->size = slice_size;
                raw_data += slice_size;
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++quantile_state_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (quantile_state_cur != quantile_state_end) {
            slice_size = quantile_state_cur->get_serialized_size();
            quantile_state_cur->serialize((uint8_t*)raw_data);

            slice->data = raw_data;
            slice->size = slice_size;
            raw_data += slice_size;

            ++slice;
            ++quantile_state_cur;
        }
        assert(slice == _slice.get_end_ptr());
    }
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorHLL::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnHLL* column_hll = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_hll = assert_cast<const vectorized::ColumnHLL*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_hll = assert_cast<const vectorized::ColumnHLL*>(_typed_column.column.get());
    }

    assert(column_hll);
    HyperLogLog* hll_value = const_cast<HyperLogLog*>(column_hll->get_data().data() + _row_pos);
    HyperLogLog* hll_value_cur = hll_value;
    HyperLogLog* hll_value_end = hll_value_cur + _num_rows;

    size_t total_size = 0;
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (hll_value_cur != hll_value_end) {
            if (!*nullmap_cur) {
                total_size += hll_value_cur->max_serialized_size();
            }
            ++nullmap_cur;
            ++hll_value_cur;
        }
    } else {
        while (hll_value_cur != hll_value_end) {
            total_size += hll_value_cur->max_serialized_size();
            ++hll_value_cur;
        }
    }
    _raw_data.resize(total_size);

    size_t slice_size;
    char* raw_data = _raw_data.data();
    Slice* slice = _slice.data();

    hll_value_cur = hll_value;
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (hll_value_cur != hll_value_end) {
            if (!*nullmap_cur) {
                slice_size = hll_value_cur->serialize((uint8_t*)raw_data);

                slice->data = raw_data;
                slice->size = slice_size;
                raw_data += slice_size;
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++hll_value_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (hll_value_cur != hll_value_end) {
            slice_size = hll_value_cur->serialize((uint8_t*)raw_data);

            slice->data = raw_data;
            slice->size = slice_size;
            raw_data += slice_size;

            ++slice;
            ++hll_value_cur;
        }
        assert(slice == _slice.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorChar
OlapBlockDataConvertor::OlapColumnDataConvertorChar::OlapColumnDataConvertorChar(size_t length)
        : _length(length) {
    assert(length > 0);
}

void OlapBlockDataConvertor::OlapColumnDataConvertorChar::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    _slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorChar::get_data() const {
    return _slice.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorChar::get_data_at(size_t offset) const {
    UInt8 null_flag = 0;
    if (get_nullmap()) {
        null_flag = get_nullmap()[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorChar::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnString* column_string = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_string = assert_cast<const vectorized::ColumnString*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_string = assert_cast<const vectorized::ColumnString*>(_typed_column.column.get());
    }

    // If column_string is not padded to full, we should do padding here.
    if (should_padding(column_string, _length)) {
        _column = clone_and_padding(column_string, _length);
        column_string = assert_cast<const vectorized::ColumnString*>(_column.get());
    }

    for (size_t i = 0; i < _num_rows; i++) {
        if (!_nullmap || !_nullmap[i + _row_pos]) {
            _slice[i] = column_string->get_data_at(i + _row_pos).to_slice();
            DCHECK(_slice[i].size == _length)
                    << "char type data length not equal to schema, schema=" << _length
                    << ", real=" << _slice[i].size;
        }
    }

    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorVarChar
OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::OlapColumnDataConvertorVarChar(
        bool check_length)
        : _check_length(check_length) {}

void OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    _slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::get_data() const {
    return _slice.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::get_data_at(
        size_t offset) const {
    assert(offset < _slice.size());
    UInt8 null_flag = 0;
    if (get_nullmap()) {
        null_flag = get_nullmap()[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::convert_to_olap(
        const UInt8* null_map, const ColumnString* column_string) {
    assert(column_string);
    const char* char_data = (const char*)(column_string->get_chars().data());
    const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + _row_pos;
    const ColumnString::Offset* offset_end = offset_cur + _num_rows;

    Slice* slice = _slice.data();
    size_t string_offset = *(offset_cur - 1);
    if (null_map) {
        const UInt8* nullmap_cur = null_map + _row_pos;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                slice->data = const_cast<char*>(char_data + string_offset);
                slice->size = *offset_cur - string_offset;
                if (UNLIKELY(slice->size > config::string_type_length_soft_limit_bytes &&
                             _check_length)) {
                    return Status::NotSupported(
                            "Not support string len over than "
                            "`string_type_length_soft_limit_bytes` in vec engine.");
                }
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            string_offset = *offset_cur;
            ++nullmap_cur;
            ++slice;
            ++offset_cur;
        }
        assert(nullmap_cur == null_map + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (offset_cur != offset_end) {
            slice->data = const_cast<char*>(char_data + string_offset);
            slice->size = *offset_cur - string_offset;
            if (UNLIKELY(slice->size > config::string_type_length_soft_limit_bytes &&
                         _check_length)) {
                return Status::NotSupported(
                        "Not support string len over than `string_type_length_soft_limit_bytes`"
                        " in vec engine.");
            }
            string_offset = *offset_cur;
            ++slice;
            ++offset_cur;
        }
        assert(slice == _slice.get_end_ptr());
    }
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnString* column_string = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_string = assert_cast<const vectorized::ColumnString*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_string = assert_cast<const vectorized::ColumnString*>(_typed_column.column.get());
    }
    RETURN_IF_ERROR(convert_to_olap(_nullmap, column_string));
    return Status::OK();
}

void OlapBlockDataConvertor::OlapColumnDataConvertorAggState::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    _slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorAggState::get_data() const {
    return _slice.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorAggState::get_data_at(
        size_t offset) const {
    assert(offset < _slice.size());
    UInt8 null_flag = 0;
    if (get_nullmap()) {
        null_flag = get_nullmap()[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorAggState::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnFixedLengthObject* column_fixed_object = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_fixed_object = assert_cast<const vectorized::ColumnFixedLengthObject*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_fixed_object =
                assert_cast<const vectorized::ColumnFixedLengthObject*>(_typed_column.column.get());
    }

    assert(column_fixed_object);
    auto item_size = column_fixed_object->item_size();

    auto cur_values = (uint8_t*)(column_fixed_object->get_data().data()) + (item_size * _row_pos);
    auto end_values = cur_values + (item_size * _num_rows);
    Slice* slice = _slice.data();

    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (cur_values != end_values) {
            if (!*nullmap_cur) {
                slice->data = reinterpret_cast<char*>(cur_values);
                slice->size = item_size;
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++nullmap_cur;
            ++slice;
            cur_values = cur_values + item_size;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (cur_values != end_values) {
            slice->data = reinterpret_cast<char*>(cur_values);
            slice->size = item_size;
            ++slice;
            cur_values = cur_values + item_size;
        }
        assert(slice == _slice.get_end_ptr());
    }
    return Status::OK();
}

void OlapBlockDataConvertor::OlapColumnDataConvertorDate::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorPaddedPODArray<uint24_t>::set_source_column(
            typed_column, row_pos, num_rows);
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDate::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnVector<vectorized::Int64>* column_datetime = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                _typed_column.column.get());
    }

    assert(column_datetime);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + _row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + _num_rows;
    uint24_t* value = _values.data();
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (datetime_cur != datetime_end) {
            if (!*nullmap_cur) {
                *value = datetime_cur->to_olap_date();
            } else {
                // do nothing
            }
            ++value;
            ++datetime_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && value == _values.get_end_ptr());
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_date();
            ++value;
            ++datetime_cur;
        }
        assert(value == _values.get_end_ptr());
    }
    return Status::OK();
}

void OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorPaddedPODArray<uint64_t>::set_source_column(
            typed_column, row_pos, num_rows);
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnVector<vectorized::Int64>* column_datetime = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                _typed_column.column.get());
    }

    assert(column_datetime);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + _row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + _num_rows;
    uint64_t* value = _values.data();
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (datetime_cur != datetime_end) {
            if (!*nullmap_cur) {
                *value = datetime_cur->to_olap_datetime();
            } else {
                // do nothing
            }
            ++value;
            ++datetime_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && value == _values.get_end_ptr());
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_datetime();
            ++value;
            ++datetime_cur;
        }
        assert(value == _values.get_end_ptr());
    }
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDecimal::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnDecimal<vectorized::Decimal128V2>* column_decimal = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128V2>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128V2>*>(
                _typed_column.column.get());
    }

    assert(column_decimal);

    const DecimalV2Value* decimal_cur =
            (const DecimalV2Value*)(column_decimal->get_data().data()) + _row_pos;
    const DecimalV2Value* decimal_end = decimal_cur + _num_rows;
    decimal12_t* value = _values.data();
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (decimal_cur != decimal_end) {
            if (!*nullmap_cur) {
                value->integer = decimal_cur->int_value();
                value->fraction = decimal_cur->frac_value();
            } else {
                // do nothing
            }
            ++value;
            ++decimal_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && value == _values.get_end_ptr());
    } else {
        while (decimal_cur != decimal_end) {
            value->integer = decimal_cur->int_value();
            value->fraction = decimal_cur->frac_value();
            ++value;
            ++decimal_cur;
        }
        assert(value == _values.get_end_ptr());
    }
    return Status::OK();
}

void OlapBlockDataConvertor::OlapColumnDataConvertorStruct::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorStruct::get_data() const {
    return _results.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorStruct::get_data_at(
        size_t offset) const {
    // Todo(xy): struct not supported
    return nullptr;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorStruct::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnStruct* column_struct = nullptr;
    const vectorized::DataTypeStruct* data_type_struct = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_struct = assert_cast<const vectorized::ColumnStruct*>(
                nullable_column->get_nested_column_ptr().get());
        data_type_struct = assert_cast<const DataTypeStruct*>(
                (assert_cast<const DataTypeNullable*>(_typed_column.type.get())->get_nested_type())
                        .get());
    } else {
        column_struct = assert_cast<const vectorized::ColumnStruct*>(_typed_column.column.get());
        data_type_struct = assert_cast<const DataTypeStruct*>(_typed_column.type.get());
    }
    assert(column_struct);
    assert(data_type_struct);

    size_t fields_num = column_struct->tuple_size();
    size_t data_cursor = 0;
    size_t null_map_cursor = data_cursor + fields_num;
    for (size_t i = 0; i < fields_num; i++) {
        ColumnPtr sub_column = column_struct->get_column_ptr(i);
        DataTypePtr sub_type = data_type_struct->get_element(i);
        ColumnWithTypeAndName sub_typed_column = {sub_column, sub_type, ""};
        _sub_convertors[i]->set_source_column(sub_typed_column, _row_pos, _num_rows);
        RETURN_IF_ERROR(_sub_convertors[i]->convert_to_olap());
        _results[data_cursor] = _sub_convertors[i]->get_data();
        _results[null_map_cursor] = _sub_convertors[i]->get_nullmap();
        data_cursor++;
        null_map_cursor++;
    }
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorArray::convert_to_olap() {
    const ColumnArray* column_array = nullptr;
    const DataTypeArray* data_type_array = nullptr;
    if (_nullmap) {
        const auto* nullable_column =
                assert_cast<const ColumnNullable*>(_typed_column.column.get());
        column_array =
                assert_cast<const ColumnArray*>(nullable_column->get_nested_column_ptr().get());
        data_type_array = assert_cast<const DataTypeArray*>(
                (assert_cast<const DataTypeNullable*>(_typed_column.type.get())->get_nested_type())
                        .get());
    } else {
        column_array = assert_cast<const ColumnArray*>(_typed_column.column.get());
        data_type_array = assert_cast<const DataTypeArray*>(_typed_column.type.get());
    }
    assert(column_array);
    assert(data_type_array);

    return convert_to_olap(column_array, data_type_array);
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorArray::convert_to_olap(
        const ColumnArray* column_array, const DataTypeArray* data_type_array) {
    ColumnPtr item_data = column_array->get_data_ptr();

    auto start_offset = column_array->offset_at(_row_pos);
    auto end_offset = column_array->offset_at(_row_pos + _num_rows);
    auto elem_size = end_offset - start_offset;

    _offsets.clear();
    // we need all offsets, so reserve num_rows + 1 to make sure last offset can be got in offset column, instead of according to nested item column
    _offsets.reserve(_num_rows + 1);
    for (int i = 0; i <= _num_rows; ++i) {
        _offsets.push_back(column_array->offset_at(i + _row_pos) - start_offset + _base_offset);
    }

    _base_offset += elem_size;

    ColumnWithTypeAndName item_typed_column = {item_data, data_type_array->get_nested_type(),
                                               "array.item"};
    _item_convertor->set_source_column(item_typed_column, start_offset, elem_size);
    RETURN_IF_ERROR(_item_convertor->convert_to_olap());

    _results[0] = (void*)elem_size;
    _results[1] = _offsets.data();
    _results[2] = _item_convertor->get_data();
    _results[3] = _item_convertor->get_nullmap();
    return Status::OK();
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorMap::convert_to_olap() {
    const ColumnMap* column_map = nullptr;
    if (_nullmap) {
        const auto* nullable_column =
                assert_cast<const ColumnNullable*>(_typed_column.column.get());
        column_map = assert_cast<const ColumnMap*>(nullable_column->get_nested_column_ptr().get());
    } else {
        column_map = assert_cast<const ColumnMap*>(_typed_column.column.get());
    }
    assert(column_map);

    return convert_to_olap(column_map);
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorMap::convert_to_olap(
        const ColumnMap* column_map) {
    ColumnPtr key_data = column_map->get_keys_ptr();
    ColumnPtr value_data = column_map->get_values_ptr();

    // NOTICE here are two situation:
    // 1. Multi-SegmentWriter with different olap_convertor to convert same column_map(in memory which is from same block)
    //   eg: Block(6 row): column_map offsets in memory: [10, 21, 33, 43, 54, 66]
    //   After SegmentWriter1 with olap_convertor1 deal with first 3 rows:  _offsets(pre-disk)=[0, 10, 21], _base_offset=33
    //   then SegmentWriter may flush data (see BetaRowsetWriter::_add_block(max_row_add < 1))
    //   ColumnWriter will flush offset array to disk [0, 10, 21, 33]
    //                                                 ---------  ----
    //                                                 |--_offsets  |--set_next_array_item_ordinal(_kv_writers[0]->get_next_rowid())
    //   new SegmentWriter2 with olap_convertor2 deal with next map offsets [43, 54, 66]
    //   but in disk here is new segment file offset should start with 0, so after convert:
    //      _offsets(pre-disk)=[0, 10, 21], _base_row=33, After flush data finally in disk: [0, 10, 21, 33]
    //2. One-SegmentWriter with olap_convertor to convertor different blocks into one page
    //    eg: Two blocks -> block1 [10, 21, 33] and block2 [1, 3, 6]
    //      After first convert: _offsets_1(pre-disk)=[0, 10, 21], _base_row=33, without flush, just append to page,
    //      then deal with coming block2, after current convert:
    //      _offsets_2=[33, 34, 36], _base_offset=39
    //      if we flush here, finally in disk offsets:[0, 10, 21, 33, 34, 36,        39]
    //                                                ----------  ----------         ---
    //                                                 |--_offsets_1  |--_offsets_2   |--set_next_array_item_ordinal(_kv_writers[0]->get_next_rowid())
    auto start_offset = column_map->offset_at(_row_pos);
    auto end_offset = column_map->offset_at(_row_pos + _num_rows);
    auto elem_size = end_offset - start_offset;

    _offsets.clear();
    // we need all offsets, so reserve num_rows + 1 to make sure last offset can be got in offset column, instead of according to nested item column
    _offsets.reserve(_num_rows + 1);
    for (int i = 0; i <= _num_rows; ++i) {
        _offsets.push_back(column_map->offset_at(i + _row_pos) - start_offset + _base_offset);
    }
    _base_offset += elem_size;
    ColumnWithTypeAndName key_typed_column = {key_data, _data_type.get_key_type(), "map.key"};
    _key_convertor->set_source_column(key_typed_column, start_offset, elem_size);
    RETURN_IF_ERROR(_key_convertor->convert_to_olap());

    ColumnWithTypeAndName value_typed_column = {value_data, _data_type.get_value_type(),
                                                "map.value"};
    _value_convertor->set_source_column(value_typed_column, start_offset, elem_size);
    RETURN_IF_ERROR(_value_convertor->convert_to_olap());

    // todo (Amory). put this value into MapValue
    _results[0] = (void*)elem_size;
    _results[1] = _offsets.data();
    _results[2] = _key_convertor->get_data();
    _results[3] = _value_convertor->get_data();
    _results[4] = _key_convertor->get_nullmap();
    _results[5] = _value_convertor->get_nullmap();

    return Status::OK();
}

void OlapBlockDataConvertor::OlapColumnDataConvertorVariant::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    // set
    const ColumnNullable* nullable_column = nullptr;
    if (typed_column.column->is_nullable()) {
        nullable_column = assert_cast<const ColumnNullable*>(typed_column.column.get());
        _nullmap = nullable_column->get_null_map_data().data();
    }
    const auto& variant =
            nullable_column == nullptr
                    ? assert_cast<const vectorized::ColumnObject&>(*typed_column.column)
                    : assert_cast<const vectorized::ColumnObject&>(
                              nullable_column->get_nested_column());
    if (variant.is_null_root()) {
        auto root_type = make_nullable(std::make_shared<ColumnObject::MostCommonType>());
        auto root_col = root_type->create_column();
        root_col->insert_many_defaults(variant.rows());
        const_cast<ColumnObject&>(variant).create_root(root_type, std::move(root_col));
        variant.check_consistency();
    }
    // ensure data finalized
    _source_column_ptr = &const_cast<ColumnObject&>(variant);
    _source_column_ptr->finalize(false);
    _root_data_convertor = std::make_unique<OlapColumnDataConvertorVarChar>(true);
    _root_data_convertor->set_source_column(
            {_source_column_ptr->get_root()->get_ptr(), nullptr, ""}, row_pos, num_rows);
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
}

// convert root data
Status OlapBlockDataConvertor::OlapColumnDataConvertorVariant::convert_to_olap() {
    RETURN_IF_ERROR(vectorized::schema_util::encode_variant_sparse_subcolumns(*_source_column_ptr));
#ifndef NDEBUG
    _source_column_ptr->check_consistency();
#endif
    const auto* nullable = assert_cast<const ColumnNullable*>(_source_column_ptr->get_root().get());
    const auto* root_column = assert_cast<const ColumnString*>(&nullable->get_nested_column());
    RETURN_IF_ERROR(_root_data_convertor->convert_to_olap(_nullmap, root_column));
    return Status::OK();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorVariant::get_data() const {
    return _root_data_convertor->get_data();
}
const void* OlapBlockDataConvertor::OlapColumnDataConvertorVariant::get_data_at(
        size_t offset) const {
    return _root_data_convertor->get_data_at(offset);
}

} // namespace doris::vectorized
