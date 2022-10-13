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

#include "olap/tablet_schema.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_array.h"

namespace doris::vectorized {

OlapBlockDataConvertor::OlapBlockDataConvertor(const TabletSchema* tablet_schema) {
    assert(tablet_schema);
    const auto& columns = tablet_schema->columns();
    for (const auto& col : columns) {
        _convertors.emplace_back(create_olap_column_data_convertor(col));
    }
}

OlapBlockDataConvertor::OlapColumnDataConvertorBaseUPtr
OlapBlockDataConvertor::create_olap_column_data_convertor(const TabletColumn& column) {
    switch (column.type()) {
    case FieldType::OLAP_FIELD_TYPE_OBJECT: {
        return std::make_unique<OlapColumnDataConvertorBitMap>();
    }
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        return std::make_unique<OlapColumnDataConvertorHLL>();
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<OlapColumnDataConvertorChar>(column.length());
    }
    case FieldType::OLAP_FIELD_TYPE_MAP:
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
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return std::make_unique<OlapColumnDataConvertorDateTimeV2>();
        break;
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
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128: {
        return std::make_unique<OlapColumnDataConvertorDecimalV3<Decimal128>>();
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        return std::make_unique<OlapColumnDataConvertorJsonb>();
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
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Float32>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return std::make_unique<OlapColumnDataConvertorSimple<vectorized::Float64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        const auto& sub_column = column.get_sub_column(0);
        return std::make_unique<OlapColumnDataConvertorArray>(
                create_olap_column_data_convertor(sub_column));
    }
    default: {
        DCHECK(false) << "Invalid type in RowBlockV2:" << column.type();
        return nullptr;
    }
    }
} // namespace doris::vectorized

void OlapBlockDataConvertor::set_source_content(const vectorized::Block* block, size_t row_pos,
                                                size_t num_rows) {
    assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() &&
           block->columns() == _convertors.size());
    size_t cid = 0;
    for (const auto& typed_column : *block) {
        _convertors[cid]->set_source_column(typed_column, row_pos, num_rows);
        ++cid;
    }
}

void OlapBlockDataConvertor::clear_source_content() {
    for (auto& convertor : _convertors) {
        convertor->clear_source_column();
    }
}

std::pair<Status, IOlapColumnDataAccessor*> OlapBlockDataConvertor::convert_column_data(
        size_t cid) {
    assert(cid < _convertors.size());
    auto status = _convertors[cid]->convert_to_olap();
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

// This should be called only in SegmentWriter. If you want to access nullmap in Convertor,
// use `_nullmap` directly.
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
    if (_nullmap) {
        null_flag = _nullmap[offset];
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
                bitmap_value_cur->write(raw_data);

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
            bitmap_value_cur->write(raw_data);

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
    if (_nullmap) {
        null_flag = _nullmap[offset];
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
    if (_nullmap) {
        null_flag = _nullmap[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
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

    assert(column_string);

    const char* char_data = (const char*)(column_string->get_chars().data());
    const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + _row_pos;
    const ColumnString::Offset* offset_end = offset_cur + _num_rows;

    Slice* slice = _slice.data();
    size_t string_offset = *(offset_cur - 1);
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
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
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
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

// class OlapBlockDataConvertor::OlapColumnDataConvertorJsonb
void OlapBlockDataConvertor::OlapColumnDataConvertorJsonb::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    _slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorJsonb::get_data() const {
    return _slice.data();
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorJsonb::get_data_at(size_t offset) const {
    assert(offset < _num_rows && _num_rows == _slice.size());
    UInt8 null_flag = 0;
    if (_nullmap) {
        null_flag = _nullmap[offset];
    }
    return null_flag ? nullptr : _slice.data() + offset;
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorJsonb::convert_to_olap() {
    assert(_typed_column.column);
    const vectorized::ColumnJsonb* column_json = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_json = assert_cast<const vectorized::ColumnJsonb*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_json = assert_cast<const vectorized::ColumnJsonb*>(_typed_column.column.get());
    }

    assert(column_json);

    const char* char_data = (const char*)(column_json->get_chars().data());
    const ColumnJsonb::Offset* offset_cur = column_json->get_offsets().data() + _row_pos;
    const ColumnJsonb::Offset* offset_end = offset_cur + _num_rows;

    Slice* slice = _slice.data();
    size_t string_offset = *(offset_cur - 1);
    if (_nullmap) {
        const UInt8* nullmap_cur = _nullmap + _row_pos;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                slice->data = const_cast<char*>(char_data + string_offset);
                slice->size = *offset_cur - string_offset - 1;
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
        assert(nullmap_cur == _nullmap + _row_pos + _num_rows && slice == _slice.get_end_ptr());
    } else {
        while (offset_cur != offset_end) {
            slice->data = const_cast<char*>(char_data + string_offset);
            slice->size = *offset_cur - string_offset - 1;
            string_offset = *offset_cur;
            ++slice;
            ++offset_cur;
        }
        assert(slice == _slice.get_end_ptr());
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
    const vectorized::ColumnDecimal<vectorized::Decimal128>* column_decimal = nullptr;
    if (_nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
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

    return convert_to_olap(_nullmap, column_array, data_type_array);
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorArray::convert_to_olap(
        const UInt8* null_map, const ColumnArray* column_array,
        const DataTypeArray* data_type_array) {
    const UInt8* item_null_map = nullptr;
    ColumnPtr item_data = column_array->get_data_ptr();
    if (column_array->get_data().is_nullable()) {
        const auto& data_nullable_column =
                assert_cast<const ColumnNullable&>(column_array->get_data());
        item_null_map = data_nullable_column.get_null_map_data().data();
        item_data = data_nullable_column.get_nested_column_ptr();
    }

    const auto& offsets = column_array->get_offsets();
    int64_t start_index = _row_pos - 1;
    int64_t end_index = _row_pos + _num_rows - 1;
    auto start = offsets[start_index];
    auto size = offsets[end_index] - start;

    ColumnWithTypeAndName item_typed_column = {
            item_data, remove_nullable(data_type_array->get_nested_type()), ""};
    _item_convertor->set_source_column(item_typed_column, start, size);
    _item_convertor->convert_to_olap();

    CollectionValue* collection_value = _values.data();
    for (size_t i = 0; i < _num_rows; ++i, ++collection_value) {
        int64_t cur_pos = _row_pos + i;
        int64_t prev_pos = cur_pos - 1;
        if (_nullmap && _nullmap[cur_pos]) {
            continue;
        }
        auto offset = offsets[prev_pos];
        auto size = offsets[cur_pos] - offsets[prev_pos];
        new (collection_value) CollectionValue(size);

        if (size == 0) {
            continue;
        }

        if (column_array->get_data().is_nullable()) {
            collection_value->set_has_null(true);
            collection_value->set_null_signs(
                    const_cast<bool*>(reinterpret_cast<const bool*>(item_null_map + offset)));
        }
        // get_data_at should use offset - offsets[start_index] since
        // start_index may be changed after OlapColumnDataConvertorArray::set_source_column.
        // Using just offset may access the memory out of _item_convertor's data range,
        collection_value->set_data(
                const_cast<void*>(_item_convertor->get_data_at(offset - offsets[start_index])));
    }
    return Status::OK();
}

} // namespace doris::vectorized
