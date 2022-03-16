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

#include "vec/columns/column_complex.h"
#include "vec/columns/column_vector.h"

namespace doris::vectorized {

// class OlapBlockDataConvertor
OlapBlockDataConvertor::OlapBlockDataConvertor(const TabletSchema* tablet_schema) {
    assert(tablet_schema);
    const auto& columns = tablet_schema->columns();
    for (const auto& col : columns) {
        switch (col.type()) {
        case FieldType::OLAP_FIELD_TYPE_OBJECT: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorObject>());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_HLL: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorHLL>());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_CHAR: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorChar>(col.length()));
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_MAP:
        case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorVarChar>(false));
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_STRING: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorVarChar>(true));
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_DATE: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorDate>());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_DATETIME: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorDateTime>());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
            m_convertors.emplace_back(std::make_shared<OlapColumnDataConvertorDecimal>());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_BOOL: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::UInt8> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_TINYINT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Int8> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Int16> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_INT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Int32> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_BIGINT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Int64> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Int128> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_FLOAT: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Float32> >());
            break;
        }
        case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
            m_convertors.emplace_back(
                    std::make_shared<OlapColumnDataConvertorSimple<vectorized::Float64> >());
            break;
        }
        default: {
            DCHECK(false) << "Invalid type in RowBlockV2:" << col.type();
        }
        }
    }
}

void OlapBlockDataConvertor::set_source_content(const vectorized::Block* block, size_t row_pos,
                                                size_t num_rows) {
    assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() &&
           block->columns() == m_convertors.size());
    size_t cid = 0;
    for (const auto& typed_column : *block) {
        m_convertors[cid]->set_source_column(typed_column, row_pos, num_rows);
        ++cid;
    }
}

void OlapBlockDataConvertor::clear_source_content() {
    for (auto& convertor : m_convertors) {
        convertor->clear_source_column();
    }
}

std::pair<Status, IOlapColumnDataAccessorSPtr> OlapBlockDataConvertor::convert_column_data(
        size_t cid) {
    assert(cid < m_convertors.size());
    auto status = m_convertors[cid]->convert_to_olap();
    return {status, m_convertors[cid]};
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorBase
void OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    assert(num_rows > 0 && row_pos + num_rows <= typed_column.column->size());
    m_typed_column = typed_column;
    m_row_pos = row_pos;
    m_num_rows = num_rows;
}

void OlapBlockDataConvertor::OlapColumnDataConvertorBase::clear_source_column() {
    // just to reduce the source column's ref count to 1
    m_typed_column.column = nullptr;
}

const UInt8* OlapBlockDataConvertor::OlapColumnDataConvertorBase::get_nullmap() const {
    assert(m_typed_column.column);
    const UInt8* nullmap = nullptr;
    if (m_typed_column.column->is_nullable()) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        nullmap = nullable_column->get_null_map_data().data();
    }
    return nullmap;
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorObject
void OlapBlockDataConvertor::OlapColumnDataConvertorObject::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_raw_data.clear();
    m_slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorObject::get_data() const {
    return m_slice.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorObject::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_slice.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_slice.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorObject::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnBitmap* column_bitmap = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_bitmap = assert_cast<const vectorized::ColumnBitmap*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_bitmap = assert_cast<const vectorized::ColumnBitmap*>(m_typed_column.column.get());
    }

    assert(column_bitmap);
    BitmapValue* bitmap_value_cur =
            const_cast<BitmapValue*>(column_bitmap->get_data().data() + m_row_pos);
    BitmapValue* bitmap_value_end = bitmap_value_cur + m_num_rows;
    size_t slice_size;
    size_t old_size;
    char* raw_data;
    Slice* slice = m_slice.data();
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
        while (bitmap_value_cur != bitmap_value_end) {
            if (!*nullmap_cur) {
                slice_size = bitmap_value_cur->getSizeInBytes();
                old_size = m_raw_data.size();
                m_raw_data.resize(old_size + slice_size);

                raw_data = m_raw_data.data() + old_size;
                bitmap_value_cur->write(raw_data);

                slice->data = raw_data;
                slice->size = slice_size;
            } else {
                // TODO: this may not be neccessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++bitmap_value_cur;
        }
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && slice == m_slice.get_end_ptr());
    } else {
        while (bitmap_value_cur != bitmap_value_end) {
            slice_size = bitmap_value_cur->getSizeInBytes();
            old_size = m_raw_data.size();
            m_raw_data.resize(old_size + slice_size);

            raw_data = m_raw_data.data() + old_size;
            bitmap_value_cur->write(raw_data);

            slice->data = raw_data;
            slice->size = slice_size;

            ++slice;
            ++bitmap_value_cur;
        }
        assert(slice == m_slice.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorHLL
void OlapBlockDataConvertor::OlapColumnDataConvertorHLL::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_raw_data.clear();
    m_slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorHLL::get_data() const {
    return m_slice.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorHLL::get_data_at(size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_slice.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_slice.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorHLL::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnHLL* column_hll = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_hll = assert_cast<const vectorized::ColumnHLL*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_hll = assert_cast<const vectorized::ColumnHLL*>(m_typed_column.column.get());
    }

    assert(column_hll);
    HyperLogLog* hll_value_cur =
            const_cast<HyperLogLog*>(column_hll->get_data().data() + m_row_pos);
    HyperLogLog* hll_value_end = hll_value_cur + m_num_rows;
    size_t slice_size;
    size_t old_size;
    char* raw_data;
    Slice* slice = m_slice.data();
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
        while (hll_value_cur != hll_value_end) {
            if (!*nullmap_cur) {
                slice_size = hll_value_cur->max_serialized_size();
                old_size = m_raw_data.size();
                m_raw_data.resize(old_size + slice_size);

                raw_data = m_raw_data.data() + old_size;
                slice_size = hll_value_cur->serialize((uint8_t*)raw_data);
                m_raw_data.resize(old_size + slice_size);

                slice->data = raw_data;
                slice->size = slice_size;
            } else {
                // TODO: this may not be neccessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++hll_value_cur;
        }
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && slice == m_slice.get_end_ptr());
    } else {
        while (hll_value_cur != hll_value_end) {
            slice_size = hll_value_cur->max_serialized_size();
            old_size = m_raw_data.size();
            m_raw_data.resize(old_size + slice_size);

            raw_data = m_raw_data.data() + old_size;
            slice_size = hll_value_cur->serialize((uint8_t*)raw_data);
            m_raw_data.resize(old_size + slice_size);

            slice->data = raw_data;
            slice->size = slice_size;

            ++slice;
            ++hll_value_cur;
        }
        assert(slice == m_slice.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorChar
OlapBlockDataConvertor::OlapColumnDataConvertorChar::OlapColumnDataConvertorChar(size_t length)
        : m_length(length) {
    assert(length > 0);
}

void OlapBlockDataConvertor::OlapColumnDataConvertorChar::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_raw_data.resize(m_length * num_rows);
    memset(m_raw_data.data(), 0, m_length * num_rows);
    m_slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorChar::get_data() const {
    return m_slice.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorChar::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_slice.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_slice.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorChar::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnString* column_string = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_string = assert_cast<const vectorized::ColumnString*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_string = assert_cast<const vectorized::ColumnString*>(m_typed_column.column.get());
    }

    assert(column_string);

    const ColumnString::Char* char_data = column_string->get_chars().data();
    const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + m_row_pos;
    const ColumnString::Offset* offset_end = offset_cur + m_num_rows;
    char* raw_data = m_raw_data.data();
    Slice* slice = m_slice.data();
    size_t string_length;
    size_t string_offset = *(offset_cur - 1);
    size_t slice_size = m_length;
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                string_length = *offset_cur - string_offset - 1;
                assert(string_length <= slice_size);
                memcpy(raw_data, char_data + string_offset, string_length);

                slice->data = raw_data;
                slice->size = slice_size;
            } else {
                // TODO: this may not be neccessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }

            string_offset = *offset_cur;
            ++nullmap_cur;
            ++slice;
            ++offset_cur;
            raw_data += slice_size;
        }
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && slice == m_slice.get_end_ptr());
    } else {
        while (offset_cur != offset_end) {
            string_length = *offset_cur - string_offset - 1;
            assert(string_length <= slice_size);
            memcpy(raw_data, char_data + string_offset, string_length);

            slice->data = raw_data;
            slice->size = slice_size;

            string_offset = *offset_cur;
            ++slice;
            ++offset_cur;
            raw_data += slice_size;
        }
        assert(slice == m_slice.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorVarChar
OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::OlapColumnDataConvertorVarChar(
        bool check_length)
        : m_check_length(check_length) {}

void OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_slice.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::get_data() const {
    return m_slice.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_slice.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_slice.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorVarChar::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnString* column_string = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_string = assert_cast<const vectorized::ColumnString*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_string = assert_cast<const vectorized::ColumnString*>(m_typed_column.column.get());
    }

    assert(column_string);

    const char* char_data = (const char*)(column_string->get_chars().data());
    const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + m_row_pos;
    const ColumnString::Offset* offset_end = offset_cur + m_num_rows;

    Slice* slice = m_slice.data();
    size_t string_offset = *(offset_cur - 1);
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                slice->data = const_cast<char*>(char_data + string_offset);
                slice->size = *offset_cur - string_offset - 1;
                if (UNLIKELY(slice->size > MAX_SIZE_OF_VEC_STRING && m_check_length)) {
                    return Status::NotSupported(
                            "Not support string len over than 1MB in vec engine.");
                }
            } else {
                // TODO: this may not be neccessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            string_offset = *offset_cur;
            ++nullmap_cur;
            ++slice;
            ++offset_cur;
        }
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && slice == m_slice.get_end_ptr());
    } else {
        while (offset_cur != offset_end) {
            slice->data = const_cast<char*>(char_data + string_offset);
            slice->size = *offset_cur - string_offset - 1;
            if (UNLIKELY(slice->size > MAX_SIZE_OF_VEC_STRING && m_check_length)) {
                return Status::NotSupported("Not support string len over than 1MB in vec engine.");
            }
            string_offset = *offset_cur;
            ++slice;
            ++offset_cur;
        }
        assert(slice == m_slice.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorDate
void OlapBlockDataConvertor::OlapColumnDataConvertorDate::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_values.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorDate::get_data() const {
    return m_values.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorDate::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_values.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_values.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDate::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnVector<vectorized::Int64>* column_datetime = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                m_typed_column.column.get());
    }

    assert(column_datetime);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + m_row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + m_num_rows;
    uint24_t* value = m_values.data();
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
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
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && value == m_values.get_end_ptr());
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_date();
            ++value;
            ++datetime_cur;
        }
        assert(value == m_values.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorDateTime
void OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_values.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::get_data() const {
    return m_values.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_values.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_values.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDateTime::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnVector<vectorized::Int64>* column_datetime = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_datetime = assert_cast<const vectorized::ColumnVector<vectorized::Int64>*>(
                m_typed_column.column.get());
    }

    assert(column_datetime);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + m_row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + m_num_rows;
    uint64_t* value = m_values.data();
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
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
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && value == m_values.get_end_ptr());
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_datetime();
            ++value;
            ++datetime_cur;
        }
        assert(value == m_values.get_end_ptr());
    }
    return Status::OK();
}

// class OlapBlockDataConvertor::OlapColumnDataConvertorDecimal
void OlapBlockDataConvertor::OlapColumnDataConvertorDecimal::set_source_column(
        const ColumnWithTypeAndName& typed_column, size_t row_pos, size_t num_rows) {
    OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos,
                                                                           num_rows);
    m_values.resize(num_rows);
}

const void* OlapBlockDataConvertor::OlapColumnDataConvertorDecimal::get_data() const {
    return m_values.data();
}

OlapFieldData OlapBlockDataConvertor::OlapColumnDataConvertorDecimal::get_data_at(
        size_t offset) const {
    assert(offset < m_num_rows && m_num_rows == m_values.size());
    UInt8 null_flag = 0;
    auto null_map = get_nullmap();
    if (null_map) {
        null_flag = null_map[offset];
    }
    return {null_flag, m_values.data() + offset};
}

Status OlapBlockDataConvertor::OlapColumnDataConvertorDecimal::convert_to_olap() {
    assert(m_typed_column.column);
    const vectorized::ColumnDecimal<vectorized::Decimal128>* column_decimal = nullptr;
    const UInt8* nullmap = get_nullmap();
    if (nullmap) {
        auto nullable_column =
                assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                nullable_column->get_nested_column_ptr().get());
    } else {
        column_decimal = assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                m_typed_column.column.get());
    }

    assert(column_decimal);

    const DecimalV2Value* decimal_cur =
            (const DecimalV2Value*)(column_decimal->get_data().data()) + m_row_pos;
    const DecimalV2Value* decimal_end = decimal_cur + m_num_rows;
    decimal12_t* value = m_values.data();
    if (nullmap) {
        const UInt8* nullmap_cur = nullmap + m_row_pos;
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
        assert(nullmap_cur == nullmap + m_row_pos + m_num_rows && value == m_values.get_end_ptr());
    } else {
        while (decimal_cur != decimal_end) {
            value->integer = decimal_cur->int_value();
            value->fraction = decimal_cur->frac_value();
            ++value;
            ++decimal_cur;
        }
        assert(value == m_values.get_end_ptr());
    }
    return Status::OK();
}

} // namespace doris::vectorized