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

#include "olap/row_block2.h"

#include <algorithm>
#include <sstream>
#include <utility>

#include "gutil/strings/substitute.h"
#include "olap/row_cursor.h"
#include "util/bitmap.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"

using strings::Substitute;
namespace doris {

RowBlockV2::RowBlockV2(const Schema& schema, uint16_t capacity)
        : _schema(schema),
          _capacity(capacity),
          _column_vector_batches(_schema.num_columns()),
          _pool(new MemPool("RowBlockV2")),
          _selection_vector(nullptr) {
    for (auto cid : _schema.column_ids()) {
        Status status = ColumnVectorBatch::create(
                _capacity, _schema.column(cid)->is_nullable(), _schema.column(cid)->type_info(),
                const_cast<Field*>(_schema.column(cid)), &_column_vector_batches[cid]);
        if (!status.ok()) {
            LOG(ERROR) << "failed to create ColumnVectorBatch for type: "
                       << _schema.column(cid)->type();
            return;
        }
    }
    _selection_vector = new uint16_t[_capacity];
    clear();
}

RowBlockV2::~RowBlockV2() {
    delete[] _selection_vector;
}

Status RowBlockV2::convert_to_row_block(RowCursor* helper, RowBlock* dst) {
    for (auto cid : _schema.column_ids()) {
        bool is_nullable = _schema.column(cid)->is_nullable();
        if (is_nullable) {
            for (uint16_t i = 0; i < _selected_size; ++i) {
                uint16_t row_idx = _selection_vector[i];
                dst->get_row(i, helper);
                bool is_null = _column_vector_batches[cid]->is_null_at(row_idx);
                if (is_null) {
                    helper->set_null(cid);
                } else {
                    helper->set_not_null(cid);
                    helper->set_field_content_shallow(
                            cid,
                            reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)));
                }
            }
        } else {
            for (uint16_t i = 0; i < _selected_size; ++i) {
                uint16_t row_idx = _selection_vector[i];
                dst->get_row(i, helper);
                helper->set_not_null(cid);
                helper->set_field_content_shallow(
                        cid, reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)));
            }
        }
    }
    // swap MemPool to copy string content
    dst->mem_pool()->exchange_data(_pool.get());
    dst->set_pos(0);
    dst->set_limit(_selected_size);
    dst->finalize(_selected_size);
    return Status::OK();
}

Status RowBlockV2::_copy_data_to_column(int cid,
                                        doris::vectorized::MutableColumnPtr& origin_column) {
    auto* column = origin_column.get();
    bool nullable_mark_array[_selected_size];

    bool column_nullable = origin_column->is_nullable();
    bool origin_nullable = _schema.column(cid)->is_nullable();
    if (column_nullable) {
        auto nullable_column = assert_cast<vectorized::ColumnNullable*>(origin_column.get());
        auto& null_map = nullable_column->get_null_map_data();
        column = nullable_column->get_nested_column_ptr().get();

        if (origin_nullable) {
            for (uint16_t i = 0; i < _selected_size; ++i) {
                uint16_t row_idx = _selection_vector[i];
                null_map.push_back(_column_vector_batches[cid]->is_null_at(row_idx));
                nullable_mark_array[i] = null_map.back();
            }
        } else {
            null_map.resize_fill(null_map.size() + _selected_size, 0);
            memset(nullable_mark_array, false, _selected_size * sizeof(bool));
        }
    } else {
        memset(nullable_mark_array, false, _selected_size * sizeof(bool));
    }

    auto insert_data_directly = [this, &nullable_mark_array](int cid, auto& column) {
        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                column->insert_data(
                        reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)), 0);
            } else {
                column->insert_default();
            }
        }
    };

    switch (_schema.column(cid)->type()) {
    case OLAP_FIELD_TYPE_OBJECT: {
        auto column_bitmap = assert_cast<vectorized::ColumnBitmap*>(column);
        for (uint16_t j = 0; j < _selected_size; ++j) {
            column_bitmap->insert_default();
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto slice = reinterpret_cast<const Slice*>(column_block(cid).cell_ptr(row_idx));

                BitmapValue* pvalue = &column_bitmap->get_element(column_bitmap->size() - 1);

                if (slice->size != 0) {
                    BitmapValue value;
                    value.deserialize(slice->data);
                    *pvalue = std::move(value);
                } else {
                    *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice->data));
                }
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_HLL: {
        auto column_hll = assert_cast<vectorized::ColumnHLL*>(column);
        for (uint16_t j = 0; j < _selected_size; ++j) {
            column_hll->insert_default();
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto slice = reinterpret_cast<const Slice*>(column_block(cid).cell_ptr(row_idx));

                HyperLogLog* pvalue = &column_hll->get_element(column_hll->size() - 1);

                if (slice->size != 0) {
                    HyperLogLog value;
                    value.deserialize(*slice);
                    *pvalue = std::move(value);
                } else {
                    *pvalue = std::move(*reinterpret_cast<HyperLogLog*>(slice->data));
                }
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_VARCHAR: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);

        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto slice = reinterpret_cast<const Slice*>(column_block(cid).cell_ptr(row_idx));
                column_string->insert_data(slice->data, slice->size);
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_STRING: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);
        size_t limit = config::string_type_length_soft_limit_bytes;
        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto slice = reinterpret_cast<const Slice*>(column_block(cid).cell_ptr(row_idx));
                if (LIKELY(slice->size <= limit)) {
                    column_string->insert_data(slice->data, slice->size);
                } else {
                    return Status::NotSupported(fmt::format(
                            "Not support string len over than {} in vec engine.", limit));
                }
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_CHAR: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);

        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto slice = reinterpret_cast<const Slice*>(column_block(cid).cell_ptr(row_idx));
                column_string->insert_data(slice->data, strnlen(slice->data, slice->size));
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);

        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto ptr = reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx));

                uint64_t value = 0;
                value = *(unsigned char*)(ptr + 2);
                value <<= 8;
                value |= *(unsigned char*)(ptr + 1);
                value <<= 8;
                value |= *(unsigned char*)(ptr);
                vectorized::VecDateTimeValue date =
                        vectorized::VecDateTimeValue::create_from_olap_date(value);
                (column_int)->insert_data(reinterpret_cast<char*>(&date), 0);
            } else {
                column_int->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);

        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto ptr = reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx));

                uint64_t value = *reinterpret_cast<const uint64_t*>(ptr);
                vectorized::VecDateTimeValue datetime =
                        vectorized::VecDateTimeValue::create_from_olap_datetime(value);
                (column_int)->insert_data(reinterpret_cast<char*>(&datetime), 0);
            } else {
                column_int->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        auto column_decimal =
                assert_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(column);

        for (uint16_t j = 0; j < _selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint16_t row_idx = _selection_vector[j];
                auto ptr = reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx));

                int64_t int_value = *(int64_t*)(ptr);
                int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
                DecimalV2Value data(int_value, frac_value);
                column_decimal->insert_data(reinterpret_cast<char*>(&data), 0);
            } else {
                column_decimal->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_ARRAY: {
        auto column_array = assert_cast<vectorized::ColumnArray*>(column);
        auto nested_col = (*column_array->get_data_ptr()).assume_mutable();
        auto src_col = static_cast<ArrayColumnVectorBatch*>(_column_vector_batches[cid].get());

        auto& offsets_col = column_array->get_offsets();
        offsets_col.reserve(_selected_size);
        uint32_t offset = offsets_col.back();
        for (uint16_t j = 0; j < _selected_size; ++j) {
            uint16_t row_idx = _selection_vector[j];
            auto cv = reinterpret_cast<const CollectionValue*>(column_block(cid).cell_ptr(row_idx));
            if (!nullable_mark_array[j]) {
                offset += cv->length();
                _append_data_to_column(src_col->elements(), src_col->item_offset(row_idx),
                                       cv->length(), nested_col);
                offsets_col.push_back(offset);
            } else {
                column_array->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int32>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_BOOL: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int8>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int16>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int128>*>(column);
        insert_data_directly(cid, column_int);
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        auto column_float = assert_cast<vectorized::ColumnVector<vectorized::Float32>*>(column);
        insert_data_directly(cid, column_float);
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        auto column_float = assert_cast<vectorized::ColumnVector<vectorized::Float64>*>(column);
        insert_data_directly(cid, column_float);
        break;
    }
    default: {
        DCHECK(false) << "Invalid type in RowBlockV2:" << _schema.column(cid)->type();
    }
    }

    return Status::OK();
}

Status RowBlockV2::_append_data_to_column(const ColumnVectorBatch* batch, size_t start,
                                          uint32_t len,
                                          doris::vectorized::MutableColumnPtr& origin_column) {
    auto* column = origin_column.get();
    uint32_t selected_size = len;
    bool nullable_mark_array[selected_size];

    bool column_nullable = origin_column->is_nullable();
    bool origin_nullable = batch->is_nullable();
    if (column_nullable) {
        auto nullable_column = assert_cast<vectorized::ColumnNullable*>(origin_column.get());
        auto& null_map = nullable_column->get_null_map_data();
        column = nullable_column->get_nested_column_ptr().get();

        if (origin_nullable) {
            for (uint32_t i = 0; i < selected_size; ++i) {
                uint32_t row_idx = i + start;
                null_map.push_back(batch->is_null_at(row_idx));
                nullable_mark_array[i] = null_map.back();
            }
        } else {
            null_map.resize_fill(null_map.size() + selected_size, 0);
            memset(nullable_mark_array, false, selected_size * sizeof(bool));
        }
    } else {
        memset(nullable_mark_array, false, selected_size * sizeof(bool));
    }

    auto insert_data_directly = [&nullable_mark_array](auto& batch, auto& column, auto& start,
                                                       auto& length) {
        for (uint32_t j = 0; j < length; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                column->insert_data(reinterpret_cast<const char*>(batch->cell_ptr(row_idx)), 0);
            } else {
                column->insert_default();
            }
        }
    };

    switch (batch->type_info()->type()) {
    case OLAP_FIELD_TYPE_OBJECT: {
        auto column_bitmap = assert_cast<vectorized::ColumnBitmap*>(column);
        for (uint32_t j = 0; j < selected_size; ++j) {
            column_bitmap->insert_default();
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto slice = reinterpret_cast<const Slice*>(batch->cell_ptr(row_idx));

                BitmapValue* pvalue = &column_bitmap->get_element(column_bitmap->size() - 1);

                if (slice->size != 0) {
                    BitmapValue value;
                    value.deserialize(slice->data);
                    *pvalue = std::move(value);
                } else {
                    *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice->data));
                }
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_HLL: {
        auto column_hll = assert_cast<vectorized::ColumnHLL*>(column);
        for (uint32_t j = 0; j < selected_size; ++j) {
            column_hll->insert_default();
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto slice = reinterpret_cast<const Slice*>(batch->cell_ptr(row_idx));

                HyperLogLog* pvalue = &column_hll->get_element(column_hll->size() - 1);

                if (slice->size != 0) {
                    HyperLogLog value;
                    value.deserialize(*slice);
                    *pvalue = std::move(value);
                } else {
                    *pvalue = std::move(*reinterpret_cast<HyperLogLog*>(slice->data));
                }
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_VARCHAR: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto slice = reinterpret_cast<const Slice*>(batch->cell_ptr(row_idx));
                column_string->insert_data(slice->data, slice->size);
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_STRING: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto slice = reinterpret_cast<const Slice*>(batch->cell_ptr(row_idx));
                if (LIKELY(slice->size <= config::string_type_length_soft_limit_bytes)) {
                    column_string->insert_data(slice->data, slice->size);
                } else {
                    return Status::NotSupported(
                            "Not support string len over than "
                            "`string_type_length_soft_limit_bytes` in vec engine.");
                }
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_CHAR: {
        auto column_string = assert_cast<vectorized::ColumnString*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto slice = reinterpret_cast<const Slice*>(batch->cell_ptr(row_idx));
                column_string->insert_data(slice->data, strnlen(slice->data, slice->size));
            } else {
                column_string->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DATE: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto ptr = reinterpret_cast<const char*>(batch->cell_ptr(row_idx));

                uint64_t value = 0;
                value = *(unsigned char*)(ptr + 2);
                value <<= 8;
                value |= *(unsigned char*)(ptr + 1);
                value <<= 8;
                value |= *(unsigned char*)(ptr);
                vectorized::VecDateTimeValue date =
                        vectorized::VecDateTimeValue::create_from_olap_date(value);
                (column_int)->insert_data(reinterpret_cast<char*>(&date), 0);
            } else {
                column_int->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto ptr = reinterpret_cast<const char*>(batch->cell_ptr(row_idx));

                uint64_t value = *reinterpret_cast<const uint64_t*>(ptr);
                vectorized::VecDateTimeValue datetime =
                        vectorized::VecDateTimeValue::create_from_olap_datetime(value);
                (column_int)->insert_data(reinterpret_cast<char*>(&datetime), 0);
            } else {
                column_int->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_DECIMAL: {
        auto column_decimal =
                assert_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(column);

        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto ptr = reinterpret_cast<const char*>(batch->cell_ptr(row_idx));

                int64_t int_value = *(int64_t*)(ptr);
                int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
                DecimalV2Value data(int_value, frac_value);
                column_decimal->insert_data(reinterpret_cast<char*>(&data), 0);
            } else {
                column_decimal->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_ARRAY: {
        auto array_batch = reinterpret_cast<const ArrayColumnVectorBatch*>(batch);
        auto column_array = assert_cast<vectorized::ColumnArray*>(column);
        auto nested_col = (*column_array->get_data_ptr()).assume_mutable();

        auto& offsets_col = column_array->get_offsets();
        uint32_t offset = offsets_col.back();
        for (uint32_t j = 0; j < selected_size; ++j) {
            if (!nullable_mark_array[j]) {
                uint32_t row_idx = j + start;
                auto cv = reinterpret_cast<const CollectionValue*>(batch->cell_ptr(row_idx));
                offset += cv->length();
                _append_data_to_column(array_batch->elements(), array_batch->item_offset(row_idx),
                                       cv->length(), nested_col);
                offsets_col.push_back(offset);
            } else {
                column_array->insert_default();
            }
        }
        break;
    }
    case OLAP_FIELD_TYPE_INT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int32>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_BOOL: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::UInt8>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_TINYINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int8>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_SMALLINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int16>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_BIGINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int64>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_LARGEINT: {
        auto column_int = assert_cast<vectorized::ColumnVector<vectorized::Int128>*>(column);
        insert_data_directly(batch, column_int, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_FLOAT: {
        auto column_float = assert_cast<vectorized::ColumnVector<vectorized::Float32>*>(column);
        insert_data_directly(batch, column_float, start, len);
        break;
    }
    case OLAP_FIELD_TYPE_DOUBLE: {
        auto column_float = assert_cast<vectorized::ColumnVector<vectorized::Float64>*>(column);
        insert_data_directly(batch, column_float, start, len);
        break;
    }
    default: {
        DCHECK(false) << "Invalid type in RowBlockV2:" << batch->type_info()->type();
    }
    }

    return Status::OK();
}

Status RowBlockV2::convert_to_vec_block(vectorized::Block* block) {
    DCHECK_LE(block->columns(), _schema.column_ids().size());
    for (int i = 0; i < block->columns(); ++i) {
        auto cid = _schema.column_ids()[i];
        auto column = (*std::move(block->get_by_position(i).column)).assume_mutable();
        RETURN_IF_ERROR(_copy_data_to_column(cid, column));
    }
    _pool->clear();
    return Status::OK();
}

std::string RowBlockRow::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < _block->schema()->num_column_ids(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ColumnId cid = _block->schema()->column_ids()[i];
        auto col_schema = _block->schema()->column(cid);
        if (col_schema->is_nullable() && is_null(cid)) {
            ss << "NULL";
        } else {
            ss << col_schema->type_info()->to_string(cell_ptr(cid));
        }
    }
    ss << "]";
    return ss.str();
}
std::string RowBlockV2::debug_string() {
    std::stringstream ss;
    for (int i = 0; i < num_rows(); ++i) {
        ss << row(i).debug_string();
        if (i != num_rows() - 1) {
            ss << "\n";
        }
    }
    return ss.str();
}
} // namespace doris
