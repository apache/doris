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

#include "column_vector.h"

namespace doris {

ColumnVectorBatch::~ColumnVectorBatch() {
    SAFE_DELETE_ARRAY(_null_signs);
}

Status ColumnVectorBatch::resize(size_t new_cap) {
    if (_nullable) {
        resize_buff(_capacity, new_cap, reinterpret_cast<uint8_t**>(&_null_signs));
    }
    _capacity = new_cap;
    return Status::OK();
}

Status ColumnVectorBatch::resize_buff(size_t copy_bytes, size_t new_cap, uint8_t** _buf_ptr) {
    const uint8_t* old_buf = *_buf_ptr;
    if (old_buf) {
        *_buf_ptr = new uint8_t[new_cap];
        memcpy(*_buf_ptr, old_buf, copy_bytes);
        delete[] old_buf;
    } else {
        *_buf_ptr = new uint8_t[new_cap];
    }
    return Status::OK();
}

Status ColumnVectorBatch::create(size_t init_capacity,
        bool is_nullable,
        const TypeInfo* type_info,
        std::unique_ptr<ColumnVectorBatch>* column_vector_batch) {
    if (is_scalar_type(type_info->type())) {
        std::unique_ptr<ColumnVectorBatch> local(
                new ScalarColumnVectorBatch(type_info, is_nullable, init_capacity));
        *column_vector_batch = std::move(local);
        return Status::OK();
    } else {
        switch (type_info->type()) {
        case FieldType::OLAP_FIELD_TYPE_LIST: {
            std::unique_ptr<ColumnVectorBatch> local(new ListColumnVectorBatch(type_info, is_nullable, init_capacity));
            *column_vector_batch = std::move(local);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnVectorBatch: " + std::to_string(type_info->type()));
        }
    }
}

ScalarColumnVectorBatch::ScalarColumnVectorBatch(const TypeInfo* type_info, bool is_nullable, size_t init_capacity)
: ColumnVectorBatch(type_info, is_nullable) {
    resize(init_capacity);
}
ScalarColumnVectorBatch::~ScalarColumnVectorBatch() {
    SAFE_DELETE_ARRAY(_data);
}

Status ScalarColumnVectorBatch::resize(size_t new_cap) {
    if (get_capacity() < new_cap) { // before first init, _capacity is 0.
        size_t type_size = type_info()->size();
        auto copy_bytes = get_capacity() * type_size;
        size_t new_data_size = new_cap * type_size;
        resize_buff(copy_bytes, new_data_size, &_data);
        return ColumnVectorBatch::resize(new_cap);
    }
    return Status::OK();
}

uint8_t* ScalarColumnVectorBatch::data() const { return _data; };

const uint8_t* ScalarColumnVectorBatch::cell_ptr(size_t idx) const { return _data + idx * type_info()->size(); };

uint8_t* ScalarColumnVectorBatch::mutable_cell_ptr(size_t idx) const { return _data + idx * type_info()->size(); };


ListColumnVectorBatch::ListColumnVectorBatch(const TypeInfo* type_info, bool is_nullable, size_t init_capacity)
: ColumnVectorBatch(type_info, is_nullable) {
    auto list_type_info = reinterpret_cast<const ListTypeInfo*>(type_info);
    ColumnVectorBatch::create(init_capacity * 2, true, list_type_info->item_type_info(), &_elements);
    resize(init_capacity);
}

ListColumnVectorBatch::~ListColumnVectorBatch() {
    SAFE_DELETE_ARRAY(_data);
    SAFE_DELETE_ARRAY(_item_offsets);
}

Status ListColumnVectorBatch::resize(size_t new_cap) {
    if (get_capacity() < new_cap) {
        size_t collection_type_size = sizeof(collection);
        size_t copy_bytes = get_capacity() * collection_type_size;
        size_t new_data_size = new_cap * collection_type_size;
        RETURN_IF_ERROR(resize_buff(copy_bytes, new_data_size, reinterpret_cast<uint8_t**>(&_data)));

        size_t offset_type_size = sizeof(size_t);
        size_t offset_copy_bytes = (get_capacity() + 1) * offset_type_size;
        size_t new_offset_size = (new_cap + 1) * offset_type_size;
        RETURN_IF_ERROR(resize_buff(offset_copy_bytes, new_offset_size, reinterpret_cast<uint8_t**>(&_item_offsets)));

        RETURN_IF_ERROR(ColumnVectorBatch::resize(new_cap));
    }
    return Status::OK();
}

uint8_t* ListColumnVectorBatch::data() const { return reinterpret_cast<uint8*>(_data); }

const uint8_t* ListColumnVectorBatch::cell_ptr(size_t idx) const { return reinterpret_cast<uint8*>(_data + idx); }

uint8_t* ListColumnVectorBatch::mutable_cell_ptr(size_t idx) const { return reinterpret_cast<uint8*>(_data + idx); }

size_t ListColumnVectorBatch::item_offset(size_t idx) const { return _item_offsets[idx]; }

void ListColumnVectorBatch::put_item_ordinal(segment_v2::ordinal_t* ordinals, size_t start_idx, size_t size) {
    size_t first_offset = _item_offsets[start_idx];
    segment_v2::ordinal_t first_ordinal = ordinals[0];
    size_t i = 0;
    while (++i < size) {
        _item_offsets[start_idx + i] = first_offset + (ordinals[i] - first_ordinal);
    }
}

void ListColumnVectorBatch::transform_offsets_and_elements_to_data(size_t start_idx, size_t end_idx) {
    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        if (!is_null_at(idx)) {
            _data[idx] = collection(_elements->mutable_cell_ptr(_item_offsets[idx]),
            _item_offsets[idx + 1] - _item_offsets[idx],
            _elements->get_null_signs(_item_offsets[idx]));
        }
    }
}

} // namespace doris end
