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

#include "olap/field.h"

namespace doris {

ColumnVectorBatch::~ColumnVectorBatch() = default;

Status ColumnVectorBatch::resize(size_t new_cap) {
    if (_nullable) {
        _null_signs.resize(new_cap);
    }
    _capacity = new_cap;
    return Status::OK();
}

Status ColumnVectorBatch::create(size_t init_capacity, bool is_nullable, const TypeInfo* type_info,
                                 Field* field,
                                 std::unique_ptr<ColumnVectorBatch>* column_vector_batch) {
    if (is_scalar_type(type_info->type())) {
        std::unique_ptr<ColumnVectorBatch> local;
        switch (type_info->type()) {
        case OLAP_FIELD_TYPE_BOOL:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_BOOL>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_TINYINT:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_TINYINT>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_SMALLINT>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_INT:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_INT>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            local.reset(new ScalarColumnVectorBatch<
                        CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_INT>::CppType>(type_info,
                                                                              is_nullable));
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_BIGINT>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            local.reset(new ScalarColumnVectorBatch<
                        CppTypeTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::CppType>(type_info,
                                                                                 is_nullable));
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_LARGEINT>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_FLOAT:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_FLOAT>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_DOUBLE:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_DOUBLE>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_DECIMAL:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_DECIMAL>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_DATE:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_DATE>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_DATETIME:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_DATETIME>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_CHAR:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_CHAR>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            local.reset(
                    new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_VARCHAR>::CppType>(
                            type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_HLL:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_HLL>::CppType>(
                    type_info, is_nullable));
            break;
        case OLAP_FIELD_TYPE_OBJECT:
            local.reset(new ScalarColumnVectorBatch<CppTypeTraits<OLAP_FIELD_TYPE_OBJECT>::CppType>(
                    type_info, is_nullable));
            break;
        default:
            return Status::NotSupported("unsupported type for ColumnVectorBatch: " +
                                        std::to_string(type_info->type()));
        }
        RETURN_IF_ERROR(local->resize(init_capacity));
        *column_vector_batch = std::move(local);
        return Status::OK();
    } else {
        switch (type_info->type()) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            if (field == nullptr) {
                return Status::NotSupported(
                        "When create ArrayColumnVectorBatch, `Field` is indispensable");
            }
            std::unique_ptr<ColumnVectorBatch> local(
                    new ArrayColumnVectorBatch(type_info, is_nullable, init_capacity, field));
            RETURN_IF_ERROR(local->resize(init_capacity));
            *column_vector_batch = std::move(local);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnVectorBatch: " +
                                        std::to_string(type_info->type()));
        }
    }
}

template <class ScalarType>
ScalarColumnVectorBatch<ScalarType>::ScalarColumnVectorBatch(const TypeInfo* type_info,
                                                             bool is_nullable)
        : ColumnVectorBatch(type_info, is_nullable), _data(0) {}

template <class ScalarType>
ScalarColumnVectorBatch<ScalarType>::~ScalarColumnVectorBatch() = default;

template <class ScalarType>
Status ScalarColumnVectorBatch<ScalarType>::resize(size_t new_cap) {
    if (capacity() < new_cap) { // before first init, _capacity is 0.
        RETURN_IF_ERROR(ColumnVectorBatch::resize(new_cap));
        _data.resize(new_cap);
    }
    return Status::OK();
}

ArrayColumnVectorBatch::ArrayColumnVectorBatch(const TypeInfo* type_info, bool is_nullable,
                                               size_t init_capacity, Field* field)
        : ColumnVectorBatch(type_info, is_nullable), _data(0), _item_offsets(1) {
    auto array_type_info = reinterpret_cast<const ArrayTypeInfo*>(type_info);
    _item_offsets[0] = 0;
    ColumnVectorBatch::create(init_capacity * 2, field->get_sub_field(0)->is_nullable(),
                              array_type_info->item_type_info(), field->get_sub_field(0),
                              &_elements);
}

ArrayColumnVectorBatch::~ArrayColumnVectorBatch() = default;

Status ArrayColumnVectorBatch::resize(size_t new_cap) {
    if (capacity() < new_cap) {
        RETURN_IF_ERROR(ColumnVectorBatch::resize(new_cap));
        _data.resize(new_cap);
        _item_offsets.resize(new_cap + 1);
    }
    return Status::OK();
}

void ArrayColumnVectorBatch::put_item_ordinal(segment_v2::ordinal_t* ordinals, size_t start_idx,
                                              size_t size) {
    size_t first_offset = _item_offsets[start_idx];
    segment_v2::ordinal_t first_ordinal = ordinals[0];
    size_t i = 0;
    while (++i < size) {
        _item_offsets[start_idx + i] = first_offset + (ordinals[i] - first_ordinal);
    }
}

void ArrayColumnVectorBatch::prepare_for_read(size_t start_idx, size_t end_idx,
                                              bool item_has_null) {
    for (size_t idx = start_idx; idx < end_idx; ++idx) {
        if (!is_null_at(idx)) {
            _data[idx] = Collection(
                    _elements->mutable_cell_ptr(_item_offsets[idx]),
                    _item_offsets[idx + 1] - _item_offsets[idx], item_has_null,
                    _elements->is_nullable()
                            ? const_cast<bool*>(&_elements->null_signs()[_item_offsets[idx]])
                            : nullptr);
        }
    }
}

template <class T>
DataBuffer<T>::DataBuffer(size_t new_size) : buf(nullptr), current_size(0), current_capacity(0) {
    resize(new_size);
}

template <class T>
DataBuffer<T>::~DataBuffer() {
    for (uint64_t i = current_size; i > 0; --i) {
        (buf + i - 1)->~T();
    }
    if (buf) {
        std::free(buf);
    }
}

template <class T>
void DataBuffer<T>::resize(size_t new_size) {
    if (new_size > current_capacity || !buf) {
        if (buf) {
            T* buf_old = buf;
            buf = reinterpret_cast<T*>(std::malloc(sizeof(T) * new_size));
            memcpy(buf, buf_old, sizeof(T) * current_size);
            std::free(buf_old);
        } else {
            buf = reinterpret_cast<T*>(std::malloc(sizeof(T) * new_size));
        }
        current_capacity = new_size;
    }
    current_size = new_size;
}

} // namespace doris
