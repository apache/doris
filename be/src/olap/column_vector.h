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

#pragma once

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h" // for ordinal_t
#include "olap/types.h"

namespace doris {

template <class T>
class DataBuffer {
private:
    T* buf;
    // current size
    size_t current_size;
    // maximal capacity (actual allocated memory)
    size_t current_capacity;

public:
    explicit DataBuffer(size_t size = 0);
    ~DataBuffer();
    T* data() { return buf; }

    const T* data() const { return buf; }

    size_t size() { return current_size; }

    size_t capacity() { return current_capacity; }

    T& operator[](size_t i) { return buf[i]; }

    T& operator[](size_t i) const { return buf[i]; }

    void resize(size_t _size);
};

// struct that contains column data(null bitmap), data array in sub class.
class ColumnVectorBatch {
public:
    explicit ColumnVectorBatch(std::shared_ptr<const TypeInfo> type_info, bool is_nullable)
            : _type_info(type_info),
              _capacity(0),
              _delete_state(DEL_NOT_SATISFIED),
              _nullable(is_nullable),
              _null_signs(0) {}

    virtual ~ColumnVectorBatch();

    std::shared_ptr<const TypeInfo> type_info() const { return _type_info; }

    size_t capacity() const { return _capacity; }

    bool is_nullable() const { return _nullable; }

    bool is_null_at(size_t row_idx) const { return _nullable && _null_signs[row_idx]; }

    void set_is_null(size_t idx, bool is_null) {
        if (_nullable) {
            _null_signs[idx] = is_null;
        }
    }

    void set_null_bits(size_t offset, size_t num_rows, bool val) {
        if (_nullable) {
            memset(&_null_signs[offset], val, num_rows);
        }
    }

    const bool* null_signs() const { return _null_signs.data(); }

    void set_delete_state(DelCondSatisfied delete_state) { _delete_state = delete_state; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    /**
     * Change the number of slots to at least the given capacity.
     * This function is not recursive into subtypes.
     * Tips: This function will change `_capacity` attribute.
     */
    virtual Status resize(size_t new_cap);

    // Get the start of the data.
    virtual uint8_t* data() const = 0;

    // Get the idx's cell_ptr
    virtual const uint8_t* cell_ptr(size_t idx) const = 0;

    // Get thr idx's cell_ptr for write
    virtual uint8_t* mutable_cell_ptr(size_t idx) = 0;

    static Status create(size_t init_capacity, bool is_nullable, std::shared_ptr<const TypeInfo> type_info,
                         Field* field, std::unique_ptr<ColumnVectorBatch>* column_vector_batch);

private:
    std::shared_ptr<const TypeInfo> _type_info;
    size_t _capacity;
    DelCondSatisfied _delete_state;
    const bool _nullable;
    DataBuffer<bool> _null_signs;
};

template <class ScalarCppType>
class ScalarColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ScalarColumnVectorBatch(std::shared_ptr<const TypeInfo> type_info, bool is_nullable);

    ~ScalarColumnVectorBatch() override;

    Status resize(size_t new_cap) override;

    // Get the start of the data.
    uint8_t* data() const override {
        return const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(_data.data()));
    }

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override {
        return reinterpret_cast<uint8_t*>(&_data[idx]);
    }

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) override {
        return reinterpret_cast<uint8_t*>(&_data[idx]);
    }

    ScalarCppType* scalar_cell_ptr(size_t idx) { return &_data[idx]; }

private:
    DataBuffer<ScalarCppType> _data;
};

// util class for read array's null signs.
class ArrayNullColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ArrayNullColumnVectorBatch(ColumnVectorBatch* array)
            : ColumnVectorBatch(get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_TINYINT), false),
              _array(array) {}

    ~ArrayNullColumnVectorBatch() override = default;

    Status resize(size_t new_cap) override {
        return Status::NotSupported("unsupported for resize ArrayNullColumnVectorBatch");
    }

    uint8_t* data() const override {
        return const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(_array->null_signs()));
    }

    const uint8_t* cell_ptr(size_t idx) const override {
        return reinterpret_cast<const uint8_t*>(_array->null_signs() + idx);
    }

    uint8_t* mutable_cell_ptr(size_t idx) override {
        return const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(_array->null_signs() + idx));
    }

private:
    ColumnVectorBatch* _array;
};

class ArrayColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ArrayColumnVectorBatch(std::shared_ptr<const TypeInfo> type_info, bool is_nullable,
                                    ScalarColumnVectorBatch<uint32_t>* offsets,
                                    ColumnVectorBatch* elements);
    ~ArrayColumnVectorBatch() override;
    Status resize(size_t new_cap) override;

    ColumnVectorBatch* elements() const { return _elements.get(); }

    ColumnVectorBatch* offsets() const { return _offsets.get(); }

    // Get the start of the data.
    uint8_t* data() const override {
        return reinterpret_cast<uint8*>(const_cast<CollectionValue*>(_data.data()));
    }

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override {
        return reinterpret_cast<const uint8*>(&_data[idx]);
    }

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) override { return reinterpret_cast<uint8*>(&_data[idx]); }

    size_t item_offset(size_t idx) const { return *(_offsets->scalar_cell_ptr(idx)); }

    /**
     * Change array size to offset in this batch
     *
     * We should ensure that _offset[start_idx] is the sum of the lengths of the arrays from 0 to start_idx - 1
     * and that the lengths of the arrays from start_idx to start_idx + size - 1 has been written correctly
     * to _offset[start_idx + 1 ... start_idx + size] before exec this method
     *
     * Ex:
     * get_offset_by_length(2, 3)
     *
     * before exec:
     *
     * _offsets: [ 0      3      5      2      1      3 ]
     *
     * 1)
     *
     * _offsets: [ 0      3      5     (7)     1      3 ]
     *
     * 2)
     *
     * _offsets: [ 0      3      5      7     (8)     3 ]
     *
     * 3)
     *
     * _offsets: [ 0      3      5      7      8     (11) ]
     *
     * @param start_idx the starting position of the first array that we want to change
     * @param size the number of array that we want to change
     */
    void get_offset_by_length(size_t start_idx, size_t size);

    size_t get_item_size(size_t start_idx, size_t size) {
        return *(_offsets->scalar_cell_ptr(start_idx + size)) -
               *(_offsets->scalar_cell_ptr(start_idx));
    }

    ArrayNullColumnVectorBatch get_null_as_batch() { return ArrayNullColumnVectorBatch(this); }

    // Generate collection slots.
    void prepare_for_read(size_t start_idx, size_t end_idx, bool item_has_null);

private:
    DataBuffer<CollectionValue> _data;

    std::unique_ptr<ColumnVectorBatch> _elements;

    // Stores each array's start offsets in _elements.
    std::unique_ptr<ScalarColumnVectorBatch<uint32_t>> _offsets;
};

} // namespace doris
