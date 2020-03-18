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

// struct that contains column data(null bitmap), data array in sub class.
struct ColumnVectorBatch {
public:
    explicit ColumnVectorBatch(const TypeInfo* type_info, bool is_nullable)
    : _type_info(type_info), _capacity(0), _nullable(is_nullable) {}

    virtual ~ColumnVectorBatch();

    const TypeInfo* type_info() const { return _type_info; }

    size_t get_capacity() { return _capacity; }

    inline bool is_nullable() {return _nullable; }

    inline bool is_null_at(size_t row_idx) const {
        return _nullable && _null_signs[row_idx];
    }

    inline void set_is_null(size_t idx, bool is_null) const {
        if (_nullable) {
            _null_signs[idx] = is_null;
        }
    }

    inline void set_null_bits(size_t offset, size_t num_rows, bool val) const {
        for (size_t i = 0; i < num_rows; ++i) {
            set_is_null(offset + i, val);
        }
    }

    inline bool* get_null_signs(size_t idx) { return _null_signs + idx; }

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
    virtual uint8_t* mutable_cell_ptr(size_t idx) const = 0;


    static Status create(size_t init_capacity,
                         bool is_nullable,
                         const TypeInfo* type_info,
                         std::unique_ptr<ColumnVectorBatch>* column_vector_batch);

protected:
    static Status resize_buff(size_t old_cap, size_t new_cap, uint8_t** _buf);

private:
    const TypeInfo* _type_info;
    size_t _capacity;
    const bool _nullable;
    bool* _null_signs = nullptr;
};

struct ScalarColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ScalarColumnVectorBatch(const TypeInfo* type_info, bool is_nullable, size_t init_capacity);

    ~ScalarColumnVectorBatch() override ;

    Status resize(size_t new_cap) override;

    // Get the start of the data.
    uint8_t* data() const override;

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override;

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) const override;

private:
    uint8_t* _data = nullptr;
};

struct ListColumnVectorBatch : public ColumnVectorBatch {
public:
    explicit ListColumnVectorBatch(const TypeInfo* type_info, bool is_nullable, size_t init_capacity);
    ~ListColumnVectorBatch() override;
    Status resize(size_t new_cap) override;

    ColumnVectorBatch* get_elements() const { return _elements.get(); }

    // Get the start of the data.
    uint8_t* data() const override;

    // Get the idx's cell_ptr
    const uint8_t* cell_ptr(size_t idx) const override;

    // Get thr idx's cell_ptr for write
    uint8_t* mutable_cell_ptr(size_t idx) const override;

    size_t item_offset(size_t idx) const;

    // From `start_idx`, put `size` ordinals to _item_offsets
    // Ex:
    // original _item_offsets: 0 3 5 9; ordinals to be added: 100 105 111; size: 3; satart_idx: 3
    // --> _item_offsets: 0 3 5 (9 + 100 - 100) (9 + 105 - 100) (9 + 111 - 100)
    // _item_offsets becomes 0 3 5 9 14 20
    void put_item_ordinal(segment_v2::ordinal_t* ordinals, size_t start_idx, size_t size);

    // Generate collection slots.
    void transform_offsets_and_elements_to_data(size_t start_idx, size_t end_idx);

private:
    collection* _data = nullptr;

    std::unique_ptr<ColumnVectorBatch> _elements;

    // Stores each collection's start offsets in _elements.
    size_t* _item_offsets = nullptr;
};

} // namespace doris end
