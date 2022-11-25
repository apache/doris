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

#include "common/object_pool.h"
#include "gutil/integral_types.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/bitmap_value.h"

namespace doris {

// only used in Runtime Filter
class BitmapFilterFuncBase {
public:
    virtual void insert(const void* data) = 0;
    virtual void insert_many(const std::vector<const BitmapValue*> bitmaps) = 0;
    virtual bool find(uint64 data) const = 0;
    virtual bool is_empty() = 0;
    virtual Status assign(BitmapValue* bitmap_value) = 0;
    virtual void light_copy(BitmapFilterFuncBase* other) { _not_in = other->_not_in; };
    virtual uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap,
                                                uint16_t* offsets, int number) = 0;
    virtual void find_batch(const char* data, const uint8* nullmap, int number,
                            uint8* results) const = 0;
    void set_not_in(bool not_in) { _not_in = not_in; }
    virtual ~BitmapFilterFuncBase() = default;

protected:
    // true -> not in bitmap, false -> in bitmap
    bool _not_in {false};
};

template <PrimitiveType type>
class BitmapFilterFunc : public BitmapFilterFuncBase {
public:
    using CppType = typename PrimitiveTypeTraits<type>::CppType;

    BitmapFilterFunc() : _bitmap_value(std::make_shared<BitmapValue>()), _empty(true) {}

    ~BitmapFilterFunc() override = default;

    void insert(const void* data) override;

    void insert_many(const std::vector<const BitmapValue*> bitmaps) override;

    bool find(uint64 data) const override { return _not_in ^ _bitmap_value->contains(data); }

    uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap, uint16_t* offsets,
                                        int number) override;

    void find_batch(const char* data, const uint8* nullmap, int number,
                    uint8* results) const override;

    bool is_empty() override { return _empty; }

    Status assign(BitmapValue* bitmap_value) override {
        *_bitmap_value = *bitmap_value;
        return Status::OK();
    }

    void light_copy(BitmapFilterFuncBase* bloomfilter_func) override;

private:
    std::shared_ptr<BitmapValue> _bitmap_value;
    bool _empty;
};

template <PrimitiveType type>
void BitmapFilterFunc<type>::insert(const void* data) {
    if (data == nullptr) {
        return;
    }

    *_bitmap_value |= *reinterpret_cast<const BitmapValue*>(data);
    _empty = false;
}

template <PrimitiveType type>
void BitmapFilterFunc<type>::insert_many(const std::vector<const BitmapValue*> bitmaps) {
    if (bitmaps.empty()) {
        return;
    }
    _bitmap_value->fastunion(bitmaps);
    _empty = false;
}

template <PrimitiveType type>
uint16_t BitmapFilterFunc<type>::find_fixed_len_olap_engine(const char* data, const uint8* nullmap,
                                                            uint16_t* offsets, int number) {
    uint16_t new_size = 0;
    for (int i = 0; i < number; i++) {
        uint16_t idx = offsets[i];
        if (nullmap != nullptr && nullmap[idx]) {
            continue;
        }
        if (!find(*((CppType*)data + idx))) {
            continue;
        }
        offsets[new_size++] = idx;
    }
    return new_size;
}

template <PrimitiveType type>
void BitmapFilterFunc<type>::find_batch(const char* data, const uint8* nullmap, int number,
                                        uint8* results) const {
    for (int i = 0; i < number; i++) {
        results[i] = false;
        if (nullmap != nullptr && nullmap[i]) {
            continue;
        }
        if (!find(*((CppType*)data + i))) {
            continue;
        }
        results[i] = true;
    }
}

template <PrimitiveType type>
void BitmapFilterFunc<type>::light_copy(BitmapFilterFuncBase* bitmapfilter_func) {
    BitmapFilterFuncBase::light_copy(bitmapfilter_func);
    auto other_func = reinterpret_cast<BitmapFilterFunc*>(bitmapfilter_func);
    _empty = other_func->_empty;
    _bitmap_value = other_func->_bitmap_value;
}

} // namespace doris
