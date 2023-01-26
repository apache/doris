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

#include "runtime/collection_value.h"

#include <functional>

#include "common/object_pool.h"
#include "common/utils.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/types.h"
#include "util/mem_util.hpp"
#include "vec/common/string_ref.h"

namespace doris {

void CollectionValue::to_collection_val(CollectionVal* val) const {
    val->length = _length;
    val->data = _data;
    val->null_signs = _null_signs;
    val->has_null = _has_null;
}

void CollectionValue::shallow_copy(const CollectionValue* value) {
    _length = value->_length;
    _null_signs = value->_null_signs;
    _data = value->_data;
    _has_null = value->_has_null;
}

void CollectionValue::copy_null_signs(const CollectionValue* other) {
    if (other->_has_null) {
        memcpy(_null_signs, other->_null_signs, other->size());
    } else {
        _null_signs = nullptr;
    }
}

size_t CollectionValue::get_byte_size(const TypeDescriptor& item_type) const {
    size_t result = 0;
    if (_length == 0) {
        return result;
    }
    if (_has_null) {
        result += _length * sizeof(bool);
    }
    auto iterator = CollectionValue::iterator(item_type.type);
    result += _length * iterator.type_size();

    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        result += iterator.get_byte_size(item_type);
        iterator.next();
    }
    return result;
}

CollectionValue CollectionValue::from_collection_val(const CollectionVal& val) {
    return CollectionValue(val.data, val.length, val.has_null, val.null_signs);
}

// Deep copy collection.
// NOTICE: The CollectionValue* shallow_copied_cv must be initialized by calling memcpy function first (
// copy data from origin collection value).
void CollectionValue::deep_copy_collection(CollectionValue* shallow_copied_cv,
                                           const TypeDescriptor& item_type,
                                           const GenMemFootprintFunc& gen_mem_footprint,
                                           bool convert_ptrs) {
    CollectionValue* cv = shallow_copied_cv;
    if (cv->length() == 0) {
        return;
    }

    auto iterator = cv->iterator(item_type.type);
    uint64_t coll_byte_size = cv->length() * iterator.type_size();
    uint64_t nulls_size = cv->has_null() ? cv->length() * sizeof(bool) : 0;

    MemFootprint footprint = gen_mem_footprint(coll_byte_size + nulls_size);
    int64_t offset = footprint.first;
    char* coll_data = reinterpret_cast<char*>(footprint.second);

    // copy and assign null_signs
    if (cv->has_null()) {
        memory_copy(convert_to<bool*>(coll_data), cv->null_signs(), nulls_size);
        cv->set_null_signs(convert_to<bool*>(coll_data));
    } else {
        cv->set_null_signs(nullptr);
    }
    // copy and assign data
    memory_copy(coll_data + nulls_size, cv->data(), coll_byte_size);
    cv->set_data(coll_data + nulls_size);

    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        iterator.self_deep_copy(item_type, gen_mem_footprint, convert_ptrs);
        iterator.next();
    }

    if (convert_ptrs) {
        cv->set_data(convert_to<char*>(offset + nulls_size));
        if (cv->has_null()) {
            cv->set_null_signs(convert_to<bool*>(offset));
        }
    }
}

void CollectionValue::deserialize_collection(CollectionValue* cv, const char* tuple_data,
                                             const TypeDescriptor& item_type) {
    if (cv->length() == 0) {
        new (cv) CollectionValue(cv->length());
        return;
    }
    // assign data and null_sign pointer position in tuple_data
    int64_t data_offset = convert_to<int64_t>(cv->data());
    cv->set_data(convert_to<char*>(tuple_data + data_offset));
    if (cv->has_null()) {
        int64_t null_offset = convert_to<int64_t>(cv->null_signs());
        cv->set_null_signs(convert_to<bool*>(tuple_data + null_offset));
    }
    auto iterator = cv->iterator(item_type.type);
    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        iterator.deserialize(tuple_data, item_type);
        iterator.next();
    }
}
} // namespace doris
