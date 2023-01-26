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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/tuple.cpp
// and modified by Doris

#include "runtime/tuple.h"

#include <functional>
#include <iostream>
#include <vector>

#include "common/utils.h"
#include "runtime/collection_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "util/mem_util.hpp"
#include "vec/common/string_ref.h"

namespace doris {

int64_t Tuple::total_byte_size(const TupleDescriptor& desc) const {
    int64_t result = desc.byte_size();
    if (!desc.has_varlen_slots()) {
        return result;
    }
    result += varlen_byte_size(desc);
    return result;
}

int64_t Tuple::varlen_byte_size(const TupleDescriptor& desc) const {
    int64_t result = 0;
    std::vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
    for (; slot != desc.string_slots().end(); ++slot) {
        DCHECK((*slot)->type().is_string_type());
        if (is_null((*slot)->null_indicator_offset())) {
            continue;
        }
        const StringRef* string_val = get_string_slot((*slot)->tuple_offset());
        result += string_val->size;
    }

    return result;
}

Tuple* Tuple::dcopy_with_new(const TupleDescriptor& desc, MemPool* pool, int64_t* bytes) {
    Tuple* result = (Tuple*)(pool->allocate(desc.byte_size()));
    *bytes = dcopy_with_new(result, desc);
    return result;
}

int64_t Tuple::dcopy_with_new(Tuple* dst, const TupleDescriptor& desc) {
    memory_copy(dst, this, desc.byte_size());

    int64_t bytes = 0;
    // allocate in the same pool and then copy all non-null string slots
    for (auto slot : desc.string_slots()) {
        DCHECK(slot->type().is_string_type());

        if (!dst->is_null(slot->null_indicator_offset())) {
            StringRef* string_v = dst->get_string_slot(slot->tuple_offset());
            bytes += string_v->size;
            if (string_v->size != 0) {
                char* string_copy = new char[string_v->size];
                memory_copy(string_copy, string_v->data, string_v->size);
                string_v->data = string_copy;
            } else {
                string_v->data = nullptr;
            }
        }
    }
    return bytes;
}

int64_t Tuple::release_string(const TupleDescriptor& desc) {
    int64_t bytes = 0;
    for (auto slot : desc.string_slots()) {
        if (!is_null(slot->null_indicator_offset())) {
            StringRef* string_v = get_string_slot(slot->tuple_offset());
            delete[] string_v->data;
            string_v->data = nullptr;
            bytes += string_v->size;
        }
    }
    return bytes;
}

} // namespace doris
