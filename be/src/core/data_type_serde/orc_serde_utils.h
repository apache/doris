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

#include <cstring>
#include <orc/Vector.hh>

#include "core/arena.h"

namespace doris {

inline void copy_orc_string_data_to_arena(orc::ColumnVectorBatch* batch, Arena& arena) {
    if (auto* strings = dynamic_cast<orc::StringVectorBatch*>(batch)) {
        size_t total_size = 0;
        for (size_t i = 0; i < strings->numElements; ++i) {
            const size_t length = static_cast<size_t>(strings->length[i]);
            // Some serdes already allocate their payload in this Arena; copying it again would
            // double the serialized string memory without extending its lifetime.
            if (length > 0 && !arena.contains(strings->data[i], length)) {
                total_size += length;
            }
        }
        char* cursor = total_size == 0 ? nullptr : arena.alloc(total_size);
        for (size_t i = 0; i < strings->numElements; ++i) {
            const size_t length = static_cast<size_t>(strings->length[i]);
            const char* source = strings->data[i];
            if (length > 0 && !arena.contains(source, length)) {
                std::memcpy(cursor, source, length);
                strings->data[i] = cursor;
                cursor += length;
            } else if (length == 0) {
                static char empty_string_sentinel = '\0';
                // ORC treats a null data pointer as absent even when length is zero, so retain a
                // stable non-null address to keep empty strings in min/max statistics.
                strings->data[i] = &empty_string_sentinel;
            }
        }
        return;
    }
    if (auto* structure = dynamic_cast<orc::StructVectorBatch*>(batch)) {
        for (orc::ColumnVectorBatch* field : structure->fields) {
            copy_orc_string_data_to_arena(field, arena);
        }
        return;
    }
    if (auto* list = dynamic_cast<orc::ListVectorBatch*>(batch)) {
        copy_orc_string_data_to_arena(list->elements.get(), arena);
        return;
    }
    if (auto* map = dynamic_cast<orc::MapVectorBatch*>(batch)) {
        copy_orc_string_data_to_arena(map->keys.get(), arena);
        copy_orc_string_data_to_arena(map->elements.get(), arena);
        return;
    }
    if (auto* union_batch = dynamic_cast<orc::UnionVectorBatch*>(batch)) {
        for (orc::ColumnVectorBatch* child : union_batch->children) {
            copy_orc_string_data_to_arena(child, arena);
        }
    }
}

} // namespace doris
