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
#include <vector>

#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/data_type_serde/data_type_serde.h"

namespace doris {
namespace orc_serde_utils {

size_t orc_decode_row_count(size_t rows, const std::vector<size_t>* selected_rows);
size_t orc_source_row_at(size_t row, const std::vector<size_t>* selected_rows);
bool orc_row_is_null(const ::orc::ColumnVectorBatch& batch, size_t row);

struct RoundedOrcTimestamp {
    int64_t seconds;
    uint64_t microseconds;
    bool carry;
};

Status round_orc_timestamp_to_microseconds(int64_t seconds, int64_t nanoseconds,
                                           RoundedOrcTimestamp* result);

DecodedColumnView make_orc_decoded_view(const OrcDecodedColumnView& orc_view,
                                        DecodedValueKind value_kind);

Status read_decoded_values(const DataTypeSerDe& serde, IColumn& column, DecodedColumnView* view);

void fill_orc_decoded_null_map(const ::orc::ColumnVectorBatch& batch, size_t rows,
                               const std::vector<size_t>* selected_rows, NullMap* null_map);

Status append_orc_offsets(ColumnArray::Offsets64& doris_offsets,
                          const ::orc::DataBuffer<int64_t>& orc_offsets, size_t rows,
                          size_t* element_size, const std::vector<size_t>* selected_rows,
                          std::vector<size_t>* element_selection);

OrcDecodedColumnView make_child_orc_view(const OrcDecodedColumnView& parent_view,
                                         const ::orc::Type* file_type,
                                         const ::orc::Type* selected_type,
                                         const ::orc::ColumnVectorBatch* batch, size_t rows,
                                         const std::vector<size_t>* selected_rows);

Status read_orc_child_column(const DataTypeSerDeSPtr& child_serde, MutableColumnPtr& child_column,
                             const OrcDecodedColumnView& child_view);

inline void copy_orc_string_data_to_arena(orc::ColumnVectorBatch* batch, Arena& arena) {
    if (auto* strings = dynamic_cast<orc::StringVectorBatch*>(batch)) {
        size_t total_size = 0;
        for (size_t i = 0; i < strings->numElements; ++i) {
            if (strings->hasNulls && !strings->notNull[i]) {
                // Nullable serdes may leave borrowed payload behind a NULL position; it has no
                // logical lifetime requirement and must not inflate the writer Arena.
                continue;
            }
            const size_t length = static_cast<size_t>(strings->length[i]);
            // Some serdes already allocate their payload in this Arena; copying it again would
            // double the serialized string memory without extending its lifetime.
            if (length > 0 && !arena.contains(strings->data[i], length)) {
                total_size += length;
            }
        }
        char* cursor = total_size == 0 ? nullptr : arena.alloc(total_size);
        for (size_t i = 0; i < strings->numElements; ++i) {
            if (strings->hasNulls && !strings->notNull[i]) {
                strings->data[i] = nullptr;
                strings->length[i] = 0;
                continue;
            }
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

} // namespace orc_serde_utils
} // namespace doris
