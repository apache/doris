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

#include "olap/null_predicate.h"

#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "vec/columns/column_nullable.h"

using namespace doris::vectorized;

namespace doris {

NullPredicate::NullPredicate(uint32_t column_id, bool is_null, bool opposite)
        : ColumnPredicate(column_id), _is_null(opposite != is_null) {}

PredicateType NullPredicate::type() const {
    return _is_null ? PredicateType::IS_NULL : PredicateType::IS_NOT_NULL;
}

void NullPredicate::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
    uint16_t new_size = 0;
    if (!block->is_nullable() && _is_null) {
        *size = 0;
        return;
    }
    for (uint16_t i = 0; i < *size; ++i) {
        uint16_t idx = sel[i];
        sel[new_size] = idx;
        new_size += (block->cell(idx).is_null() == _is_null);
    }
    *size = new_size;
}

void NullPredicate::evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,
                                bool* flags) const {
    if (!block->is_nullable() && _is_null) {
        memset(flags, true, size);
    } else {
        for (uint16_t i = 0; i < size; ++i) {
            if (flags[i]) continue;
            uint16_t idx = sel[i];
            flags[i] |= (block->cell(idx).is_null() == _is_null);
        }
    }
}

void NullPredicate::evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,
                                 bool* flags) const {
    if (!block->is_nullable() && _is_null) {
        return;
    } else {
        for (uint16_t i = 0; i < size; ++i) {
            if (!flags[i]) continue;
            uint16_t idx = sel[i];
            flags[i] &= (block->cell(idx).is_null() == _is_null);
        }
    }
}

Status NullPredicate::evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                               roaring::Roaring* roaring) const {
    if (iterator != nullptr) {
        roaring::Roaring null_bitmap;
        RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap));
        if (_is_null) {
            *roaring &= null_bitmap;
        } else {
            *roaring -= null_bitmap;
        }
    }
    return Status::OK();
}

uint16_t NullPredicate::evaluate(const vectorized::IColumn& column, uint16_t* sel,
                                 uint16_t size) const {
    uint16_t new_size = 0;
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (!nullable->has_null()) {
            return _is_null ? 0 : size;
        }
        auto& null_map = nullable->get_null_map_data();
        for (uint16_t i = 0; i < size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            new_size += (null_map[idx] == _is_null);
        }
        return new_size;
    } else {
        if (_is_null) return 0;
    }
    return size;
}

void NullPredicate::evaluate_or(const IColumn& column, const uint16_t* sel, uint16_t size,
                                bool* flags) const {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (!nullable->has_null()) {
            if (!_is_null) {
                memset(flags, true, size);
            }
        } else {
            auto& null_map = nullable->get_null_map_data();
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) continue;
                uint16_t idx = sel[i];
                flags[i] |= (null_map[idx] == _is_null);
            }
        }
    } else {
        if (!_is_null) memset(flags, true, size);
    }
}

void NullPredicate::evaluate_and(const IColumn& column, const uint16_t* sel, uint16_t size,
                                 bool* flags) const {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (!nullable->has_null()) {
            if (_is_null) {
                memset(flags, false, size);
            }
        } else {
            auto& null_map = nullable->get_null_map_data();
            for (uint16_t i = 0; i < size; ++i) {
                if (flags[i]) continue;
                uint16_t idx = sel[i];
                flags[i] &= (null_map[idx] == _is_null);
            }
        }
    } else {
        if (_is_null) memset(flags, false, size);
    }
}

void NullPredicate::evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                                 bool* flags) const {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (!nullable->has_null()) {
            memset(flags, !_is_null, size);
        }
        auto& null_map = nullable->get_null_map_data();
        for (uint16_t i = 0; i < size; ++i) {
            flags[i] = (null_map[i] == _is_null);
        }
    } else {
        if (_is_null) memset(flags, false, size);
    }
}

} //namespace doris
