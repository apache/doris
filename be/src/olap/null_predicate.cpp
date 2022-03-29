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
#include "runtime/vectorized_row_batch.h"
#include "vec/columns/column_nullable.h"

using namespace doris::vectorized;

namespace doris {

NullPredicate::NullPredicate(uint32_t column_id, bool is_null, bool opposite)
        : ColumnPredicate(column_id), _is_null(opposite != is_null) {}

PredicateType NullPredicate::type() const {
    return _is_null ? PredicateType::IS_NULL : PredicateType::NOT_IS_NULL;
}

void NullPredicate::evaluate(VectorizedRowBatch* batch) const {
    uint16_t n = batch->size();
    if (n == 0) {
        return;
    }
    uint16_t* sel = batch->selected();
    bool* null_array = batch->column(_column_id)->is_null();
    if (batch->column(_column_id)->no_nulls() && _is_null) {
        batch->set_size(0);
        batch->set_selected_in_use(true);
        return;
    }

    if (batch->column(_column_id)->no_nulls() && !_is_null) {
        return;
    }

    uint16_t new_size = 0;
    if (batch->selected_in_use()) {
        for (uint16_t j = 0; j != n; ++j) {
            uint16_t i = sel[j];
            sel[new_size] = i;
            new_size += (null_array[i] == _is_null);
        }
        batch->set_size(new_size);
    } else {
        for (uint16_t i = 0; i != n; ++i) {
            sel[new_size] = i;
            new_size += (null_array[i] == _is_null);
        }
        if (new_size < n) {
            batch->set_size(new_size);
            batch->set_selected_in_use(true);
        }
    }
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

Status NullPredicate::evaluate(const Schema& schema,
                               const std::vector<BitmapIndexIterator*>& iterators,
                               uint32_t num_rows, roaring::Roaring* roaring) const {
    if (iterators[_column_id] != nullptr) {
        roaring::Roaring null_bitmap;
        RETURN_IF_ERROR(iterators[_column_id]->read_null_bitmap(&null_bitmap));
        if (_is_null) {
            *roaring &= null_bitmap;
        } else {
            *roaring -= null_bitmap;
        }
    }
    return Status::OK();
}

void NullPredicate::evaluate(vectorized::IColumn& column, uint16_t* sel, uint16_t* size) const {
    uint16_t new_size = 0;
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        auto& null_map = nullable->get_null_map_data();
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            new_size += (null_map[idx] == _is_null);
        }
        *size = new_size;
    } else {
        if (_is_null) *size = 0;
    }
}

void NullPredicate::evaluate_or(IColumn& column, uint16_t* sel, uint16_t size, bool* flags) const {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        auto& null_map = nullable->get_null_map_data();
        for (uint16_t i = 0; i < size; ++i) {
            if (flags[i]) continue;
            uint16_t idx = sel[i];
            flags[i] |= (null_map[idx] == _is_null);
        }
    } else {
        if (!_is_null) memset(flags, true, size);
    }
}

void NullPredicate::evaluate_and(IColumn& column, uint16_t* sel, uint16_t size, bool* flags) const {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        auto& null_map = nullable->get_null_map_data();
        for (uint16_t i = 0; i < size; ++i) {
            if (flags[i]) continue;
            uint16_t idx = sel[i];
            flags[i] &= (null_map[idx] == _is_null);
        }
    } else {
        if (_is_null) memset(flags, false, size);
    }
}
} //namespace doris
