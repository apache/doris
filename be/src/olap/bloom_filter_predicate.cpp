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

#include "olap/bloom_filter_predicate.h"

#include "olap/field.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"

namespace doris {

BloomFilterColumnPredicate::BloomFilterColumnPredicate(
        uint32_t column_id, const std::shared_ptr<BloomFilterFuncBase>& filter)
        : ColumnPredicate(column_id), _filter(filter) {}

// blomm filter column predicate do not support in segment v1
void BloomFilterColumnPredicate::evaluate(VectorizedRowBatch* batch) const {
    uint16_t n = batch->size();
    uint16_t* sel = batch->selected();
    if (!batch->selected_in_use()) {
        for (uint16_t i = 0; i != n; ++i) {
            sel[i] = i;
        }
    }
}

void BloomFilterColumnPredicate::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
    uint16_t new_size = 0;
    if (block->is_nullable()) {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size += (!block->cell(idx).is_null() && _filter->find_olap_engine(cell_value));
        }
    } else {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size += _filter->find_olap_engine(cell_value);
        }
    }
    *size = new_size;
}

Status BloomFilterColumnPredicate::evaluate(const Schema& schema,
                                            const std::vector<BitmapIndexIterator*>& iterators,
                                            uint32_t num_rows, Roaring* result) const {
    return Status::OK();
}

} //namespace doris
