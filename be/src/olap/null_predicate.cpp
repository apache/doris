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

#include "olap/field.h"
#include "olap/null_predicate.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"

namespace doris {

NullPredicate::NullPredicate(int32_t column_id, bool is_null)
    : _column_id(column_id), _is_null(is_null) {}

NullPredicate::~NullPredicate() {}

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
        batch->set_size(n);
        batch->set_selected_in_use(false);
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

} //namespace doris
