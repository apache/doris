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

#include "olap/schema.h"

#include "olap/row_block2.h"

namespace doris {

int Schema::compare(const RowBlockRow& lhs, const RowBlockRow& rhs) const {
    for (int i = 0; i < _num_key_columns; ++i) {
        auto& col = _cols[i];
        if (col.is_nullable()) {
            bool l_null = lhs.is_null(i);
            bool r_null = rhs.is_null(i);
            if (l_null != r_null) {
                return l_null ? -1 : 1;
            } else if (l_null) {
                continue;
            }
        }
        auto cmp = col.compare(lhs.cell_ptr(i), rhs.cell_ptr(i));
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;
}

void Schema::reset(const std::vector<ColumnSchema>& cols, size_t num_key_columns) {
    _cols = cols;
    _num_key_columns = num_key_columns;

    int offset = 0;
    _col_offsets.resize(_cols.size());
    for (int i = 0; i < _cols.size(); ++i) {
        _col_offsets[i] = offset;
        // 1 for null byte
        offset += _cols[i].size() + 1;

        if (_cols[i].type() == OLAP_FIELD_TYPE_HLL) {
            _hll_col_ids.push_back(i);
        }
    }
}

}
