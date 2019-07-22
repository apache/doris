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

int SchemaV2::compare(const RowBlockRow& lhs, const RowBlockRow& rhs) const {
    for (int i = 0; i < _num_key_columns; ++i) {
        auto& col_schema = _column_schemas[i];
        if (col_schema.is_nullable()) {
            if (lhs.is_null(i)) {
                if (rhs.is_null(i)) {
                    continue;
                } else {
                    return -1;
                }
            }
            if (rhs.is_null(i)) {
                return 1;
            }
        }
        auto cmp = col_schema.type_info()->cmp(lhs.cell_ptr(i), rhs.cell_ptr(i));
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;
}

}
