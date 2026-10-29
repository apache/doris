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

#include <gen_cpp/PlanNodes_types.h>

#include "core/column/column.h"
#include "util/simd/bits.h"

namespace doris {

template <TJoinOp::type JoinOpType, typename Method>
void build_join_hash_table(Method& method, const ColumnRawPtrs& columns, uint32_t rows,
                           int batch_size, const uint8_t* null_map, bool has_null_key,
                           bool keep_null_key) {
    method.hash_table->template prepare_build<JoinOpType>(rows, batch_size, has_null_key,
                                                          method.direct_mapping_range());
    if (columns.size() == 1 && null_map && has_null_key) {
        const_cast<IColumn*>(columns[0])->replace_column_null_data(null_map);
    }
    method.init_serialized_keys(columns, rows, null_map, true, true,
                                method.hash_table->get_bucket_size());
    method.hash_table->build(method.keys, method.bucket_nums.data(), rows, keep_null_key);
    method.bucket_nums.resize(batch_size);
    method.bucket_nums.shrink_to_fit();
}

template <typename Method>
void prepare_join_probe(Method& method, const ColumnRawPtrs& columns, uint32_t rows,
                        const uint8_t* null_map) {
    method.arena.clear();
    if (columns.size() == 1 && null_map && simd::contain_one(null_map, rows)) {
        const_cast<IColumn*>(columns[0])->replace_column_null_data(null_map);
    }
    method.init_serialized_keys(columns, rows, null_map, true, false,
                                method.hash_table->get_bucket_size());
    method.hash_table->pre_build_idxs(method.bucket_nums);
}

} // namespace doris
