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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
Status HashJoinNode::_extract_join_column_variants2(Block& block,ColumnUInt8::MutablePtr& null_map,
                                          ColumnRawPtrs& raw_ptrs,
                                          const std::vector<int>& res_col_ids) {
    return std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return _extract_join_column<true>(block, null_map, raw_ptrs, res_col_ids);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
                __builtin_unreachable();
            },
            _hash_table_variants);
}

}