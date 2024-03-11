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
#include <cstdint>
#include <memory>

#include "vec/exec/vaggregation_node.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class ObjectPool;
class RuntimeState;

namespace vectorized {

// select c_name from customer union select c_name from customer
// this sql used agg node to get distinct row of c_name,
// so it's could output data when it's inserted into hashmap.
// phase1: (_is_merge:false, _needs_finalize:false, Streaming Preaggregation:true, agg size:0, limit:-1)
// phase2: (_is_merge:false, _needs_finalize:true,  Streaming Preaggregation:false,agg size:0, limit:-1)
class DistinctAggregationNode final : public AggregationNode {
public:
    DistinctAggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~DistinctAggregationNode() override = default;
    Status _distinct_pre_agg_with_serialized_key(Block* in_block, Block* out_block);
    void add_num_rows_returned(int64_t rows) { _num_rows_returned += rows; }
    vectorized::VExprContextSPtrs get_conjuncts() { return _conjuncts; }

private:
    void _emplace_into_hash_table_to_distinct(IColumn::Selector& distinct_row,
                                              ColumnRawPtrs& key_columns, const size_t num_rows,
                                              bool* stop_emplace_flag);

    char* dummy_mapped_data = nullptr;
    IColumn::Selector _distinct_row;
};
} // namespace vectorized
} // namespace doris
