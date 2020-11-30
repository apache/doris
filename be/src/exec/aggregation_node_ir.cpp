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

#include "exec/aggregation_node.h"
#include "exec/hash_table.hpp"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"

namespace doris {

void AggregationNode::process_row_batch_no_grouping(RowBatch* batch, MemPool* pool) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        update_tuple(_singleton_output_tuple, batch->get_row(i));
    }
}

void AggregationNode::process_row_batch_with_grouping(RowBatch* batch, MemPool* pool) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->get_row(i);
        Tuple* agg_tuple = NULL;
        HashTable::Iterator it = _hash_tbl->find(row);

        if (it.at_end()) {
            agg_tuple = construct_intermediate_tuple();
            _hash_tbl->insert(reinterpret_cast<TupleRow*>(&agg_tuple));
        } else {
            agg_tuple = it.get_row()->get_tuple(0);
        }

        update_tuple(agg_tuple, row);
    }
}

} // namespace doris
