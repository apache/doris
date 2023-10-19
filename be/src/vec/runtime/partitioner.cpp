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

#include "partitioner.h"

#include "runtime/thread_context.h"
#include "vec/columns/column_const.h"

namespace doris::vectorized {

template <typename HashValueType>
Status Partitioner<HashValueType>::do_partitioning(RuntimeState* state, Block* block,
                                                   MemTracker* mem_tracker) const {
    int rows = block->rows();

    if (rows > 0) {
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        std::vector<int> result(result_size);

        _hash_vals.resize(rows);
        auto* __restrict hashes = _hash_vals.data();
        {
            SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
            RETURN_IF_ERROR(_get_partition_column_result(block, result));
        }
        for (int j = 0; j < result_size; ++j) {
            _do_hash(unpack_if_const(block->get_by_position(result[j]).column).first, hashes, j);
        }

        for (int i = 0; i < rows; i++) {
            hashes[i] = hashes[i] % _partition_count;
        }

        {
            SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
            Block::erase_useless_column(block, column_to_keep);
        }
    }
    return Status::OK();
}

void BucketHashPartitioner::_do_hash(const ColumnPtr& column, uint32_t* __restrict result, int idx) const {
    column->update_crcs_with_value(result, _partition_expr_ctxs[idx]->root()->type().type,
                                   column->size());
}

void HashPartitioner::_do_hash(const ColumnPtr& column, uint64_t* __restrict result, int /*idx*/) const {
    column->update_hashes_with_value(result);
}

} // namespace doris::vectorized
