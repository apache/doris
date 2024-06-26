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

#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "runtime/thread_context.h"
#include "vec/columns/column_const.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {

template <typename HashValueType, typename ChannelIds>
Status Partitioner<HashValueType, ChannelIds>::do_partitioning(RuntimeState* state, Block* block,
                                                               MemTracker* mem_tracker) const {
    int rows = block->rows();

    if (rows > 0) {
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        std::vector<int> result(result_size);

        _hash_vals.resize(rows);
        std::fill(_hash_vals.begin(), _hash_vals.end(), 0);
        auto* __restrict hashes = _hash_vals.data();
        {
            SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
            RETURN_IF_ERROR(_get_partition_column_result(block, result));
        }
        for (int j = 0; j < result_size; ++j) {
            _do_hash(unpack_if_const(block->get_by_position(result[j]).column).first, hashes, j);
        }

        for (int i = 0; i < rows; i++) {
            hashes[i] = ChannelIds()(hashes[i], _partition_count);
        }

        {
            SCOPED_CONSUME_MEM_TRACKER(mem_tracker);
            Block::erase_useless_column(block, column_to_keep);
        }
    }
    return Status::OK();
}

template <typename ChannelIds>
void Crc32HashPartitioner<ChannelIds>::_do_hash(const ColumnPtr& column,
                                                uint32_t* __restrict result, int idx) const {
    column->update_crcs_with_value(result, Base::_partition_expr_ctxs[idx]->root()->type().type,
                                   column->size());
}

template <typename ChannelIds>
void XXHashPartitioner<ChannelIds>::_do_hash(const ColumnPtr& column, uint64_t* __restrict result,
                                             int /*idx*/) const {
    column->update_hashes_with_value(result);
}

template <typename ChannelIds>
Status XXHashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                            std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new XXHashPartitioner(Base::_partition_count);
    partitioner.reset(new_partitioner);
    new_partitioner->_partition_expr_ctxs.resize(Base::_partition_expr_ctxs.size());
    for (size_t i = 0; i < Base::_partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(Base::_partition_expr_ctxs[i]->clone(
                state, new_partitioner->_partition_expr_ctxs[i]));
    }
    return Status::OK();
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32HashPartitioner(Base::_partition_count);
    partitioner.reset(new_partitioner);
    new_partitioner->_partition_expr_ctxs.resize(Base::_partition_expr_ctxs.size());
    for (size_t i = 0; i < Base::_partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(Base::_partition_expr_ctxs[i]->clone(
                state, new_partitioner->_partition_expr_ctxs[i]));
    }
    return Status::OK();
}

template class Partitioner<size_t, pipeline::LocalExchangeChannelIds>;
template class XXHashPartitioner<pipeline::LocalExchangeChannelIds>;
template class Partitioner<size_t, ShuffleChannelIds>;
template class XXHashPartitioner<ShuffleChannelIds>;
template class Partitioner<uint32_t, ShuffleChannelIds>;
template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;

} // namespace doris::vectorized
