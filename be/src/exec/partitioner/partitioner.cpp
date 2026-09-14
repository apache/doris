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

#include "exec/partitioner/partitioner.h"

#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "exec/exchange/vdata_stream_sender.h"
#include "runtime/thread_context.h"

namespace doris {
#include "common/compile_check_begin.h"

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::do_partitioning(RuntimeState* state, Block* block) const {
    size_t rows = block->rows();

    if (rows > 0) {
        auto column_to_keep = block->columns();

        int result_size = cast_set<int>(_partition_expr_ctxs.size());
        std::vector<int> result(result_size);

        _initialize_hash_vals(rows);
        auto* __restrict hashes = _hash_vals.data();
        RETURN_IF_ERROR(_get_partition_column_result(block, result));
        for (int j = 0; j < result_size; ++j) {
            const auto& [col, is_const] = unpack_if_const(block->get_by_position(result[j]).column);
            if (is_const) {
                continue;
            }
            _do_hash(col, hashes, j);
        }

        for (size_t i = 0; i < rows; i++) {
            hashes[i] = ChannelIds()(hashes[i], _partition_count);
        }

        Block::erase_useless_column(block, column_to_keep);
    }
    return Status::OK();
}

template <typename ChannelIds>
void Crc32HashPartitioner<ChannelIds>::_do_hash(const ColumnPtr& column,
                                                HashValType* __restrict result, int idx) const {
    column->update_crcs_with_value(
            result, _partition_expr_ctxs[idx]->root()->data_type()->get_primitive_type(),
            cast_set<HashValType>(column->size()));
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32HashPartitioner<ChannelIds>(_partition_count);
    partitioner.reset(new_partitioner);
    return _clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs);
}

void Crc32CHashPartitioner::_do_hash(const ColumnPtr& column, HashValType* __restrict result,
                                     int idx) const {
    column->update_crc32c_batch(result, nullptr);
}

Status Crc32CHashPartitioner::clone(RuntimeState* state,
                                    std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32CHashPartitioner(_partition_count);
    partitioner.reset(new_partitioner);
    return _clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs);
}

HashPartitionFunction::HashPartitionFunction(HashValType partition_count,
                                             ShuffleHashMethod hash_method)
        : _partition_count(partition_count), _hash_method(hash_method) {}

Status HashPartitionFunction::init(const std::vector<TExpr>& texprs) {
    if (_hash_method == ShuffleHashMethod::CRC32C) {
        _partitioner = std::make_unique<Crc32CHashPartitioner>(_partition_count);
    } else {
        _partitioner = std::make_unique<Crc32HashPartitioner<ShuffleChannelIds>>(_partition_count);
    }
    return _partitioner->init(texprs);
}

Status HashPartitionFunction::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    return _partitioner->prepare(state, row_desc);
}

Status HashPartitionFunction::open(RuntimeState* state) {
    return _partitioner->open(state);
}

Status HashPartitionFunction::close(RuntimeState* state) {
    return _partitioner->close(state);
}

Status HashPartitionFunction::get_partitions(RuntimeState* state, Block* block,
                                             size_t partition_count,
                                             std::vector<HashValType>& partitions) const {
    if (partition_count != _partition_count) {
        return Status::InvalidArgument("Hash partition count {} does not match planned count {}",
                                       partition_count, _partition_count);
    }
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, block));
    partitions = _partitioner->get_channel_ids();
    return Status::OK();
}

Status HashPartitionFunction::clone(RuntimeState* state,
                                    std::unique_ptr<PartitionFunction>& function) const {
    auto cloned = std::make_unique<HashPartitionFunction>(_partition_count, _hash_method);
    RETURN_IF_ERROR(_partitioner->clone(state, cloned->_partitioner));
    function = std::move(cloned);
    return Status::OK();
}

template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;
template class Crc32HashPartitioner<SpillRePartitionChannelIds>;

} // namespace doris
