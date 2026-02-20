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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/spill/spill_stream.h"

namespace doris {
class RuntimeState;
class RuntimeProfile;

namespace vectorized {
class Block;
class PartitionerBase;
} // namespace vectorized

namespace pipeline {

/// SpillRepartitioner reads data from an input SpillStream and redistributes it
/// into FANOUT output SpillStreams by computing hash on key columns.
///
/// This is the core building block for multi-level spill partitioning used by both
/// Hash Join and Aggregation operators.
///
/// Two modes of operation:
/// 1. Partitioner mode (init): Uses a PartitionerBase with expression contexts to
///    compute hash. Suitable for Hash Join where blocks match the child's row descriptor.
/// 2. Column-index mode (init_with_key_columns): Computes CRC32 hash directly on
///    specified column indices. Suitable for Aggregation where spill blocks have a
///    different schema (key columns at fixed positions 0..N-1).
///
/// For each level of repartitioning, the partitioner uses the same CRC32 hash function
/// on key columns but with the SpillPartitionChannelIds functor (bit rotation + modulo).
/// Since all rows in the input stream already belong to the same parent partition (they
/// had identical partition indices at the previous level), re-hashing with the same
/// function still produces a valid and well-distributed split into FANOUT sub-partitions.
///
/// Processing is incremental: each call to repartition() processes up to MAX_BATCH_BYTES
/// (32 MB) of data and then returns, allowing the pipeline scheduler to yield and
/// re-schedule. The caller should loop calling repartition() until `done` is true.
class SpillRepartitioner {
public:
    static constexpr int MAX_DEPTH = 8;
    static constexpr size_t MAX_BATCH_BYTES = 32 * 1024 * 1024; // 32 MB yield threshold

    SpillRepartitioner() = default;
    ~SpillRepartitioner() = default;

    /// Initialize the repartitioner with a partitioner (for Hash Join).
    /// @param partitioner  A cloned partitioner with partition_count == configured fanout.
    ///                     Used to compute hash-based routing for each block.
    /// @param profile      RuntimeProfile for counters.
    // Initialize the repartitioner with a partitioner (for Hash Join).
    // @param partitioner  A cloned partitioner with partition_count == fanout.
    // @param profile      RuntimeProfile for counters.
    // @param fanout       Number of output sub-partitions. Defaults to 8.
    void init(std::unique_ptr<vectorized::PartitionerBase> partitioner, RuntimeProfile* profile,
              int fanout);

    /// Initialize the repartitioner with explicit key column indices (for Aggregation).
    /// Computes CRC32 hash directly on the specified columns without using VExpr.
    /// This is needed because Agg spill blocks have a different schema from the child's
    /// row descriptor, so VSlotRef-based expressions cannot resolve column indices.
    ///
    /// @param key_column_indices  Column indices in the spill block to hash on (typically 0..N-1)
    /// @param key_data_types      Data types of the key columns (for CRC hash type dispatch)
    /// @param profile             RuntimeProfile for counters.
    // Initialize the repartitioner with explicit key column indices (for Aggregation).
    // @param key_column_indices  Column indices in the spill block to hash on (typically 0..N-1)
    // @param key_data_types      Data types of the key columns (for CRC hash type dispatch)
    // @param profile             RuntimeProfile for counters.
    // @param fanout              Number of output sub-partitions. Defaults to 8.
    void init_with_key_columns(std::vector<size_t> key_column_indices,
                               std::vector<vectorized::DataTypePtr> key_data_types,
                               RuntimeProfile* profile, int fanout);

    /// Repartition data from input_stream into output_streams.
    ///
    /// Reads blocks from input_stream, computes hash values via the partitioner,
    /// and routes each row to the corresponding output stream.
    ///
    /// Processing stops after MAX_BATCH_BYTES of input data has been read.
    /// Call repeatedly until done == true.
    ///
    /// @param state           RuntimeState
    /// @param input_stream    SpillStream to read from (previous level's partition data)
    /// @param output_streams  Pre-allocated vector of FANOUT output SpillStreams
    /// @param done            [out] true when input_stream is fully consumed
    Status repartition(RuntimeState* state, vectorized::SpillStreamSPtr& input_stream,
                       std::vector<vectorized::SpillStreamSPtr>& output_streams, bool* done);

    /// Route a single in-memory block into output streams.
    /// Useful for flushing hash table data during Agg repartitioning.
    /// The output_buffers are created/managed internally; data is flushed at the end.
    Status route_block(RuntimeState* state, vectorized::Block& block,
                       std::vector<vectorized::SpillStreamSPtr>& output_streams);

    /// Finalize all output streams that have data (call close()).
    /// Call after the last repartition() returns done == true.
    static Status finalize(std::vector<vectorized::SpillStreamSPtr>& output_streams);

    /// Create FANOUT output SpillStreams registered with the spill stream manager.
    static Status create_output_streams(RuntimeState* state, int node_id,
                                        const std::string& label_prefix,
                                        RuntimeProfile* operator_profile,
                                        std::vector<vectorized::SpillStreamSPtr>& output_streams,
                                        int fanout);

private:
    /// Route a block using the partitioner (Hash Join mode).
    Status _route_block(RuntimeState* state, vectorized::Block& block,
                        std::vector<vectorized::SpillStreamSPtr>& output_streams,
                        std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers);

    /// Route a block using direct column-index hashing (Aggregation mode).
    Status _route_block_by_columns(
            RuntimeState* state, vectorized::Block& block,
            std::vector<vectorized::SpillStreamSPtr>& output_streams,
            std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers);

    Status _flush_buffer(RuntimeState* state, vectorized::SpillStreamSPtr& stream,
                         std::unique_ptr<vectorized::MutableBlock>& buffer);

    Status _flush_all_buffers(
            RuntimeState* state, std::vector<vectorized::SpillStreamSPtr>& output_streams,
            std::vector<std::unique_ptr<vectorized::MutableBlock>>& output_buffers, bool force);

    // Partitioner mode (used by Hash Join)
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;

    // Column-index mode (used by Aggregation)
    std::vector<size_t> _key_column_indices;
    std::vector<vectorized::DataTypePtr> _key_data_types;
    bool _use_column_index_mode = false;

    RuntimeProfile::Counter* _repartition_timer = nullptr;
    RuntimeProfile::Counter* _repartition_rows = nullptr;
    // dynamic fanout to allow operator-configured partition counts
    int _fanout = 8;

public:
    // Accessor for configured fanout
    int fanout() const { return _fanout; }
};

} // namespace pipeline
} // namespace doris
