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
#include "core/data_type/data_type.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"

namespace doris {
class RuntimeState;
class RuntimeProfile;

class Block;
class PartitionerBase;

/// SpillRepartitioner reads data from an input SpillFile and redistributes it
/// into FANOUT output SpillFiles by computing hash on key columns.
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
/// For repartitioning, hash computation and final channel mapping are separated:
/// - a partitioner can provide either direct channel ids or raw hash values
///   (e.g. SpillRePartitionChannelIds returns raw hash),
/// - SpillRepartitioner then applies the final channel mapping strategy.
/// This keeps repartition policy centralized and allows level-aware mapping.
///
/// Processing is incremental: each call to repartition() processes up to MAX_BATCH_BYTES
/// (32 MB) of data and then returns, allowing the pipeline scheduler to yield and
/// re-schedule. The caller should loop calling repartition() until `done` is true.
///
/// Usage pattern:
///   // 1. Initialize
///   repartitioner.init(...) or repartitioner.init_with_key_columns(...)
///   // 2. Create output files and set up writers
///   SpillRepartitioner::create_output_spill_files(state, ..., output_files, fanout);
///   repartitioner.setup_output(state, output_files);
///   // 3. Route blocks and/or repartition files
///   repartitioner.route_block(state, block);         // from hash table
///   repartitioner.repartition(state, input_file, &done);  // from spill file
///   // 4. Finalize
///   repartitioner.finalize();
class SpillRepartitioner {
public:
    static constexpr int MAX_DEPTH = 8;
    static constexpr size_t MAX_BATCH_BYTES = 32 * 1024 * 1024; // 32 MB yield threshold

    SpillRepartitioner() = default;
    ~SpillRepartitioner() = default;

    /// Initialize the repartitioner with a partitioner (for Hash Join).
    void init(std::unique_ptr<PartitionerBase> partitioner, RuntimeProfile* profile, int fanout,
              int repartition_level);

    /// Initialize the repartitioner with explicit key column indices (for Aggregation).
    void init_with_key_columns(std::vector<size_t> key_column_indices,
                               std::vector<DataTypePtr> key_data_types, RuntimeProfile* profile,
                               int fanout, int repartition_level);

    /// Set up output SpillFiles and create persistent writers for them.
    /// Must be called before repartition() or route_block().
    Status setup_output(RuntimeState* state, std::vector<SpillFileSPtr>& output_spill_files);

    /// Repartition data from input_spill_file into output files.
    /// The input reader is created lazily and persists across yield calls.
    /// Call repeatedly until done == true.
    Status repartition(RuntimeState* state, SpillFileSPtr& input_spill_file, bool* done);

    /// Repartition data using an existing reader (continues from its current
    /// position). Useful when the caller has already partially read the file
    /// and wants to repartition only the remaining data without re-reading
    /// from the beginning. Ownership of the reader is transferred on completion.
    /// Call repeatedly until done == true.
    Status repartition(RuntimeState* state, SpillFileReaderSPtr& reader, bool* done);

    /// Route a single in-memory block into output files via persistent writers.
    Status route_block(RuntimeState* state, Block& block);

    /// Finalize: close all output writers and update SpillFile stats.
    /// Also resets internal reader state.
    Status finalize();

    /// Create FANOUT output SpillFiles registered with the SpillFileManager.
    static Status create_output_spill_files(RuntimeState* state, int node_id,
                                            const std::string& label_prefix, int fanout,
                                            std::vector<SpillFileSPtr>& output_spill_files);

    int fanout() const { return _fanout; }

private:
    /// Route a block using the partitioner (Hash Join mode).
    Status _route_block(RuntimeState* state, Block& block,
                        std::vector<std::unique_ptr<MutableBlock>>& output_buffers);

    /// Route a block using direct column-index hashing (Aggregation mode).
    Status _route_block_by_columns(RuntimeState* state, Block& block,
                                   std::vector<std::unique_ptr<MutableBlock>>& output_buffers);

    Status _flush_buffer(RuntimeState* state, int partition_idx,
                         std::unique_ptr<MutableBlock>& buffer);

    Status _flush_all_buffers(RuntimeState* state,
                              std::vector<std::unique_ptr<MutableBlock>>& output_buffers,
                              bool force);

    uint32_t _map_hash_to_partition(uint32_t hash) const;

    // Partitioner mode (used by Hash Join)
    std::unique_ptr<PartitionerBase> _partitioner;

    // Column-index mode (used by Aggregation)
    std::vector<size_t> _key_column_indices;
    std::vector<DataTypePtr> _key_data_types;
    bool _use_column_index_mode = false;

    RuntimeProfile::Counter* _repartition_timer = nullptr;
    RuntimeProfile::Counter* _repartition_rows = nullptr;
    RuntimeProfile* _operator_profile = nullptr;
    int _fanout = 8;
    int _repartition_level = 0;

    // ── Persistent state across repartition/route_block calls ──────
    // Output writers (one per partition), created by setup_output()
    std::vector<SpillFileWriterSPtr> _output_writers;
    // Pointer to caller's output SpillFiles vector (for finalize)
    std::vector<SpillFileSPtr>* _output_spill_files = nullptr;
    // Input reader for repartition(), persists across yield calls
    SpillFileReaderSPtr _input_reader;
    SpillFileSPtr _current_input_file;
};

} // namespace doris
