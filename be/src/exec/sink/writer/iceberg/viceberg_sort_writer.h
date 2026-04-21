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
#include <limits>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "exec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "exec/sink/writer/iceberg/vpartition_writer_base.h"
#include "exec/sort/sorter.h"
#include "runtime/runtime_profile.h"

// Forward declarations to minimize header dependencies.
// Previously, spill_stream.h and spill_stream_manager.h were included directly
// in this header, causing heavy transitive includes for all files that include
// viceberg_sort_writer.h. Moving implementations to .cpp allows us to forward-declare
// these types and only include their headers in the .cpp file.

namespace doris {
class SpillFile;
using SpillFileSPtr = std::shared_ptr<SpillFile>;
class RuntimeState;
class RuntimeProfile;

/**
 * VIcebergSortWriter is a decorator around VIcebergPartitionWriter that adds sort-order support.
 *
 * Architecture:
 *   IPartitionWriterBase (abstract base class)
 *       ├── VIcebergPartitionWriter  (writes data directly to Parquet/ORC files)
 *       └── VIcebergSortWriter       (sorts data before delegating to VIcebergPartitionWriter)
 *
 * Key behaviors:
 * 1. In-memory sorting: Accumulates data in a FullSorter. When accumulated data reaches
 *    _target_file_size_bytes, sorts and flushes to a file, then opens a new writer.
 * 2. Spill to disk: When triggered by the memory management system via trigger_spill(),
 *    sorts and writes data to a SpillStream on disk.
 * 3. Multi-way merge: When closing, merges all spilled streams using a VSortedRunMerger
 *    to produce final sorted output files.
 */
class VIcebergSortWriter : public IPartitionWriterBase {
public:
    // Lambda type for creating new VIcebergPartitionWriter instances.
    // Used when a file is completed and a new file needs to be opened.
    using CreateWriterLambda = std::function<std::shared_ptr<VIcebergPartitionWriter>(
            const std::string* file_name, int file_name_index)>;

    /**
     * Constructor.
     * @param partition_writer The underlying writer that handles actual file I/O
     * @param sort_info Sort specification (columns, asc/desc, nulls first/last)
     * @param target_file_size_bytes Target file size before splitting to a new file
     * @param create_writer_lambda Lambda for creating new writers when file splitting occurs
     */
    VIcebergSortWriter(std::shared_ptr<VIcebergPartitionWriter> partition_writer,
                       TSortInfo sort_info, int64_t target_file_size_bytes,
                       CreateWriterLambda create_writer_lambda = nullptr)
            : _sort_info(std::move(sort_info)),
              _iceberg_partition_writer(std::move(partition_writer)),
              _create_writer_lambda(std::move(create_writer_lambda)),
              _target_file_size_bytes(target_file_size_bytes) {}

    // Initialize sort expressions, create FullSorter, and open the underlying writer
    Status open(RuntimeState* state, RuntimeProfile* profile,
                const RowDescriptor* row_desc) override;

    // Append data block to the sorter; triggers flush when target file size is reached
    Status write(Block& block) override;

    // Sort remaining data, perform multi-way merge if spill occurred, and close the writer.
    // Error handling: Tracks internal errors from intermediate operations and propagates
    // the actual error status (not the original caller status) to the underlying writer.
    Status close(const Status& status) override;

    inline const std::string& file_name() const override {
        return _iceberg_partition_writer->file_name();
    }

    inline int file_name_index() const override {
        return _iceberg_partition_writer->file_name_index();
    }

    inline size_t written_len() const override { return _iceberg_partition_writer->written_len(); }

    // Returns a raw pointer to the FullSorter, used by SpillIcebergTableSinkLocalState
    // to query memory usage (data_size, get_reserve_mem_size)
    auto sorter() const { return _sorter.get(); }

    // Called by the memory management system to trigger spilling data to disk
    Status trigger_spill() { return _do_spill(); }

private:
    // Calculate average row size from the first non-empty block to determine
    // the optimal batch row count for spill operations
    void _update_spill_block_batch_row_count(const Block& block);

    // Sort in-memory data and flush to a Parquet/ORC file, then open a new writer
    Status _flush_to_file();

    // Read sorted data from the sorter and write to the underlying partition writer
    Status _write_sorted_data();

    // Close the current partition writer and create a new one with an incremented file index
    Status _close_current_writer_and_open_next();

    // Get the batch size for spill operations, clamped to int32_t max
    int32_t _get_spill_batch_size() const;

    // Sort the current in-memory data and write it to a new spill stream on disk.
    // Explicitly calls do_sort() before prepare_for_read() to guarantee sorted output.
    Status _do_spill();

    // Merge all spilled streams and output final sorted data to Parquet/ORC files.
    // Handles file splitting when output exceeds target file size.
    Status _combine_files_output();

    // Perform an intermediate merge when there are too many spill streams to merge at once.
    // Merges a subset of streams into a single new stream.
    Status _do_intermediate_merge();

    // Calculate the maximum number of streams that can be merged simultaneously
    // based on memory limits
    int _calc_max_merge_streams() const;

    // Create a VSortedRunMerger for merging spill streams
    // @param is_final_merge If true, merges all remaining streams
    // @param batch_size Number of rows per batch during merge
    // @param num_streams Maximum number of streams to merge (used for intermediate merges)
    Status _create_merger(bool is_final_merge, size_t batch_size, int num_streams);

    // Create the final merger that merges all remaining spill streams
    Status _create_final_merger();

    // Release all spill stream resources (both pending and currently merging)
    void _cleanup_spill_streams();

    RuntimeState* _runtime_state = nullptr;
    RuntimeProfile* _profile = nullptr;
    const RowDescriptor* _row_desc = nullptr;
    ObjectPool _pool;
    TSortInfo _sort_info;
    VExprContextSPtrs _ordering_expr_ctxs;
    // The underlying partition writer that handles actual Parquet/ORC file I/O
    std::shared_ptr<VIcebergPartitionWriter> _iceberg_partition_writer;
    // Lambda for creating new writers when file splitting occurs
    CreateWriterLambda _create_writer_lambda;

    // Sorter and merger for handling in-memory sorting and multi-way merge
    std::unique_ptr<FullSorter> _sorter;
    std::unique_ptr<VSortedRunMerger> _merger;

    // Queue of spill files waiting to be merged (FIFO order)
    std::deque<SpillFileSPtr> _sorted_spill_files;
    // Files currently being consumed by the merger
    std::vector<SpillFileSPtr> _current_merging_spill_files;

    // Target file size in bytes; files are split when this threshold is exceeded
    // Default: config::iceberg_sink_max_file_size (1GB)
    int64_t _target_file_size_bytes = 0;
    // Average row size in bytes, computed from the first non-empty block
    size_t _avg_row_bytes = 0;
    // Number of rows per spill batch, computed from average row size and spill_sort_batch_bytes
    size_t _spill_block_batch_row_count = 4096;

    // Counter tracking how many times spill has been triggered
    RuntimeProfile::Counter* _do_spill_count_counter = nullptr;
};

} // namespace doris