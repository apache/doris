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

#include "exec/sink/writer/iceberg/viceberg_sort_writer.h"

#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {

Status VIcebergSortWriter::open(RuntimeState* state, RuntimeProfile* profile,
                                const RowDescriptor* row_desc) {
    // row_desc is required for initializing sort expressions
    DCHECK(row_desc != nullptr);
    _runtime_state = state;
    _profile = profile;
    _row_desc = row_desc;

    // Initialize sort expressions from sort_info (contains ordering columns, asc/desc, nulls first/last)
    RETURN_IF_ERROR(_vsort_exec_exprs.init(_sort_info, &_pool));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, *row_desc, *row_desc));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));

    // Create FullSorter for in-memory sorting with spill support enabled.
    // Parameters: limit=-1 (no limit), offset=0 (no offset)
    _sorter = FullSorter::create_unique(_vsort_exec_exprs, -1, 0, &_pool, _sort_info.is_asc_order,
                                        _sort_info.nulls_first, *row_desc, state, _profile);
    _sorter->init_profile(_profile);
    // Enable spill support so the sorter can be used with the spill framework
    _sorter->set_enable_spill();
    _do_spill_count_counter = ADD_COUNTER(_profile, "IcebergDoSpillCount", TUnit::UNIT);

    // Open the underlying partition writer that handles actual file I/O
    RETURN_IF_ERROR(_iceberg_partition_writer->open(state, profile, row_desc));
    return Status::OK();
}

Status VIcebergSortWriter::write(Block& block) {
    // Append incoming block data to the sorter's internal buffer
    RETURN_IF_ERROR(_sorter->append_block(&block));
    _update_spill_block_batch_row_count(block);

    // When accumulated data size reaches the target file size threshold,
    // sort the data in memory and flush it directly to a Parquet/ORC file.
    // This avoids holding too much data in memory before writing.
    if (_sorter->data_size() >= _target_file_size_bytes) {
        return _flush_to_file();
    }

    // If data size is below threshold, wait for more data.
    // Note: trigger_spill() may be called externally by the memory management
    // system if memory pressure is high.
    return Status::OK();
}

Status VIcebergSortWriter::close(const Status& status) {
    // Track the actual internal status of operations performed during close.
    // This is important because if intermediate operations (like do_sort()) fail,
    // we need to propagate the actual error status to the underlying partition writer's
    // close() call, rather than the original status parameter which could be OK.
    Status internal_status = Status::OK();
    // Track the close status of the underlying partition writer.
    // If _iceberg_partition_writer->close() fails (e.g., Parquet file flush error),
    // we must propagate this error to the caller to avoid silent data loss.
    Status close_status = Status::OK();

    // Defer ensures the underlying partition writer is always closed and
    // spill streams are cleaned up, regardless of whether intermediate operations succeed.
    // Uses internal_status to propagate any errors that occurred during close operations.
    Defer defer {[&]() {
        // If any intermediate operation failed, pass that error to the partition writer;
        // otherwise, pass the original status from the caller.
        close_status =
                _iceberg_partition_writer->close(internal_status.ok() ? status : internal_status);
        if (!close_status.ok()) {
            LOG(WARNING) << fmt::format("_iceberg_partition_writer close failed, reason: {}",
                                        close_status.to_string());
        }
        _cleanup_spill_streams();
    }};

    // If the original status is already an error or the query is cancelled,
    // skip all close operations and propagate the original error
    if (!status.ok() || _runtime_state->is_cancelled()) {
        return status;
    }

    // If sorter was never initialized (e.g., no data was written), nothing to do
    if (_sorter == nullptr) {
        return Status::OK();
    }

    // Check if there is any remaining data in the sorter (either unsorted or already sorted blocks)
    if (!_sorter->merge_sort_state()->unsorted_block()->empty() ||
        !_sorter->merge_sort_state()->get_sorted_block().empty()) {
        if (_sorted_spill_files.empty()) {
            // No spill has occurred, all data is in memory.
            // Sort the remaining data, prepare for reading, and write to file.
            internal_status = _sorter->do_sort();
            if (!internal_status.ok()) {
                return internal_status;
            }
            internal_status = _sorter->prepare_for_read(false);
            if (!internal_status.ok()) {
                return internal_status;
            }
            internal_status = _write_sorted_data();
            return internal_status;
        }

        // Some data has already been spilled to disk.
        // Spill the remaining in-memory data to a new spill stream.
        internal_status = _do_spill();
        if (!internal_status.ok()) {
            return internal_status;
        }
    }

    // Merge all spilled streams using multi-way merge sort and output final sorted data to files
    if (!_sorted_spill_files.empty()) {
        internal_status = _combine_files_output();
        if (!internal_status.ok()) {
            return internal_status;
        }
    }

    // Return close_status if internal operations succeeded but the underlying
    // partition writer's close() failed (e.g., file flush error).
    // This prevents silent data loss where the caller thinks the write succeeded
    // but the file was not properly closed.
    return close_status;
}

void VIcebergSortWriter::_update_spill_block_batch_row_count(const Block& block) {
    auto rows = block.rows();
    // Calculate average row size from the first non-empty block to determine
    // the optimal batch size for spill operations
    if (rows > 0 && 0 == _avg_row_bytes) {
        _avg_row_bytes = std::max(1UL, block.bytes() / rows);
        int64_t spill_batch_bytes = _runtime_state->spill_buffer_size_bytes(); // default 8MB
        // Calculate how many rows fit in one spill batch (ceiling division)
        _spill_block_batch_row_count = (spill_batch_bytes + _avg_row_bytes - 1) / _avg_row_bytes;
    }
}

Status VIcebergSortWriter::_flush_to_file() {
    // Sort the accumulated data in memory
    RETURN_IF_ERROR(_sorter->do_sort());
    // Prepare the sorted data for sequential reading (builds merge tree if needed)
    RETURN_IF_ERROR(_sorter->prepare_for_read(false));
    // Write the sorted data to the current Parquet/ORC file
    RETURN_IF_ERROR(_write_sorted_data());
    // Close the current file (it has reached the target size) and open a new writer
    RETURN_IF_ERROR(_close_current_writer_and_open_next());
    // Reset the sorter state to accept new data for the next file
    _sorter->reset();
    return Status::OK();
}

Status VIcebergSortWriter::_write_sorted_data() {
    // Read sorted blocks from the sorter one by one and write them
    // to the underlying partition writer (Parquet/ORC file)
    bool eos = false;
    Block block;
    while (!eos && !_runtime_state->is_cancelled()) {
        RETURN_IF_ERROR(_sorter->get_next(_runtime_state, &block, &eos));
        RETURN_IF_ERROR(_iceberg_partition_writer->write(block));
        block.clear_column_data();
    }
    return Status::OK();
}

Status VIcebergSortWriter::_close_current_writer_and_open_next() {
    // Save the current file name and index before closing, so the next file
    // can use an incremented index (e.g., file_0, file_1, file_2, ...)
    std::string current_file_name = _iceberg_partition_writer->file_name();
    int current_file_index = _iceberg_partition_writer->file_name_index();
    RETURN_IF_ERROR(_iceberg_partition_writer->close(Status::OK()));

    // Use the lambda to create a new partition writer with the next file index
    _iceberg_partition_writer = _create_writer_lambda(&current_file_name, current_file_index + 1);
    if (!_iceberg_partition_writer) {
        return Status::InternalError("Failed to create new partition writer");
    }

    RETURN_IF_ERROR(_iceberg_partition_writer->open(_runtime_state, _profile, _row_desc));
    return Status::OK();
}

int32_t VIcebergSortWriter::_get_spill_batch_size() const {
    // Clamp the batch row count to int32_t max to prevent overflow
    if (_spill_block_batch_row_count > std::numeric_limits<int32_t>::max()) {
        return std::numeric_limits<int32_t>::max();
    }
    return static_cast<int32_t>(_spill_block_batch_row_count);
}

Status VIcebergSortWriter::_do_spill() {
    COUNTER_UPDATE(_do_spill_count_counter, 1);

    // Explicitly sort the data before preparing for spill read.
    // Although FullSorter::prepare_for_read(is_spill=true) internally calls do_sort()
    // when there is unsorted data (see sorter.cpp), we call do_sort() explicitly here
    // for clarity and to guarantee that the data written to the spill stream is sorted.
    // This ensures correctness of the subsequent multi-way merge phase.
    RETURN_IF_ERROR(_sorter->do_sort());

    // prepare_for_read(is_spill=true) adjusts limit/offset for spill mode
    // and builds the merge tree for reading sorted data
    RETURN_IF_ERROR(_sorter->prepare_for_read(true));

    // Register a new spill file to store the sorted data on disk
    SpillFileSPtr spilling_file;
    auto relative_path = fmt::format("{}/{}-{}-{}-{}", print_id(_runtime_state->query_id()),
                                     "MultiCastSender", 1 /* node_id */, _runtime_state->task_id(),
                                     ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                spilling_file));
    _sorted_spill_files.emplace_back(spilling_file);

    SpillFileWriterSPtr writer;
    RETURN_IF_ERROR(spilling_file->create_writer(_runtime_state, _profile, writer));

    // Read sorted data from the sorter in batches and write to the spill stream
    bool eos = false;
    Block block;
    while (!eos && !_runtime_state->is_cancelled()) {
        // Use _get_spill_batch_size() for safe narrowing conversion from size_t to int32_t
        // instead of C-style cast, which includes bounds checking
        RETURN_IF_ERROR(_sorter->merge_sort_read_for_spill(_runtime_state, &block,
                                                           _get_spill_batch_size(), &eos));
        RETURN_IF_ERROR(writer->write_block(_runtime_state, block));
        block.clear_column_data();
    }
    // Reset the sorter to free memory and accept new data
    _sorter->reset();
    return Status::OK();
}

Status VIcebergSortWriter::_combine_files_output() {
    // If there are too many spill files to merge at once (limited by memory),
    // perform intermediate merges to reduce the number of files
    while (_sorted_spill_files.size() > static_cast<size_t>(_calc_max_merge_streams())) {
        RETURN_IF_ERROR(_do_intermediate_merge());
    }

    // Create the final merger that combines all remaining spill files
    RETURN_IF_ERROR(_create_final_merger());

    // Read merged sorted data and write to Parquet/ORC files,
    // splitting into new files when the target file size is exceeded
    bool eos = false;
    Block output_block;
    size_t current_file_bytes = _iceberg_partition_writer->written_len();
    while (!eos && !_runtime_state->is_cancelled()) {
        RETURN_IF_ERROR(_merger->get_next(&output_block, &eos));
        if (output_block.rows() > 0) {
            size_t block_bytes = output_block.bytes();
            RETURN_IF_ERROR(_iceberg_partition_writer->write(output_block));
            current_file_bytes += block_bytes;
            // If the current file exceeds the target size, close it and open a new one
            if (current_file_bytes > _target_file_size_bytes) {
                RETURN_IF_ERROR(_close_current_writer_and_open_next());
                current_file_bytes = 0;
            }
        }
        output_block.clear_column_data();
    }
    return Status::OK();
}

Status VIcebergSortWriter::_do_intermediate_merge() {
    int max_stream_count = _calc_max_merge_streams();
    // Merge a subset of streams (non-final merge) to reduce total stream count
    RETURN_IF_ERROR(_create_merger(false, _spill_block_batch_row_count, max_stream_count));

    // register new spill stream for merged output
    SpillFileSPtr tmp_spill_file;
    auto relative_path = fmt::format("{}/{}-{}-{}-{}", print_id(_runtime_state->query_id()),
                                     "MultiCastSender", 1 /* node_id */, _runtime_state->task_id(),
                                     ExecEnv::GetInstance()->spill_file_mgr()->next_id());
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_file_mgr()->create_spill_file(relative_path,
                                                                                tmp_spill_file));

    _sorted_spill_files.emplace_back(tmp_spill_file);

    SpillFileWriterSPtr tmp_spill_writer;
    RETURN_IF_ERROR(tmp_spill_file->create_writer(_runtime_state, _profile, tmp_spill_writer));

    // Merge the selected files and write the result to the new spill file
    bool eos = false;
    Block merge_sorted_block;
    while (!eos && !_runtime_state->is_cancelled()) {
        merge_sorted_block.clear_column_data();
        RETURN_IF_ERROR(_merger->get_next(&merge_sorted_block, &eos));
        RETURN_IF_ERROR(tmp_spill_writer->write_block(_runtime_state, merge_sorted_block));
    }

    // Clean up the files that were consumed during this intermediate merge
    for (auto& file : _current_merging_spill_files) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(file);
    }
    _current_merging_spill_files.clear();
    return Status::OK();
}

int VIcebergSortWriter::_calc_max_merge_streams() const {
    // Calculate the maximum number of streams that can be merged simultaneously
    // based on the available memory limit and per-stream batch size
    auto count = _runtime_state->spill_sort_merge_mem_limit_bytes() /
                 _runtime_state->spill_buffer_size_bytes();
    if (count > std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }
    // Ensure at least 2 streams can be merged (minimum for a merge operation)
    return std::max(2, static_cast<int>(count));
}

Status VIcebergSortWriter::_create_merger(bool is_final_merge, size_t batch_size, int num_streams) {
    // Create a multi-way merge sorter that reads from multiple sorted spill streams
    std::vector<BlockSupplier> child_block_suppliers;
    _merger = std::make_unique<VSortedRunMerger>(_sorter->get_sort_description(), batch_size, -1, 0,
                                                 _profile);
    _current_merging_spill_files.clear();

    // For final merge: merge all remaining streams
    // For intermediate merge: merge only num_streams streams
    size_t streams_to_merge = is_final_merge ? _sorted_spill_files.size() : num_streams;

    for (size_t i = 0; i < streams_to_merge && !_sorted_spill_files.empty(); ++i) {
        auto spill_file = _sorted_spill_files.front();
        _current_merging_spill_files.emplace_back(spill_file);
        SpillFileReaderSPtr reader = spill_file->create_reader(_runtime_state, _profile);
        child_block_suppliers.emplace_back(
                [reader](Block* block, bool* eos) { return reader->read(block, eos); });
        _sorted_spill_files.pop_front();
    }

    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

Status VIcebergSortWriter::_create_final_merger() {
    // Final merger uses the runtime batch size and merges all remaining streams
    return _create_merger(true, _runtime_state->batch_size(), 1);
}

void VIcebergSortWriter::_cleanup_spill_streams() {
    // Clean up all remaining spill files to release disk resources
    for (auto& file : _sorted_spill_files) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(file);
    }
    _sorted_spill_files.clear();

    // Also clean up any files that are currently being merged
    for (auto& file : _current_merging_spill_files) {
        ExecEnv::GetInstance()->spill_file_mgr()->delete_spill_file(file);
    }
    _current_merging_spill_files.clear();
}

} // namespace doris
