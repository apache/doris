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

#include "common/config.h"
#include "common/object_pool.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/block.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "vec/sink/writer/iceberg/vpartition_writer_base.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {
class RuntimeState;
class RuntimeProfile;

namespace vectorized {
class VIcebergSortWriter : public IPartitionWriterBase {
public:
    using CreateWriterLambda = std::function<std::shared_ptr<VIcebergPartitionWriter>(
            const std::string* file_name, int file_name_index)>;

    VIcebergSortWriter(std::shared_ptr<VIcebergPartitionWriter> partition_writer,
                       TSortInfo sort_info, int64_t target_file_size_bytes,
                       CreateWriterLambda create_writer_lambda = nullptr)
            : _sort_info(std::move(sort_info)),
              _iceberg_partition_writer(std::move(partition_writer)),
              _create_writer_lambda(std::move(create_writer_lambda)),
              _target_file_size_bytes(target_file_size_bytes) {}

    Status open(RuntimeState* state, RuntimeProfile* profile,
                const RowDescriptor* row_desc) override {
        DCHECK(row_desc != nullptr);
        _runtime_state = state;
        _profile = profile;
        _row_desc = row_desc;

        RETURN_IF_ERROR(_vsort_exec_exprs.init(_sort_info, &_pool));
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, *row_desc, *row_desc));
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));

        _sorter = vectorized::FullSorter::create_unique(
                _vsort_exec_exprs, -1, 0, &_pool, _sort_info.is_asc_order, _sort_info.nulls_first,
                *row_desc, state, _profile);
        _sorter->init_profile(_profile);
        _sorter->set_enable_spill();
        _do_spill_count_counter = ADD_COUNTER(_profile, "IcebergDoSpillCount", TUnit::UNIT);
        RETURN_IF_ERROR(_iceberg_partition_writer->open(state, profile, row_desc));
        return Status::OK();
    }

    Status write(vectorized::Block& block) override {
        RETURN_IF_ERROR(_sorter->append_block(&block));
        _update_spill_block_batch_row_count(block);
        // sort in memory and write directly to Parquet file
        if (_sorter->data_size() >= _target_file_size_bytes) {
            return _flush_to_file();
        }
        // trigger_spill() will be called by memory management system
        return Status::OK();
    }

    Status close(const Status& status) override {
        Defer defer {[&]() {
            Status st = _iceberg_partition_writer->close(status);
            if (!st.ok()) {
                LOG(WARNING) << fmt::format("_iceberg_partition_writer close failed, reason: {}",
                                            st.to_string());
            }
            _cleanup_spill_streams();
        }};

        if (!status.ok() || _runtime_state->is_cancelled()) {
            return status;
        }

        if (_sorter == nullptr) {
            return Status::OK();
        }

        if (!_sorter->merge_sort_state()->unsorted_block()->empty() ||
            !_sorter->merge_sort_state()->get_sorted_block().empty()) {
            if (_sorted_streams.empty()) {
                // data remaining in memory
                RETURN_IF_ERROR(_sorter->do_sort());
                RETURN_IF_ERROR(_sorter->prepare_for_read(false));
                RETURN_IF_ERROR(_write_sorted_data());
                return Status::OK();
            }

            // spill remaining data
            RETURN_IF_ERROR(_do_spill());
        }

        // Merge all spilled streams and output final sorted data
        if (!_sorted_streams.empty()) {
            RETURN_IF_ERROR(_combine_files_output());
        }

        return Status::OK();
    }

    inline const std::string& file_name() const override {
        return _iceberg_partition_writer->file_name();
    }

    inline int file_name_index() const override {
        return _iceberg_partition_writer->file_name_index();
    }

    inline size_t written_len() const override { return _iceberg_partition_writer->written_len(); }

    auto sorter() const { return _sorter.get(); }

    Status trigger_spill() { return _do_spill(); }

private:
    // how many rows need in spill block batch
    void _update_spill_block_batch_row_count(const vectorized::Block& block) {
        auto rows = block.rows();
        if (rows > 0 && 0 == _avg_row_bytes) {
            _avg_row_bytes = std::max(1UL, block.bytes() / rows);
            int64_t spill_batch_bytes = _runtime_state->spill_sort_batch_bytes(); // default 8MB
            _spill_block_batch_row_count =
                    (spill_batch_bytes + _avg_row_bytes - 1) / _avg_row_bytes;
        }
    }

    // have enought data, flush in-memory sorted data to file
    Status _flush_to_file() {
        RETURN_IF_ERROR(_sorter->do_sort());
        RETURN_IF_ERROR(_sorter->prepare_for_read(false));
        RETURN_IF_ERROR(_write_sorted_data());
        RETURN_IF_ERROR(_close_current_writer_and_open_next());
        _sorter->reset();
        return Status::OK();
    }

    // write data into file
    Status _write_sorted_data() {
        bool eos = false;
        Block block;
        while (!eos && !_runtime_state->is_cancelled()) {
            RETURN_IF_ERROR(_sorter->get_next(_runtime_state, &block, &eos));
            RETURN_IF_ERROR(_iceberg_partition_writer->write(block));
            block.clear_column_data();
        }
        return Status::OK();
    }

    // close current writer and open a new one with incremented file index
    Status _close_current_writer_and_open_next() {
        std::string current_file_name = _iceberg_partition_writer->file_name();
        int current_file_index = _iceberg_partition_writer->file_name_index();
        RETURN_IF_ERROR(_iceberg_partition_writer->close(Status::OK()));

        _iceberg_partition_writer =
                _create_writer_lambda(&current_file_name, current_file_index + 1);
        if (!_iceberg_partition_writer) {
            return Status::InternalError("Failed to create new partition writer");
        }

        RETURN_IF_ERROR(_iceberg_partition_writer->open(_runtime_state, _profile, _row_desc));
        return Status::OK();
    }

    // batch size max is int32_t max
    int32_t _get_spill_batch_size() const {
        if (_spill_block_batch_row_count > std::numeric_limits<int32_t>::max()) {
            return std::numeric_limits<int32_t>::max();
        }
        return static_cast<int32_t>(_spill_block_batch_row_count);
    }

    Status _do_spill() {
        COUNTER_UPDATE(_do_spill_count_counter, 1);
        RETURN_IF_ERROR(_sorter->prepare_for_read(true));
        int32_t batch_size = _get_spill_batch_size();

        SpillStreamSPtr spilling_stream;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _runtime_state, spilling_stream, print_id(_runtime_state->query_id()),
                "iceberg-sort", 1 /* node_id */, batch_size,
                _runtime_state->spill_sort_batch_bytes(), _profile));
        _sorted_streams.emplace_back(spilling_stream);

        // spill sorted data to stream
        bool eos = false;
        Block block;
        while (!eos && !_runtime_state->is_cancelled()) {
            RETURN_IF_ERROR(_sorter->merge_sort_read_for_spill(
                    _runtime_state, &block, (int)_spill_block_batch_row_count, &eos));
            RETURN_IF_ERROR(spilling_stream->spill_block(_runtime_state, block, eos));
            block.clear_column_data();
        }
        _sorter->reset();
        return Status::OK();
    }

    // merge spilled streams and output sorted data to Parquet files
    Status _combine_files_output() {
        // merge until all streams can be merged in one pass
        while (_sorted_streams.size() > static_cast<size_t>(_calc_max_merge_streams())) {
            RETURN_IF_ERROR(_do_intermediate_merge());
        }
        RETURN_IF_ERROR(_create_final_merger());

        bool eos = false;
        Block output_block;
        size_t current_file_bytes = _iceberg_partition_writer->written_len();
        while (!eos && !_runtime_state->is_cancelled()) {
            RETURN_IF_ERROR(_merger->get_next(&output_block, &eos));
            if (output_block.rows() > 0) {
                size_t block_bytes = output_block.bytes();
                RETURN_IF_ERROR(_iceberg_partition_writer->write(output_block));
                current_file_bytes += block_bytes;
                if (current_file_bytes > _target_file_size_bytes) {
                    // close current writer and commit to file
                    RETURN_IF_ERROR(_close_current_writer_and_open_next());
                    current_file_bytes = 0;
                }
            }
            output_block.clear_column_data();
        }
        return Status::OK();
    }

    Status _do_intermediate_merge() {
        int max_stream_count = _calc_max_merge_streams();
        RETURN_IF_ERROR(_create_merger(false, _spill_block_batch_row_count, max_stream_count));

        // register new spill stream for merged output
        int32_t batch_size = _get_spill_batch_size();
        SpillStreamSPtr tmp_stream;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _runtime_state, tmp_stream, print_id(_runtime_state->query_id()),
                "iceberg-sort-merge", 1 /* node_id */, batch_size,
                _runtime_state->spill_sort_batch_bytes(), _profile));

        _sorted_streams.emplace_back(tmp_stream);

        // merge current streams and write to new spill stream
        bool eos = false;
        Block merge_sorted_block;
        while (!eos && !_runtime_state->is_cancelled()) {
            merge_sorted_block.clear_column_data();
            RETURN_IF_ERROR(_merger->get_next(&merge_sorted_block, &eos));
            RETURN_IF_ERROR(tmp_stream->spill_block(_runtime_state, merge_sorted_block, eos));
        }

        // clean up merged streams
        for (auto& stream : _current_merging_streams) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
        }
        _current_merging_streams.clear();
        return Status::OK();
    }

    int _calc_max_merge_streams() const {
        auto count =
                _runtime_state->spill_sort_mem_limit() / _runtime_state->spill_sort_batch_bytes();
        if (count > std::numeric_limits<int>::max()) {
            return std::numeric_limits<int>::max();
        }
        return std::max(2, static_cast<int>(count));
    }

    // create merger for merging spill streams
    Status _create_merger(bool is_final_merge, size_t batch_size, int num_streams) {
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        _merger = std::make_unique<vectorized::VSortedRunMerger>(_sorter->get_sort_description(),
                                                                 batch_size, -1, 0, _profile);
        _current_merging_streams.clear();
        size_t streams_to_merge = is_final_merge ? _sorted_streams.size() : num_streams;

        for (size_t i = 0; i < streams_to_merge && !_sorted_streams.empty(); ++i) {
            auto stream = _sorted_streams.front();
            stream->set_read_counters(_profile);
            _current_merging_streams.emplace_back(stream);
            child_block_suppliers.emplace_back([stream](vectorized::Block* block, bool* eos) {
                return stream->read_next_block_sync(block, eos);
            });
            _sorted_streams.pop_front();
        }

        RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
        return Status::OK();
    }

    Status _create_final_merger() { return _create_merger(true, _runtime_state->batch_size(), 1); }

    // clean up all spill streams to ensure proper resource cleanup
    void _cleanup_spill_streams() {
        for (auto& stream : _sorted_streams) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
        }
        _sorted_streams.clear();

        for (auto& stream : _current_merging_streams) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
        }
        _current_merging_streams.clear();
    }

    RuntimeState* _runtime_state = nullptr;
    RuntimeProfile* _profile = nullptr;
    const RowDescriptor* _row_desc = nullptr;
    ObjectPool _pool;
    TSortInfo _sort_info;
    VSortExecExprs _vsort_exec_exprs;
    std::shared_ptr<VIcebergPartitionWriter> _iceberg_partition_writer;
    CreateWriterLambda _create_writer_lambda; // creating new writers after commit

    // Sorter and merger
    std::unique_ptr<vectorized::FullSorter> _sorter;
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;
    std::deque<vectorized::SpillStreamSPtr> _sorted_streams;
    std::vector<vectorized::SpillStreamSPtr> _current_merging_streams;

    int64_t _target_file_size_bytes = 0; //config::iceberg_sink_max_file_size default 1GB
    size_t _avg_row_bytes = 0;
    size_t _spill_block_batch_row_count = 4096;

    RuntimeProfile::Counter* _do_spill_count_counter = nullptr;
};

} // namespace vectorized
} // namespace doris