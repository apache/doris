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

//#include "runtime/types.h"
//#include "vec/functions/function_string.h"
//#include "vec/data_types/data_type_factory.hpp"
//#include "util/bit_util.h"

#include <vector>

#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/block.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/sink/writer/iceberg/partition_writer.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {

class RuntimeState;
class RuntimeProfile;

namespace iceberg {}; // namespace iceberg

namespace vectorized {
class Block;

template <typename OutputWriter>
class SortingFileWriter : public IPartitionWriter {
public:
    SortingFileWriter(const TDataSink& t_sink, std::vector<std::string> partition_values,
                      const VExprContextSPtrs& write_output_expr_ctxs,
                      const doris::iceberg::Schema& schema, const std::string* iceberg_schema_json,
                      std::vector<std::string> write_column_names, WriteInfo write_info,
                      std::string file_name, int file_name_index,
                      TFileFormatType::type file_format_type, TFileCompressType::type compress_type,
                      const std::map<std::string, std::string>& hadoop_conf)
            : _t_sink(t_sink),
              _is_asc_order(t_sink.iceberg_table_sink.sort_info.is_asc_order),
              _nulls_first(t_sink.iceberg_table_sink.sort_info.nulls_first),
              _output_writer(t_sink, std::move(partition_values), write_output_expr_ctxs, schema,
                             iceberg_schema_json, write_column_names, std::move(write_info),
                             std::move(file_name), file_name_index, file_format_type, compress_type,
                             hadoop_conf) {}

    Status open(RuntimeState* state, RuntimeProfile* profile, const RowDescriptor* row_desc,
                ObjectPool* pool) override {
        DCHECK(row_desc);
        _state = state;
        _profile = profile;

        _spill_counters = ADD_LABEL_COUNTER_WITH_LEVEL(_profile, "Spill", 1);
        _spill_timer = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillTime", "Spill", 1);
        _spill_serialize_block_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillSerializeBlockTime", "Spill", 1);
        _spill_write_disk_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWriteDiskTime", "Spill", 1);
        _spill_data_size = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillWriteDataSize",
                                                        TUnit::BYTES, "Spill", 1);
        _spill_block_count = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillWriteBlockCount",
                                                          TUnit::UNIT, "Spill", 1);
        _spill_wait_in_queue_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWaitInQueueTime", "Spill", 1);
        _spill_write_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillWriteWaitIOTime", "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadWaitIOTime", "Spill", 1);

        _spill_recover_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillRecoverTime", "Spill", 1);
        _spill_read_data_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadDataTime", "Spill", 1);
        _spill_deserialize_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillDeserializeTime", "Spill", 1);
        _spill_read_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SpillReadDataSize",
                                                         TUnit::BYTES, "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "SpillReadWaitIOTime", "Spill", 1);

        int limit = -1;
        int64_t offset = 0;
        auto& iceberg_table_sink = _t_sink.iceberg_table_sink;
        RETURN_IF_ERROR(_vsort_exec_exprs.init(iceberg_table_sink.sort_info, pool));
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, *row_desc, *row_desc));
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));

        _sorter = vectorized::FullSorter::create_unique(_vsort_exec_exprs, limit, offset, pool,
                                                        _is_asc_order, _nulls_first, *row_desc,
                                                        state, _profile);
        _sorter->set_enable_spill(true);

        _merger = std::make_unique<vectorized::VSortedRunMerger>(_sorter->get_sort_description(),
                                                                 SPILL_BLOCK_BATCH_ROW_COUNT, limit,
                                                                 offset, _profile);
        RETURN_IF_ERROR(_output_writer.open(state, profile, row_desc, pool));
        return Status::OK();
    }

    Status write(vectorized::Block& block) override {
        RETURN_IF_ERROR(_sorter->append_block(&block));
        _update_spill_block_batch_row_count(block);

        if (_sorter->state().get_sorted_block().empty()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(flush_to_temp_file());
        return Status::OK();
    }

    Status flush_to_temp_file() {
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                _state, _spilling_stream, print_id(_state->query_id()), "sort", 1 /* node_id */,
                SPILL_BLOCK_BATCH_ROW_COUNT, SORT_BLOCK_SPILL_BATCH_BYTES, _profile));

        _sorted_streams.emplace_back(_spilling_stream);

        _spilling_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                             _spill_data_size, _spill_write_disk_timer,
                                             _spill_write_wait_io_timer);

        RETURN_IF_ERROR(_spilling_stream->prepare_spill());

        RETURN_IF_ERROR(_sorter->prepare_for_read());
        bool eos = false;
        Block block;
        while (!eos) {
            RETURN_IF_ERROR(_sorter->merge_sort_read_for_spill(_state, &block,
                                                               _spill_block_batch_row_count, &eos));
            RETURN_IF_ERROR(_spilling_stream->spill_block(_state, block, eos));
            block.clear_column_data();
        }
        _sorter->reset();
        return Status::OK();
    }

    Status close(const Status& status) override {
        Defer defer {[&]() {
            Status st = _output_writer.close(status);
            if (!st.ok()) {
                LOG(WARNING) << fmt::format("_output_writer close failed, reason: {}",
                                            st.to_string());
            }
            for (auto& stream : _sorted_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
        }};
        if (status.ok() && _sorter != nullptr) {
            if (!_sorter->state().unsorted_block_empty()) {
                if (_sorted_streams.empty()) {
                    RETURN_IF_ERROR(_sorter->sort());
                    DCHECK(_sorter->state().get_sorted_block().size() == 1);
                    RETURN_IF_ERROR(_output_writer.write(_sorter->state().get_sorted_block()[0]));
                    return Status::OK();
                }
                RETURN_IF_ERROR(flush_to_temp_file());
            }
            if (_merger != nullptr) {
                RETURN_IF_ERROR(_combine_files());
                bool eos = false;
                while (!eos) {
                    Block output_block;
                    RETURN_IF_ERROR(_merger->get_next(&output_block, &eos));
                    RETURN_IF_ERROR(_output_writer.write(output_block));
                }
            }
        }
        return Status::OK();
    }

    inline const std::string& file_name() const override { return _output_writer.file_name(); }

    inline int file_name_index() const override { return _output_writer.file_name_index(); }

    inline size_t written_len() override { return _output_writer.written_len(); }

private:
    Status _combine_files() {
        Block merge_sorted_block;
        SpillStreamSPtr tmp_stream;
        while (true) {
            int max_stream_count =
                    std::max(2UL, EXTERNAL_SORT_BYTES_THRESHOLD / SORT_BLOCK_SPILL_BATCH_BYTES);
            {
                SCOPED_TIMER(_spill_recover_time);
                RETURN_IF_ERROR(_create_intermediate_merger(max_stream_count,
                                                            _sorter->get_sort_description()));
            }

            // all the remaining streams can be merged in a run
            if (_sorted_streams.empty()) {
                return Status::OK();
            }

            {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        _state, tmp_stream, print_id(_state->query_id()), "sort", 1 /* node_id */,
                        SPILL_BLOCK_BATCH_ROW_COUNT, SORT_BLOCK_SPILL_BATCH_BYTES, _profile));

                RETURN_IF_ERROR(tmp_stream->prepare_spill());

                _sorted_streams.emplace_back(tmp_stream);

                bool eos = false;
                tmp_stream->set_write_counters(_spill_serialize_block_timer, _spill_block_count,
                                               _spill_data_size, _spill_write_disk_timer,
                                               _spill_write_wait_io_timer);
                while (!eos && !_state->is_cancelled()) {
                    merge_sorted_block.clear_column_data();
                    {
                        SCOPED_TIMER(_spill_recover_time);
                        RETURN_IF_ERROR(_merger->get_next(&merge_sorted_block, &eos));
                    }
                    RETURN_IF_ERROR(tmp_stream->spill_block(_state, merge_sorted_block, eos));
                }
            }
            for (auto& stream : _current_merging_streams) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        return Status::OK();
    }

    Status _create_intermediate_merger(int num_blocks,
                                       const vectorized::SortDescription& sort_description) {
        int limit = -1;
        int64_t offset = 0;
        std::vector<vectorized::BlockSupplier> child_block_suppliers;
        _merger = std::make_unique<vectorized::VSortedRunMerger>(
                sort_description, SPILL_BLOCK_BATCH_ROW_COUNT, limit, offset, _profile);

        _current_merging_streams.clear();
        for (int i = 0; i < num_blocks && !_sorted_streams.empty(); ++i) {
            auto stream = _sorted_streams.front();
            stream->set_read_counters(_spill_read_data_time, _spill_deserialize_time,
                                      _spill_read_bytes, _spill_read_wait_io_timer);
            _current_merging_streams.emplace_back(stream);
            child_block_suppliers.emplace_back(
                    std::bind(std::mem_fn(&vectorized::SpillStream::read_next_block_sync),
                              stream.get(), std::placeholders::_1, std::placeholders::_2));

            _sorted_streams.pop_front();
        }
        RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
        return Status::OK();
    }

    size_t _avg_row_bytes = 0;
    int _spill_block_batch_row_count;

    void _update_spill_block_batch_row_count(const vectorized::Block& block) {
        auto rows = block.rows();
        if (rows > 0 && 0 == _avg_row_bytes) {
            _avg_row_bytes = std::max(1UL, block.bytes() / rows);
            _spill_block_batch_row_count =
                    (SORT_BLOCK_SPILL_BATCH_BYTES + _avg_row_bytes - 1) / _avg_row_bytes;
            LOG(INFO) << "spill sort block batch row count: " << _spill_block_batch_row_count;
        }
    }

    const TDataSink& _t_sink;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    VSortExecExprs _vsort_exec_exprs;
    VExprContextSPtrs _lhs_ordering_expr_ctxs;
    VExprContextSPtrs _rhs_ordering_expr_ctxs;

    std::unique_ptr<vectorized::FullSorter> _sorter;
    std::unique_ptr<vectorized::VSortedRunMerger> _merger;

    vectorized::SpillStreamSPtr _spilling_stream;
    std::deque<vectorized::SpillStreamSPtr> _sorted_streams;

    std::string _path;

    std::vector<std::string> _partition_values;

    RuntimeState* _state;
    RuntimeProfile* _profile;

    std::vector<vectorized::SpillStreamSPtr> _current_merging_streams;

    OutputWriter _output_writer;

    RuntimeProfile::Counter* _spill_counters = nullptr;
    RuntimeProfile::Counter* _spill_timer = nullptr;
    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;
    RuntimeProfile::Counter* _spill_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_io_timer = nullptr;

    RuntimeProfile::Counter* _spill_recover_time;
    RuntimeProfile::Counter* _spill_read_data_time;
    RuntimeProfile::Counter* _spill_deserialize_time;
    RuntimeProfile::Counter* _spill_read_bytes;
    RuntimeProfile::Counter* _spill_read_wait_io_timer = nullptr;

    static constexpr int SORT_BLOCK_SPILL_BATCH_BYTES = 8 * 1024 * 1024;
    static constexpr size_t SPILL_BLOCK_BATCH_ROW_COUNT = 4096;
    static constexpr size_t EXTERNAL_SORT_BYTES_THRESHOLD = 134217728;
};

} // namespace vectorized
} // namespace doris