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

#include "vec/sink/varrow_flight_result_writer.h"

#include <gen_cpp/Data_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>

#include "runtime/result_block_buffer.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void GetArrowResultBatchCtx::on_failure(const Status& status) {
    DCHECK(!status.ok()) << "status is ok, errmsg=" << status;
    status.to_protobuf(_result->mutable_status());
}

void GetArrowResultBatchCtx::on_close(int64_t packet_seq, int64_t /* returned_rows */) {
    Status status;
    status.to_protobuf(_result->mutable_status());
    _result->set_packet_seq(packet_seq);
    _result->set_eos(true);
}

Status GetArrowResultBatchCtx::on_data(const std::shared_ptr<vectorized::Block>& block,
                                       const int64_t packet_seq, ResultBlockBufferBase* buffer) {
    if (_result != nullptr) {
        auto* arrow_buffer = assert_cast<ArrowFlightResultBlockBuffer*>(buffer);
        size_t uncompressed_bytes = 0, compressed_bytes = 0;
        SCOPED_TIMER(arrow_buffer->_serialize_batch_ns_timer);
        RETURN_IF_ERROR(block->serialize(
                arrow_buffer->_be_exec_version, _result->mutable_block(), &uncompressed_bytes,
                &compressed_bytes, arrow_buffer->_fragment_transmission_compression_type, false));
        COUNTER_UPDATE(arrow_buffer->_uncompressed_bytes_counter, uncompressed_bytes);
        COUNTER_UPDATE(arrow_buffer->_compressed_bytes_counter, compressed_bytes);
        _result->set_packet_seq(packet_seq);
        _result->set_eos(false);
        if (packet_seq == 0) {
            _result->set_timezone(arrow_buffer->_timezone);
        }
    } else {
        _result->set_empty_batch(true);
        _result->set_packet_seq(packet_seq);
        _result->set_eos(false);
    }
    Status st = Status::OK();
    /// The size limit of proto buffer message is 2G
    if (_result->ByteSizeLong() > _max_msg_size) {
        st = Status::InternalError("Message size exceeds 2GB: {}", _result->ByteSizeLong());
        _result->clear_block();
    }
    st.to_protobuf(_result->mutable_status());
    return Status::OK();
}

Status ArrowFlightResultBlockBuffer::get_schema(std::shared_ptr<arrow::Schema>* arrow_schema) {
    if (!_status.ok()) {
        return _status;
    }

    // normal path end
    if (_arrow_schema != nullptr) {
        *arrow_schema = _arrow_schema;
        return Status::OK();
    }

    if (_is_close) {
        return Status::RuntimeError(fmt::format("Closed ()", print_id(_fragment_id)));
    }
    return Status::InternalError(fmt::format("Get Arrow Schema Abnormal Ending (), ()",
                                             print_id(_fragment_id), _status));
}

Status ArrowFlightResultBlockBuffer::get_arrow_batch(std::shared_ptr<vectorized::Block>* result) {
    std::unique_lock<std::mutex> l(_lock);
    Defer defer {[&]() { _update_dependency(); }};
    if (!_status.ok()) {
        return _status;
    }

    while (_result_batch_queue.empty() && _status.ok() && !_is_close) {
        _arrow_data_arrival.wait_for(l, std::chrono::milliseconds(20));
    }

    if (!_status.ok()) {
        return _status;
    }

    if (!_result_batch_queue.empty()) {
        *result = std::move(_result_batch_queue.front());
        _result_batch_queue.pop_front();

        for (auto it : _instance_rows_in_queue.front()) {
            _instance_rows[it.first] -= it.second;
        }
        _instance_rows_in_queue.pop_front();
        _packet_num++;
        return Status::OK();
    }

    // normal path end
    if (_is_close) {
        if (!_status.ok()) {
            return _status;
        }
        std::stringstream ss;
        _profile.pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "ResultBlockBuffer finished, fragment_id={}, is_close={}, is_cancelled={}, "
                "packet_num={}, peak_memory_usage={}, profile={}",
                print_id(_fragment_id), _is_close, !_status.ok(), _packet_num,
                _mem_tracker->peak_consumption(), ss.str());
        return Status::OK();
    }
    return Status::InternalError(
            fmt::format("Get Arrow Batch Abnormal Ending (), ()", print_id(_fragment_id), _status));
}

VArrowFlightResultWriter::VArrowFlightResultWriter(std::shared_ptr<ResultBlockBufferBase> sinker,
                                                   const VExprContextSPtrs& output_vexpr_ctxs,
                                                   RuntimeProfile* parent_profile)
        : _sinker(std::dynamic_pointer_cast<ArrowFlightResultBlockBuffer>(sinker)),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

Status VArrowFlightResultWriter::init(RuntimeState* state) {
    _init_profile();
    DCHECK(_sinker);
    _is_dry_run = state->query_options().dry_run_query;
    return Status::OK();
}

void VArrowFlightResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
    _bytes_sent_counter = ADD_COUNTER(_parent_profile, "BytesSent", TUnit::BYTES);
}

Status VArrowFlightResultWriter::write(RuntimeState* state, Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    Block block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
                                                                       input_block, &block));

    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_sinker->mem_tracker());
        std::unique_ptr<vectorized::MutableBlock> mutable_block =
                vectorized::MutableBlock::create_unique(block.clone_empty());
        RETURN_IF_ERROR(mutable_block->merge_ignore_overflow(std::move(block)));
        std::shared_ptr<vectorized::Block> output_block = vectorized::Block::create_shared();
        output_block->swap(mutable_block->to_block());

        auto num_rows = output_block->rows();
        // arrow::RecordBatch without `nbytes()` in C++
        uint64_t bytes_sent = output_block->bytes();
        {
            SCOPED_TIMER(_result_send_timer);
            // If this is a dry run task, no need to send data block
            if (!_is_dry_run) {
                status = _sinker->add_batch(state, output_block);
            }
            if (status.ok()) {
                _written_rows += num_rows;
                if (!_is_dry_run) {
                    _bytes_sent += bytes_sent;
                }
            } else {
                LOG(WARNING) << "append result batch to sink failed.";
            }
        }
    }
    return status;
}

Status VArrowFlightResultWriter::close(Status st) {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    COUNTER_UPDATE(_bytes_sent_counter, _bytes_sent);
    return Status::OK();
}

} // namespace doris::vectorized
