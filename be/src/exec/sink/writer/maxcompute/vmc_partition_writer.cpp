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

#include "exec/sink/writer/maxcompute/vmc_partition_writer.h"

#include "format/transformer/vjni_format_transformer.h"
#include "runtime/runtime_state.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {

namespace {

std::string mc_diag_param(const std::map<std::string, std::string>& params, const std::string& key) {
    auto it = params.find(key);
    return it == params.end() ? "" : it->second;
}

} // namespace

VMCPartitionWriter::VMCPartitionWriter(RuntimeState* state,
                                       const VExprContextSPtrs& output_vexpr_ctxs,
                                       const std::string& partition_spec,
                                       std::map<std::string, std::string> writer_params)
        : _state(state),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _partition_spec(partition_spec),
          _writer_params(std::move(writer_params)) {}

Status VMCPartitionWriter::open() {
    int64_t start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_WRITER_OPEN_ENTER"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec;
    _jni_format_transformer = std::make_unique<VJniFormatTransformer>(
            _state, _output_vexpr_ctxs, "org/apache/doris/maxcompute/MaxComputeJniWriter",
            _writer_params);
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_JNI_OPEN_BEFORE"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec;
    Status status = _jni_format_transformer->open();
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_JNI_OPEN_AFTER"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    return status;
}

Status VMCPartitionWriter::write(Block& block) {
    int64_t start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_WRITE_BEFORE"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec << ", rows=" << block.rows()
              << ", accumulated_rows=" << _row_count;
    Status status = _jni_format_transformer->write(block);
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_WRITE_AFTER"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec << ", rows=" << block.rows()
              << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    RETURN_IF_ERROR(status);
    _row_count += block.rows();
    return Status::OK();
}

Status VMCPartitionWriter::close(const Status& status) {
    int64_t start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_CLOSE_ENTER"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec << ", input_status=" << status.to_string()
              << ", accumulated_rows=" << _row_count;
    Status result_status;
    if (_jni_format_transformer) {
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_JNI_CLOSE_BEFORE"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec;
        result_status = _jni_format_transformer->close();
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_JNI_CLOSE_AFTER"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec
                  << ", status=" << result_status.to_string()
                  << ", cost_ms=" << MonotonicMillis() - start_ms;
        if (!result_status.ok()) {
            LOG(WARNING) << "VMCPartitionWriter close failed: " << result_status.to_string();
        }
    }
    if (result_status.ok() && status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_BUILD_COMMIT_DATA_BEFORE"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec;
        auto commit_data = _build_mc_commit_data();
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_ADD_COMMIT_DATA_BEFORE"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec
                  << ", row_count=" << commit_data.row_count
                  << ", has_commit_message=" << commit_data.__isset.commit_message
                  << ", commit_message_length="
                  << (commit_data.__isset.commit_message ? commit_data.commit_message.size() : 0)
                  << ", has_written_bytes=" << commit_data.__isset.written_bytes;
        _state->add_mc_commit_datas(commit_data);
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_ADD_COMMIT_DATA_AFTER"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec;
    }
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_CLOSE_EXIT"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec << ", status=" << result_status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    return result_status;
}

TMCCommitData VMCPartitionWriter::_build_mc_commit_data() {
    TMCCommitData commit_data;
    commit_data.__set_partition_spec(_partition_spec);
    commit_data.__set_row_count(_row_count);

    // Get statistics from Java side via JNI getStatistics()
    if (_jni_format_transformer) {
        int64_t start_ms = MonotonicMillis();
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_GET_STATS_BEFORE"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec;
        auto statistics = _jni_format_transformer->get_statistics();
        LOG(INFO) << "MC_DIAG stage=BE_PARTITION_GET_STATS_AFTER"
                  << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
                  << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
                  << ", partition_spec=" << _partition_spec
                  << ", stats_size=" << statistics.size()
                  << ", cost_ms=" << MonotonicMillis() - start_ms;
        auto it = statistics.find("mc_commit_message");
        if (it != statistics.end() && !it->second.empty()) {
            commit_data.__set_commit_message(it->second);
        }
        it = statistics.find("bytes:WrittenBytes");
        if (it != statistics.end()) {
            commit_data.__set_written_bytes(std::stoll(it->second));
        }
    }
    LOG(INFO) << "MC_DIAG stage=BE_PARTITION_BUILD_COMMIT_DATA_AFTER"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << _partition_spec
              << ", row_count=" << commit_data.row_count
              << ", has_commit_message=" << commit_data.__isset.commit_message
              << ", commit_message_length="
              << (commit_data.__isset.commit_message ? commit_data.commit_message.size() : 0)
              << ", has_written_bytes=" << commit_data.__isset.written_bytes;

    return commit_data;
}

} // namespace doris
