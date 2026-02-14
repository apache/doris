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

#include "vmc_partition_writer.h"

#include "runtime/runtime_state.h"
#include "vec/runtime/vjni_format_transformer.h"

namespace doris {
namespace vectorized {

VMCPartitionWriter::VMCPartitionWriter(RuntimeState* state,
                                       const VExprContextSPtrs& output_vexpr_ctxs,
                                       const std::string& partition_spec,
                                       std::map<std::string, std::string> writer_params)
        : _state(state),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _partition_spec(partition_spec),
          _writer_params(std::move(writer_params)) {}

Status VMCPartitionWriter::open() {
    _jni_format_transformer = std::make_unique<VJniFormatTransformer>(
            _state, _output_vexpr_ctxs, "org/apache/doris/maxcompute/MaxComputeJniWriter",
            _writer_params);
    return _jni_format_transformer->open();
}

Status VMCPartitionWriter::write(vectorized::Block& block) {
    RETURN_IF_ERROR(_jni_format_transformer->write(block));
    _row_count += block.rows();
    return Status::OK();
}

Status VMCPartitionWriter::close(const Status& status) {
    Status result_status;
    if (_jni_format_transformer) {
        result_status = _jni_format_transformer->close();
        if (!result_status.ok()) {
            LOG(WARNING) << "VMCPartitionWriter close failed: " << result_status.to_string();
        }
    }
    if (result_status.ok() && status.ok()) {
        auto commit_data = _build_mc_commit_data();
        _state->add_mc_commit_datas(commit_data);
    }
    return result_status;
}

TMCCommitData VMCPartitionWriter::_build_mc_commit_data() {
    TMCCommitData commit_data;
    commit_data.__set_partition_spec(_partition_spec);
    commit_data.__set_row_count(_row_count);

    // Get statistics from Java side via JNI getStatistics()
    if (_jni_format_transformer) {
        auto statistics = _jni_format_transformer->get_statistics();
        auto it = statistics.find("mc_commit_message");
        if (it != statistics.end() && !it->second.empty()) {
            commit_data.__set_commit_message(it->second);
        }
        it = statistics.find("bytes:WrittenBytes");
        if (it != statistics.end()) {
            commit_data.__set_written_bytes(std::stoll(it->second));
        }
    }

    return commit_data;
}

} // namespace vectorized
} // namespace doris
