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

#include "exec/operator/paimon_table_sink_operator.h"

#include "common/status.h"
#include "exec/sink/writer/paimon/vpaimon_jni_table_writer.h"
#include "exec/sink/writer/paimon/vpaimon_table_writer.h"
#include "util/string_util.h"

namespace doris {

bool should_use_paimon_jni_writer(const TDataSink& tsink) {
    if (!tsink.__isset.paimon_table_sink) {
        return false;
    }
    const auto& options = tsink.paimon_table_sink.options;
    auto it = options.find("paimon_use_jni");
    if (it == options.end()) {
        return false;
    }
    std::string value(trim(it->second));
    return iequal(value, "true") || value == "1";
}

std::unique_ptr<VPaimonTableWriter> create_paimon_table_writer(
        const TDataSink& tsink, const VExprContextSPtrs& output_vexpr_ctxs,
        std::shared_ptr<Dependency> async_writer_dependency,
        std::shared_ptr<Dependency> finish_dependency) {
    if (should_use_paimon_jni_writer(tsink)) {
        return std::make_unique<VPaimonJniTableWriter>(
                tsink, output_vexpr_ctxs, std::move(async_writer_dependency),
                std::move(finish_dependency));
    }
    return std::make_unique<VPaimonTableWriter>(tsink, output_vexpr_ctxs,
                                                std::move(async_writer_dependency),
                                                std::move(finish_dependency));
}

Status PaimonTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _writer = create_paimon_table_writer(info.tsink, _output_vexpr_ctxs, _async_writer_dependency,
                                         _finish_dependency);

    auto& p = _parent->cast<Parent>();
    RETURN_IF_ERROR(_writer->init_properties(p._pool));
    return Status::OK();
}

Status PaimonTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(exec_time_counter());
    return Base::close(state, exec_status);
}

} // namespace doris
