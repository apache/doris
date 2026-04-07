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

#include "pipeline/exec/paimon_table_sink_operator.h"

#include "common/status.h"
#include "vec/sink/vpaimon_jni_table_writer.h"
#include "vec/sink/vpaimon_table_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

Status PaimonTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    bool use_jni = false;
    if (info.tsink.__isset.paimon_table_sink) {
        const auto& options = info.tsink.paimon_table_sink.options;
        auto it = options.find("paimon_use_jni");
        if (it != options.end()) {
            use_jni = it->second == "true" || it->second == "1";
        }
    }
    if (use_jni) {
        _writer.reset(new vectorized::VPaimonJniTableWriter(info.tsink, _output_vexpr_ctxs,
                                                            _async_writer_dependency,
                                                            _finish_dependency));
    } else {
        _writer.reset(new vectorized::VPaimonTableWriter(info.tsink, _output_vexpr_ctxs,
                                                         _async_writer_dependency,
                                                         _finish_dependency));
    }

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
