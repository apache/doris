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

#include "vec/sink/vpaimon_table_sink.h"

#include "vec/sink/vpaimon_jni_table_writer.h"
#include "vec/sink/vpaimon_table_writer.h"

namespace doris {
class TExpr;

namespace vectorized {

VPaimonTableSink::VPaimonTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& texprs)
        : AsyncWriterSink<VPaimonTableWriter, VPAIMON_TABLE_SINK>(row_desc, texprs), _pool(pool) {}

VPaimonTableSink::~VPaimonTableSink() = default;

Status VPaimonTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(AsyncWriterSink::init(t_sink));
    bool use_jni = false;
    if (t_sink.__isset.paimon_table_sink) {
        const auto& options = t_sink.paimon_table_sink.options;
        auto it = options.find("paimon_use_jni");
        if (it != options.end()) {
            use_jni = (it->second == "true" || it->second == "1");
        }
    }

    if (use_jni) {
        _writer = std::make_unique<VPaimonJniTableWriter>(t_sink, _output_vexpr_ctxs);
    } else {
        _writer = std::make_unique<VPaimonTableWriter>(t_sink, _output_vexpr_ctxs);
    }
    RETURN_IF_ERROR(_writer->init_properties(_pool));
    return Status::OK();
}

Status VPaimonTableSink::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(_exec_timer);
    if (_closed) {
        return _close_status;
    }
    RETURN_IF_ERROR(DataSink::close(state, exec_status));
    _close_status = AsyncWriterSink::close(state, exec_status);
    return _close_status;
}

} // namespace vectorized
} // namespace doris
