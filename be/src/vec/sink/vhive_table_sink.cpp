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

#include "vec/sink/vhive_table_sink.h"

namespace doris {
class TExpr;

namespace vectorized {

VHiveTableSink::VHiveTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& texprs)
        : AsyncWriterSink<VHiveTableWriter, VHIVE_TABLE_SINK>(row_desc, texprs), _pool(pool) {}

VHiveTableSink::~VHiveTableSink() = default;

Status VHiveTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(AsyncWriterSink::init(t_sink));
    RETURN_IF_ERROR(_writer->init_properties(_pool));
    return Status::OK();
}

Status VHiveTableSink::close(RuntimeState* state, Status exec_status) {
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
