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

#include "vec/sink/volap_table_sink_v2.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>

#include <ranges>
#include <unordered_map>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/object_pool.h"
#include "common/status.h"
#include "olap/delta_writer_v2.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_stub.h"
#include "vec/sink/load_stream_stub_pool.h"

namespace doris {
class TExpr;

namespace vectorized {

VOlapTableSinkV2::VOlapTableSinkV2(ObjectPool* pool, const RowDescriptor& row_desc,
                                   const std::vector<TExpr>& texprs)
        : AsyncWriterSink<VTabletWriterV2, VOLAP_TABLE_SINK_V2>(row_desc, texprs), _pool(pool) {}

VOlapTableSinkV2::~VOlapTableSinkV2() = default;

Status VOlapTableSinkV2::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(AsyncWriterSink::init(t_sink));
    RETURN_IF_ERROR(_writer->init_properties(_pool));
    return Status::OK();
}

Status VOlapTableSinkV2::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(_exec_timer);
    if (_closed) {
        return _close_status;
    }
    _close_status = AsyncWriterSink::close(state, exec_status);
    return _close_status;
}

} // namespace vectorized
} // namespace doris
