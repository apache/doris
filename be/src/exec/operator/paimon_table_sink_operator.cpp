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

#include "common/logging.h"

namespace doris {

Status PaimonTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    return Base::init(state, info);
}

Status PaimonTableSinkOperatorX::sink_impl(RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), static_cast<int64_t>(in_block->rows()));

    // Delegate to AsyncWriterSink → VPaimonTableWriter for this pipeline instance.
    // Each pipeline instance has its own writer session; partition and bucket
    // routing is handled internally by the Paimon SDK inside IPaimonWriter::write().
    return local_state.sink(state, in_block, eos);
}

} // namespace doris
